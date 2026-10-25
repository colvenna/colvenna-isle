"""Atomic, strongly consistent space configuration in the single control table."""

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from botocore.exceptions import ClientError

from colvenna_isle.contracts.validation import validate


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class ControlError(Exception):
    def __init__(self, status, code, message=None):
        super().__init__(message or code)
        self.status = status
        self.code = code
        self.safe_message = message or code


@dataclass(frozen=True)
class Principal:
    principal_id: str
    spaces: frozenset[str]
    scopes: frozenset[str]
    key_id: str | None = None
    generation: str | None = None
    expires_at: str | None = None

    def authorize(self, space_id, scope):
        if not self.principal_id or space_id not in self.spaces or scope not in self.scopes:
            raise ControlError(403, "FORBIDDEN")
        if self.expires_at and datetime.fromisoformat(self.expires_at.replace("Z", "+00:00")) <= datetime.now(timezone.utc):
            raise ControlError(403, "CREDENTIAL_EXPIRED")


class Registry:
    """An immutable route and add-only datasets, returned as one atomic snapshot.

    The whole configuration is one bounded item. Conditional replacement prevents
    concurrent registrations from losing a dataset. Network ambiguity is reconciled
    by rereading the same natural identity, never by minting a replacement identity.
    """

    def __init__(self, client, table, bucket):
        self.client = client
        self.table = table
        self.bucket = bucket

    def _key(self, space_id):
        validate("identifier", space_id)
        return {"PK": {"S": f"SPACE#{space_id}"}, "SK": {"S": "META"}}

    def _read(self, space_id):
        result = self.client.get_item(
            TableName=self.table, Key=self._key(space_id), ConsistentRead=True)
        item = result.get("Item")
        if not item:
            raise ControlError(404, "SPACE_NOT_FOUND")
        try:
            value = json.loads(item["configuration"]["S"])
            validate("space_configuration", value)
        except (KeyError, ValueError):
            raise ControlError(503, "CONFIGURATION_UNAVAILABLE") from None
        if (value["space_id"] != space_id or item.get("generation") != {"S": value["generation"]}
                or item.get("revision") != {"N": str(value["revision"])}):
            raise ControlError(503, "CONFIGURATION_UNAVAILABLE")
        for registered in value["datasets"].values():
            digest = hashlib.sha256(canonical(registered["definition"]).encode()).hexdigest()
            if (registered["definition_sha256"] != digest
                    or registered["registered_revision"] > value["revision"]):
                raise ControlError(503, "CONFIGURATION_UNAVAILABLE")
        if value["database_uri"] != (
                f"s3://{self.bucket}/spaces/{space_id}/{value['generation']}/warehouse"):
            raise ControlError(503, "ROUTE_UNAVAILABLE")
        return value

    def _write(self, value, previous=None, extra=()):
        content = canonical(value)
        if len(content.encode()) > 350000:
            raise ControlError(413, "CONFIGURATION_TOO_LARGE")
        kwargs = {
            "TableName": self.table,
            "Item": {**self._key(value["space_id"]), "configuration": {"S": content},
                     "revision": {"N": str(value["revision"])},
                     "generation": {"S": value["generation"]}},
            "ConditionExpression": "attribute_not_exists(PK)",
        }
        if previous:
            kwargs.update(
                ConditionExpression="#revision = :previous AND generation = :generation",
                ExpressionAttributeNames={"#revision": "revision"},
                ExpressionAttributeValues={
                    ":previous": {"N": str(previous["revision"])},
                    ":generation": {"S": previous["generation"]}},
            )
        try:
            if extra:
                # The audit record commits exactly with the configuration change.
                self.client.transact_write_items(TransactItems=[{"Put": kwargs}, *extra])
            else:
                self.client.put_item(**kwargs)
        except ClientError as error:
            if error.response["Error"]["Code"] in {"ConditionalCheckFailedException", "TransactionCanceledException"}:
                return False
            raise
        return True

    def provision(self, space_id):
        """IAM-only deployment bootstrap; no public route invokes this method."""
        self._key(space_id)
        value = self._new_space(space_id)
        if self._write(value):
            return value
        return self._read(space_id)

    def _new_space(self, space_id):
        from colvenna_isle.intake.backpressure import DEFAULT_POLICY

        generation = "gen_" + uuid.uuid4().hex
        return {
            "contract_version": 2, "space_id": space_id, "generation": generation,
            "revision": 1, "route_version": 1,
            "database_uri": f"s3://{self.bucket}/spaces/{space_id}/{generation}/warehouse",
            "schemas": ["ods", "dwd", "dws", "ads", "ctl"], "datasets": {},
            "ingestion_policy": dict(DEFAULT_POLICY),
        }

    def restore(self, backed_up, operator, details):
        """IAM-only isolated restore: the backed-up space and generation in this environment's bucket.

        The space must not exist here; generation, revisions and registered
        datasets keep their backed-up identity so original idempotency keys keep
        their identities. No credential is restored.
        """
        from .audit import Audit

        validate("space_configuration", backed_up)
        value = {**backed_up, "database_uri": (
            f"s3://{self.bucket}/spaces/{backed_up['space_id']}/{backed_up['generation']}/warehouse")}
        audit = Audit(self.client, self.table).put(
            value["space_id"], value["generation"], "SPACE_RESTORED", operator,
            {"kind": "space", "id": value["space_id"]}, identity="restore:" + details["backup_id"],
            details={"backup_id": details["backup_id"], "commit_sequence": details["commit_sequence"],
                     "source_bucket": backed_up["database_uri"].removeprefix("s3://").split("/", 1)[0]})
        if not self._write(value, extra=[audit]):
            raise ControlError(409, "RESTORE_TARGET_SPACE_EXISTS")
        return value

    def rebuild(self, space_id, previous):
        """Operator must prove all old tasks stopped before calling this method."""
        value = self._new_space(space_id)
        value["route_version"] = previous["route_version"] + 1
        if not self._write(value, previous):
            raise ControlError(409, "CONFIGURATION_CHANGED")
        return value

    def configuration(self, principal, space_id):
        principal.authorize(space_id, "datasets:manage")
        value = self._read(space_id)
        self._authorize_generation(principal, value)
        return value

    def _authorize_generation(self, principal, configuration):
        if principal.generation and principal.generation != configuration["generation"]:
            raise ControlError(403, "SPACE_GENERATION_CHANGED")

    def dataset(self, principal, space_id, dataset, scope):
        """Business services request their actual operation scope, never body identity."""
        if scope not in {"datasets:manage", "ingest:write", "data:write", "query:read", "pipeline:manage", "pipeline:run"}:
            raise ControlError(403, "FORBIDDEN")
        principal.authorize(space_id, scope)
        snapshot = self._read(space_id)
        self._authorize_generation(principal, snapshot)
        if dataset not in snapshot["datasets"]:
            raise ControlError(404, "DATASET_NOT_FOUND")
        return snapshot, snapshot["datasets"][dataset]

    def register(self, principal, space_id, definition, *, generation):
        principal.authorize(space_id, "datasets:manage")
        validate("dataset_registration", definition)
        digest = hashlib.sha256(canonical(definition).encode()).hexdigest()
        for _ in range(8):
            current = self._read(space_id)
            self._authorize_generation(principal, current)
            if current["generation"] != generation:
                raise ControlError(409, "SPACE_REBUILT")
            existing = current["datasets"].get(definition["dataset"])
            if existing:
                if existing["definition_sha256"] != digest:
                    raise ControlError(409, "DATASET_CONFLICT")
                return existing
            if len(current["datasets"]) >= 64:
                raise ControlError(413, "DATASET_LIMIT")
            for foreign in definition.get("constraints", {}).get("foreign_keys", []):
                parent = current["datasets"].get(foreign["references"], {}).get("definition")
                if not parent or parent.get("write_owner") != "direct":
                    raise ControlError(409, "FOREIGN_KEY_TARGET_UNREGISTERED")
                constraints = parent.get("constraints", {})
                unique = ([constraints["primary_key"]] if "primary_key" in constraints else []) + constraints.get("unique", [])
                parent_columns = {column["name"]: column for column in parent["schema"]}
                columns = {column["name"]: column for column in definition["schema"]}
                if (foreign["referenced_columns"] not in unique or any(
                        columns[child]["type"] != parent_columns[target]["type"]
                        for child, target in zip(foreign["columns"], foreign["referenced_columns"]))):
                    raise ControlError(409, "FOREIGN_KEY_TARGET_MISMATCH")
            registered = {"definition": definition, "definition_sha256": digest,
                          "registered_revision": current["revision"] + 1}
            updated = {**current, "revision": current["revision"] + 1,
                       "datasets": {**current["datasets"], definition["dataset"]: registered}}
            from .audit import Audit, key_actor
            audit = Audit(self.client, self.table).put(
                space_id, generation, "DATASET_REGISTERED", key_actor(principal),
                {"kind": "dataset", "id": definition["dataset"]}, identity=definition["dataset"] + ":" + digest,
                details={"definition_sha256": digest, "write_owner": definition.get("write_owner"),
                         "registered_revision": updated["revision"]})
            if self._write(updated, current, [audit]):
                return registered
        raise ControlError(503, "CONFIGURATION_BUSY")
