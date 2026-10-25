"""Rebuildable observations; never a dispatch or business commit dependency."""

import hashlib
import json
import re
from datetime import datetime, timezone

from botocore.exceptions import BotoCoreError, ClientError

from .registry import ControlError, canonical


def checksum(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def observation(receipt, version, observed_at):
    """The native transaction assigns the version, including failed attempts."""
    state = receipt["status"]
    return {
        **receipt, "projection_version": version, "observed_at": observed_at,
        "publication": {"status": "NOT_PUBLISHED"},
        "layers": {
            "ods": {"status": state, **({"watermark": receipt["ingestion_id"]}
                                      if state == "COMMITTED" else {})},
            **{name: {"status": "NOT_IMPLEMENTED"} for name in ["dwd", "dws", "ads"]},
        },
    }


def validate_observation(value, space, generation, ingestion_id):
    if (not isinstance(value, dict) or value.get("space_id") != space
            or value.get("generation") != generation or value.get("ingestion_id") != ingestion_id
            or value.get("status") not in {"COMMITTED", "FAILED"}
            or type(value.get("projection_version")) is not int or value["projection_version"] <= 0
            or not isinstance(value.get("principal_id"), str) or not value["principal_id"]):
        raise ValueError("Invalid observation identity")
    observed = datetime.fromisoformat(value["observed_at"].replace("Z", "+00:00"))
    if not observed.tzinfo or value["publication"] != {"status": "NOT_PUBLISHED"}:
        raise ValueError("Invalid observation time/publication")
    expected = observation(value, value["projection_version"], value["observed_at"])
    if expected["layers"] != value["layers"]:
        raise ValueError("Invalid observation layers")
    if value["status"] == "COMMITTED" and (
            type(value.get("commit_sequence")) is not int or value["commit_sequence"] <= 0):
        raise ValueError("Invalid native commit sequence")
    if value["status"] == "COMMITTED" and not datetime.fromisoformat(
            value["committed_at"].replace("Z", "+00:00")).tzinfo:
        raise ValueError("Invalid native commit time")
    if len(canonical(value).encode()) > 20000:
        raise ValueError("Observation exceeds bound")
    return value


class Projection:
    """Conditional versions reject late observations; native reads repair damage."""

    def __init__(self, client, table):
        self.client, self.table = client, table

    def key(self, space, ingestion_id):
        if (not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]*", space)
                or not re.fullmatch(r"ing_[0-9a-f]{64}", ingestion_id)):
            raise ControlError(400, "INVALID_INGESTION_ID")
        return {"PK": {"S": "SPACE#" + space}, "SK": {"S": "INGESTION#" + ingestion_id}}

    def read(self, space, ingestion_id):
        names = dict(zip(["#p", "#s", "#g", "#v", "#h", "#o"],
                         ["PK", "SK", "generation", "projection_version", "observation_sha256", "observation"]))
        return self.client.get_item(
            TableName=self.table, Key=self.key(space, ingestion_id), ConsistentRead=True,
            ProjectionExpression=",".join(names), ExpressionAttributeNames=names).get("Item")

    def decode(self, item, space, generation, ingestion_id):
        if not item:
            return None
        value = json.loads(item["observation"]["S"])
        validate_observation(value, space, generation, ingestion_id)
        if (item.get("generation") != {"S": generation}
                or item.get("projection_version") != {"N": str(value["projection_version"])}
                or item.get("observation_sha256") != {"S": checksum(value)}):
            raise ValueError("Projection binding conflict")
        return value

    def item(self, value):
        validate_observation(value, value["space_id"], value["generation"], value["ingestion_id"])
        return {
            **self.key(value["space_id"], value["ingestion_id"]),
            "generation": {"S": value["generation"]},
            "projection_version": {"N": str(value["projection_version"])},
            "observation_sha256": {"S": checksum(value)},
            "observation": {"S": canonical(value)},
        }

    def write(self, value):
        """Used by the asynchronous projector; cannot overwrite a newer version."""
        item = self.item(value)
        try:
            self.client.put_item(
                TableName=self.table, Item=item,
                ConditionExpression="attribute_not_exists(PK) OR "
                                    "(generation = :generation AND projection_version < :version)",
                ExpressionAttributeValues={":generation": item["generation"],
                                           ":version": item["projection_version"]})
            return "STORED"
        except (ClientError, BotoCoreError) as error:
            # A lost reply is reconciled at this same key/version, without a new ID.
            current = self.read(value["space_id"], value["ingestion_id"])
            try:
                observed = self.decode(current, value["space_id"], value["generation"], value["ingestion_id"])
                if observed == value:
                    return "STORED"
                if observed and observed["projection_version"] > value["projection_version"]:
                    return "IGNORED_OLD"
            except (KeyError, ValueError, TypeError):
                pass
            if isinstance(error, ClientError) and error.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return "CONFLICT"
            raise

    def rebuild(self, value, prior):
        """Only a contemporaneous authoritative native read may use this CAS."""
        return self.replace(self.item(value), prior)

    def invalidate(self, space, ingestion_id, prior):
        """A confirmed native absence must not leave a usable false commit."""
        unknown = {"ingestion_id": ingestion_id, "space_id": space, "status": "UNKNOWN"}
        item = {
            **self.key(space, ingestion_id),
            "generation": prior.get("generation", {"S": "UNKNOWN"}),
            "projection_version": prior.get("projection_version", {"N": "0"}),
            "observation_sha256": {"S": checksum(unknown)}, "observation": {"S": canonical(unknown)},
        }
        return self.replace(item, prior)

    def replace(self, item, prior):
        if not prior:
            expression, names, parameters = "attribute_not_exists(PK)", {}, {}
        else:
            clauses, names, parameters = [], {}, {}
            for index, field in enumerate(["generation", "projection_version", "observation_sha256", "observation"]):
                name, parameter = "#f" + str(index), ":f" + str(index)
                names[name] = field
                if field in prior:
                    clauses.append(name + " = " + parameter)
                    parameters[parameter] = prior[field]
                else:
                    clauses.append("attribute_not_exists(" + name + ")")
            expression = " AND ".join(clauses)
        args = {"TableName": self.table, "Item": item, "ConditionExpression": expression}
        if names:
            args["ExpressionAttributeNames"] = names
        if parameters:
            args["ExpressionAttributeValues"] = parameters
        try:
            self.client.put_item(**args)
            return "REBUILT"
        except ClientError as error:
            if error.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return "RECONCILING"
            raise


class Ledger:
    """Invoke the existing private bridge bound to the current exact permit."""

    def __init__(self, ownership, client, function, source_commit):
        self.ownership, self.client = ownership, client
        self.function, self.source_commit = function, source_commit

    def read(self, configuration, ingestion_id):
        return self.invoke(configuration, "ingestion_status", ingestion_id)

    def capacity(self, configuration):
        return self.invoke(configuration, "ingestion_capacity")

    def diagnostics(self, configuration, ingestion_id):
        return self.invoke(configuration, "ingestion_diagnostics", ingestion_id)

    def invoke(self, configuration, operation, ingestion_id=None):
        permit = self.ownership.read()
        identity = permit["identity"]
        if (not self.function or permit["state"] != "ACTIVE" or not identity
                or identity["source_commit"] != self.source_commit):
            raise ControlError(503, "AUTHORITY_UNAVAILABLE")
        request = {
            "identity": identity, "epoch": permit["epoch"], "operation": operation,
            "space_id": configuration["space_id"], "generation": configuration["generation"],
        }
        if ingestion_id is not None:
            request["ingestion_id"] = ingestion_id
        response = self.client.invoke(FunctionName=self.function, Payload=canonical(request).encode())
        try:
            result = json.load(response["Payload"])
            value = result["body"]["result"] if result.get("statusCode") == 200 else None
        except (ValueError, KeyError, TypeError):
            raise ControlError(503, "AUTHORITY_UNAVAILABLE") from None
        if response.get("FunctionError") or not isinstance(value, dict):
            raise ControlError(503, "AUTHORITY_UNAVAILABLE")
        return value


class Status:
    def __init__(self, registry, projection, ledger, s3, bucket, account):
        self.registry, self.projection, self.ledger = registry, projection, ledger
        self.s3, self.bucket, self.account = s3, bucket, account

    def archive_owner(self, configuration, ingestion_id):
        # Missing observations are reconciled with a bounded set of exact keys,
        # never by scanning customer data or assuming that an archive was queued.
        for dataset, registered in configuration["datasets"].items():
            if not dataset.startswith("ods."):
                continue
            key = (f"spaces/{configuration['space_id']}/ingest/{configuration['generation']}/"
                   f"{dataset.split('.', 1)[1]}/{ingestion_id}.json")
            try:
                value = self.s3.head_object(
                    Bucket=self.bucket, Key=key, ExpectedBucketOwner=self.account)
            except ClientError as error:
                if error.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
                    continue
                raise
            metadata = value.get("Metadata", {})
            if (metadata.get("ingestion-id") == ingestion_id
                    and metadata.get("space-id") == configuration["space_id"]
                    and metadata.get("space-generation") == configuration["generation"]
                    and metadata.get("dataset") == dataset
                    and metadata.get("dataset-definition-sha256") == registered["definition_sha256"]
                    and metadata.get("principal-id")):
                return metadata["principal-id"]
            raise ControlError(503, "ARCHIVE_IDENTITY_UNAVAILABLE")
        return None

    def get(self, principal, space, ingestion_id):
        admin = "monitor:read" in principal.scopes
        principal.authorize(space, "monitor:read" if admin else "ingest:write")
        self.projection.key(space, ingestion_id)
        configuration = self.registry._read(space)
        self.registry._authorize_generation(principal, configuration)
        prior, projected, native = None, None, None
        checked_unknown = False
        projection_read = True
        try:
            prior = self.projection.read(space, ingestion_id)
            projected = self.projection.decode(prior, space, configuration["generation"], ingestion_id)
        except (ClientError, BotoCoreError):
            projection_read = False
        except (KeyError, ValueError, TypeError):
            pass  # A corrupt/missing observation needs the authoritative ledger.
        try:
            value = self.ledger.read(configuration, ingestion_id)
            if value.get("status") != "UNKNOWN":
                native = validate_observation(value, space, configuration["generation"], ingestion_id)
            else:
                checked_unknown = True
        except (ClientError, BotoCoreError, ControlError, KeyError, ValueError, TypeError):
            pass
        authoritative = native or (projected if not checked_unknown else None)
        # Cached projections cannot grant access on their own. If the native
        # owner is unavailable, independently verify the immutable archive owner.
        owner = (native["principal_id"] if native else
                 None if admin else self.archive_owner(configuration, ingestion_id))
        if not admin and owner != principal.principal_id:
            raise ControlError(403, "FORBIDDEN")
        if checked_unknown and prior:
            try:
                self.projection.invalidate(space, ingestion_id, prior)
            except (ClientError, BotoCoreError):
                pass
        if not authoritative:
            return {
                "ingestion_id": ingestion_id, "space_id": space, "status": "UNKNOWN",
                "projection_version": 0, "observed_at": datetime.now(timezone.utc).isoformat(),
                "reconciling": True, "authority": "UNAVAILABLE",
                "publication": {"status": "NOT_PUBLISHED"},
                "layers": {"ods": {"status": "UNKNOWN"},
                           **{name: {"status": "NOT_IMPLEMENTED"} for name in ["dwd", "dws", "ads"]}},
            }
        projection_state = "CURRENT" if native and projected == native else "RECONCILING"
        if native and projection_state != "CURRENT":
            try:
                projection_state = (self.projection.rebuild(native, prior) if projection_read
                                    else self.projection.write(native))
            except (ClientError, BotoCoreError):
                projection_state = "UNAVAILABLE"
        allowed = ["ingestion_id", "space_id", "dataset", "status", "commit_sequence", "committed_at", "projection_version",
                   "observed_at", "error_code", "publication", "layers"]
        return {
            **{name: authoritative[name] for name in allowed if name in authoritative},
            "authority": "NATIVE_LEDGER" if native else "PROJECTION",
            "projection_state": projection_state,
            "reconciling": native is None or projection_state in {
                "RECONCILING", "UNAVAILABLE", "IGNORED_OLD", "CONFLICT"},
        }
