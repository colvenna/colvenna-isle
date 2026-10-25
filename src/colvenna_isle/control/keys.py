"""Opaque credentials, atomic key indexes and bounded delegation in one table."""

import hashlib
import json
import re
import secrets
import uuid
from datetime import datetime, timezone

from botocore.exceptions import BotoCoreError, ClientError

from colvenna_isle.contracts.model import SCOPES
from colvenna_isle.contracts.validation import validate
from .audit import Audit, key_actor, operator_actor
from .registry import ControlError, Principal, canonical

TOKEN = re.compile(r"(?i:Bearer) ([A-Za-z0-9._~-]{32,512})")
PUBLIC_FIELDS = ["key_id", "name", "fingerprint", "scopes", "created_at", "expires_at",
                 "status", "last_used_at", "replacement_key_id", "replaces_key_id", "creation_request_id"]


def utc_now():
    return datetime.now(timezone.utc)


def epoch(value):
    elapsed = datetime.fromisoformat(value.replace("Z", "+00:00")) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return elapsed.days * 86400000000 + elapsed.seconds * 1000000 + elapsed.microseconds


def stamp(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def now_epoch():
    return epoch(stamp(utc_now()))


def digest(token):
    return hashlib.sha256(token.encode()).hexdigest()


class KeyStore:
    def __init__(self, client, table):
        self.client = client
        self.table = table
        self.audit = Audit(client, table)

    def _key(self, partition, sort="META"):
        return {"PK": {"S": partition}, "SK": {"S": sort}}

    def _get(self, partition, sort="META"):
        return self.client.get_item(
            TableName=self.table, Key=self._key(partition, sort), ConsistentRead=True).get("Item")

    def _decode(self, item):
        if not item:
            raise ControlError(403, "FORBIDDEN")
        value = json.loads(item["record"]["S"])
        validate("key_metadata", self.public(value))
        validate("identifier", value["space_id"])
        validate("identifier", value["principal_id"])
        if (set(value["scopes"]) - set(SCOPES) or item.get("status") != {"S": value["status"]}
                or item.get("revision") != {"N": str(value["revision"])}
                or item.get("expires_epoch_us") != {"N": str(epoch(value["expires_at"]))}):
            raise ControlError(503, "KEY_RECORD_UNAVAILABLE")
        return value

    def _generation(self, space_id):
        item = self._get(f"SPACE#{space_id}")
        if not item:
            raise ControlError(403, "FORBIDDEN")
        value = json.loads(item["configuration"]["S"])
        if value["space_id"] != space_id or item["generation"] != {"S": value["generation"]}:
            raise ControlError(503, "SPACE_UNAVAILABLE")
        return value["generation"]

    def public(self, value):
        result = {name: value[name] for name in PUBLIC_FIELDS if name in value}
        if result["status"] == "ACTIVE" and epoch(result["expires_at"]) <= now_epoch():
            result["status"] = "EXPIRED"
        return result

    def authenticate(self, authorization):
        """Return verified identity; SDK errors propagate so the authorizer fails closed."""
        match = TOKEN.fullmatch(authorization or "")
        if not match:
            raise ControlError(403, "FORBIDDEN")
        value = self._decode(self._get("KEYHASH#" + digest(match[1])))
        if (value["status"] != "ACTIVE" or epoch(value["expires_at"]) <= now_epoch()
                or self._generation(value["space_id"]) != value["generation"]):
            raise ControlError(403, "FORBIDDEN")
        return Principal(value["principal_id"], frozenset([value["space_id"]]),
                         frozenset(value["scopes"]), value["key_id"], value["generation"], value["expires_at"])

    def _by_id(self, key_id):
        validate("identifier", key_id)
        mapping = self._get("KEYID#" + key_id)
        if not mapping:
            raise ControlError(404, "KEY_NOT_FOUND")
        secret_digest = mapping["digest"]["S"]
        if not re.fullmatch("[0-9a-f]{64}", secret_digest):
            raise ControlError(503, "KEY_RECORD_UNAVAILABLE")
        value = self._decode(self._get("KEYHASH#" + secret_digest))
        if value["key_id"] != key_id:
            raise ControlError(503, "KEY_RECORD_UNAVAILABLE")
        return value, secret_digest

    def authenticate_id(self, key_id):
        """Private writer recheck; only the isolated proxy can supply a key ID."""
        value, _ = self._by_id(key_id)
        if (value["status"] != "ACTIVE" or epoch(value["expires_at"]) <= now_epoch()
                or self._generation(value["space_id"]) != value["generation"]):
            raise ControlError(403, "FORBIDDEN")
        return Principal(value["principal_id"], frozenset([value["space_id"]]),
                         frozenset(value["scopes"]), value["key_id"], value["generation"], value["expires_at"])

    def _actor(self, principal, space_id):
        principal.authorize(space_id, "keys:manage")
        value, secret_digest = self._by_id(principal.key_id or "")
        if (value["principal_id"] != principal.principal_id or value["space_id"] != space_id
                or frozenset(value["scopes"]) != principal.scopes or value["status"] != "ACTIVE"
                or epoch(value["expires_at"]) <= now_epoch()
                or self._generation(space_id) != value["generation"]
                or principal.generation != value["generation"]):
            raise ControlError(403, "FORBIDDEN")
        return value, secret_digest

    def _grant(self, request, actor):
        validate("key_request", request)
        requested_expiry = epoch(request["expires_at"])
        if (not set(request["scopes"]).issubset(actor["scopes"])
                or requested_expiry > epoch(actor["expires_at"])
                or requested_expiry <= now_epoch()):
            raise ControlError(403, "DELEGATION_FORBIDDEN")

    def _record(self, request, space_id, generation, principal_id=None):
        key_id = "key_" + uuid.uuid4().hex
        token = secrets.token_urlsafe(48)
        return {
            "key_id": key_id, "principal_id": principal_id or key_id,
            "space_id": space_id, "generation": generation, "name": request["name"],
            "fingerprint": key_id[-12:], "scopes": sorted(request["scopes"]),
            "created_at": stamp(utc_now()), "expires_at": request["expires_at"],
            "status": "ACTIVE", "last_used_at": None, "revision": 1,
        }, token

    def _item(self, partition, value, sort="META"):
        return {**self._key(partition, sort), "record": {"S": canonical(value)},
                "revision": {"N": str(value["revision"])}, "status": {"S": value["status"]},
                "expires_epoch_us": {"N": str(epoch(value["expires_at"]))}}

    def _put(self, item, previous=None):
        operation = {"TableName": self.table, "Item": item, "ConditionExpression": "attribute_not_exists(PK)"}
        if previous is not None:
            operation.update(
                ConditionExpression="#revision = :revision",
                ExpressionAttributeNames={"#revision": "revision"},
                ExpressionAttributeValues={":revision": {"N": str(previous["revision"])}})
        return {"Put": operation}

    def _writes(self, value, secret_digest):
        return [
            self._put(self._item("KEYHASH#" + secret_digest, value)),
            self._put({**self._key("KEYID#" + value["key_id"]), "digest": {"S": secret_digest}}),
            self._put(self._item(f"SPACE#{value['space_id']}", value, "KEY#" + value["key_id"])),
        ]

    def _condition_actor(self, actor, actor_digest):
        return {"ConditionCheck": {
            "TableName": self.table, "Key": self._key("KEYHASH#" + actor_digest),
            "ConditionExpression": "#revision = :revision AND #status = :active AND expires_epoch_us > :now",
            "ExpressionAttributeNames": {"#revision": "revision", "#status": "status"},
            "ExpressionAttributeValues": {":revision": {"N": str(actor["revision"])},
                ":active": {"S": "ACTIVE"}, ":now": {"N": str(now_epoch())}},
        }}

    def operation_conditions(self, principal, space_id, generation, scope):
        """Fence an authorized business mutation against revocation and rebuild."""
        principal.authorize(space_id, scope)
        if self.authenticate_id(principal.key_id or "") != principal or principal.generation != generation:
            raise ControlError(403, "FORBIDDEN")
        actor, actor_digest = self._by_id(principal.key_id)
        return [self._condition_actor(actor, actor_digest),
                self._condition_generation(space_id, generation)]

    def _condition_generation(self, space_id, generation):
        return {"ConditionCheck": {
            "TableName": self.table, "Key": self._key("SPACE#" + space_id),
            "ConditionExpression": "generation = :generation",
            "ExpressionAttributeValues": {":generation": {"S": generation}},
        }}

    def _transaction(self, operations, token, issued_id=None):
        try:
            self.client.transact_write_items(TransactItems=operations, ClientRequestToken=token)
        except ClientError as error:
            if error.response["Error"]["Code"] == "TransactionCanceledException":
                raise ControlError(409, "CONCURRENT_KEY_CHANGE") from None
            raise ControlError(503, "KEY_OPERATION_UNKNOWN",
                               f"Reconcile the original key {issued_id or token}") from None
        except BotoCoreError:
            raise ControlError(503, "KEY_OPERATION_UNKNOWN",
                               f"Reconcile the original key {issued_id or token}") from None

    def bootstrap(self, space_id, expires_at, operator_arn="iam-bootstrap"):
        """IAM-only first administrator; a generation can be bootstrapped once."""
        validate("identifier", space_id)
        request = {"name": "bootstrap-administrator", "scopes": SCOPES, "expires_at": expires_at}
        validate("key_request", request)
        if epoch(expires_at) <= now_epoch():
            raise ControlError(400, "EXPIRY_REQUIRED")
        generation = self._generation(space_id)
        marker = ("SPACE#" + space_id, "BOOTSTRAP#" + generation)
        if self._get(*marker):
            raise ControlError(409, "BOOTSTRAP_ALREADY_ISSUED")
        value, token = self._record(request, space_id, generation)
        value["issuer_kind"] = "IAM_BOOTSTRAP"
        operations = self._writes(value, digest(token))
        operations += [self._put({**self._key("SPACE#" + space_id, "BOOTSTRAP#" + generation),
                                 "key_id": {"S": value["key_id"]}}),
                       self._condition_generation(space_id, generation),
                       self.audit.put(space_id, generation, "KEY_BOOTSTRAPPED", operator_actor(operator_arn),
                                      {"kind": "api_key", "id": value["key_id"]}, identity=value["key_id"],
                                      details={"fingerprint": value["fingerprint"],
                                               "scopes": ",".join(value["scopes"])})]
        try:
            self._transaction(operations, value["key_id"].removeprefix("key_"), value["key_id"])
        except ControlError as error:
            # A concurrent bootstrap of the same generation is the only issued key; never a second one.
            if error.code == "CONCURRENT_KEY_CHANGE" and self._get(*marker):
                raise ControlError(409, "BOOTSTRAP_ALREADY_ISSUED") from None
            raise
        return {"key": self.public(value), "token": token}

    def create(self, principal, space_id, request, request_id=None):
        actor, actor_digest = self._actor(principal, space_id)
        self._grant(request, actor)
        value, token = self._record(request, space_id, actor["generation"])
        value.update(issued_by_key_id=actor["key_id"], issuer_principal_id=actor["principal_id"])
        if request_id:
            value["creation_request_id"] = request_id
        operations = self._writes(value, digest(token))
        operations += [self._condition_actor(actor, actor_digest),
                       self._condition_generation(space_id, actor["generation"]),
                       self.audit.put(space_id, actor["generation"], "KEY_CREATED", key_actor(principal),
                                      {"kind": "api_key", "id": value["key_id"]}, identity=value["key_id"],
                                      request_id=request_id, details=self._audit_details(value))]
        self._transaction(operations, value["key_id"].removeprefix("key_"), value["key_id"])
        return {"key": self.public(value), "token": token}

    @staticmethod
    def _audit_details(value):
        return {"fingerprint": value["fingerprint"], "scopes": ",".join(value["scopes"]),
                "expires_at": value["expires_at"], "name": value["name"]}

    def list(self, principal, space_id):
        self._actor(principal, space_id)
        keys = []
        start = None
        while True:
            kwargs = {"TableName": self.table, "ConsistentRead": True,
                      "KeyConditionExpression": "PK = :space AND begins_with(SK, :keys)",
                      "ExpressionAttributeValues": {":space": {"S": "SPACE#" + space_id},
                                                    ":keys": {"S": "KEY#"}}}
            if start:
                kwargs["ExclusiveStartKey"] = start
            page = self.client.query(**kwargs)
            keys.extend(self.public(self._decode(item)) for item in page["Items"]
                        if json.loads(item["record"]["S"])["generation"] == principal.generation)
            if len(keys) > 1000:
                raise ControlError(413, "KEY_LIST_LIMIT")
            start = page.get("LastEvaluatedKey")
            if not start:
                return {"keys": keys}

    def _manageable(self, principal, space_id, key_id):
        actor, actor_digest = self._actor(principal, space_id)
        value, secret_digest = self._by_id(key_id)
        if (value["space_id"] != space_id or value["generation"] != actor["generation"]
                or not set(value["scopes"]).issubset(actor["scopes"])):
            raise ControlError(403, "FORBIDDEN")
        return actor, actor_digest, value, secret_digest

    def revoke(self, principal, space_id, key_id):
        actor, actor_digest, value, secret_digest = self._manageable(principal, space_id, key_id)
        if value["status"] == "REVOKED":
            return self.public(value)
        updated = {**value, "status": "REVOKED", "revision": value["revision"] + 1,
                   "revoked_at": stamp(utc_now()), "revoked_by_key_id": actor["key_id"]}
        operations = [
            self._put(self._item("KEYHASH#" + secret_digest, updated), value),
            self._put(self._item("SPACE#" + space_id, updated, "KEY#" + key_id), value),
            self._condition_generation(space_id, actor["generation"]),
            self.audit.put(space_id, actor["generation"], "KEY_REVOKED", key_actor(principal),
                           {"kind": "api_key", "id": key_id}, identity=key_id + ":revoke",
                           details={"fingerprint": value["fingerprint"]}),
        ]
        if actor["key_id"] != key_id:
            operations.append(self._condition_actor(actor, actor_digest))
        self._transaction(operations, uuid.uuid4().hex, key_id)
        return self.public(updated)

    def rotate(self, principal, space_id, key_id, request, request_id=None):
        actor, actor_digest, previous, previous_digest = self._manageable(principal, space_id, key_id)
        self._grant(request, actor)
        if not set(request["scopes"]).issubset(previous["scopes"]):
            raise ControlError(403, "DELEGATION_FORBIDDEN")
        if previous["status"] != "ACTIVE" or previous.get("replacement_key_id"):
            raise ControlError(409, "ROTATION_ALREADY_ISSUED")
        value, token = self._record(request, space_id, actor["generation"], previous["principal_id"])
        value.update(issued_by_key_id=actor["key_id"], issuer_principal_id=actor["principal_id"])
        value["replaces_key_id"] = previous["key_id"]
        if request_id:
            value["creation_request_id"] = request_id
        updated = {**previous, "replacement_key_id": value["key_id"], "revision": previous["revision"] + 1,
                   "rotated_at": stamp(utc_now()), "rotated_by_key_id": actor["key_id"]}
        operations = self._writes(value, digest(token)) + [
            self._put(self._item("KEYHASH#" + previous_digest, updated), previous),
            self._put(self._item("SPACE#" + space_id, updated, "KEY#" + key_id), previous),
            self._condition_generation(space_id, actor["generation"]),
            self.audit.put(space_id, actor["generation"], "KEY_ROTATED", key_actor(principal),
                           {"kind": "api_key", "id": value["key_id"]}, identity=value["key_id"],
                           request_id=request_id,
                           details={**self._audit_details(value), "replaces_key_id": key_id}),
        ]
        if actor["key_id"] != key_id:
            operations.append(self._condition_actor(actor, actor_digest))
        self._transaction(operations, value["key_id"].removeprefix("key_"), value["key_id"])
        return {"key": self.public(value), "token": token}
