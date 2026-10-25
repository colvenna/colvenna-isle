"""One service-wide permit, independent of heartbeats and storage fencing."""

import json
import time
import uuid

from botocore.exceptions import ClientError

from colvenna_isle.control.registry import ControlError, canonical

OWNER_PK = "WRITER#PRIMARY"
IDENTITY_FIELDS = (
    "task_arn", "task_definition", "image_digest", "source_commit", "process_nonce",
    "pid", "start_ticks", "boot_id", "address",
)


def matches(left, right):
    return all(left.get(field) == right.get(field) and field in left for field in IDENTITY_FIELDS)


def terminal_task(task, expected):
    """Desired STOPPED, missing observations and heartbeat age are insufficient."""
    containers = [entry for entry in task.get("containers", []) if entry.get("name") == "baseline"]
    if (task.get("taskArn") != expected["task_arn"]
            or task.get("taskDefinitionArn") != expected["task_definition"]
            or task.get("lastStatus") != "STOPPED" or not task.get("stoppedAt")
            or not task.get("stopCode") or len(containers) != 1
            or containers[0].get("lastStatus") != "STOPPED"
            or containers[0].get("imageDigest") != expected["image_digest"]
            or "exitCode" not in containers[0]):
        raise ControlError(503, "OLD_WRITER_TERMINATION_UNKNOWN")
    return task


class Ownership:
    """Operator alone changes permits; task IAM can only read them.

    The writer service uses a single process for all registered spaces, a
    stronger exclusion boundary than one process per database. A task restart,
    IP reuse or service replacement never inherits its predecessor's permit.
    """

    def __init__(self, client, table):
        self.client, self.table = client, table

    def key(self):
        return {"PK": {"S": OWNER_PK}, "SK": {"S": "CURRENT"}}

    def registration_key(self, task_arn):
        return {"PK": {"S": "WRITERPROCESS#" + task_arn}, "SK": {"S": "META"}}

    def _get(self, key):
        item = self.client.get_item(
            TableName=self.table, Key=key, ConsistentRead=True).get("Item")
        return json.loads(item["value"]["S"]) if item else None

    def read(self):
        value = self._get(self.key())
        if value is None:
            witness = self._get({"PK": {"S": OWNER_PK},
                                 "SK": {"S": "REVISION#00000000000000000001"}})
            if witness is not None:
                # A lost permit is not a fresh environment. Preserve the grant
                # history and require explicit reconciliation of the original.
                raise ControlError(503, "WRITER_OWNERSHIP_UNKNOWN")
            return {"revision": 0, "epoch": 0, "state": "STOPPED", "identity": None}
        if (value.get("state") not in {"ACTIVE", "REVOKED", "STOPPED"}
                or not isinstance(value.get("revision"), int)
                or not isinstance(value.get("epoch"), int)):
            raise ControlError(503, "WRITER_OWNERSHIP_UNKNOWN")
        return value

    def registration(self, task_arn):
        return self._get(self.registration_key(task_arn))

    def register(self, identity):
        key = self.registration_key(identity["task_arn"])
        value = {"identity": identity, "state": "WAITING", "observed_at": time.time(),
                 "opened_databases": [], "last_commit": None}
        try:
            self.client.put_item(TableName=self.table, Item={**key, "value": {"S": canonical(value)},
                "nonce": {"S": identity["process_nonce"]}}, ConditionExpression="attribute_not_exists(PK)")
        except Exception:
            existing = self.registration(identity["task_arn"])
            if not existing or not matches(existing["identity"], identity):
                raise ControlError(503, "PROCESS_REGISTRATION_UNKNOWN") from None
        return value

    def heartbeat(self, identity, state, opened, last_commit):
        value = {"identity": identity, "state": state, "observed_at": time.time(),
                 "opened_databases": opened, "last_commit": last_commit}
        self.client.update_item(TableName=self.table, Key=self.registration_key(identity["task_arn"]),
            UpdateExpression="SET #value = :value", ConditionExpression="nonce = :nonce",
            ExpressionAttributeNames={"#value": "value"},
            ExpressionAttributeValues={":value": {"S": canonical(value)},
                                       ":nonce": {"S": identity["process_nonce"]}})

    def require(self, identity, epoch=None):
        value = self.read()
        if (value["state"] != "ACTIVE" or not matches(value["identity"] or {}, identity)
                or (epoch is not None and epoch != value["epoch"])):
            raise ControlError(503, "WRITER_NOT_APPROVED")
        return value

    def _replace(self, previous, value, event):
        """Atomic permit+immutable audit; uncertain replies reread this exact revision."""
        value = {**value, "revision": previous["revision"] + 1,
                 "observed_at": time.time(), "operation_id": uuid.uuid4().hex}
        item = {**self.key(), "value": {"S": canonical(value)},
                "revision": {"N": str(value["revision"])}}
        condition = "attribute_not_exists(PK)"
        attributes = {}
        if previous["revision"]:
            condition = "revision = :previous"
            attributes["ExpressionAttributeValues"] = {":previous": {"N": str(previous["revision"])}}
        audit = {"PK": {"S": OWNER_PK}, "SK": {"S": f"REVISION#{value['revision']:020d}"},
                 "value": {"S": canonical({"event": event, "previous": previous, "current": value})}}
        try:
            self.client.transact_write_items(ClientRequestToken=value["operation_id"],
                TransactItems=[
                    {"Put": {"TableName": self.table, "Item": item,
                             "ConditionExpression": condition, **attributes}},
                    {"Put": {"TableName": self.table, "Item": audit,
                             "ConditionExpression": "attribute_not_exists(PK)"}}])
        except Exception as error:
            current = self.read()
            if current == value:
                return current
            if isinstance(error, ClientError) and error.response["Error"]["Code"] in {
                    "TransactionCanceledException", "ConditionalCheckFailedException"}:
                raise ControlError(409, "WRITER_OWNERSHIP_CHANGED") from None
            raise ControlError(503, "WRITER_OWNERSHIP_UNKNOWN") from None
        return value

    def grant(self, identity, previous):
        if previous["state"] != "STOPPED":
            raise ControlError(503, "OLD_WRITER_TERMINATION_UNKNOWN")
        if previous["identity"]:
            terminal_task(previous.get("terminal_task", {}), previous["identity"])
            if identity["task_arn"] == previous["identity"]["task_arn"]:
                raise ControlError(409, "STOPPED_TASK_CANNOT_REACQUIRE")
        registration = self.registration(identity["task_arn"])
        if not registration or not matches(registration["identity"], identity):
            raise ControlError(503, "WRITER_REGISTRATION_UNKNOWN")
        return self._replace(previous, {
            "state": "ACTIVE", "epoch": previous["epoch"] + 1, "identity": identity,
            "predecessor_terminal": previous.get("terminal_task"),
        }, "GRANT")

    def revoke(self, previous):
        if previous["state"] != "ACTIVE":
            raise ControlError(409, "WRITER_NOT_ACTIVE")
        return self._replace(previous, {**previous, "state": "REVOKED"}, "REVOKE")

    def stopped(self, previous, task):
        if previous["state"] != "REVOKED" or not previous["identity"]:
            raise ControlError(409, "WRITER_NOT_REVOKED")
        terminal_task(task, previous["identity"])
        # SDK datetime objects are normalized by the operator before this call.
        return self._replace(previous, {**previous, "state": "STOPPED", "terminal_task": task}, "STOPPED")
