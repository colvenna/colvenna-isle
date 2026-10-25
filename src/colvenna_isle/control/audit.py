"""Immutable per-space audit records, written in the audited change's own transaction.

An audit record exists exactly when its change committed: callers add the
record's Put to the same DynamoDB transaction. Records never contain tokens,
key digests, SQL text, result values or AWS credentials; actors are named by
principal, key ID and public fingerprint only.
"""

import base64
import json
import re
import time
from datetime import datetime, timezone

from colvenna_isle.contracts.model import AUDIT_ACTIONS
from colvenna_isle.contracts.validation import validate
from .registry import ControlError, canonical

MAX_DETAIL_TEXT = 256


def stamp(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def digest(value):
    import hashlib

    return hashlib.sha256(canonical(value).encode()).hexdigest()


def partition(space, generation):
    return f"AUDIT#{space}#{generation}"


def key_actor(principal):
    return {"kind": "API_KEY", "principal_id": principal.principal_id, "key_id": principal.key_id,
            "fingerprint": principal.key_id[-12:] if principal.key_id else None}


def operator_actor(arn):
    """An IAM operator is recorded by a stable hash of its assumed-role ARN."""
    return {"kind": "IAM_OPERATOR", "principal_id": "iam_" + digest(arn)[:32], "key_id": None, "fingerprint": None}


SYSTEM_ACTOR = {"kind": "SYSTEM", "principal_id": "colvenna_retention_collector", "key_id": None, "fingerprint": None}


def _details(values):
    result = {}
    for name, value in sorted((values or {}).items()):
        if not re.fullmatch(r"[a-z][a-z0-9_.]*", name) or len(result) >= 32:
            raise ValueError("Invalid audit detail")
        if isinstance(value, str):
            value = value[:MAX_DETAIL_TEXT]
        elif value is not None and not isinstance(value, (bool, int, float)):
            raise ValueError("Audit details are scalar")
        result[name] = value
    return result


class Audit:
    def __init__(self, client, table, *, clock=time.time):
        self.client, self.table, self.clock = client, table, clock

    def record(self, space, generation, action, actor, target, *, identity, request_id=None,
               outcome="COMMITTED", details=None):
        now = self.clock()
        value = {"audit_id": "aud_" + digest([space, generation, action, identity])[:40], "space_id": space,
                 "generation": generation, "at": stamp(now), "action": action, "actor": dict(actor),
                 "target": {name: member for name, member in target.items() if member is not None},
                 "request_id": request_id, "outcome": outcome, "details": _details(details)}
        validate("audit_record", value)
        return value, f"{int(now * 1000000):020d}#{value['audit_id']}"

    def put(self, space, generation, action, actor, target, **fields):
        """The transaction item; the condition makes a replayed item a no-op failure."""
        value, sort = self.record(space, generation, action, actor, target, **fields)
        return {"Put": {"TableName": self.table, "Item": {
            "PK": {"S": partition(space, generation)}, "SK": {"S": sort}, "record": {"S": canonical(value)},
            "action": {"S": action}, "target_kind": {"S": value["target"]["kind"]}},
            "ConditionExpression": "attribute_not_exists(PK)"}}

    def write(self, space, generation, action, actor, target, **fields):
        """A stand-alone audited event (operator CLI or collector), never a business change."""
        item = self.put(space, generation, action, actor, target, **fields)["Put"]
        self.client.put_item(**item)
        return json.loads(item["Item"]["record"]["S"])

    def list(self, principal, configuration, *, limit=50, page_token=None, action=None):
        space, generation = configuration["space_id"], configuration["generation"]
        principal.authorize(space, "audit:read")
        if principal.generation != generation:
            raise ControlError(403, "SPACE_GENERATION_CHANGED")
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ControlError(400, "INVALID_AUDIT_LIMIT")
        request = {"TableName": self.table, "ConsistentRead": True, "ScanIndexForward": False, "Limit": limit,
                   "KeyConditionExpression": "PK = :pk",
                   "ExpressionAttributeValues": {":pk": {"S": partition(space, generation)}}}
        if action is not None:
            if action not in AUDIT_ACTIONS:
                raise ControlError(400, "INVALID_AUDIT_ACTION")
            request.update(FilterExpression="#action = :action", ExpressionAttributeNames={"#action": "action"})
            request["ExpressionAttributeValues"][":action"] = {"S": action}
        if page_token is not None:
            request["ExclusiveStartKey"] = {"PK": {"S": partition(space, generation)},
                                            "SK": {"S": self._decode(page_token)}}
        page = self.client.query(**request)
        records = []
        for item in page.get("Items", []):
            value = json.loads(item["record"]["S"])
            if value["space_id"] != space or value["generation"] != generation:
                raise ControlError(503, "AUDIT_RECORD_UNAVAILABLE")
            records.append(value)
        last = page.get("LastEvaluatedKey")
        result = {"records": records, "next_page_token": self._encode(last["SK"]["S"]) if last else None}
        validate("audit_list", result)
        return result

    @staticmethod
    def _encode(sort):
        return base64.urlsafe_b64encode(sort.encode()).decode().rstrip("=")

    @staticmethod
    def _decode(token):
        try:
            sort = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)).decode()
        except (ValueError, UnicodeError):
            raise ControlError(400, "INVALID_PAGE_TOKEN") from None
        if not re.fullmatch(r"[0-9]{20}#aud_[0-9a-f]{40}", sort):
            raise ControlError(400, "INVALID_PAGE_TOKEN")
        return sort
