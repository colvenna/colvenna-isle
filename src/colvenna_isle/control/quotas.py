"""Per-space admission quotas and their observed usage.

The IAM operator sets a space's quota policy; a space key, including a
keys:manage administrator, can only read it. Exceeding a quota either rejects
the new request before any durable effect or leaves admitted work waiting:
already confirmed work is never discarded to make room.
"""

import copy
import json
import time
from datetime import datetime, timezone

from colvenna_isle.contracts.model import PROFILES, QUOTA_POLICY, TASK_BUDGETS
from colvenna_isle.contracts.validation import validate
from .audit import Audit, operator_actor
from .registry import ControlError, canonical

DEFAULTS = {name: bounds[0] for name, bounds in QUOTA_POLICY.items()} | {"allowed_profiles": sorted(PROFILES)}


def stamp(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def usage(name, limit, used, unit, enforcement, source, observed, code=None):
    """One quota observation; a missing observation is UNKNOWN, never within quota."""
    status = ("UNKNOWN" if used is None or observed is None
              else "EXHAUSTED" if limit is not None and used >= limit else "OK")
    value = {"name": name, "limit": limit, "used": used, "unit": unit, "status": status,
             "observed_at": stamp(observed) if observed is not None else None,
             "enforcement": enforcement, "source": source}
    if code:
        value["code"] = code
    return value


class Quotas:
    def __init__(self, client, table, *, clock=time.time):
        self.client, self.table, self.clock = client, table, clock
        self.audit = Audit(client, table)

    @staticmethod
    def key(space, generation):
        return {"PK": {"S": "SPACE#" + space}, "SK": {"S": "QUOTA#" + generation}}

    @staticmethod
    def usage_key(space, generation):
        return {"PK": {"S": "SPACE#" + space}, "SK": {"S": "CODEUSAGE#" + generation}}

    def policy(self, space, generation):
        item = self.client.get_item(TableName=self.table, Key=self.key(space, generation),
                                    ConsistentRead=True).get("Item")
        if not item:
            return copy.deepcopy(DEFAULTS)
        try:
            stored = json.loads(item["record"]["S"])
            validate("quota_policy", stored)
        except (KeyError, ValueError):
            raise ControlError(503, "QUOTA_POLICY_UNAVAILABLE") from None
        return {**copy.deepcopy(DEFAULTS), **stored}

    def set(self, configuration, policy, operator_arn):
        """IAM operator only; there is no public route for raising a quota."""
        validate("quota_policy", policy)
        space, generation = configuration["space_id"], configuration["generation"]
        operations = [
            {"Put": {"TableName": self.table, "Item": {**self.key(space, generation),
                                                       "record": {"S": canonical(policy)}}}},
            {"ConditionCheck": {"TableName": self.table, "Key": {"PK": {"S": "SPACE#" + space}, "SK": {"S": "META"}},
                                "ConditionExpression": "generation = :generation",
                                "ExpressionAttributeValues": {":generation": {"S": generation}}}},
            self.audit.put(space, generation, "QUOTA_POLICY_CHANGED", operator_actor(operator_arn),
                           {"kind": "space", "id": space}, identity=canonical([policy, self.clock()]),
                           details={name: value for name, value in sorted(policy.items())
                                    if not isinstance(value, list)}),
        ]
        self.client.transact_write_items(TransactItems=operations)
        return self.policy(space, generation)

    # -- enforcement helpers ----------------------------------------------------------------------

    def check_profile(self, policy, profile):
        budget = TASK_BUDGETS[profile]
        if (profile not in policy["allowed_profiles"]
                or budget.get("memory_bytes", 0) > policy["max_task_memory_bytes"]
                or budget["workspace_bytes"] > policy["max_task_workspace_bytes"]):
            raise ControlError(422, "SPACE_PROFILE_QUOTA")

    def code_bytes(self, space, generation):
        item = self.client.get_item(TableName=self.table, Key=self.usage_key(space, generation),
                                    ConsistentRead=True).get("Item")
        return int(item["stored_bytes"]["N"]) if item else 0

    def reserve_code(self, space, generation, policy, size):
        """Transaction item: stored code bytes stay within quota or the admission is rejected."""
        if size > policy["max_stored_code_bytes"]:
            raise ControlError(413, "SPACE_ARTIFACT_QUOTA")
        return {"Update": {
            "TableName": self.table, "Key": self.usage_key(space, generation),
            "UpdateExpression": "SET stored_bytes = if_not_exists(stored_bytes, :zero) + :size",
            "ConditionExpression": "attribute_not_exists(stored_bytes) OR stored_bytes <= :room",
            "ExpressionAttributeValues": {":zero": {"N": "0"}, ":size": {"N": str(size)},
                                          ":room": {"N": str(policy["max_stored_code_bytes"] - size)}}}}

    def release_code(self, space, generation, size):
        """Collector side: reclaimed artifact bytes leave the stored total."""
        self.client.update_item(
            TableName=self.table, Key=self.usage_key(space, generation),
            UpdateExpression="SET stored_bytes = stored_bytes - :size",
            ConditionExpression="stored_bytes >= :size",
            ExpressionAttributeValues={":size": {"N": str(size)}})
