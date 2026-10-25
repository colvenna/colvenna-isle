"""IAM-only faults and reads, restricted to the admitted qualification (probe_*) generation."""

import os
import re
import time
from datetime import timedelta

from colvenna_isle.control.registry import ControlError
from .writes import table_name, typed_rows, quoted


class LoseReply(RuntimeError):
    """A qualification response is physically dropped after native COMMIT."""


class WriteQualification:
    def __init__(self, runtime):
        self.runtime, self.faults = runtime, {}

    def require(self, configuration):
        if (not self.runtime.probe_space.startswith("probe_")
                or configuration["space_id"] != self.runtime.probe_space
                or configuration["generation"] != self.runtime.probe_generation):
            raise ControlError(403, "QUALIFICATION_DISABLED")

    def before_commit(self, identity):
        fault = self.faults.get(identity)
        if fault not in {"before_commit_abort", "before_commit_delay"}:
            return
        self.faults.pop(identity)
        self.runtime.log("DIRECT_QUALIFICATION_FAULT", {"write_id": identity, "fault": fault})
        if fault == "before_commit_abort":
            os._exit(137)
        time.sleep(3)  # Real elapsed wait; public deadline is unchanged.

    def after_commit(self, identity):
        fault = self.faults.get(identity)
        if fault not in {"after_commit_abort", "after_commit_disconnect"}:
            return
        self.faults.pop(identity)
        self.runtime.log("DIRECT_QUALIFICATION_FAULT", {"write_id": identity, "fault": fault})
        if fault == "after_commit_abort":
            os._exit(137)
        raise LoseReply("DIRECT_REPLY_DROPPED")

    def execute(self, operation, configuration, request, database):
        self.require(configuration)
        identity = request.get("write_id")
        if not re.fullmatch(r"write_[0-9a-f]{64}", identity or ""):
            raise ControlError(400, "INVALID_WRITE_ID")
        if operation == "probe_write_fault":
            fault = request.get("fault")
            if fault not in {None, "before_commit_abort", "before_commit_delay",
                             "after_commit_abort", "after_commit_disconnect"}:
                raise ControlError(400, "INVALID_WRITE_FAULT")
            if fault is None:
                self.faults.pop(identity, None)
            else:
                if len(self.faults) >= 16 and identity not in self.faults:
                    raise ControlError(429, "QUALIFICATION_FAULT_LIMIT")
                self.faults[identity] = fault
            return {"write_id": identity, "fault": fault}
        existing = self.runtime.direct.intents.read(
            configuration["space_id"], configuration["generation"], identity)
        if not existing:
            raise ControlError(409, "QUALIFICATION_ORIGINAL_REQUIRED")
        original = existing[0]
        store = database.writes
        receipt = store.require_committed(identity, original["principal_id"])
        if operation == "probe_write_abort":
            self.runtime.log("DIRECT_SUCCESS_ABORT", {"write_id": identity, "commit_sequence": receipt["commit_sequence"]})
            os._exit(137)
        if operation == "probe_write_expiry":
            store.native.transaction(lambda: store.connection.execute(
                "UPDATE warehouse.ctl.writes SET retry_until=? WHERE write_id=?",
                [store.clock() - timedelta(microseconds=1), identity]))
            return {"write_id": identity, "expired": True, "mode": "SIMULATED_CLOCK_BOUNDARY",
                    "original_receipt_unchanged": store.status(identity, original["principal_id"]) == receipt}
        if operation == "probe_after_write":
            principal = self.runtime.direct.keys.authenticate_id(request.get("key_id", ""))
            principal.authorize(configuration["space_id"], "query:read")
            current = store.require_committed(identity, principal.principal_id, "datasets:manage" in principal.scopes)
            return {"precondition": current, "data": store.probe(current["dataset"]),
                    "read_watermark": store.connection.execute(
                        "SELECT commit_sequence FROM warehouse.ctl.dataset_watermarks WHERE dataset=?",
                        [current["dataset"]]).fetchone()[0],
                    "scope": "CONTROLLED_NATIVE_READ_NOT_PUBLIC_QUERY"}
        if operation != "probe_write_not_null":
            raise ControlError(400, "UNKNOWN_WRITE_QUALIFICATION")
        definition = configuration["datasets"][original["dataset"]]["definition"]
        column = request.get("column")
        metadata = next((member for member in definition["schema"] if member["name"] == column), None)
        if (not metadata or metadata["nullable"] or column in definition["key_columns"]
                or not definition["key_columns"]
                or original["request"]["operation"] not in {"insert", "upsert"}):
            raise ControlError(400, "QUALIFICATION_NON_NULL_COLUMN_REQUIRED")
        row = typed_rows(original["request"], definition)[0]
        keys = definition["key_columns"]
        predicate = " AND ".join(quoted(name) + "=?" for name in keys)
        values = [row[name] for name in keys]
        if not store.connection.execute(
                "SELECT count(*) FROM " + table_name(definition["dataset"]) + " WHERE " + predicate, values
                ).fetchone()[0]:
            raise ControlError(409, "QUALIFICATION_EXISTING_ROW_REQUIRED")
        import duckdb
        try:
            store.native.transaction(lambda: store.connection.execute(
                "UPDATE " + table_name(definition["dataset"]) + " SET " + quoted(column)
                + "=NULL WHERE " + predicate, values))
        except duckdb.ConstraintException as error:
            if str(error).startswith("Constraint Error: NOT NULL constraint failed:"):
                return {"write_id": identity, "constraint": "NOT NULL", "rejected": True,
                        "original_receipt_unchanged": store.status(identity, original["principal_id"]) == receipt}
            raise ControlError(409, "DIFFERENT_NATIVE_CONSTRAINT") from None
        raise ControlError(409, "NATIVE_NOT_NULL_NOT_ENFORCED")
