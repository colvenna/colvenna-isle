"""The isolated synchronous listener admits bounded work to the native scheduler."""

import re
import threading
import time
from concurrent.futures import TimeoutError
from datetime import datetime

from colvenna_isle.contracts.validation import validate
from colvenna_isle.control.registry import ControlError
from colvenna_isle.timing import Timing
from .ingestion import CommitUnknown
from .ownership import matches
from .scheduler import NotSubmitted
from .writes import write_id


class DirectWrites:
    def __init__(self, runtime, keys, intents):
        self.runtime, self.keys, self.intents = runtime, keys, intents
        # Serialize reservation plus scheduler admission. A fresh original
        # rejected here cannot race another caller into native execution.
        self.admission = threading.Lock()

    def authorize(self, request, scope="data:write"):
        runtime = self.runtime
        if not matches(request.get("identity", {}), runtime.identity):
            raise ControlError(409, "WRITER_IDENTITY_CHANGED")
        epoch = request.get("epoch")
        if type(epoch) is not int or epoch < 1:
            raise ControlError(400, "WRITER_EPOCH_REQUIRED")
        runtime.guard(epoch)
        principal = self.keys.authenticate_id(request.get("key_id", ""))
        space = request.get("space_id")
        if (principal.principal_id != request.get("principal_id")
                or principal.generation != request.get("generation")):
            raise ControlError(403, "FORBIDDEN")
        if scope == "status" and "datasets:manage" in principal.scopes:
            principal.authorize(space, "datasets:manage")
        else:
            principal.authorize(space, "data:write")
        configuration = runtime.registry._read(space)
        if configuration["generation"] != request.get("generation"):
            raise ControlError(409, "SPACE_GENERATION_CHANGED")
        return principal, configuration

    def execute(self, request):
        operation = request.get("operation")
        if operation not in {"write", "write_status"}:
            raise ControlError(400, "UNKNOWN_DIRECT_OPERATION")
        if operation == "write":
            # Correlates with the proxy's line through the gateway request ID.
            timing = Timing("direct_write", request_id=request.get("request_id"), space_id=request.get("space_id"))
            try:
                value = self.write(request, timing)
            except ControlError as error:
                timing.emit(error.code, "writer")
                raise
            value["timing"] = timing.emit(value["result"]["status"], "writer")
            return value
        principal, configuration = self.authorize(request, "status")
        space, generation = configuration["space_id"], configuration["generation"]
        if operation == "write_status":
            identity = request.get("write_id")
            if not re.fullmatch(r"write_[0-9a-f]{64}", identity or ""):
                raise ControlError(400, "INVALID_WRITE_ID")
            admin = "datasets:manage" in principal.scopes
            self.intents.authorize_status(space, generation, identity, principal.principal_id, admin)
            with self.runtime.lock:
                self.authorize(request, "status")
                value = self.runtime.database(configuration).writes.status(identity, principal.principal_id, admin)
            return {"status": 200 if value["status"] != "UNKNOWN" else 503, "result": value}

    def write(self, request, timing):
        with timing.segment("authorize"):
            principal, configuration = self.authorize(request, "data:write")
        space, generation = configuration["space_id"], configuration["generation"]
        payload, idempotency_key = request.get("payload"), request.get("idempotency_key")
        validate("write_request", payload)
        if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", idempotency_key or ""):
            raise ControlError(428, "IDEMPOTENCY_KEY_REQUIRED")
        identity = write_id(space, generation, payload["dataset"], idempotency_key)
        timing.bind(generation=generation, write_id=identity, dataset=payload["dataset"],
                    operation=payload["operation"])
        # One form per operation: records (insert/upsert), updates or keys.
        timing.count("records", len(payload.get("records") or payload.get("updates") or payload.get("keys") or []))
        registered = configuration["datasets"].get(payload["dataset"])
        if not registered:
            raise ControlError(404, "DATASET_NOT_FOUND")
        definition = registered["definition"]
        if definition.get("write_owner") != "direct":
            raise ControlError(403, "DIRECT_WRITE_TARGET_REQUIRED")
        if payload["schema_version"] != definition["schema_version"]:
            raise ControlError(409, "SCHEMA_VERSION_MISMATCH")
        validate("write_request", payload, definition=definition)
        deadline = request.get("deadline_unix_ms")
        if type(deadline) is not int or deadline > int(time.time() * 1000) + 20000:
            raise ControlError(400, "INVALID_WAIT_DEADLINE")
        def recheck():
            actor, current = self.authorize(request)
            target = current["datasets"].get(payload["dataset"])
            if (actor.principal_id != principal.principal_id
                    or current["route_version"] != configuration["route_version"]
                    or not target or target["definition_sha256"] != registered["definition_sha256"]
                    or target["definition"].get("write_owner") != "direct"):
                raise ControlError(409, "SPACE_CONFIGURATION_CHANGED")

        def commit():
            with timing.segment("unit", "execution_wait"):
                with timing.segment("recheck", "unit"):
                    recheck()
                with timing.segment("database", "unit"):
                    database = self.runtime.database(configuration)
                result = database.writes.commit(
                    identity, principal.principal_id, original["request"], registered, recheck,
                    before_commit=lambda: self.runtime.write_qualification.before_commit(identity),
                    retry_until=datetime.fromisoformat(original["retry_until"]), timing=timing)
            if result["status"] == "COMMITTED":
                self.runtime.last_commit = {
                    "space_id": space, "generation": generation, "write_id": identity,
                    "commit_sequence": result["commit_sequence"], "observed_at": time.time(),
                }
            self.runtime.log("DIRECT_WRITE_RESULT", {
                "space_id": space, "generation": generation, "write_id": identity,
                "status": result["status"], "commit_sequence": result.get("commit_sequence"),
                "intent_version": reference["version_id"], "intent_sha256": reference["sha256"],
            })
            if result["status"] == "COMMITTED":
                self.runtime.write_qualification.after_commit(identity)
            return result

        with timing.segment("admission_wait"):
            admitted = self.admission.acquire(timeout=max(0, deadline / 1000 - time.time()))
        if not admitted:
            return {"status": 504, "result": {
                "write_id": identity, "status": "UNKNOWN", "code": "WRITE_WAIT_TIMEOUT",
            }}
        try:
            with timing.segment("intent"):
                identity, original, reference, fresh = self.intents.reserve(
                    configuration, principal.principal_id, payload, registered, idempotency_key)
            try:
                work = self.runtime.scheduler.submit("direct", identity, commit, configuration["space_id"])
                if fresh:
                    work.unexecuted_original = True
            except NotSubmitted:
                # A preexisting durable original may already have committed.
                # A full queue cannot label it NOT_SUBMITTED without reconciliation.
                return {"status": 429 if fresh else 503, "result": {
                    "write_id": identity, "status": "NOT_SUBMITTED" if fresh else "UNKNOWN",
                    "code": "WRITER_QUEUE_FULL", "retry_after_ms": 1000,
                }}
        finally:
            self.admission.release()
        remaining = max(0, deadline / 1000 - time.time())
        try:
            with timing.segment("execution_wait"):
                try:
                    result = work.future.result(timeout=remaining)
                finally:
                    if work.started_at is not None:
                        timing.record("scheduler_wait", (work.started_at - work.queued_at) * 1000, "execution_wait")
        except TimeoutError:
            if self.runtime.scheduler.cancel_pending(work):
                known = work.unexecuted_original
                return {"status": 429 if known else 504, "result": {
                    "write_id": identity, "status": "NOT_SUBMITTED" if known else "UNKNOWN",
                    "code": "WAIT_BUDGET_BEFORE_EXECUTION", "retry_after_ms": 1000,
                }}
            return {"status": 504, "result": {
                "write_id": identity, "status": "UNKNOWN", "code": "WRITE_WAIT_TIMEOUT",
            }}
        except NotSubmitted:
            known = work.unexecuted_original
            return {"status": 429 if known else 504, "result": {
                "write_id": identity, "status": "NOT_SUBMITTED" if known else "UNKNOWN",
                "code": "WAIT_BUDGET_BEFORE_EXECUTION", "retry_after_ms": 1000,
            }}
        except CommitUnknown:
            self.runtime.consumer.paused = True
            return {"status": 503, "result": {
                "write_id": identity, "status": "UNKNOWN", "code": "NATIVE_RESULT_UNKNOWN",
            }}
        return {"status": 200 if result["status"] == "COMMITTED" else 422, "result": result}
