"""Receipt-gated query copies on the sole Writer's fair, bounded read lane."""

import threading
import time
from concurrent.futures import TimeoutError

from colvenna_isle.control.registry import ControlError
from colvenna_isle.query.snapshots import capture
from colvenna_isle.query.sql import references
from .ingestion import CommitUnknown
from .ownership import matches
from .scheduler import NotSubmitted

CAPTURE_SECONDS = 5


class QueryReads:
    def __init__(self, runtime, jobs):
        self.runtime, self.jobs = runtime, jobs
        self.slots = threading.BoundedSemaphore(4)

    def authorize(self, request, expected=None):
        runtime = self.runtime
        if (not matches(request.get("identity", {}), runtime.identity)
                or type(request.get("epoch")) is not int or request["epoch"] < 1):
            raise ControlError(409, "WRITER_IDENTITY_CHANGED")
        runtime.guard(request["epoch"])
        actor = runtime.direct.keys.authenticate_id(request.get("key_id", ""))
        actor.authorize(request.get("space_id"), "query:read")
        if (actor.principal_id != request.get("principal_id")
                or actor.key_id != request.get("key_id")
                or actor.generation != request.get("generation")):
            raise ControlError(403, "FORBIDDEN")
        job = self.jobs.get(actor, request["space_id"], request["generation"], request.get("query_id", ""),
                            data=True)
        if (job.get("execution_id") != request.get("execution_id")
                or job["key_id"] != actor.key_id or job["source_commit"] != self.jobs.source
                or job["image_digest"] != self.jobs.image
                or expected is not None and any(job.get(name) != expected.get(name)
                    for name in ["execution_id", "executor", "input"])):
            raise ControlError(503, "QUERY_EXECUTION_CUSTODY_UNKNOWN")
        if job["status"] != "RUNNING":
            raise ControlError(409, "QUERY_CANCELLED")
        return actor, job

    def execute(self, request):
        if (request.get("operation") != "query_snapshot" or set(request) != {
                "operation", "identity", "epoch", "space_id", "generation", "key_id", "principal_id",
                "query_id", "execution_id", "datasets"}):
            raise ControlError(400, "UNKNOWN_QUERY_SOURCE_OPERATION")
        if not self.slots.acquire(blocking=False):
            raise ControlError(429, "QUERY_SOURCE_EXECUTOR_QUOTA")
        work = None
        try:
            actor, job = self.authorize(request)
            original = self.jobs.input(job)
            configuration = original["configuration"]
            mode = original["request"].get("consistency", "published")
            if mode not in {"after_write", "after_ingestion"}:
                raise ControlError(400, "INVALID_QUERY_CONSISTENCY")
            condition = {name: original["request"][name] for name in [
                "consistency", "write_id" if mode == "after_write" else "ingestion_id"]}
            # Copy exactly the datasets the original, bound SQL names.
            if request["datasets"] != references([original["request"]["sql"]], configuration["datasets"]):
                raise ControlError(503, "QUERY_EXECUTION_CUSTODY_UNKNOWN")

            def guard():
                current_actor, current_job = self.authorize(request, job)
                current = self.runtime.registry._read(configuration["space_id"])
                if (current_actor != actor or any(current.get(name) != configuration.get(name) for name in [
                        "generation", "route_version", "database_uri", "datasets"])
                        or current_job.get("source_read") is not None and job.get("source_read") is not None
                        and any(current_job["source_read"].get(name) != job["source_read"].get(name)
                            for name in ["capture_id", "producer"])):
                    raise ControlError(409, "SPACE_CONFIGURATION_CHANGED")

            guard()
            producer = {**self.runtime.identity, "epoch": request["epoch"]}
            job = self.jobs.source_admit(job, producer)

            def closed(proof):
                self.jobs.source_closed(job, proof)

            def prepare():
                deadline = time.monotonic() + CAPTURE_SECONDS
                finished = threading.Event()
                database = None
                watchdog = None
                checked = [0.0]

                def bounded_guard(force=False):
                    if time.monotonic() >= deadline:
                        raise ControlError(504, "QUERY_SOURCE_TIMEOUT")
                    if force or time.monotonic() - checked[0] >= 0.1:
                        guard()
                        checked[0] = time.monotonic()

                try:
                    bounded_guard(force=True)
                    database = self.runtime.database(configuration)
                    database.ingestions.require()

                    def interrupt():
                        if not finished.wait(max(0, deadline - time.monotonic())):
                            database.connection.interrupt()

                    watchdog = threading.Thread(target=interrupt, daemon=True, name="query-source-timeout")
                    watchdog.start()
                    value = capture(database.connection, configuration, actor, condition, bounded_guard,
                                    request["datasets"])
                    bounded_guard(force=True)
                    proof = {"outcome": "CAPTURED", "sha256": value["sha256"], "bytes": value["bytes"],
                             "data_versions": [table["version"] for table in value["snapshot"]["tables"]]}
                except BaseException as error:
                    # An unconfirmed rollback retains custody until this exact
                    # producer is positively stopped, rather than claiming closure.
                    code = error.code if isinstance(error, ControlError) else (
                        "NATIVE_RESULT_UNKNOWN" if isinstance(error, CommitUnknown) else
                        "QUERY_SOURCE_TIMEOUT" if time.monotonic() >= deadline else "QUERY_SOURCE_UNCONFIRMED")
                    if code == "NATIVE_READ_CLOSURE_UNKNOWN":
                        database.ingestions.unknown = True
                        self.runtime.consumer.paused = True
                    else:
                        if isinstance(error, CommitUnknown):
                            self.runtime.consumer.paused = True
                        closed({"outcome": "FAILED", "error_code": code})
                    raise ControlError(error.status if isinstance(error, ControlError) else 503, code) from None
                finally:
                    finished.set()
                    if watchdog is not None:
                        watchdog.join()
                closed(proof)
                return {**value, "capture_id": job["source_read"]["capture_id"]}

            try:
                work = self.runtime.scheduler.submit("export", "query:" + job["execution_id"], prepare,
                                                     job["space_id"])
            except NotSubmitted:
                closed({"outcome": "NOT_STARTED", "error_code": "QUERY_SOURCE_EXECUTOR_QUOTA"})
                raise ControlError(429, "QUERY_SOURCE_EXECUTOR_QUOTA") from None
            # Slots follow actual work closure, including a lost/timed-out waiter.
            work.future.add_done_callback(lambda future: self.slots.release())
            try:
                return work.future.result(timeout=15)
            except TimeoutError:
                if self.runtime.scheduler.cancel_pending(work):
                    closed({"outcome": "NOT_STARTED", "error_code": "QUERY_SOURCE_TIMEOUT"})
                raise ControlError(503, "QUERY_SOURCE_UNCONFIRMED") from None
        finally:
            if work is None:
                self.slots.release()
