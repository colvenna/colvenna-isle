"""Freshly authorized input exports on the active native writer instance."""

import copy
import shutil
import threading
import time
from pathlib import Path

from colvenna_isle.control.registry import ControlError
from colvenna_isle.pipeline.input_exports import InputExports, validate_bindings
from colvenna_isle.pipeline.input_objects import InputObjects, MAX_FILE_BYTES
from colvenna_isle.pipeline.native_inputs import capture
from .ownership import matches
from .scheduler import NotSubmitted
from .ingestion import CommitUnknown, digest


class Inputs:
    def __init__(self, runtime):
        self.runtime, self.stores = runtime, {}
        self.slots = threading.BoundedSemaphore(4)
        self.objects = InputObjects(runtime.consumer.s3, runtime.consumer.bucket, runtime.consumer.account)

    def authorize(self, request):
        runtime = self.runtime
        if not matches(request.get("identity", {}), runtime.identity):
            raise ControlError(409, "WRITER_IDENTITY_CHANGED")
        if type(request.get("epoch")) is not int or request["epoch"] < 1:
            raise ControlError(400, "WRITER_EPOCH_REQUIRED")
        runtime.guard(request["epoch"])
        principal = runtime.direct.keys.authenticate_id(request.get("key_id", ""))
        principal.authorize(request.get("space_id"), "pipeline:run")
        if (principal.principal_id != request.get("principal_id")
                or principal.generation != request.get("generation")):
            raise ControlError(403, "FORBIDDEN")
        configuration = runtime.registry._read(request["space_id"])
        if configuration["generation"] != request["generation"]:
            raise ControlError(403, "SPACE_GENERATION_CHANGED")
        return principal, configuration

    def store(self, configuration):
        key = configuration["space_id"], configuration["generation"]
        database = self.runtime.database(configuration)
        database.ingestions.require()
        if key not in self.stores:
            self.stores[key] = InputExports(database.ingestions)
        return database, self.stores[key]

    @staticmethod
    def summary(record):
        value = {name: record[name] for name in ["export_id", "execution_id", "state",
            "space_id", "generation", "source_commit", "image_digest", "retain_until_unix", "retained_bytes"]}
        if record["state"] == "ADMITTED":
            value["state"] = "UNKNOWN"
        if "staged" in record:
            value["metrics"] = {**record["staged"]["metrics"],
                **({"uploaded_bytes": record["publication_metrics"]["uploaded_bytes"],
                    "upload_elapsed_ms": record["publication_metrics"]["elapsed_ms"]}
                   if "publication_metrics" in record else {})}
        if "error_code" in record:
            value["error_code"] = record["error_code"]
        if "sample_rows" in record:
            value["sample_rows"] = record["sample_rows"]
        if "collection" in record:
            value["collection"] = {name: record["collection"].get(name) for name in [
                "collection_id", "reason", "started_unix", "finished_unix", "objects", "bytes", "evidence_sha256"]}
        return value

    def execute(self, request):
        # The native scheduler only bounds capture. Hold a separate slot across
        # uploads and consumer reads too, including simultaneous original retries.
        if not self.slots.acquire(blocking=False):
            raise ControlError(429, "INPUT_EXPORT_EXECUTOR_QUOTA")
        try:
            return self._execute(request)
        finally:
            self.slots.release()

    def _execute(self, request):
        operation = request.get("operation")
        if operation not in {"input_prepare", "input_status", "input_manifest", "input_retain", "input_release"}:
            raise ControlError(400, "UNKNOWN_INPUT_OPERATION")
        actor, configuration = self.authorize(request)

        def guard():
            current_actor, current = self.authorize(request)
            if current_actor != actor or current["route_version"] != configuration["route_version"]:
                raise ControlError(403, "SPACE_CONFIGURATION_CHANGED")

        if operation != "input_prepare":
            with self.runtime.lock:
                guard()
                _, store = self.store(configuration)
                record = store.get(actor, request.get("export_id", ""))
                if operation == "input_retain":
                    record = store.retain(record, request.get("consumer_id", ""),
                                          request.get("retain_until_unix"), guard)
                if operation == "input_release":
                    record = store.release(record, request.get("consumer_id", ""), guard)
                    return {**self.summary(record), "references": record["references"]}
                if operation == "input_status":
                    return self.summary(record)
                if record["state"] in {"COLLECTING", "COLLECTED"}:
                    raise ControlError(410, "INPUT_EXPORT_COLLECTED")
                if record["state"] != "READY":
                    raise ControlError(409, "INPUT_EXPORT_NOT_READY")
                if not store.available(record):
                    raise ControlError(410, "INPUT_RETENTION_EXPIRED")
            manifest = self.objects.read(record, guard)
            return {**self.summary(record), "manifest": manifest}

        bindings = copy.deepcopy(request.get("inputs"))
        validate_bindings(bindings)

        def source_guard():
            guard()
            current = self.runtime.registry._read(configuration["space_id"])
            for binding in (bindings or {}).values():
                name = binding["dataset"]
                if (name not in current["datasets"] or name not in configuration["datasets"]
                        or current["datasets"][name]["definition_sha256"]
                        != configuration["datasets"][name]["definition_sha256"]):
                    raise ControlError(409, "SPACE_CONFIGURATION_CHANGED")

        def prepare():
            source_guard()
            database, store = self.store(configuration)
            record, fresh = store.admit(actor, configuration, bindings,
                request.get("client_request_id"), request.get("retention_seconds", 86400),
                {**self.runtime.identity, "epoch": request["epoch"]}, source_guard,
                sample_rows=request.get("sample_rows"))
            exported = None
            if fresh:
                path = database.work / "inputs" / record["execution_id"]
                try:
                    exported = capture(database.connection, configuration, bindings, path,
                        maximum_bytes=MAX_FILE_BYTES, maximum_rows=100000, guard=source_guard,
                        sample_rows=record.get("sample_rows"))
                    record = store.stage(record, exported, source_guard)
                except Exception as error:
                    code = error.code if isinstance(error, ControlError) else "INPUT_EXPORT_FAILED"
                    if code == "NATIVE_READ_CLOSURE_UNKNOWN" or isinstance(error, CommitUnknown):
                        # The original reservation survives until this writer
                        # is stopped and a fresh Native handle reconciles it.
                        database.ingestions.unknown = True
                        self.runtime.consumer.paused = True
                        raise
                    store.failed(record, code, lambda: self.runtime.guard(request["epoch"]))
                    raise
            elif record["state"] == "STAGED":
                candidate = copy.deepcopy(record["staged"])
                for alias, value in candidate["inputs"].items():
                    value["file"]["local_path"] = str(database.work / "inputs" /
                        record["execution_id"] / (alias + ".parquet"))
                if all(Path(value["file"]["local_path"]).is_file()
                       for value in candidate["inputs"].values()):
                    exported = candidate
            return store, record, exported

        # Different payloads using one nonce must independently reach the
        # native binding check, even while the first capture remains queued.
        key = digest([configuration["space_id"], configuration["generation"], actor.principal_id, actor.key_id,
            request.get("client_request_id"), bindings, request.get("retention_seconds", 86400),
            request.get("sample_rows")])
        try:
            work = self.runtime.scheduler.submit("export", key, prepare, configuration["space_id"])
        except NotSubmitted:
            raise ControlError(429, "INPUT_EXPORT_EXECUTOR_QUOTA") from None
        store, record, exported = work.future.result()
        if record["state"] in {"COLLECTING", "COLLECTED"}:
            raise ControlError(410, "INPUT_EXPORT_COLLECTED")
        if record["state"] == "READY":
            if not store.available(record):
                raise ControlError(410, "INPUT_RETENTION_EXPIRED")
            return {**self.summary(record), "manifest": self.objects.read(record, guard)}
        if record["state"] != "STAGED":
            raise ControlError(503, "INPUT_EXPORT_CUSTODY_UNKNOWN")
        if not store.available(record):
            raise ControlError(410, "INPUT_RETENTION_EXPIRED")
        # Immutable uploads and verification run outside the native transaction
        # lock, allowing FIFO/direct commits to continue using the same instance.
        started = time.monotonic()
        prepared = (self.objects.prepare(record, exported, guard) if exported is not None
                    else self.objects.recover(record, guard))
        with self.runtime.lock:
            guard()
            record = store.manifest_intent(record, prepared["intent"], guard)
        reference = self.objects.publish(record, prepared, guard)
        manifest = self.objects.read({**record, "manifest": reference}, guard)
        with self.runtime.lock:
            guard()
            record = store.ready(record, reference, {
                "uploaded_bytes": record["staged"]["metrics"]["copied_bytes"] + reference["bytes"],
                "elapsed_ms": round((time.monotonic() - started) * 1000)}, guard)
            local = self.runtime.database(configuration).work / "inputs" / record["execution_id"]
        # READY consumers read only the exact S3 versions; the local copy is
        # no longer authoritative and must not accumulate on the writer disk.
        shutil.rmtree(local, ignore_errors=True)
        return {**self.summary(record), "manifest": manifest}
