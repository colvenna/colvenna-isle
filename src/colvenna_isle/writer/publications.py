"""Authenticated private publication commands on the sole native writer."""

import copy
import tempfile
import threading
import time

from colvenna_isle.contracts.validation import validate, validate_parameters
from colvenna_isle.control.http import decode_json
from colvenna_isle.control.pipeline_artifacts import PipelineArtifacts
from colvenna_isle.control.pipelines import Pipelines
from colvenna_isle.control.environments import Environments
from colvenna_isle.control.registry import ControlError
from colvenna_isle.pipeline.candidates import Candidates
from colvenna_isle.pipeline.input_exports import InputExports
from colvenna_isle.pipeline.publications import Publications, resolve_release
from colvenna_isle.pipeline.task_descriptor import (
    MAX_DESCRIPTOR_BYTES, check_descriptor, check_processing, task_environment)
from colvenna_isle.pipeline.task_objects import TaskObjects
from .ingestion import CommitUnknown, digest
from .publication_qualification import PublicationQualification
from .scheduler import NotSubmitted

OPERATIONS = {"publication_target", "publication_grant", "publication_status",
              "publication_revoke", "publication_import", "publication_preview"}
MAX_PREVIEW_ROWS = 100


def preview_value(value):
    """Exact JSON-safe text for typed values; never a rounded number."""
    import base64
    import datetime
    import decimal

    if value is None or isinstance(value, bool) or isinstance(value, str):
        return value
    if isinstance(value, int):
        return value if abs(value) <= 9007199254740991 else str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, (datetime.date, datetime.datetime, datetime.time)):
        return value.isoformat()
    if isinstance(value, bytes):
        return "base64:" + base64.b64encode(value).decode()
    import json
    return json.dumps(value, default=str, sort_keys=True)


class Publisher:
    def __init__(self, runtime, environment_policy):
        self.runtime = runtime
        self.inputs = {}
        self.lock = threading.Lock()
        self.workers = {}
        self.slots = threading.BoundedSemaphore(2)
        # The space of the command this thread serves, for per-space scheduler fairness.
        self.local = threading.local()
        # Read-only use of READY environments; no builder or infrastructure API.
        environments = Environments(runtime.registry, None, "", environment_policy, None, runtime.direct.keys)
        self.pipelines = Pipelines(runtime.registry, PipelineArtifacts(runtime.consumer.s3, runtime.consumer.bucket),
            environments, runtime.direct.keys, writer_source=runtime.identity["source_commit"])
        self.qualification = PublicationQualification(self)

    def actor(self, request):
        return self.runtime.inputs.authorize(request)

    def binding(self, actor, configuration, descriptor):
        p = self.pipelines
        record = p._read(p._key(configuration, "PIPELINE_VERSION",
            descriptor["pipeline_id"], descriptor["pipeline_version"]), missing="PIPELINE_VERSION_NOT_FOUND")
        p._verify_binding(actor, configuration, record, "pipeline:run")
        snapshot, package = record["snapshot"], record["package"]
        tasks = [task for task in snapshot["definition"]["tasks"]
                 if task["task_id"] == descriptor["task"]["task_id"]]
        if (len(tasks) != 1 or tasks[0] != descriptor["task"]
                or descriptor["snapshot_sha256"] != digest(snapshot)
                or descriptor["definition_epoch"] != snapshot["definition_epoch"]
                or descriptor["environment"] != task_environment(snapshot, descriptor["task"]["task_id"])
                or descriptor["package"] != {"s3_uri": package["package_uri"],
                    "object_version": package["package_object_version"],
                    "sha256": package["package_sha256"], "bytes": package["package_bytes"]}):
            raise ControlError(409, "PUBLICATION_DEFINITION_BINDING_CHANGED")
        validate_parameters(snapshot["definition"].get("parameters", {}), descriptor["parameters"])
        return record

    def store(self, request, actor, configuration):
        # Called only on the fair scheduler while holding the native lock.
        database = self.runtime.database(configuration)
        key = configuration["space_id"], configuration["generation"]
        if key not in self.inputs:
            self.inputs[key] = InputExports(database.ingestions)
        inputs = self.inputs[key]
        inputs.initialize()

        def authorize(record):
            current_actor, current = self.actor(request)
            if (current_actor != actor or current["route_version"] != configuration["route_version"]
                    or record["authority"]["principal_id"] != actor.principal_id):
                raise ControlError(403, "PUBLICATION_PRINCIPAL_CHANGED")
            descriptor, authority = record["descriptor"], record["authority"]
            self.binding(actor, current, descriptor)
            # A terminal Native outcome no longer consumes its input pin.
            # Keep fresh caller/permit/generation/contract checks above, while
            # original committed receipts survive positive release and expiry.
            if record["status"] in {"PUBLISHED", "REVOKED"}:
                return
            original = inputs.get(actor, authority["input_export_id"])
            manifest = authority["input_manifest"]
            if (original["state"] != "READY" or not inputs.available(original)
                    or original["manifest"]["sha256"] != digest(manifest)
                    or original["references"].get(record["job_id"], {}).get("state") != "ACTIVE"
                    or manifest["export_id"] != original["export_id"]
                    or manifest["generation"] != configuration["generation"]
                    or manifest["space_id"] != configuration["space_id"]):
                raise ControlError(409, "PUBLICATION_INPUT_AUTHORITY_CHANGED")
            expected = {}
            for alias, value in manifest["inputs"].items():
                approved = value["manifest"]
                expected[alias] = {**{name: copy.deepcopy(approved[name]) for name in [
                    "dataset", "release_id", "watermark", "schema", "files"]},
                    "manifest_sha256": digest(approved), "selection": value["selection"]}
                stored = original["bindings"][alias]
                if (original["definitions"][alias] != current["datasets"][approved["dataset"]]["definition_sha256"]
                        or stored["dataset"] != approved["dataset"] or stored["selection"] != value["selection"]):
                    raise ControlError(409, "PUBLICATION_INPUT_AUTHORITY_CHANGED")
            if descriptor["inputs"] != expected:
                raise ControlError(409, "PUBLICATION_INPUT_AUTHORITY_CHANGED")
            # Re-derive the controller's interval/state/partition claims from
            # the writer's own exported native versions, not the descriptor.
            check_processing(descriptor["task"], descriptor["processing"], manifest["inputs"],
                             descriptor["output_state"]["replace_partitions"])

        result = Publications(database.ingestions, authorize)
        result.initialize()
        return database, result

    def unit(self, key, action):
        # Only a proven queue rejection may be retried. A lost COMMIT reply is
        # reconciled by a newly approved writer's original status, not replayed.
        try:
            return self.runtime.scheduler.submit("import", key, action,
                                                 getattr(self.local, "space", None)).future.result()
        except NotSubmitted:
            raise ControlError(429, "PUBLICATION_QUEUE_FULL") from None

    @staticmethod
    def summary(value):
        return {name: copy.deepcopy(value[name]) for name in [
            "job_id", "epoch", "status", "execution_id", "generation_id", "release"] if name in value}

    def execute(self, request):
        if request.get("operation") not in OPERATIONS:
            raise ControlError(400, "UNKNOWN_PUBLICATION_OPERATION")
        actor, configuration = self.actor(request)
        operation = request["operation"]
        self.local.space = configuration["space_id"]

        def guard():
            current_actor, current = self.actor(request)
            if current_actor != actor or current["route_version"] != configuration["route_version"]:
                raise ControlError(403, "SPACE_CONFIGURATION_CHANGED")

        objects = TaskObjects(self.runtime.consumer.s3, self.runtime.consumer.bucket,
                              configuration["space_id"], configuration["generation"])
        if operation == "publication_target":
            # This trusted read is used before writing the immutable descriptor.
            bound = self.pipelines.bind_run(actor, configuration["space_id"],
                request.get("pipeline_id", ""), request.get("pipeline_version", ""))
            tasks = [task for task in bound["snapshot"]["definition"]["tasks"]
                     if task["task_id"] == request.get("task_id")]
            if len(tasks) != 1:
                raise ControlError(404, "TASK_NOT_FOUND")
            dataset = tasks[0]["output"]["dataset"]

            def target():
                guard()
                self.pipelines.confirm_run_binding(actor, configuration["space_id"],
                    request["pipeline_id"], request["pipeline_version"], bound)
                database = self.runtime.database(configuration)
                database.ingestions.initialize()
                return resolve_release(database.connection, configuration, dataset)

            release = self.unit(digest(request), target)
            state_contract = configuration["datasets"][dataset]["definition"].get("publication_state")
            previous = None
            if release is not None:
                # The controller derives the next interval, state and impact
                # base from this authoritative release, never a DDB projection.
                previous = {name: copy.deepcopy(release.get(name)) for name in [
                    "release_id", "pipeline_id", "pipeline_version", "task_id", "run_id", "job_id",
                    "parameters_sha256", "task_sha256", "definition_sha256", "commit_sequence", "row_count",
                    "input_watermarks", "input_versions", "processing", "write_mode"]}
                previous["partitions"] = [{name: entry[name] for name in ["partition", "rows", "replaced"]}
                                          for entry in release.get("partitions", [])][:10000]
            return {"dataset": dataset, "expected_previous_release": release["release_id"] if release else None,
                    "previous": previous,
                    **({"state_contract": copy.deepcopy(state_contract)} if state_contract is not None else {})}

        if operation == "publication_preview":
            return self.preview(request, actor, configuration, objects, guard)

        if operation == "publication_grant":
            descriptor = decode_json(objects.read(request.get("descriptor"), "task-contexts",
                maximum_bytes=MAX_DESCRIPTOR_BYTES, guard=guard))
            check_descriptor(descriptor)
            bound = self.pipelines.bind_run(actor, configuration["space_id"],
                descriptor["pipeline_id"], descriptor["pipeline_version"])
            if digest(bound["snapshot"]) != descriptor["snapshot_sha256"]:
                raise ControlError(409, "PUBLICATION_DEFINITION_BINDING_CHANGED")
            # Exact original manifest/files are independently read before Native
            # admission. Subsequent transactions compare its hash to Native READY.
            original = self.runtime.inputs.execute({**request, "operation": "input_manifest",
                "export_id": request.get("input_export_id", "")})
            authority = {"principal_id": actor.principal_id, "input_export_id": original["export_id"],
                         "input_manifest": original["manifest"]}

            def grant():
                _, store = self.store(request, actor, configuration)
                return store.grant(request["descriptor"], descriptor, authority,
                                   expected_epoch=request.get("expected_epoch", 0))

            return self.summary(self.unit(digest(request), grant))

        validate("identifier", request.get("job_id", ""))

        def status():
            return self.store(request, actor, configuration)[1].status(request["job_id"])

        if operation == "publication_revoke":
            def revoke():
                return self.store(request, actor, configuration)[1].revoke(
                    request["job_id"], expected_epoch=request.get("eligibility_epoch"))
            return self.summary(self.unit(digest(request), revoke))
        original = self.unit(digest([request, "status"]), status)
        if operation == "publication_status":
            key = (configuration["space_id"], configuration["generation"],
                   original["job_id"], original["epoch"])
            with self.lock:
                worker = copy.deepcopy(self.workers.get(key))
            return {**self.summary(original), **({"worker": worker} if worker else {})}
        validate("identifier", request.get("execution_id", ""))
        validate("task_object", request.get("completion"))
        epoch, attempt, execution = request.get("eligibility_epoch"), request.get("attempt_id"), request["execution_id"]
        if (original["epoch"] != epoch or original["descriptor"]["attempt_id"] != attempt
                or original["status"] == "REVOKED"):
            raise ControlError(409, "PUBLICATION_ELIGIBILITY_CHANGED")
        if original["status"] == "PUBLISHED":
            if execution != original["execution_id"] or request["completion"] != original["selection"]["completion"]:
                raise ControlError(409, "PUBLICATION_EXECUTION_CONFLICT")
            return self.summary(original)
        worker_key = (configuration["space_id"], configuration["generation"], request["job_id"], epoch)
        intent = digest([attempt, execution, request["completion"]])
        with self.lock:
            prior = self.workers.get(worker_key)
            if prior:
                if prior["intent"] != intent:
                    raise ControlError(409, "PUBLICATION_EXECUTION_CONFLICT")
                if "finished_unix" not in prior:
                    # In-memory progress never grants eligibility or publication.
                    return {**self.summary(original), "worker": copy.deepcopy(prior)}
                # status above positively reconciled Native on this approved
                # handle. Resume only these same original immutable objects.
            if not self.slots.acquire(blocking=False):
                raise ControlError(429, "PUBLICATION_EXECUTOR_QUOTA")
            worker = {"intent": intent, "state": "PREPARING"}
            self.workers[worker_key] = worker
        threading.Thread(target=self.run, args=(request, actor, configuration, original, objects, worker),
                         daemon=True, name="native-publication-" + request["job_id"][-12:]).start()
        return {**self.summary(original), "worker": copy.deepcopy(worker)}

    def preview(self, request, actor, configuration, objects, guard):
        """Verify a staged trial candidate exactly like an import, without any native job.

        Nothing is granted, selected or imported; rows are returned only to a
        caller that also holds query:read, and only when asked for.
        """
        import pyarrow.parquet as pq
        from pathlib import Path

        rows = request.get("rows", 0)
        if type(rows) is not int or not 0 <= rows <= MAX_PREVIEW_ROWS:
            raise ControlError(400, "INVALID_PREVIEW_ROWS")
        if rows and "query:read" not in actor.scopes:
            raise ControlError(403, "PREVIEW_QUERY_SCOPE_REQUIRED")
        validate("identifier", request.get("execution_id", ""))
        validate("task_object", request.get("completion"))
        descriptor = decode_json(objects.read(request.get("descriptor"), "task-contexts",
                                              maximum_bytes=MAX_DESCRIPTOR_BYTES, guard=guard))
        check_descriptor(descriptor)
        if (descriptor["space_id"], descriptor["generation"]) != (configuration["space_id"], configuration["generation"]):
            raise ControlError(403, "PUBLICATION_SPACE_GENERATION_CHANGED")
        self.binding(actor, configuration, descriptor)
        target = configuration["datasets"].get(descriptor["task"]["output"]["dataset"])
        if not target or target["definition"].get("write_owner") != "pipeline":
            raise ControlError(403, "PUBLICATION_TARGET_FORBIDDEN")
        if not self.slots.acquire(blocking=False):
            raise ControlError(429, "PUBLICATION_EXECUTOR_QUOTA")
        try:
            work = self.unit(digest([request, "preview-store"]), lambda: self.runtime.database(configuration).work)
            directory = work / "previews"
            directory.mkdir(exist_ok=True, mode=0o700)
            grant = {"descriptor": descriptor, "reference": request["descriptor"], "status": "GRANTED",
                     "state_contract": target["definition"].get("publication_state")}
            with tempfile.TemporaryDirectory(dir=directory, prefix="preview-") as temporary:
                try:
                    prepared = Candidates(objects).prepare(grant, request["execution_id"], request["completion"],
                                                           temporary, guard)
                except ControlError as error:
                    return {"validation": {"status": "REJECTED", "error_code": error.code, "row_count": None,
                                           "files": None, "bytes": None}, "preview": None}
                schema = prepared["manifest"]["schema"]
                values, truncated = [], False
                for file in prepared["manifest"]["files"]:
                    if len(values) >= rows:
                        truncated = truncated or bool(file["rows"])
                        continue
                    for batch in pq.ParquetFile(Path(prepared["directory"]) / file["path"]).iter_batches(batch_size=100):
                        data = batch.to_pydict()
                        for index in range(batch.num_rows):
                            if len(values) >= rows:
                                truncated = True
                                break
                            values.append([preview_value(data[column["name"]][index]) for column in schema])
                        if len(values) >= rows:
                            break
                total = prepared["manifest"]["row_count"]
                return {"validation": {"status": "PASSED", "error_code": None, "row_count": total,
                                       "files": len(prepared["files"]),
                                       "bytes": sum(file["bytes"] for file in prepared["manifest"]["files"])},
                        "preview": {"columns": [{"name": column["name"], "type": column["type"],
                                                 "encoding": "native_text"} for column in schema],
                                    "rows": values, "truncated": truncated or total > len(values)}}
        finally:
            self.slots.release()

    def run(self, request, actor, configuration, original, objects, worker):
        epoch, attempt, execution = request["eligibility_epoch"], request["attempt_id"], request["execution_id"]
        worker.update(started_unix=time.time(), imported_chunks=0, total_chunks=None)
        self.local.space = configuration["space_id"]
        try:
            database, store = self.unit(digest([request, "store"]),
                                       lambda: self.store(request, actor, configuration))

            def guard():
                current_actor, current = self.actor(request)
                if current_actor != actor or current["route_version"] != configuration["route_version"]:
                    raise ControlError(403, "SPACE_CONFIGURATION_CHANGED")

            directory = database.work / "publications"
            directory.mkdir(exist_ok=True, mode=0o700)
            with tempfile.TemporaryDirectory(dir=directory, prefix="verify-") as temporary:
                prepared = Candidates(objects).prepare(original, execution, request["completion"], temporary, guard)
                self.unit(digest([request, "select"]), lambda: store.select(
                    request["job_id"], epoch, attempt, execution, prepared["completion"], prepared["files"],
                    state_files=prepared["state"]["files"] if prepared["state"] is not None else None))
                worker["state"] = "IMPORTING"
                from colvenna_isle.pipeline.publications import CHUNK_ROWS
                worker["total_chunks"] = sum(-(-file["rows"] // CHUNK_ROWS) for image in [
                    prepared, prepared.get("state")] if image is not None for file in image["files"])
                for file, chunk, rows in Candidates.chunks(prepared, guard):
                    self.unit(digest([request, file, chunk]), lambda f=file, c=chunk, r=rows: self.qualification.run(
                        store, request, "chunk", lambda: store.chunk(
                            request["job_id"], epoch, attempt, execution, f, c, r),
                        image="data", file_number=f, chunk_number=c))
                    worker["imported_chunks"] += 1
                for file, chunk, rows in Candidates.state_chunks(prepared, guard):
                    self.unit(digest([request, "state", file, chunk]), lambda f=file, c=chunk, r=rows: self.qualification.run(
                        store, request, "chunk", lambda: store.chunk(
                            request["job_id"], epoch, attempt, execution, f, c, r, image="state"),
                        image="state", file_number=f, chunk_number=c))
                    worker["imported_chunks"] += 1
                worker["state"] = "PUBLISHING"
                self.unit(digest([request, "publish"]),
                    lambda: self.qualification.run(store, request, "publish",
                        lambda: store.publish(request["job_id"], epoch, attempt, execution)))
            worker["state"] = "COMPLETE"
        except CommitUnknown:
            worker.update(state="UNKNOWN", error_code="NATIVE_RESULT_UNKNOWN")
        except ControlError as error:
            # This diagnostic describes the worker, never a fabricated Native
            # FAILED/PUBLISHED result. Native status remains the authority.
            worker.update(state="REJECTED", error_code=error.code)
        except Exception:
            worker.update(state="UNKNOWN", error_code="PUBLICATION_WORKER_UNKNOWN")
        finally:
            worker["finished_unix"] = time.time()
            worker["elapsed_ms"] = round((worker["finished_unix"] - worker["started_unix"]) * 1000)
            self.slots.release()
