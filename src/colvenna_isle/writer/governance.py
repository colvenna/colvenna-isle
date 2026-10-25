"""Monitoring reads and retention commands on the sole native writer.

Reads are bounded native transactions on the fair scheduler; nothing here scans
business tables. Retention changes native custody first (an export becomes
COLLECTING before any object is deleted; a release is dropped and recorded in
one transaction) and never touches SlateDB objects, whose own protocol keeps
the manifest and WAL references of the open database.
"""

import copy
import json
import time

from colvenna_isle.contracts.validation import validate
from colvenna_isle.control.registry import ControlError
from colvenna_isle.pipeline.input_exports import MAX_EXPORTS, MAX_RETAINED_BYTES, InputExports
from colvenna_isle.pipeline.publications import MAX_GENERATION_BYTES, MAX_JOBS, Publications
from colvenna_isle.query.native import describe
from .ingestion import digest
from .ownership import matches
from .scheduler import NotSubmitted

READS = {"space_overview", "governance_read"}
RETENTION = {"retention_candidates", "retention_mark_exports", "retention_finish_exports", "retention_collect_native"}
OPERATIONS = READS | RETENTION


class Governance:
    def __init__(self, runtime):
        self.runtime = runtime
        self.stores = {}

    def authorize(self, request, scope):
        runtime = self.runtime
        if not matches(request.get("identity", {}), runtime.identity):
            raise ControlError(409, "WRITER_IDENTITY_CHANGED")
        if type(request.get("epoch")) is not int or request["epoch"] < 1:
            raise ControlError(400, "WRITER_EPOCH_REQUIRED")
        runtime.guard(request["epoch"])
        principal = runtime.direct.keys.authenticate_id(request.get("key_id", ""))
        principal.authorize(request.get("space_id"), scope)
        if (principal.principal_id != request.get("principal_id")
                or principal.generation != request.get("generation")):
            raise ControlError(403, "FORBIDDEN")
        configuration = runtime.registry._read(request["space_id"])
        if configuration["generation"] != request["generation"]:
            raise ControlError(403, "SPACE_GENERATION_CHANGED")
        return principal, configuration

    def unit(self, kind, request, configuration, action):
        try:
            return self.runtime.scheduler.submit(kind, digest([request, time.monotonic_ns()]), action,
                                                 configuration["space_id"]).future.result()
        except NotSubmitted:
            raise ControlError(429, "WRITER_QUEUE_FULL") from None

    def execute(self, request):
        operation = request.get("operation")
        if operation not in OPERATIONS:
            raise ControlError(400, "UNKNOWN_GOVERNANCE_OPERATION")
        actor, configuration = self.authorize(request, "monitor:read" if operation in READS else "retention:manage")

        def guard():
            current_actor, current = self.authorize(
                request, "monitor:read" if operation in READS else "retention:manage")
            if current_actor != actor or current["route_version"] != configuration["route_version"]:
                raise ControlError(403, "SPACE_CONFIGURATION_CHANGED")

        def stores():
            # Called on the scheduler under the native lock; ledgers are created once per handle.
            database = self.runtime.database(configuration)
            key = configuration["space_id"], configuration["generation"], id(database)
            if key not in self.stores:
                database.ingestions.initialize()
                exports = InputExports(database.ingestions)
                exports.initialize()
                # Read-only and retention use: no publication authority is exercised here.
                publications = Publications(database.ingestions, lambda record: None)
                publications.initialize()
                self.stores[key] = exports, publications
            return (database, *self.stores[key])

        if operation == "space_overview":
            return self.overview(request, configuration, guard, stores)
        if operation == "governance_read":
            return self.read(request, configuration, stores)
        if operation == "retention_candidates":
            def candidates():
                _, exports, publications = stores()
                native = publications.retention_candidates()
                protected = set(native["protected"]) | exports.pinned_releases()
                return {"exports": exports.candidates(request.get("protected_exports", [])),
                        "releases": native["releases"], "generations": native["generations"],
                        "protected_releases": sorted(protected), "observed_unix": time.time()}
            return self.unit("export", request, configuration, candidates)
        collection = request.get("collection_id", "")
        validate("identifier", collection)
        if operation == "retention_mark_exports":
            identities = self._identities(request.get("export_ids"))
            protected = self._identities(request.get("protected_exports", []), minimum=0)

            def mark():
                database, exports, _ = stores()
                marked = []
                for identity in identities:
                    record = exports.mark_collecting(identity, collection, protected, guard)
                    marked.append({"export_id": identity, "state": record["state"],
                                   "prefix": f"spaces/{record['space_id']}/inputs/{record['generation']}/{identity}/",
                                   "reason": record["collection"]["reason"],
                                   "previous_state": record["collection"]["previous_state"]})
                return {"marked": marked}
            return self.unit("import", request, configuration, mark)
        if operation == "retention_finish_exports":
            results = request.get("results")
            if not isinstance(results, list) or not 1 <= len(results) <= 128:
                raise ControlError(400, "INVALID_RETENTION_REQUEST")

            def finish():
                _, exports, _ = stores()
                finished = []
                for value in results:
                    validate("identifier", value.get("export_id", ""))
                    evidence = value.get("evidence")
                    if not isinstance(evidence, dict) or not isinstance(evidence.get("objects"), list):
                        raise ControlError(400, "INVALID_RETENTION_REQUEST")
                    record = exports.mark_collected(value["export_id"], collection, evidence, guard)
                    finished.append({"export_id": value["export_id"], "state": record["state"],
                                     "evidence_sha256": record["collection"]["evidence_sha256"]})
                return {"finished": finished}
            return self.unit("import", request, configuration, finish)
        releases = self._identities(request.get("release_ids", []), minimum=0)
        generations = self._identities(request.get("generation_ids", []), minimum=0)
        protected = self._identities(request.get("protected_releases", []), minimum=0, maximum=4096)

        def collect():
            _, exports, publications = stores()
            pinned = set(protected) | exports.pinned_releases()
            return {"collected": publications.collect(collection, releases, generations, pinned, guard)}
        return self.unit("import", request, configuration, collect)

    @staticmethod
    def _identities(values, minimum=1, maximum=128):
        if not isinstance(values, list) or not minimum <= len(values) <= maximum or len(set(values)) != len(values):
            raise ControlError(400, "INVALID_RETENTION_REQUEST")
        for value in values:
            validate("identifier", value)
        return values

    def overview(self, request, configuration, guard, stores):
        def native():
            database, exports, publications = stores()
            _, versions = describe(database.connection, configuration, guard)
            c = database.connection
            c.execute("BEGIN")
            try:
                published = {}
                for dataset, release_id, record in c.execute(
                        "SELECT p.dataset,p.release_id,r.record FROM warehouse.ctl.release_pointers p "
                        "JOIN warehouse.ctl.dataset_releases r USING (release_id)").fetchall():
                    published[dataset] = json.loads(record).get("published_unix")
                count, reserved = c.execute(
                    "SELECT count(*),coalesce(sum(g.reserved_bytes),0) FROM warehouse.ctl.publication_generations g "
                    "JOIN warehouse.ctl.publication_jobs j USING (job_id) "
                    "WHERE json_extract_string(j.record,'$.status') NOT IN ('PUBLISHED','REVOKED')").fetchone()
                records = exports._records()
                c.execute("COMMIT")
            except BaseException:
                c.execute("ROLLBACK")
                raise
            datasets = []
            seen = set()
            for version in versions:
                seen.add(version["dataset"])
                datasets.append({**{name: version.get(name) for name in [
                    "dataset", "release_id", "commit_sequence", "snapshot_sequence"]},
                    "published_unix": published.get(version["dataset"])})
            for name in configuration["datasets"]:
                if name not in seen:
                    datasets.append({"dataset": name, "release_id": None, "commit_sequence": 0,
                                     "snapshot_sequence": None, "published_unix": None})
            return {"datasets": datasets,
                    "exports": {"live": sum(exports.live(record) for record in records),
                                "retained_bytes": sum(record["retained_bytes"] for record in records
                                                      if exports.live(record)),
                                "max_exports": MAX_EXPORTS, "max_retained_bytes": MAX_RETAINED_BYTES},
                    "publications": {"in_flight": count, "reserved_bytes": reserved, "max_jobs": MAX_JOBS,
                                     "max_generation_bytes": MAX_GENERATION_BYTES}}
        value = self.unit("export", request, configuration, native)
        consumer = self.runtime.consumer
        value.update(observed_unix=time.time(),
                     scheduler=self.runtime.scheduler.observation(configuration["space_id"]),
                     consumer={"paused": consumer.paused, "capacity": consumer.capacity()},
                     ingestion_latency=consumer.latency(configuration["space_id"]))
        return value

    def read(self, request, configuration, stores):
        kind, identity = request.get("kind"), request.get("id", "")
        validate("identifier", identity)
        if kind not in {"publication_job", "input_export", "release"}:
            raise ControlError(400, "INVALID_GOVERNANCE_READ")

        def native():
            _, exports, publications = stores()
            if kind == "input_export":
                record = exports._read(identity)
                if record is None:
                    raise ControlError(404, "INPUT_EXPORT_NOT_FOUND")
                return {"export_id": identity, "state": record["state"], "created_unix": record["created_unix"],
                        "retain_until_unix": record["retain_until_unix"], "retained_bytes": record["retained_bytes"],
                        "references": {name: {key: value.get(key) for key in ["state", "retain_until_unix",
                                                                              "released_unix"]}
                                       for name, value in sorted(record["references"].items())},
                        "manifest": copy.deepcopy(record.get("manifest")), "error_code": record.get("error_code"),
                        "sample_rows": record.get("sample_rows"), "collection": copy.deepcopy(record.get("collection")),
                        "source_commit": record["source_commit"], "image_digest": record["image_digest"],
                        "metrics": copy.deepcopy((record.get("staged") or {}).get("metrics"))}
            if kind == "publication_job":
                value = publications._get(identity)
                if value is None:
                    raise ControlError(404, "PUBLICATION_JOB_NOT_FOUND")
                release = value.get("release") or {}
                return {"job_id": identity, "status": value["status"], "epoch": value["epoch"],
                        "attempt_id": value["descriptor"]["attempt_id"], "execution_id": value.get("execution_id"),
                        "generation_id": value.get("generation_id"), "release_id": release.get("release_id"),
                        "commit_sequence": release.get("commit_sequence"),
                        "completion": copy.deepcopy((value.get("selection") or {}).get("completion"))}
            row = publications.connection.execute(
                "SELECT dataset,record FROM warehouse.ctl.dataset_releases WHERE release_id=?", [identity]).fetchone()
            if row is None:
                raise ControlError(404, "PUBLICATION_RELEASE_NOT_FOUND")
            value = json.loads(row[1])
            return {name: copy.deepcopy(value.get(name)) for name in [
                "release_id", "dataset", "status", "commit_sequence", "published_unix", "row_count", "run_id",
                "job_id", "attempt_id", "execution_id", "eligibility_epoch", "pipeline_id", "pipeline_version",
                "task_id", "input_watermarks", "input_versions", "completion", "write_mode", "collection",
                "expected_previous_release", "parameters_sha256", "task_sha256"]}
        return self.unit("export", request, configuration, native)
