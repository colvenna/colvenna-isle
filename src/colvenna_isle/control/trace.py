"""Correlated diagnostics: one identity traced to its source, execution, artifacts and ledgers.

Each node names the authority it was read from and when. A projection that
disagrees with its authority, or an authority that cannot be read, is UNKNOWN
with both raw values; nothing is inferred from the absence of a record. SQL text
is returned only to an audit:read principal, and query results never are.
"""

import hashlib
import json
import time
from datetime import datetime, timezone

from botocore.exceptions import BotoCoreError, ClientError

from colvenna_isle.account import account
from colvenna_isle.contracts.validation import validate
from .registry import ControlError



def stamp(value):
    if value is None:
        return None
    return datetime.fromtimestamp(value, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def scalars(values):
    """Bounded scalar attributes; nested values become canonical JSON text."""
    result = {}
    for name, value in values.items():
        if value is None or isinstance(value, (bool, int, float, str)):
            result[name] = value if not isinstance(value, str) else value[:1024]
        else:
            result[name] = json.dumps(value, sort_keys=True, separators=(",", ":"))[:1024]
    return dict(list(result.items())[:64])


class Trace:
    def __init__(self, registry, keys, writer, ledger, runs, s3, ecs, bucket, *, clock=time.time):
        self.registry, self.keys, self.writer, self.ledger, self.runs = registry, keys, writer, ledger, runs
        self.s3, self.ecs, self.bucket, self.clock = s3, ecs, bucket, clock
        self.client, self.table = registry.client, registry.table

    def get(self, principal, space, kind, identity, pipeline=None):
        principal.authorize(space, "monitor:read")
        configuration = self.registry._read(space)
        self.registry._authorize_generation(principal, configuration)
        if self.keys.authenticate_id(principal.key_id or "") != principal:
            raise ControlError(403, "FORBIDDEN")
        validate("identifier", identity)
        self.nodes, self.principal, self.configuration = [], principal, configuration
        if kind == "run":
            if pipeline is None:
                raise ControlError(400, "TRACE_PIPELINE_REQUIRED")
            validate("identifier", pipeline)
            self._run(pipeline, identity)
        elif kind == "ingestion":
            self._ingestion(identity)
        elif kind == "query":
            self._query(identity)
        elif kind == "release":
            self._release(identity)
        else:
            raise ControlError(400, "INVALID_TRACE_KIND")
        result = {"space_id": space, "kind": kind, "id": identity, "observed_at": stamp(self.clock()),
                  "status": "PARTIAL" if any(node["status"] == "UNKNOWN" for node in self.nodes) else "COMPLETE",
                  "nodes": self.nodes[:512]}
        validate("trace", result)
        return result

    # -- helpers ----------------------------------------------------------------------------------

    def node(self, kind, identity, status, authority, observed, attributes=None, links=(), conflict=None):
        value = {"kind": kind, "id": str(identity)[:1024], "status": status, "authority": authority,
                 "observed_at": stamp(observed), "attributes": scalars(attributes or {}),
                 "links": [{"kind": link_kind, "id": str(link_id)[:1024]} for link_kind, link_id in links
                           if link_id][:128]}
        if conflict:
            value["conflict"] = conflict
        self.nodes.append(value)
        return value

    def unknown(self, kind, identity, reason, links=()):
        return self.node(kind, identity, "UNKNOWN", "UNAVAILABLE", None, {"reason": reason}, links)

    def native(self, kind, identity):
        try:
            return self.writer.call(self.principal, self.configuration, "governance_read", kind=kind, id=identity), None
        except ControlError as error:
            return None, error.code

    def _object(self, uri, version, kind="object"):
        """The exact S3 version exists with its recorded size; never the latest substitute."""
        bucket, key = uri.removeprefix("s3://").split("/", 1)
        try:
            head = self.s3.head_object(Bucket=bucket, Key=key, VersionId=version, ExpectedBucketOwner=account())
            return self.node(kind, uri, "PRESENT", "S3_OBJECT", self.clock(),
                             {"object_version": version, "bytes": head["ContentLength"],
                              "sha256": head.get("Metadata", {}).get("sha256")})
        except ClientError as error:
            code = error.response["Error"]["Code"]
            if code in {"404", "NoSuchKey", "NoSuchVersion", "NotFound"}:
                return self.node(kind, uri, "ABSENT", "S3_OBJECT", self.clock(), {"object_version": version})
            return self.unknown(kind, uri, "S3_" + code)
        except BotoCoreError:
            return self.unknown(kind, uri, "S3_UNAVAILABLE")

    # -- runs ---------------------------------------------------------------------------------------

    def _run(self, pipeline, run_id):
        configuration = self.configuration
        record = self.runs.pipelines._read(self.runs._key(configuration, pipeline, run_id), missing="RUN_NOT_FOUND")
        jobs = record["tasks"]
        self.node("run", run_id, record["status"], "CONTROL_RECORD", record["updated_unix"], {
            "pipeline_id": pipeline, "pipeline_version": record["pipeline_version"], "mode": record["request"]["mode"],
            "trigger": record["trigger"]["kind"], "created_at": stamp(record["created_unix"]),
            "finished_at": stamp(record.get("finished_unix")), "error_code": record.get("error_code"),
            "principal_fingerprint": (record["principal"]["key_id"] or "")[-12:],
            "snapshot_sha256": record["snapshot_sha256"], "root_inputs": record.get("root_inputs"),
            "projection_version": record["record_revision"]},
            [("pipeline_version", record["pipeline_version"])] + [("job", job["job_id"]) for job in jobs.values()])
        try:
            version = self.runs.pipelines._read(self.runs.pipelines._key(
                configuration, "PIPELINE_VERSION", pipeline, record["pipeline_version"]))
            package = version["package"]
            self.node("pipeline_version", record["pipeline_version"], "PUBLISHED", "CONTROL_RECORD",
                      version.get("created_unix"), {
                          "package_uri": package["package_uri"], "package_sha256": package["package_sha256"],
                          "package_object_version": package["package_object_version"],
                          "artifacts": len(version["snapshot"]["artifacts"]),
                          "environments": {task: value["reference"]
                                           for task, value in version["snapshot"]["environments"].items()},
                          "executors": version["snapshot"]["executors"]},
                      [("environment", value["reference"]["environment_id"] + "/" + value["reference"]["version"])
                       for value in version["snapshot"]["environments"].values()])
            self._object(package["package_uri"], package["package_object_version"], "code_package")
        except ControlError as error:
            self.unknown("pipeline_version", record["pipeline_version"], error.code)
        for task_id, job in jobs.items():
            self._job(record, task_id, job)

    def _job(self, record, task_id, job):
        if job.get("skipped"):
            return self._skipped(record, task_id, job)
        native, reason = (self.native("publication_job", job["job_id"])
                          if job.get("epoch") or job["status"] in {"IMPORTING", "PUBLISHED", "UNKNOWN"}
                          else (None, "NOT_GRANTED"))
        conflict = None
        status = job["status"]
        if native is not None:
            projection_published = job["status"] == "PUBLISHED"
            native_published = native["status"] == "PUBLISHED"
            if projection_published != native_published or (
                    projection_published and job.get("release_id") != native["release_id"]):
                conflict = {"projection": job["status"] + ":" + str(job.get("release_id")),
                            "authority": native["status"] + ":" + str(native["release_id"])}
                status = "UNKNOWN"
        elif reason not in {None, "NOT_GRANTED", "PUBLICATION_JOB_NOT_FOUND"} and job["status"] not in {
                "FAILED", "CANCELLED", "VALIDATED"}:
            status = "UNKNOWN"
        attempts = job["attempts"]
        self.node("job", job["job_id"], status, "CONTROL_RECORD", job.get("observed_unix"), {
            "run_id": record["run_id"], "task_id": task_id, "executor": job["executor"],
            "projection_status": job["status"], "eligibility_epoch": job.get("epoch"),
            "release_id": job.get("release_id"), "error_code": job.get("error_code"),
            "input_export_id": job.get("export_id"), "input_metrics": job.get("input_metrics"),
            "import_metrics": job.get("import_metrics"), "processing": (job.get("plan") or {}).get("processing")},
            [("attempt", job["job_id"] + "/" + attempt["attempt_id"]) for attempt in attempts]
            + [("input_export", job.get("export_id")), ("release", job.get("release_id"))]
            + [("publication_job", job["job_id"])] * (native is not None), conflict)
        if native is not None:
            self.node("publication_job", job["job_id"], native["status"], "NATIVE_LEDGER", self.clock(), native,
                      [("release", native.get("release_id"))])
        elif reason not in {None, "NOT_GRANTED"}:
            self.unknown("publication_job", job["job_id"], reason)
        if job.get("export_id"):
            export, reason = self.native("input_export", job["export_id"])
            if export is None:
                self.unknown("input_export", job["export_id"], reason)
            else:
                self.node("input_export", job["export_id"], export["state"], "NATIVE_LEDGER", self.clock(), export,
                          [("object", (export.get("manifest") or {}).get("key"))])
        for attempt in attempts:
            self._attempt(job, attempt)

    def _attempt(self, job, attempt):
        dispatch = attempt.get("dispatch") or {}
        identity = job["job_id"] + "/" + attempt["attempt_id"]
        links = [("descriptor", (attempt.get("descriptor") or {}).get("s3_uri")),
                 ("completion", (attempt.get("completion") or {}).get("s3_uri"))]
        self.node("attempt", identity, attempt["status"], "CONTROL_RECORD", attempt.get("ended_unix")
                  or attempt.get("staged_unix") or attempt.get("dispatched_unix") or attempt.get("created_unix"), {
                      "executor": dispatch.get("executor"), "lambda_request_id": dispatch.get("request_id"),
                      "function_arn": dispatch.get("function_arn"), "ecs_task_arn": dispatch.get("task_arn"),
                      "task_definition_arn": dispatch.get("task_definition_arn"), "dispatch_state": dispatch.get("state"),
                      "eligibility_epoch": attempt.get("epoch"), "execution_id": attempt.get("execution_id"),
                      "code": attempt.get("code"), "dispatched_at": stamp(attempt.get("dispatched_unix")),
                      "deadline_at": stamp(attempt.get("deadline_unix")), "ended_at": stamp(attempt.get("ended_unix")),
                      "candidates": attempt.get("candidates"), "usage": attempt.get("usage"),
                      "invocation": attempt.get("invocation"),
                      "notifications": attempt.get("notifications")}, links)
        if attempt.get("completion"):
            self._object(attempt["completion"]["s3_uri"], attempt["completion"]["object_version"], "completion")
        if dispatch.get("task_arn"):
            try:
                result = self.ecs.describe_tasks(cluster=self.runs.dispatcher.cluster, tasks=[dispatch["task_arn"]])
                tasks = result.get("tasks", [])
                if len(tasks) == 1:
                    task = tasks[0]
                    container = next((value for value in task.get("containers", []) if value["name"] == "runtime"), {})
                    self.node("ecs_task", dispatch["task_arn"], task["lastStatus"], "ECS", self.clock(), {
                        "exit_code": container.get("exitCode"), "stop_code": task.get("stopCode"),
                        "stopped_reason": task.get("stoppedReason"), "image_digest": container.get("imageDigest"),
                        "started_at": task["startedAt"].isoformat() if task.get("startedAt") else None,
                        "stopped_at": task["stoppedAt"].isoformat() if task.get("stoppedAt") else None,
                        "cpu": task.get("cpu"), "memory": task.get("memory")})
                else:
                    # ECS retains stopped tasks only briefly; absence is not a result.
                    self.unknown("ecs_task", dispatch["task_arn"], "ECS_TASK_NOT_RETAINED")
            except (ClientError, BotoCoreError, AttributeError):
                self.unknown("ecs_task", dispatch["task_arn"], "ECS_UNAVAILABLE")

    # -- ingestions ---------------------------------------------------------------------------------

    def _ingestion(self, identity):
        configuration = self.configuration
        authority, reason = None, None
        try:
            authority = self.ledger.read(configuration, identity)
        except (ControlError, ClientError, BotoCoreError) as error:
            reason = getattr(error, "code", "AUTHORITY_UNAVAILABLE")
        item = self.client.get_item(TableName=self.table, ConsistentRead=True, Key={
            "PK": {"S": "SPACE#" + configuration["space_id"]}, "SK": {"S": "INGESTION#" + identity}}).get("Item")
        projection = json.loads(item["observation"]["S"]) if item else None
        conflict, status = None, (authority or {}).get("status", "UNKNOWN")
        if authority is None:
            status = "UNKNOWN"
        if authority and projection and projection.get("status") != authority.get("status"):
            # A disagreeing observation is UNKNOWN until reconciled; both raw values are shown.
            conflict = {"projection": projection.get("status"), "authority": authority.get("status")}
            status = "UNKNOWN"
        dataset = (authority or projection or {}).get("dataset")
        # The intake archives under the ingestion-facing name: ods.<name> -> <name>.
        key = (f"spaces/{configuration['space_id']}/ingest/{configuration['generation']}/"
               f"{dataset.split('.', 1)[1]}/{identity}.json") if dataset and dataset.startswith("ods.") else None
        uri = "s3://" + self.bucket + "/" + key if key else None
        self.node("ingestion", identity, status, "NATIVE_LEDGER" if authority else "UNAVAILABLE",
                  self.clock() if authority else None, {
                      "dataset": dataset, "authority_status": (authority or {}).get("status"),
                      "commit_sequence": (authority or {}).get("commit_sequence"),
                      "committed_at": (authority or {}).get("committed_at"),
                      "error_code": (authority or {}).get("error_code"), "authority_reason": reason,
                      "projection_status": (projection or {}).get("status"),
                      "projection_version": (projection or {}).get("projection_version"),
                      "projection_observed_at": (projection or {}).get("observed_at")},
                  [("archive", uri)], conflict)
        if key:
            try:
                head = self.s3.head_object(Bucket=self.bucket, Key=key, ExpectedBucketOwner=account())
                metadata = head.get("Metadata", {})
                if (metadata.get("ingestion-id"), metadata.get("space-id"), metadata.get("space-generation"),
                        metadata.get("dataset")) != (identity, configuration["space_id"],
                                                     configuration["generation"], dataset):
                    self.unknown("archive", uri, "ARCHIVE_IDENTITY_MISMATCH")
                else:
                    self.node("archive", uri, "PRESENT", "S3_OBJECT", self.clock(), {
                        "object_version": head.get("VersionId"), "bytes": head["ContentLength"],
                        "raw_sha256": metadata.get("raw-sha256"), "payload_sha256": metadata.get("payload-sha256"),
                        "accepted_at": head["LastModified"].isoformat()})
            except ClientError as error:
                self.unknown("archive", uri, "S3_" + error.response["Error"]["Code"])
            except BotoCoreError:
                self.unknown("archive", uri, "S3_UNAVAILABLE")
        if authority:
            try:
                diagnostics = self.ledger.diagnostics(configuration, identity)
                self.node("ingestion_diagnostics", identity, (diagnostics.get("status") or {}).get("status", "UNKNOWN"),
                          "NATIVE_LEDGER", self.clock(), {"attempts": diagnostics.get("attempts"),
                                                          "dlq_reconciliations": diagnostics.get("dlq_reconciliations")})
            except (ControlError, ClientError, BotoCoreError) as error:
                self.unknown("ingestion_diagnostics", identity, getattr(error, "code", "AUTHORITY_UNAVAILABLE"))
        sequence = (authority or {}).get("commit_sequence")
        if dataset and sequence:
            self._downstream(dataset, sequence)

    def _downstream(self, dataset, sequence):
        """The first recent run of each pipeline whose fixed root watermark includes this commit."""
        configuration = self.configuration
        for item in self.runs.pipelines._records(configuration, "PIPELINE#" + configuration["generation"] + "#", 512):
            pipeline = item["SK"]["S"].rsplit("#", 1)[1]
            page = self.client.query(
                TableName=self.table, ConsistentRead=True, ScanIndexForward=False, Limit=100,
                KeyConditionExpression="PK = :pk AND begins_with(SK, :prefix)",
                ExpressionAttributeValues={":pk": {"S": "SPACE#" + configuration["space_id"]}, ":prefix": {
                    "S": f"RUNHISTORY#{configuration['generation']}#{pipeline}#"}})
            first = None
            for entry in page.get("Items", []):
                record = self.runs.pipelines._read(self.runs._key(configuration, pipeline, entry["run_id"]["S"]),
                                                   missing=None)
                root = ((record or {}).get("root_inputs") or {}).get(dataset) or {}
                if record and record["request"]["mode"] != "trial" and root.get("to_sequence", 0) >= sequence:
                    first = record
            if first is not None:
                self.node("downstream_run", first["run_id"], first["status"], "CONTROL_RECORD", first["updated_unix"], {
                    "pipeline_id": pipeline, "to_sequence": first["root_inputs"][dataset]["to_sequence"],
                    "releases": {task: job.get("release_id") for task, job in first["tasks"].items()}},
                    [("run", first["run_id"])])

    # -- queries ------------------------------------------------------------------------------------

    def _query(self, identity):
        configuration = self.configuration
        item = self.client.get_item(TableName=self.table, ConsistentRead=True, Key={
            "PK": {"S": f"QUERY#{configuration['space_id']}#{configuration['generation']}"},
            "SK": {"S": identity}}).get("Item")
        if not item:
            raise ControlError(404, "QUERY_NOT_FOUND")
        job = json.loads(item["record"]["S"])
        result = job.get("result") or {}
        attributes = {
            "principal_fingerprint": (job.get("key_id") or "")[-12:], "source_commit": job.get("source_commit"),
            "image_digest": job.get("image_digest"), "execution_id": job.get("execution_id"),
            "executor_task_arn": (job.get("executor") or {}).get("task_arn"),
            "created_at": stamp(job.get("created_unix")), "started_at": stamp(job.get("started_unix")),
            "finished_at": stamp(job.get("finished_unix")), "error_code": job.get("error_code"),
            "result_bytes": result.get("bytes"), "result_sha256": result.get("sha256"),
            "result_expires_at": stamp(job.get("result_expires_unix")),
            "result_visibility": "OWNER_ONLY"}
        attributes.update(self._sql(job))
        self.node("query", identity, job["status"], "QUERY_RECORD", job.get("finished_unix") or job.get("started_unix")
                  or job.get("created_unix"), attributes, [("object", result.get("key"))])

    def _sql(self, job):
        """SQL identity for monitors; SQL text only for audit:read, from the original intent version."""
        reference = job.get("input") or {}
        result = {"sql_visibility": "AUDIT_READ_REQUIRED"}
        if not reference.get("version_id") and not reference.get("object_version"):
            return {**result, "sql_visibility": "UNAVAILABLE"}
        try:
            key = (f"spaces/{job['space_id']}/query-intents/{job['generation']}/{job['query_id']}.json")
            response = self.s3.get_object(Bucket=self.bucket, Key=key, ExpectedBucketOwner=account(),
                                          VersionId=reference.get("version_id") or reference.get("object_version"))
            with response["Body"] as stream:
                intent = json.loads(stream.read(1048577))
            sql = intent["request"]["sql"]
        except (ClientError, BotoCoreError, KeyError, ValueError):
            return {**result, "sql_visibility": "UNAVAILABLE"}
        result.update(sql_sha256=hashlib.sha256(sql.encode()).hexdigest(), sql_bytes=len(sql.encode()))
        if "audit:read" in self.principal.scopes:
            result.update(sql_visibility="AUDIT_READ", sql=sql[:1024], sql_truncated=len(sql) > 1024)
        return result

    # -- releases -----------------------------------------------------------------------------------

    def _skipped(self, record, task_id, job):
        """A job with no affected partitions publishes nothing; it binds the task's current release,
        which the native ledger must confirm for this pipeline task (never a projection-only claim)."""
        release_id = job.get("release_id")
        value, reason = self.native("release", release_id) if release_id else (None, "RELEASE_NOT_RECORDED")
        status, conflict = job["status"], None
        if value is None:
            status = "UNKNOWN"
        elif (value.get("pipeline_id"), value.get("task_id")) != (record["pipeline_id"], task_id):
            conflict = {"projection": record["pipeline_id"] + "/" + task_id,
                        "authority": str(value.get("pipeline_id")) + "/" + str(value.get("task_id"))}
            status = "UNKNOWN"
        self.node("job", job["job_id"], status, "CONTROL_RECORD", job.get("observed_unix"), {
            "run_id": record["run_id"], "task_id": task_id, "executor": job["executor"],
            "projection_status": job["status"], "skipped": job["skipped"], "release_id": release_id,
            "processing": (job.get("plan") or {}).get("processing")}, [("release", release_id)], conflict)
        if value is None:
            self.unknown("release", release_id, reason)
        else:
            self.node("release", release_id, value["status"], "NATIVE_LEDGER", value.get("published_unix"), value,
                      [("run", value.get("run_id")), ("job", value.get("job_id"))])

    def _release(self, identity):
        value, reason = self.native("release", identity)
        if value is None:
            self.unknown("release", identity, reason)
            return
        links = [("run", value.get("run_id")), ("job", value.get("job_id")),
                 ("completion", (value.get("completion") or {}).get("s3_uri"))]
        links += [("release", binding.get("release_id")) for binding in (value.get("input_watermarks") or {}).values()]
        self.node("release", identity, value["status"], "NATIVE_LEDGER", value.get("published_unix"), value, links)
        if value.get("run_id") and value.get("pipeline_id"):
            record = self.runs.pipelines._read(self.runs._key(self.configuration, value["pipeline_id"], value["run_id"]),
                                               missing=None)
            if record is None:
                self.unknown("run", value["run_id"], "RUN_RECORD_NOT_FOUND")
            else:
                self.node("run", record["run_id"], record["status"], "CONTROL_RECORD", record["updated_unix"], {
                    "pipeline_id": value["pipeline_id"], "pipeline_version": record["pipeline_version"],
                    "root_inputs": record.get("root_inputs")}, [("job", value.get("job_id"))])
