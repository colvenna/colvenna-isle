"""Run controller semantics over simulated writer-ledger and compute authorities.

The writer double enforces the native protocol's identities (export nonce,
eligibility epoch, selected execution, revoke/publish order). Actual Native,
Lambda, ECS and DynamoDB behavior is qualified separately on AWS.
"""

import copy
import hashlib
import json
import time
import unittest
from unittest.mock import ANY

from botocore.exceptions import ClientError

from colvenna_isle.contracts.model import KEY_STATE_METADATA
from colvenna_isle.control.dispatch import DispatchUnknown, token
from colvenna_isle.control.environments import Environments
from colvenna_isle.control.pipeline_artifacts import PipelineArtifacts, checksum
from colvenna_isle.control.pipelines import Pipelines
from colvenna_isle.control.registry import ControlError, Principal, Registry, canonical
from colvenna_isle.control.runs import Runs
from colvenna_isle.control.scheduler import Scheduler, parse_notification, window
from colvenna_isle.control.writer_client import WriterUnknown
from colvenna_isle.pipeline.task_descriptor import digest
from colvenna_isle.pipeline.task_objects import TaskObjects
from test_environments import Artifacts, Builds, Table, specification
from test_pipelines import Objects

LAMBDA = "arn:aws:lambda:us-west-2:123456789012:function:colvenna-isle-env-test:1"
TASKDEF = "arn:aws:ecs:us-west-2:123456789012:task-definition/colvenna-isle-env-test:1"
IMAGE = "123456789012.dkr.ecr.us-west-2.amazonaws.com/colvenna-isle-environments@sha256:" + "b" * 64
CLEAN = [{"name": "order_id", "type": "BIGINT", "nullable": False},
         {"name": "order_date", "type": "DATE", "nullable": False},
         {"name": "amount", "type": "DECIMAL(18,2)", "nullable": False}]
DAILY = [{"name": "order_date", "type": "DATE", "nullable": False},
         {"name": "revenue", "type": "DECIMAL(38,2)", "nullable": False}]
SUMMARY = [{"name": "metric", "type": "VARCHAR", "nullable": False},
           {"name": "value", "type": "DECIMAL(38,2)", "nullable": False}]
METADATA = [{"name": "_colvenna_version", "type": "BIGINT", "nullable": False},
            {"name": "_colvenna_sequence", "type": "BIGINT", "nullable": False}]


class RunTable(Table):
    def put_item(self, **request):
        with self.lock:
            self.items[self.key(request["Item"])] = copy.deepcopy(request["Item"])

    def delete_item(self, **request):
        with self.lock:
            self.items.pop(self.key(request["Key"]), None)

    def query(self, **request):
        prefix = request["ExpressionAttributeValues"][":pk"]["S"]
        with self.lock:
            items = [copy.deepcopy(value) for key, value in sorted(self.items.items()) if key[0] == prefix]
        return {"Items": items[:request.get("Limit", len(items))]}

    class exceptions:
        ConditionalCheckFailedException = ClientError

    def update_item(self, **request):
        if ":window" not in request["ExpressionAttributeValues"]:
            return super().update_item(**request)
        with self.lock:
            key = self.key(request["Key"])
            current = self.items.get(key)
            new = int(request["ExpressionAttributeValues"][":window"]["N"])
            if current is None or "last_window" in current and int(current["last_window"]["N"]) >= new:
                raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "UpdateItem")
            current["last_window"] = {"N": str(new)}


class AllBindings(Artifacts):
    def verify(self, record, actual):
        value = super().verify(record, actual)
        value["executor_bindings"] = [
            {"executor": "lambda", "profile": profile, "lambda_mode": "standard", "function_arn": LAMBDA}
            for profile in ["python_lambda_standard", "sql_lambda_standard"]] + [
            {"executor": "ecs_task", "profile": profile, "task_definition_arn": TASKDEF, "image": IMAGE}
            for profile in ["python_ecs_medium", "sql_ecs_medium"]]
        return value


class Keys:
    def __init__(self, principals):
        self.principals, self.revoked = principals, set()

    def authenticate_id(self, key_id):
        if key_id in self.revoked or key_id not in self.principals:
            raise ControlError(403, "FORBIDDEN")
        return self.principals[key_id]

    def operation_conditions(self, principal, space, generation, scope):
        principal.authorize(space, scope)
        return []


class Compute:
    """Simulated executions: each run of an attempt stages one completion."""

    def __init__(self, objects):
        self.objects, self.staged, self.counter = objects, {}, 0
        self.silent, self.duplicates, self.invocations = set(), set(), []

    def run(self, reference, executor):
        raw = self.objects.objects[reference["s3_uri"].split("/", 3)[3]]["Body"]
        descriptor = json.loads(raw)
        identity = (descriptor["run_id"], descriptor["job_id"], descriptor["attempt_id"])
        self.invocations.append((executor, identity))
        if (descriptor["task"]["task_id"], descriptor["attempt_id"]) in self.silent:
            return  # Lost/timed-out compute: no completion ever appears.
        for _ in range(2 if descriptor["task"]["task_id"] in self.duplicates else 1):
            self.counter += 1
            execution = "exec_" + str(self.counter)
            prefix = (f"s3://test-bucket/spaces/{descriptor['space_id']}/staging/{descriptor['generation']}/"
                      f"{descriptor['pipeline_id']}/{descriptor['run_id']}/{descriptor['job_id']}/"
                      f"{descriptor['attempt_id']}/{execution}/")
            self.staged.setdefault(identity, []).append({
                "execution_id": execution, "modified": self.counter, "invocation": {"executor": executor},
                "reference": {"s3_uri": prefix + "completion.json", "object_version": "c" + str(self.counter),
                              "sha256": "c" * 64, "bytes": 100}})


class Dispatcher:
    def __init__(self, compute):
        self.compute, self.tasks, self.stopped = compute, {}, []
        self.lose_run_task = False
        self.invokes = 0

    def lambda_invoke(self, binding, reference):
        self.invokes += 1
        self.compute.run(reference, "lambda")
        return {"executor": "lambda", "function_arn": binding["function_arn"], "request_id": "req-" + str(self.invokes)}

    def ecs_run(self, binding, reference, tags):
        identity = token(reference)
        if identity not in self.tasks:
            self.tasks[identity] = "arn:aws:ecs:us-west-2:123456789012:task/cluster/" + identity
            self.compute.run(reference, "ecs_task")
        if self.lose_run_task:
            self.lose_run_task = False
            raise DispatchUnknown("ECS_RUN_TASK_UNKNOWN")
        return {"executor": "ecs_task", "task_arn": self.tasks[identity], "token": identity,
                "task_definition_arn": binding["task_definition_arn"]}

    def ecs_find(self, identity):
        return [self.tasks[identity]] if identity in self.tasks else []

    def ecs_observe(self, arn):
        return {"last_status": "STOPPED", "exit_code": 0}

    def ecs_stop(self, arn, reason):
        self.stopped.append(arn)
        return True

    def completions(self, descriptor):
        return copy.deepcopy(self.compute.staged.get(
            (descriptor["run_id"], descriptor["job_id"], descriptor["attempt_id"]), []))


class Writer:
    """Native-ledger double: exports, eligibility epochs, selection and publication."""

    def __init__(self, test):
        self.test, self.calls = test, []
        self.sequence, self.ods = 0, 0
        self.releases, self.current, self.tasks = {}, {}, {}
        self.exports, self.jobs, self.workers = {}, {}, {}
        self.lose = set()
        self.partitions = [{"order_date": "2026-10-01"}]
        self.identical = True
        self.revoke_publishes = False
        self.on_prepare = None
        self.pending_workers = set()
        self.unavailable = 0
        self.defer_import = set()
        self.samples, self.preview_rejects = {}, set()

    def call(self, principal, configuration, operation, **fields):
        self.calls.append((operation, copy.deepcopy(fields)))
        if self.unavailable:
            self.unavailable -= 1
            raise ControlError(503, "WRITER_AUTHORITY_UNAVAILABLE")
        result = getattr(self, operation)(principal, **fields)
        if operation in self.lose:
            self.lose.discard(operation)
            raise WriterUnknown()
        return copy.deepcopy(result)

    def ingest(self, count=1):
        self.sequence += count
        self.ods = self.sequence

    def output(self, task_id):
        return next(task["output"]["dataset"] for task in self.test.definition["tasks"] if task["task_id"] == task_id)

    def pipeline_watermarks(self, principal, pipeline_id, datasets):
        result = {"datasets": {}, "tasks": copy.deepcopy(self.tasks), "snapshot_sequence": self.sequence}
        for name in datasets:
            release = self.current.get(name)
            result["datasets"][name] = {"commit_sequence": self.ods if name.startswith("ods.") else
                                        release["commit_sequence"] if release else 0,
                                        "release_id": release["release_id"] if release else None,
                                        "snapshot_sequence": self.sequence}
        return result

    def publication_target(self, principal, pipeline_id, pipeline_version, task_id):
        dataset = self.output(task_id)
        release = self.current.get(dataset)
        return {"dataset": dataset, "expected_previous_release": release["release_id"] if release else None,
                "previous": copy.deepcopy(release)}

    def input_prepare(self, principal, client_request_id, inputs, retention_seconds, sample_rows=None):
        self.samples[client_request_id] = sample_rows
        if self.on_prepare:
            hook, self.on_prepare = self.on_prepare, None
            hook()
        identity = "input_" + digest([principal.principal_id, client_request_id])[:32]
        if identity in self.exports:
            if self.exports[identity]["bindings"] != inputs:
                raise ControlError(409, "INPUT_EXPORT_BINDING_CONFLICT")
            return self.input_manifest(principal, identity)
        manifest = {}
        for alias, binding in inputs.items():
            version = {"dataset": binding["dataset"], "release_id": binding.get("release_id"),
                       "commit_sequence": binding.get("to_sequence", self.releases.get(
                           binding.get("release_id"), {}).get("commit_sequence", 0)),
                       "snapshot_sequence": self.sequence, "generation": self.test.generation}
            if binding["selection"]["mode"] == "changes":
                version.update(from_sequence=binding["from_sequence"], to_sequence=binding["to_sequence"])
            if binding["selection"]["mode"] == "affected_keys":
                version["state_release_id"] = binding["release_id"]
            schema = [{"name": "id", "type": "BIGINT", "nullable": False}]
            manifest[alias] = {"selection": binding["selection"], "native_version": version, "manifest": {
                "contract_version": 2, "dataset": binding["dataset"],
                "release_id": "input_" + digest([identity, alias])[:40], "watermark": {"ods_sequence":
                    version["commit_sequence"]}, "schema": schema,
                "files": [{"s3_uri": f"s3://test-bucket/spaces/space_one/inputs/{self.test.generation}/"
                                     f"{identity}/exec/{alias}/part-00000.parquet",
                           "object_version": "v1", "sha256": "a" * 64, "rows": 1, "bytes": 10}]}}
        self.exports[identity] = {"bindings": copy.deepcopy(inputs), "inputs": manifest, "references": {}}
        return self.input_manifest(principal, identity)

    def input_manifest(self, principal, export_id):
        return {"state": "READY", "export_id": export_id, "space_id": "space_one", "generation": self.test.generation,
                "metrics": {"copied_bytes": 10, "copied_rows": 1, "elapsed_ms": 5, "uploaded_bytes": 20,
                            "upload_elapsed_ms": 3},
                "manifest": {"space_id": "space_one", "generation": self.test.generation, "export_id": export_id,
                             "inputs": copy.deepcopy(self.exports[export_id]["inputs"])}}

    def input_retain(self, principal, export_id, consumer_id, retain_until_unix):
        self.exports[export_id]["references"][consumer_id] = "ACTIVE"
        return {"state": "READY"}

    def input_release(self, principal, export_id, consumer_id):
        self.exports[export_id]["references"][consumer_id] = "RELEASED"
        return {"state": "READY"}

    def publication_grant(self, principal, descriptor, input_export_id, expected_epoch):
        value = json.loads(self.test.objects.objects[descriptor["s3_uri"].split("/", 3)[3]]["Body"])
        job = self.jobs.get(value["job_id"])
        if job and job["reference"] == descriptor:
            return self.summary(job)
        if job and (job["epoch"] != expected_epoch or job["status"] == "PUBLISHED"):
            raise ControlError(409, "PUBLICATION_ELIGIBILITY_CHANGED")
        if not job and expected_epoch:
            raise ControlError(409, "PUBLICATION_ELIGIBILITY_CHANGED")
        if self.exports[input_export_id]["references"].get(value["job_id"]) != "ACTIVE":
            # The writer grants only while this job pins its fixed input.
            raise ControlError(409, "PUBLICATION_INPUT_AUTHORITY_CHANGED")
        self.jobs[value["job_id"]] = {"job_id": value["job_id"], "epoch": expected_epoch + 1, "status": "GRANTED",
                                      "reference": descriptor, "descriptor": value, "export_id": input_export_id}
        return self.summary(self.jobs[value["job_id"]])

    def summary(self, job):
        return {name: copy.deepcopy(job[name]) for name in ["job_id", "epoch", "status", "execution_id", "release"]
                if name in job}

    def writer_task_execute(self, principal, descriptor):
        key = canonical(descriptor)
        if key not in self.workers:
            self.workers[key] = "STAGED"
            self.test.compute.run(descriptor, "writer")
        return {"state": "RUNNING"}

    def writer_task_status(self, principal, descriptor):
        return {"state": self.workers.get(canonical(descriptor), "UNKNOWN")}

    def publication_import(self, principal, job_id, eligibility_epoch, attempt_id, execution_id, completion):
        job = self.jobs[job_id]
        if job["epoch"] != eligibility_epoch or job["descriptor"]["attempt_id"] != attempt_id or job["status"] == "REVOKED":
            raise ControlError(409, "PUBLICATION_ELIGIBILITY_CHANGED")
        if job["status"] == "PUBLISHED":
            return self.summary(job)
        if job.get("execution_id", execution_id) != execution_id:
            raise ControlError(409, "PUBLICATION_EXECUTION_CONFLICT")
        job["execution_id"] = execution_id
        if job_id in self.defer_import:
            # The import worker started; a writer replacement loses it.
            self.defer_import.discard(job_id)
            self.pending_workers.add(job_id)
            return {**self.summary(job), "worker": {"state": "PREPARING"}}
        self.pending_workers.discard(job_id)
        descriptor = job["descriptor"]
        dataset = descriptor["task"]["output"]["dataset"]
        self.sequence += 1
        release = {"release_id": "release_" + digest([job_id, eligibility_epoch])[:32], "dataset": dataset,
                   "commit_sequence": self.sequence, "pipeline_version": descriptor["pipeline_version"],
                   "task_id": descriptor["task"]["task_id"], "task_sha256": digest(descriptor["task"]),
                   "parameters_sha256": digest(descriptor["parameters"]),
                   "input_versions": {alias: value["native_version"]
                                      for alias, value in self.exports[job["export_id"]]["inputs"].items()},
                   "processing": descriptor["processing"], "execution_id": execution_id,
                   "partitions": [{"partition": part, "rows": 1, "replaced": True}
                                  for part in descriptor["output_state"]["replace_partitions"]]}
        if descriptor["output_state"]["expected_previous_release"] != (self.current.get(dataset) or {}).get("release_id"):
            raise ControlError(409, "PUBLICATION_PREVIOUS_RELEASE_CONFLICT")
        self.releases[release["release_id"]] = release
        self.current[dataset] = release
        self.tasks[release["task_id"]] = release
        job.update(status="PUBLISHED", release=release)
        return self.summary(job)

    def publication_status(self, principal, job_id):
        if job_id not in self.jobs:
            raise ControlError(404, "PUBLICATION_JOB_NOT_FOUND")
        value = self.summary(self.jobs[job_id])
        if job_id in self.pending_workers:
            value["worker"] = {"state": "IMPORTING"}
        return value

    def publication_revoke(self, principal, job_id, eligibility_epoch):
        job = self.jobs[job_id]
        if self.revoke_publishes and job["status"] != "PUBLISHED":
            # The publication transaction committed before the revocation.
            execution = self.test.compute.staged[(job["descriptor"]["run_id"], job_id,
                                                  job["descriptor"]["attempt_id"])][0]["execution_id"]
            self.publication_import(principal, job_id, job["epoch"], job["descriptor"]["attempt_id"], execution, {})
        if job["status"] == "PUBLISHED":
            return self.summary(job)
        if job["epoch"] != eligibility_epoch:
            raise ControlError(409, "PUBLICATION_ELIGIBILITY_CHANGED")
        job.update(status="REVOKED", epoch=eligibility_epoch + 1)
        return self.summary(job)

    def publication_preview(self, principal, descriptor, execution_id, completion, rows):
        """Trial validation: the same candidate checks as an import, never a native job."""
        value = json.loads(self.test.objects.objects[descriptor["s3_uri"].split("/", 3)[3]]["Body"])
        if value["job_id"] in self.jobs:
            raise AssertionError("A trial attempt must never hold a native publication job")
        if value["task"]["task_id"] in self.preview_rejects:
            return {"validation": {"status": "REJECTED", "error_code": "OUTPUT_SCHEMA_MISMATCH", "row_count": None,
                                   "files": None, "bytes": None}, "preview": None}
        if rows and "query:read" not in principal.scopes:
            raise ControlError(403, "PREVIEW_QUERY_SCOPE_REQUIRED")
        return {"validation": {"status": "PASSED", "error_code": None, "row_count": 3, "files": 1, "bytes": 640},
                "preview": {"columns": [{"name": "order_id", "type": "BIGINT", "encoding": "native_text"}],
                            "rows": [[1], [2], [3]][:rows], "truncated": rows < 3}}

    def pipeline_impact(self, principal, dataset, from_release, to_release, partition_columns):
        if not partition_columns:
            return {"bounded": True, "partitions": [] if self.identical else [{}]}
        return {"bounded": True, "partitions": copy.deepcopy(self.partitions)}


class RunControllerTests(unittest.TestCase):
    def setUp(self):
        self.now = 1_800_000_000.0
        self.table = RunTable()
        self.registry = Registry(self.table, "control", "test-bucket")
        self.generation = self.registry.provision("space_one")["generation"]
        scopes = ["pipeline:manage", "pipeline:run", "runtime:manage", "datasets:manage"]
        self.owner = Principal("owner", frozenset(["space_one"]), frozenset(scopes), "key_owner", self.generation)
        self.other = Principal("other", frozenset(["space_one"]), frozenset(["pipeline:run"]), "key_other",
                               self.generation)
        self.monitor = Principal("monitor", frozenset(["space_one"]), frozenset(["monitor:read"]), "key_monitor",
                                 self.generation)
        self.keys = Keys({"key_owner": self.owner, "key_other": self.other, "key_monitor": self.monitor})
        for value in [
            {"dataset": "ods.orders", "schema_version": 1, "write_owner": "ingestion", "key_columns": ["order_id"],
             "partition_columns": [], "schema": [{"name": "event_id", "type": "VARCHAR", "nullable": False},
                                                 {"name": "order_id", "type": "BIGINT", "nullable": False},
                                                 {"name": "version", "type": "BIGINT", "nullable": False}],
             "ingestion": {"operations": ["upsert", "delete"], "event_id_column": "event_id",
                           "source_version_column": "version"}},
            {"dataset": "dwd.orders", "schema_version": 1, "write_owner": "pipeline", "schema": CLEAN,
             "key_columns": ["order_id"], "partition_columns": [],
             "publication_state": {"protocol": "key_state_v1", "key_columns": ["order_id"], "allow_empty": True,
                                   "schema": [CLEAN[0]] + KEY_STATE_METADATA}},
            {"dataset": "dws.daily", "schema_version": 1, "write_owner": "pipeline", "schema": DAILY,
             "key_columns": ["order_date"], "partition_columns": ["order_date"]},
            {"dataset": "ads.summary", "schema_version": 1, "write_owner": "pipeline", "schema": SUMMARY,
             "key_columns": ["metric"], "partition_columns": []}]:
            self.registry.register(self.owner, "space_one", value, generation=self.generation)
        builds = Builds()
        self.environments = Environments(self.registry, builds, "approved", {
            "source_location": "test-bucket/artifacts/builders/approved.zip",
            "source_version": "source-version"}, AllBindings())
        self.environments.create(self.owner, "space_one", specification(
            content="duckdb==1.5.6\npandas==2.2.3\npyarrow==24.0.0"))
        self.environments.build(self.owner, "space_one", "analysis", "v1", "original")
        builds.status = "SUCCEEDED"
        self.environments.get(self.owner, "space_one", "analysis", "v1")
        self.objects = Objects()
        self.pipelines = Pipelines(self.registry, PipelineArtifacts(self.objects, "test-bucket"), self.environments,
                                   writer_source="a" * 40)
        self.compute = Compute(self.objects)
        self.dispatcher = Dispatcher(self.compute)
        self.writer = Writer(self)
        self.runs = Runs(self.pipelines, self.keys, self.writer, self.dispatcher,
                         lambda space, generation: TaskObjects(self.objects, "test-bucket", space, generation),
                         clock=lambda: self.now)
        self.definition = self.dag()
        self.version = self.publish(self.definition)

    def dag(self, sql="SELECT 1"):
        environment = {"runtime_environment": {"environment_id": "analysis", "version": "v1"},
                       "input_mode": "local_files"}
        return {"spec_version": 1, "pipeline_id": "orders", "name": "Mixed", "max_active_runs": 1,
                "coalesce_pending": True, "schedule": {"owner": "none"}, "parameters": {
                    "threshold": {"type": "decimal_string", "default": "1.00"}},
                "tasks": [
            {"task_id": "clean", "language": "sql", "executor": "writer", "resource_profile": "writer_short",
             "entrypoint": "sql/clean.sql", "timeout_seconds": 30, "max_attempts": 2,
             "processing": {"mode": "incremental", "bootstrap": "full", "on_missing_state": "full"},
             "inputs": {"changes": {"dataset": "ods.orders", "selection": "changes"},
                        "key_state": {"state": "target_key_state", "selection": "affected_keys"}},
             "output": {"dataset": "dwd.orders", "write_mode": "merge_keys", "key_columns": ["order_id"],
                        "allow_empty": True, "schema": CLEAN, "change_metadata": METADATA}},
            {"task_id": "daily", "language": "sql", "executor": "ecs_task", "resource_profile": "sql_ecs_medium",
             "entrypoint": "sql/daily.sql", "timeout_seconds": 600, "max_attempts": 2, "depends_on": ["clean"],
             "processing": {"mode": "partition_recompute", "partition_columns": ["order_date"],
                            "on_unbounded_impact": "full"},
             "inputs": {"orders": {"task": "clean", "selection": "affected_partitions"}},
             "output": {"dataset": "dws.daily", "write_mode": "replace_partitions", "key_columns": ["order_date"],
                        "partition_columns": ["order_date"], "allow_empty": True, "schema": DAILY}, **environment},
            {"task_id": "summary", "language": "python", "executor": "lambda",
             "resource_profile": "python_lambda_standard", "entrypoint": "python/summary.py",
             "timeout_seconds": 300, "max_attempts": 2, "depends_on": ["daily"], "processing": {"mode": "full"},
             "inputs": {"daily": {"task": "daily", "selection": "snapshot"}},
             "output": {"dataset": "ads.summary", "write_mode": "versioned_replace", "key_columns": ["metric"],
                        "allow_empty": True, "schema": SUMMARY}, **environment}]}

    def publish(self, definition, sql="SELECT 1"):
        files = {"sql/clean.sql": sql.encode(), "sql/daily.sql": b"SELECT 2",
                 "python/summary.py": b"print('summary')\n"}
        artifacts = [{"path": path, "upload_id": "pending", "sha256": hashlib.sha256(raw).hexdigest(),
                      "bytes": len(raw)} for path, raw in files.items()]
        try:
            created = self.pipelines.create(self.owner, "space_one", {"definition": definition, "artifacts": artifacts})
        except ControlError:
            created = self.pipelines.get(self.owner, "space_one", "orders")
        for artifact in artifacts:
            receipt = self.pipelines.upload(self.owner, "space_one", "orders", {
                name: artifact[name] for name in ["path", "sha256", "bytes"]})
            self.objects.put_object(Key=self.objects.signed["Params"]["Key"], Body=files[artifact["path"]],
                                    ChecksumSHA256=checksum(artifact["sha256"]))
            artifact["upload_id"] = receipt["upload_id"]
        current = self.pipelines.get(self.owner, "space_one", "orders")
        draft = self.pipelines.replace_draft(self.owner, "space_one", "orders",
            {"definition": definition, "artifacts": artifacts}, '"' + current["revision"] + '"')
        return self.pipelines.publish(self.owner, "space_one", "orders", {"draft_revision": draft["revision"]},
                                      "publish-" + digest(sql)[:8])["pipeline_version"]

    def start_run(self, nonce="run-1", **request):
        return self.runs.create(self.owner, "space_one", "orders",
                                {"pipeline_version": self.version, "mode": "snapshot", **request}, nonce)

    def record(self, run_id):
        return self.runs.read(self.owner, "space_one", "orders", run_id)

    def drive(self, run_id, rounds=20):
        for _ in range(rounds):
            record = self.runs.advance(self.record(run_id))
            if record["status"] in {"PUBLISHED", "FAILED", "CANCELLED"}:
                return record
        return self.record(run_id)

    def test_mixed_dag_publishes_in_order_with_fixed_root_watermark_and_idempotent_admission(self):
        self.writer.ingest(5)
        receipt = self.start_run()
        self.assertEqual(self.start_run(), receipt)
        with self.assertRaisesRegex(ControlError, "IDEMPOTENCY_KEY_CONFLICT"):
            self.start_run(parameters={"threshold": "9.00"})
        # Continuous ingestion after the run starts; it keeps its bound interval.
        self.writer.on_prepare = lambda: self.writer.ingest(3)
        record = self.drive(receipt["run_id"])
        self.assertEqual(record["root_inputs"], {"ods.orders": {"to_sequence": 5}})
        self.assertEqual(self.writer.ods, 8)
        self.assertEqual(record["status"], "PUBLISHED")
        clean, daily, summary = (record["tasks"][name] for name in ["clean", "daily", "summary"])
        self.assertEqual(clean["plan"]["processing"], {"mode": "incremental", "from_watermark": {"ods_sequence": 0},
            "to_watermark": {"ods_sequence": 5}, "bootstrap": True, "state_release_id": None})
        self.assertEqual(daily["plan"]["processing"]["affected_partitions"], [{"order_date": "2026-10-01"}])
        published = [call[1]["job_id"] for call in self.writer.calls if call[0] == "publication_import"]
        self.assertEqual(published, [clean["job_id"], daily["job_id"], summary["job_id"]])
        self.assertEqual([value[0] for value in self.compute.invocations], ["writer", "ecs_task", "lambda"])
        status = self.runs.get(self.owner, "space_one", "orders", receipt["run_id"])
        self.assertEqual([task["status"] for task in status["tasks"]], ["PUBLISHED"] * 3)
        self.assertEqual(status["tasks"][0]["release_id"], clean["release_id"])
        self.assertEqual(self.writer.exports[clean["export_id"]]["references"][clean["job_id"]], "RELEASED")
        # Next run: previous release interval and state; new data only.
        second = self.start_run("run-2")
        record = self.drive(second["run_id"])
        processing = record["tasks"]["clean"]["plan"]["processing"]
        self.assertEqual((processing["from_watermark"], processing["to_watermark"], processing["bootstrap"]),
                         ({"ods_sequence": 5}, {"ods_sequence": self.writer.ods}, False))
        self.assertEqual(processing["state_release_id"], clean["release_id"])

    def test_rule_change_bootstraps_and_scopes_are_checked(self):
        self.writer.ingest(2)
        first = self.drive(self.start_run()["run_id"])
        self.version = self.publish(self.dag(), sql="SELECT 99")
        self.writer.ingest(1)
        second = self.drive(self.start_run("run-2")["run_id"])
        self.assertTrue(second["tasks"]["clean"]["plan"]["processing"]["bootstrap"])
        self.assertEqual(second["tasks"]["clean"]["plan"]["processing"]["from_watermark"], {"ods_sequence": 0})
        with self.assertRaisesRegex(ControlError, "RUN_OWNER_REQUIRED"):
            self.runs.get(self.other, "space_one", "orders", first["run_id"])
        self.assertEqual(self.runs.get(self.monitor, "space_one", "orders", first["run_id"])["status"], "PUBLISHED")
        with self.assertRaises(ControlError):
            self.runs.create(self.monitor, "space_one", "orders", {"pipeline_version": self.version,
                                                                  "mode": "snapshot"}, "denied")
        with self.assertRaisesRegex(ControlError, "PIPELINE_VERSION_NOT_FOUND"):
            self.runs.create(self.owner, "space_one", "orders", {"pipeline_version": "pv_missing",
                                                                "mode": "snapshot"}, "wrong")

    def test_lost_compute_retries_with_new_attempt_epoch_and_late_candidate_cannot_publish(self):
        self.writer.ingest(1)
        self.compute.silent.add(("summary", "attempt_1"))
        run_id = self.start_run()["run_id"]
        record = self.drive(run_id, rounds=10)
        summary = record["tasks"]["summary"]
        self.assertEqual((summary["status"], summary["epoch"]), ("RUNNING", 1))
        self.now += 300 + 121
        record = self.drive(run_id)
        summary = record["tasks"]["summary"]
        self.assertEqual(record["status"], "PUBLISHED")
        self.assertEqual([attempt["status"] for attempt in summary["attempts"]], ["FAILED", "PUBLISHED"])
        self.assertEqual(summary["epoch"], 2)
        # The first attempt's late completion is fenced by the native epoch.
        with self.assertRaisesRegex(ControlError, "PUBLICATION_ELIGIBILITY_CHANGED"):
            self.writer.publication_import(self.owner, summary["job_id"], 1, "attempt_1", "exec_late", {})

    def test_duplicate_invocations_select_one_candidate_and_publish_once(self):
        self.writer.ingest(1)
        self.compute.duplicates.add("summary")
        record = self.drive(self.start_run()["run_id"])
        attempt = record["tasks"]["summary"]["attempts"][-1]
        self.assertEqual((attempt["candidates"], record["status"]), (2, "PUBLISHED"))
        imports = [call for call in self.writer.calls if call[0] == "publication_import"
                   and call[1]["job_id"] == record["tasks"]["summary"]["job_id"]]
        self.assertEqual({call[1]["execution_id"] for call in imports}, {attempt["execution_id"]})

    def test_cancel_revokes_unpublished_jobs_and_keeps_published_upstream(self):
        self.writer.ingest(1)
        self.compute.silent.add(("summary", "attempt_1"))
        run_id = self.start_run()["run_id"]
        record = self.drive(run_id, rounds=10)
        self.assertEqual(record["tasks"]["summary"]["status"], "RUNNING")
        result = self.runs.cancel(self.owner, "space_one", "orders", run_id)
        self.assertEqual(result["status"], "CANCELLED")
        record = self.record(run_id)
        self.assertEqual([record["tasks"][name]["status"] for name in ["clean", "daily", "summary"]],
                         ["PUBLISHED", "PUBLISHED", "CANCELLED"])
        job = self.writer.jobs[record["tasks"]["summary"]["job_id"]]
        self.assertEqual((job["status"], job["epoch"]), ("REVOKED", 2))
        with self.assertRaisesRegex(ControlError, "PUBLICATION_ELIGIBILITY_CHANGED"):
            self.writer.publication_import(self.owner, job["job_id"], 1, "attempt_1", "exec_late", {})
        self.assertEqual(self.runs.cancel(self.owner, "space_one", "orders", run_id)["status"], "CANCELLED")

    def test_publication_that_wins_cancellation_stays_published(self):
        self.writer.ingest(1)
        run_id = self.start_run()["run_id"]
        record = self.runs.advance(self.record(run_id), deadline=time.monotonic() + 30)
        while record["tasks"]["summary"]["status"] not in {"STAGED", "IMPORTING", "PUBLISHED"}:
            record = self.runs.advance(record)
            if record["tasks"]["summary"]["status"] == "STAGED":
                break
        self.writer.revoke_publishes = True
        self.assertEqual(self.runs.cancel(self.owner, "space_one", "orders", run_id)["status"], "PUBLISHED")

    def test_lost_import_and_grant_replies_reconcile_without_duplicate_publication(self):
        self.writer.ingest(1)
        self.writer.lose.update({"publication_import", "publication_grant"})
        run_id = self.start_run()["run_id"]
        record = self.drive(run_id)
        self.assertEqual(record["status"], "PUBLISHED")
        self.assertEqual(len(self.writer.releases), 3)
        self.assertTrue(any(event.get("event") == "PUBLISHED" for event in record["events"]))

    def test_uncertain_run_task_reconciles_original_task_by_started_by(self):
        self.writer.ingest(1)
        self.dispatcher.lose_run_task = True
        record = self.drive(self.start_run()["run_id"])
        self.assertEqual(record["status"], "PUBLISHED")
        self.assertEqual(len(self.dispatcher.tasks), 1)
        self.assertEqual([value[0] for value in self.compute.invocations].count("ecs_task"), 1)

    def test_retry_failed_run_preserves_published_upstream_and_unknown_blocks_retry(self):
        self.writer.ingest(1)
        self.compute.silent.update({("summary", "attempt_1"), ("summary", "attempt_2")})
        run_id = self.start_run()["run_id"]
        self.drive(run_id, rounds=10)
        self.now += 421
        self.drive(run_id, rounds=10)
        self.now += 421
        record = self.drive(run_id, rounds=10)
        self.assertEqual((record["status"], record["tasks"]["summary"]["status"]), ("FAILED", "FAILED"))
        releases = {name: record["tasks"][name]["release_id"] for name in ["clean", "daily"]}
        first = self.runs.retry(self.owner, "space_one", "orders", run_id, "retry-1")
        self.assertEqual(self.runs.retry(self.owner, "space_one", "orders", run_id, "retry-1"), first)
        record = self.drive(run_id)
        self.assertEqual(record["status"], "PUBLISHED")
        self.assertEqual({name: record["tasks"][name]["release_id"] for name in ["clean", "daily"]}, releases)
        self.assertEqual(record["tasks"]["summary"]["attempt_number"], 3)
        self.assertEqual(record["tasks"]["summary"]["export_id"],
                         record["tasks"]["summary"]["attempts"][0]["descriptor"] and
                         record["tasks"]["summary"]["export_id"])
        with self.assertRaisesRegex(ControlError, "RUN_NOT_RETRYABLE"):
            self.runs.retry(self.owner, "space_one", "orders", run_id, "retry-2")

    def test_queued_run_waits_for_active_run_and_scheduler_coalesces_by_window(self):
        self.writer.ingest(1)
        self.compute.silent.add(("summary", "attempt_1"))
        first = self.start_run()["run_id"]
        self.assertEqual(self.runs.advance(self.record(first))["tasks"]["summary"]["status"], "RUNNING")
        second = self.start_run("run-2")["run_id"]
        self.assertEqual(self.runs.advance(self.record(second))["status"], "QUEUED")
        self.now += 421
        self.assertEqual(self.drive(first)["status"], "PUBLISHED")
        record = self.drive(second, rounds=4)
        self.now += 421
        self.assertEqual(self.drive(second)["status"], "PUBLISHED")

    def test_colvenna_schedule_triggers_once_per_window_only_with_new_data_and_pause_stops_new_runs(self):
        metadata = self.pipelines.get(self.owner, "space_one", "orders")
        metadata = self.pipelines.activate(self.owner, "space_one", "orders", {"pipeline_version": self.version},
                                           '"' + metadata["revision"] + '"')
        enabled = self.pipelines.schedule(self.owner, "space_one", "orders", {
            "owner": "colvenna", "expression": "rate(1 minute)", "timezone": "UTC", "state": "ENABLED"},
            '"' + metadata["revision"] + '"')
        scheduler = Scheduler(self.runs, self.keys, clock=lambda: self.now)
        self.writer.ingest(2)
        decisions = scheduler.triggers()
        self.assertEqual(decisions[0]["decision"], "RUN_ADMITTED")
        run_id = decisions[0]["run_id"]
        self.assertEqual(scheduler.triggers()[0]["decision"], "NOT_DUE")
        self.now += 60
        self.assertEqual(scheduler.triggers()[0]["decision"], "ACTIVE_RUN_COALESCES")
        self.drive(run_id)
        self.now += 60
        self.assertEqual(scheduler.triggers()[0]["decision"], "NO_NEW_DATA")
        self.writer.ingest(1)
        self.now += 60
        decision = scheduler.triggers()[0]
        self.assertEqual(decision["decision"], "RUN_ADMITTED")
        self.assertEqual(self.record(decision["run_id"])["trigger"]["kind"], "schedule")
        self.pipelines.schedule(self.owner, "space_one", "orders", {
            "owner": "external", "state": "ENABLED"}, '"' + enabled["revision"] + '"')
        self.now += 60
        self.assertEqual(scheduler.triggers(), [])
        # External owner: API runs are recorded as external; Colvenna Isle still drives the DAG.
        receipt = self.start_run("airflow-1")
        self.assertEqual(self.record(receipt["run_id"])["trigger"]["kind"], "external")

    def test_schedule_windows_and_notification_paths(self):
        self.assertEqual(window("rate(5 minutes)", "UTC", 1_800_000_123), (1_800_000_123 // 60) // 5 * 5)
        self.assertEqual(window("cron(0 12 * * ? *)", "UTC", 1_800_000_000) % 1440, 12 * 60)
        body = json.dumps({"requestContext": {"requestId": "r1", "condition": "Success"},
            "requestPayload": {"operation": "task_execute", "descriptor": {"s3_uri":
                "s3://bucket/spaces/space_one/task-contexts/gen_x/run_1/job_1/attempt_1/descriptor.json"}},
            "responsePayload": {"status": "FAILED", "code": "TASK_SCRIPT_FAILED"}})
        self.assertEqual(parse_notification(body), {"source": "lambda", "request_id": "r1", "condition": "Success",
            "status": "FAILED", "code": "TASK_SCRIPT_FAILED", "execution_id": None, "space_id": "space_one",
            "generation": "gen_x", "run_id": "run_1", "job_id": "job_1", "attempt_id": "attempt_1"})
        self.assertIsNone(parse_notification("not json"))

    def test_lambda_failure_notification_ends_attempt_before_deadline(self):
        self.writer.ingest(1)
        self.compute.silent.add(("summary", "attempt_1"))
        run_id = self.start_run()["run_id"]
        record = self.drive(run_id, rounds=10)
        attempt = record["tasks"]["summary"]["attempts"][-1]
        self.runs.notify({"source": "lambda", "request_id": attempt["dispatch"]["request_id"], "status": "FAILED",
                          "code": "TASK_SCRIPT_FAILED", "space_id": "space_one", "generation": self.generation,
                          "run_id": run_id, "job_id": record["tasks"]["summary"]["job_id"], "attempt_id": "attempt_1"})
        record = self.drive(run_id)
        self.assertEqual(record["status"], "PUBLISHED")
        self.assertEqual(record["tasks"]["summary"]["attempts"][0]["code"], "TASK_SCRIPT_FAILED")

    def test_uncertain_input_preparation_reconciles_by_the_same_export_nonce(self):
        self.writer.ingest(1)
        self.writer.lose.add("input_prepare")
        run_id = self.start_run()["run_id"]
        record = self.runs.advance(self.record(run_id))
        record = self.drive(run_id)
        self.assertEqual(record["status"], "PUBLISHED")
        nonces = [call[1]["client_request_id"] for call in self.writer.calls if call[0] == "input_prepare"]
        self.assertEqual(len(set(nonces)), 3)
        self.assertEqual(len(self.writer.exports), 3)

    def test_import_resumes_after_writer_replacement_loses_the_worker(self):
        self.writer.ingest(1)
        run_id = self.start_run()["run_id"]
        clean_job = "job_" + digest([run_id, "clean"])[:40]
        self.writer.defer_import.add(clean_job)
        record = self.runs.advance(self.record(run_id))
        self.assertEqual(record["tasks"]["clean"]["status"], "IMPORTING")
        self.writer.pending_workers.clear()  # Writer replaced: no in-memory worker.
        record = self.drive(run_id)
        self.assertEqual(record["status"], "PUBLISHED")
        self.assertEqual(sum(release["task_id"] == "clean" for release in self.writer.releases.values()), 1)

    def test_temporary_writer_unavailability_is_retried_not_failed(self):
        self.writer.ingest(1)
        run_id = self.start_run()["run_id"]
        self.writer.unavailable = 3
        record = self.drive(run_id)
        self.assertEqual(record["status"], "PUBLISHED")
        self.assertTrue(all(job["attempt_number"] == 1 for job in record["tasks"].values()))

    def test_cancel_during_dispatch_revokes_a_grant_the_record_never_saw(self):
        self.writer.ingest(1)
        run_id = self.start_run()["run_id"]
        original = self.writer.publication_grant

        def grant_then_cancel(principal, **fields):
            result = original(principal, **fields)
            # Cancellation is admitted between the native grant and its record update.
            current = self.record(run_id)
            updated = copy.deepcopy(current)
            updated.update(cancel_requested=True, status="CANCELLING")
            self.runs._save(updated, current)
            return result
        self.writer.publication_grant = grant_then_cancel
        record = self.drive(run_id)
        self.assertEqual(record["status"], "CANCELLED")
        native = self.writer.jobs[record["tasks"]["clean"]["job_id"]]
        self.assertEqual(native["status"], "REVOKED")
        self.assertFalse(self.compute.invocations)

    def test_retry_keeps_root_watermarks_and_releases_failed_inputs(self):
        self.writer.ingest(1)
        self.compute.silent.update({("summary", "attempt_1"), ("summary", "attempt_2")})
        run_id = self.start_run()["run_id"]
        for _ in range(3):
            self.drive(run_id, rounds=10)
            self.now += 421
        record = self.drive(run_id)
        self.assertEqual(record["status"], "FAILED")
        export = record["tasks"]["summary"]["export_id"]
        self.assertEqual(self.writer.exports[export]["references"][record["tasks"]["summary"]["job_id"]], "RELEASED")
        roots = copy.deepcopy(record["root_inputs"])
        self.writer.ingest(5)
        self.runs.retry(self.owner, "space_one", "orders", run_id, "retry-roots")
        record = self.drive(run_id)
        self.assertEqual((record["status"], record["root_inputs"]), ("PUBLISHED", roots))
        # The retry re-pinned the same original export, then released it again at its end.
        job = record["tasks"]["summary"]
        self.assertEqual((job["export_id"], job["attempt_number"]), (export, 3))
        self.assertIn(("input_retain", {"export_id": export, "consumer_id": job["job_id"],
                                        "retain_until_unix": ANY}), self.writer.calls)
        self.assertEqual(self.writer.exports[export]["references"][job["job_id"]], "RELEASED")

    def test_status_is_readable_immediately_after_admission(self):
        receipt = self.runs.create(self.owner, "space_one", "orders", {"pipeline_version": self.version,
                                                                        "mode": "snapshot"}, "before-first-step")
        value = self.runs.get(self.owner, "space_one", "orders", receipt["run_id"])
        self.assertEqual((value["status"], value["observed_at"], value["created_at"]),
                         ("QUEUED", value["created_at"], value["created_at"]))
        self.assertEqual({task["status"] for task in value["tasks"]}, {"BLOCKED"})

    def test_republished_identical_snapshot_input_keeps_partition_impact_bounded(self):
        days = ["2026-10-01", "2026-10-02", "2026-10-03"]
        task = {"task_id": "daily", "processing": {"mode": "partition_recompute", "partition_columns": ["order_date"],
                                                   "on_unbounded_impact": "full"},
                "inputs": {"orders": {"task": "clean", "selection": "affected_partitions"},
                           "flags": {"task": "flags", "selection": "snapshot"}}}
        context = {"configuration": {}, "principal": self.owner, "definition": {"tasks": [
            {"task_id": "clean", "output": {"dataset": "dwd.orders"}},
            {"task_id": "flags", "output": {"dataset": "dws.flags"}}]}}
        record = {"request": {"mode": "snapshot"}, "root_inputs": {},
                  "tasks": {"clean": {"release_id": "release_clean_2"}, "flags": {"release_id": "release_flags_2"}}}
        previous = {"input_versions": {"orders": {"release_id": "release_clean_1"},
                                       "flags": {"release_id": "release_flags_1"}},
                    "partitions": [{"partition": {"order_date": day}} for day in days]}
        self.writer.partitions = [{"order_date": days[1]}]
        # The full upstream task republished identical rows under a new release id.
        self.assertEqual(self.runs._impact(record, task, context, previous, True), [{"order_date": days[1]}])
        self.assertIn(("pipeline_impact", {"dataset": "dws.flags", "from_release": "release_flags_1",
                                           "to_release": "release_flags_2", "partition_columns": []}),
                      self.writer.calls)
        # Changed dimension rows have no declared mapping: every partition is recomputed.
        self.writer.identical = False
        self.assertEqual(self.runs._impact(record, task, context, previous, True),
                         [{"order_date": day} for day in days])


if __name__ == "__main__":
    unittest.main()
