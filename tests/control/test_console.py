"""Console/governance services over the simulated control table, writer and AWS reads."""

import copy
import hashlib
import json
import unittest
from datetime import datetime, timezone

from botocore.exceptions import ClientError

from colvenna_isle.control.audit import Audit
from colvenna_isle.control.costs import Costs
from colvenna_isle.control.handler import handle
from colvenna_isle.control.overview import Overview
from colvenna_isle.control.pipelines import Pipelines
from colvenna_isle.control.pipeline_artifacts import PipelineArtifacts
from colvenna_isle.control.quotas import Quotas
from colvenna_isle.control.registry import ControlError, Principal
from colvenna_isle.control.trace import Trace
from test_governance_runs import Fixture, query


class Ownership:
    def __init__(self, now):
        self.permit = {"state": "ACTIVE", "epoch": 3, "observed_at": now - 100, "identity": {
            "task_arn": "arn:aws:ecs:us-west-2:123456789012:task/cluster/writer", "image_digest": "sha256:" + "c" * 64,
            "source_commit": "d" * 40, "process_nonce": "n", "pid": 1, "start_ticks": 1, "boot_id": "b",
            "task_definition": "td", "address": "10.0.0.1"}}
        self.heartbeat = {"identity": copy.deepcopy(self.permit["identity"]), "state": "ACTIVE",
                          "observed_at": now - 2, "opened_databases": ["space_one/gen"], "last_commit": None}

    def read(self):
        return copy.deepcopy(self.permit)

    def registration(self, task_arn):
        return copy.deepcopy(self.heartbeat)


class Native:
    """Writer governance reads; an unavailable writer raises like the bridge."""

    def __init__(self, now):
        self.now, self.unavailable, self.calls = now, False, []
        self.jobs, self.exports, self.releases = {}, {}, {}

    def call(self, principal, configuration, operation, **fields):
        self.calls.append((operation, fields))
        if self.unavailable:
            raise ControlError(503, "WRITER_AUTHORITY_UNAVAILABLE")
        if operation == "space_overview":
            return {"observed_unix": self.now - 1, "datasets": [
                {"dataset": "ods.orders", "release_id": None, "commit_sequence": 7, "snapshot_sequence": 9,
                 "published_unix": None},
                {"dataset": "dwd.orders", "release_id": "release_a", "commit_sequence": 8, "snapshot_sequence": 9,
                 "published_unix": self.now - 30}],
                "scheduler": {"pending": 1, "max_pending": 32, "queued": {"ingestion": 1, "import": 0},
                              "wait_ms": {"ingestion": {"samples": 4, "p50": 2.0, "p95": 9.0, "max": 9.0}},
                              "rejected": {"import": 1}},
                "consumer": {"paused": False, "capacity": {"state": "MEASURED", "batches_per_second": 2.0}},
                "ingestion_latency": {"samples": 3, "p50_ms": 800, "p95_ms": 1200, "max_ms": 1200},
                "exports": {"live": 2, "retained_bytes": 4096, "max_exports": 128, "max_retained_bytes": 536870912},
                "publications": {"in_flight": 1, "reserved_bytes": 100, "max_jobs": 128,
                                 "max_generation_bytes": 536870912}}
        if operation == "governance_read":
            table = {"publication_job": self.jobs, "input_export": self.exports, "release": self.releases}[fields["kind"]]
            if fields["id"] not in table:
                raise ControlError(404, "NOT_FOUND")
            return copy.deepcopy(table[fields["id"]])
        raise AssertionError(operation)


class Admission:
    def __init__(self, now):
        self.value, self.fail = {"observed_at": datetime.fromtimestamp(now, timezone.utc).isoformat(),
                                 "pending_messages": 3, "oldest_seconds": 12.0, "oldest_observed_at": None,
                                 "dlq_messages": 0, "capacity": {"state": "MEASURED", "batches_per_second": 2.0},
                                 "queue_scope": "SHARED_QUEUE_CONSERVATIVE_BOUND"}, False

    def observe(self, configuration):
        if self.fail:
            raise ControlError(503, "QUEUE_UNAVAILABLE")
        return copy.deepcopy(self.value)


class S3:
    def __init__(self):
        self.objects = {}

    def head_object(self, Bucket, Key, ExpectedBucketOwner, VersionId=None):
        from botocore.exceptions import ClientError
        value = self.objects.get((Key, VersionId)) or (self.objects.get((Key, None)) if VersionId is None else None)
        if value is None:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return copy.deepcopy(value)

    def get_object(self, Bucket, Key, ExpectedBucketOwner, VersionId=None):
        import io
        value = self.objects[(Key, VersionId)]
        return {"Body": io.BytesIO(value["Body"]), "VersionId": VersionId}


class ECS:
    def describe_tasks(self, cluster, tasks):
        return {"tasks": []}

    def describe_task_definition(self, taskDefinition):
        return {"taskDefinition": {"cpu": "1024", "memory": "4096"}}


class ConsoleTests(unittest.TestCase):
    def setUp(self):
        self.f = Fixture()
        self.f.setUp()
        self.f.table.query = query(self.f.table)
        self.now = self.f.now
        self.quotas = Quotas(self.f.table, "control")
        self.f.pipelines.quotas = self.quotas
        self.f.environments.quotas = self.quotas
        self.f.runs.quotas = self.quotas
        self.monitor = self.f.monitor
        self.auditor = Principal("auditor", frozenset(["space_one"]), frozenset(["audit:read", "monitor:read"]),
                                 "key_auditor", self.f.generation)
        self.f.keys.principals["key_auditor"] = self.auditor
        self.ownership, self.native, self.admission = Ownership(self.now), Native(self.now), Admission(self.now)
        self.overview = Overview(self.f.registry, self.f.keys, self.ownership, self.native, self.f.runs,
                                 self.admission, self.quotas, environments=self.f.environments, clock=lambda: self.now)

    # -- overview ------------------------------------------------------------------------------

    def test_status_reports_each_source_with_time_and_never_infers_health(self):
        status = self.overview.status(self.monitor, "space_one")
        self.assertEqual((status["writer"]["status"], status["writer"]["metrics"]["epoch"]), ("OK", 3))
        self.assertEqual(status["queue"]["metrics"]["pending_messages"], 3)
        self.assertEqual(status["layers"]["ods"]["status"], "OK")
        datasets = {value["dataset"]: value for value in status["datasets"]}
        self.assertEqual(datasets["dws.daily"]["status"], "UNKNOWN")  # absent from the native read
        self.assertEqual(datasets["dwd.orders"]["release_id"], "release_a")
        self.assertEqual(status["scheduler"]["metrics"]["wait_ms_p95.ingestion"], 9.0)
        # A stale heartbeat is UNKNOWN, not healthy; a confirmed stop is STOPPED.
        self.ownership.heartbeat["observed_at"] = self.now - 60
        stale = self.overview.status(self.monitor, "space_one")
        self.assertEqual((stale["writer"]["status"], stale["writer"]["reason"]), ("UNKNOWN", "HEARTBEAT_STALE"))
        self.assertEqual(stale["layers"]["ods"]["status"], "UNKNOWN")
        self.assertIsNone(stale["layers"]["ods"]["observed_at"])
        self.ownership.permit["state"] = "STOPPED"
        self.assertEqual(self.overview.status(self.monitor, "space_one")["writer"]["status"], "STOPPED")
        self.admission.fail = True
        failed = self.overview.status(self.monitor, "space_one")
        self.assertEqual((failed["queue"]["status"], failed["dlq"]["status"]), ("UNKNOWN", "UNKNOWN"))
        with self.assertRaisesRegex(ControlError, "FORBIDDEN"):
            self.overview.status(self.f.owner, "space_one")

    def test_metrics_and_quota_usage_carry_observation_times(self):
        self.f.writer.ingest(1)
        self.f.drive(self.f.start_run()["run_id"])
        metrics = {value["name"]: value for value in self.overview.metrics(self.monitor, "space_one", 3600)["series"]}
        self.assertEqual(metrics["runs.published"]["value"], 1)
        self.assertEqual(metrics["ingestion.commit_latency_p95"]["value"], 1200)
        self.assertEqual(metrics["queries.latency_p95"]["status"], "UNKNOWN")
        self.native.unavailable = True
        unknown = {value["name"]: value for value in self.overview.metrics(self.monitor, "space_one")["series"]}
        self.assertEqual((unknown["ingestion.commit_latency_p95"]["status"],
                          unknown["ingestion.commit_latency_p95"]["reason"]),
                         ("UNKNOWN", "WRITER_AUTHORITY_UNAVAILABLE"))
        quotas = {value["name"]: value for value in self.overview.quota_status(self.monitor, "space_one")["usage"]}
        self.assertEqual((quotas["exports.live"]["status"], quotas["exports.live"]["observed_at"]), ("UNKNOWN", None))
        self.assertEqual(quotas["pipelines.defined"]["used"], 1)
        self.f.pipelines.inline_upload(self.f.owner, "space_one", "orders", {
            "path": "sql/x.sql", "sha256": hashlib.sha256(b"SELECT 3").hexdigest(), "content": "SELECT 3"})
        quotas = {value["name"]: value for value in self.overview.quota_status(self.monitor, "space_one")["usage"]}
        self.assertEqual((quotas["artifacts.stored_code_bytes"]["used"], quotas["artifacts.stored_code_bytes"]["status"]),
                         (8, "OK"))

    # -- pipelines ---------------------------------------------------------------------------

    def test_lists_readback_inline_upload_and_diagnostics(self):
        listed = self.f.pipelines.list(self.f.owner, "space_one")["pipelines"]
        self.assertEqual([(value["pipeline_id"], value["active_version"]) for value in listed], [("orders", None)])
        versions = self.f.pipelines.versions(self.f.owner, "space_one", "orders")["versions"]
        self.assertEqual([value["pipeline_version"] for value in versions], [self.f.version])
        content = self.f.pipelines.artifact(self.f.owner, "space_one", "orders", "sql/clean.sql")
        self.assertEqual((content["encoding"], content["content"]), ("utf-8", "SELECT 1"))
        with self.assertRaisesRegex(ControlError, "PIPELINE_ARTIFACT_NOT_FOUND"):
            self.f.pipelines.artifact(self.f.owner, "space_one", "orders", "sql/missing.sql")
        text = "SELECT 42 AS answer"
        sha = hashlib.sha256(text.encode()).hexdigest()
        with self.assertRaisesRegex(ControlError, "ARTIFACT_CONTENT_MISMATCH"):
            self.f.pipelines.inline_upload(self.f.owner, "space_one", "orders",
                                           {"path": "sql/clean.sql", "sha256": "0" * 64, "content": text})
        receipt = self.f.pipelines.inline_upload(self.f.owner, "space_one", "orders",
                                                 {"path": "sql/clean.sql", "sha256": sha, "content": text})
        metadata = self.f.pipelines.get(self.f.owner, "space_one", "orders")
        artifacts = [value if value["path"] != "sql/clean.sql" else {
            "path": "sql/clean.sql", "upload_id": receipt["upload_id"], "sha256": sha, "bytes": len(text)}
            for value in metadata["artifacts"]]
        draft = self.f.pipelines.replace_draft(self.f.owner, "space_one", "orders",
                                               {"definition": metadata["definition"], "artifacts": artifacts},
                                               '"' + metadata["revision"] + '"')
        self.assertEqual(self.f.pipelines.artifact(self.f.owner, "space_one", "orders", "sql/clean.sql")["content"],
                         text)
        with self.assertRaisesRegex(ControlError, "PIPELINE_REVISION_CHANGED"):
            self.f.pipelines.replace_draft(self.f.owner, "space_one", "orders",
                                           {"definition": metadata["definition"], "artifacts": artifacts},
                                           '"' + metadata["revision"] + '"')
        broken = copy.deepcopy(metadata["definition"])
        broken["tasks"][1]["output"]["dataset"] = "dws.unregistered"
        current = self.f.pipelines.get(self.f.owner, "space_one", "orders")
        draft = self.f.pipelines.replace_draft(self.f.owner, "space_one", "orders",
                                               {"definition": broken, "artifacts": artifacts},
                                               '"' + current["revision"] + '"')
        result = self.f.pipelines.validate(self.f.owner, "space_one", "orders", {"draft_revision": draft["revision"]})
        self.assertEqual(result["diagnostics"], [{
            "path": "/definition/tasks/1", "code": "PIPELINE_DATASET_NOT_REGISTERED",
            "message": "A task input or output dataset is not registered in this space."}])

    def test_artifact_profile_and_version_quotas_reject_before_effects(self):
        configuration = self.f.registry._read("space_one")
        operator = "arn:aws:sts::123456789012:assumed-role/operator/unit"
        self.quotas.set(configuration, {"max_stored_code_bytes": 1048576}, operator)
        big = "x" * 600000
        request = {"path": "sql/big.sql", "sha256": hashlib.sha256(big.encode()).hexdigest(), "content": big}
        self.f.pipelines.inline_upload(self.f.owner, "space_one", "orders", request)
        with self.assertRaisesRegex(ControlError, "SPACE_ARTIFACT_QUOTA"):
            self.f.pipelines.inline_upload(self.f.owner, "space_one", "orders", request)
        self.assertEqual(self.quotas.code_bytes("space_one", configuration["generation"]), 600000)
        self.quotas.set(configuration, {"max_pipeline_versions": 1}, operator)
        metadata = self.f.pipelines.get(self.f.owner, "space_one", "orders")
        with self.assertRaisesRegex(ControlError, "SPACE_PIPELINE_VERSION_QUOTA"):
            self.f.pipelines.publish(self.f.owner, "space_one", "orders",
                                     {"draft_revision": metadata["draft_revision"]}, "second-version")
        self.quotas.set(configuration, {"allowed_profiles": ["writer_short", "python_lambda_standard"]}, operator)
        result = self.f.pipelines.validate(self.f.owner, "space_one", "orders",
                                           {"draft_revision": metadata["draft_revision"]})
        self.assertEqual((result["diagnostics"][0]["code"], result["diagnostics"][0]["path"]),
                         ("SPACE_PROFILE_QUOTA", "/definition/tasks/1"))
        records = Audit(self.f.table, "control").list(self.auditor, configuration, action="QUOTA_POLICY_CHANGED")
        self.assertEqual(len(records["records"]), 3)
        self.assertEqual(records["records"][0]["actor"]["kind"], "IAM_OPERATOR")

    # -- trace and costs ------------------------------------------------------------------------

    def trace(self, principal, kind, identity, pipeline=None):
        return Trace(self.f.registry, self.f.keys, self.native, None, self.f.runs, S3(), ECS(), "test-bucket",
                     clock=lambda: self.now).get(principal, "space_one", kind, identity, pipeline)

    def test_run_trace_links_jobs_attempts_exports_and_flags_ledger_conflicts(self):
        self.f.writer.ingest(1)
        record = self.f.drive(self.f.start_run()["run_id"])
        clean = record["tasks"]["clean"]
        self.native.jobs[clean["job_id"]] = {"job_id": clean["job_id"], "status": "PUBLISHED", "epoch": 1,
                                             "release_id": clean["release_id"]}
        daily = record["tasks"]["daily"]
        # The native ledger disagrees with the projection for one job.
        self.native.jobs[daily["job_id"]] = {"job_id": daily["job_id"], "status": "IMPORTING", "epoch": 1,
                                             "release_id": None}
        self.native.exports[clean["export_id"]] = {"export_id": clean["export_id"], "state": "READY",
                                                   "manifest": {"key": "spaces/space_one/inputs/m.json"}}
        trace = self.trace(self.monitor, "run", record["run_id"], "orders")
        nodes = {(node["kind"], node["id"]): node for node in trace["nodes"]}
        self.assertEqual(trace["status"], "PARTIAL")
        self.assertEqual(nodes[("job", daily["job_id"])]["status"], "UNKNOWN")
        self.assertEqual(nodes[("job", daily["job_id"])]["conflict"],
                         {"projection": "PUBLISHED:" + daily["release_id"], "authority": "IMPORTING:None"})
        self.assertEqual(nodes[("job", clean["job_id"])]["status"], "PUBLISHED")
        self.assertEqual(nodes[("input_export", clean["export_id"])]["authority"], "NATIVE_LEDGER")
        attempt = nodes[("attempt", daily["job_id"] + "/attempt_1")]
        self.assertTrue(attempt["attributes"]["ecs_task_arn"].startswith("arn:aws:ecs:"))
        self.assertEqual(nodes[("ecs_task", attempt["attributes"]["ecs_task_arn"])]["status"], "UNKNOWN")
        self.native.unavailable = True
        unavailable = self.trace(self.monitor, "run", record["run_id"], "orders")
        self.assertIn("UNKNOWN", {node["status"] for node in unavailable["nodes"] if node["kind"] == "publication_job"})

    def test_skipped_job_is_traced_through_its_bound_native_release_not_a_publication_job(self):
        self.f.writer.ingest(1)
        record = self.f.drive(self.f.start_run()["run_id"])
        configuration = self.f.registry._read("space_one")
        key = self.f.runs._key(configuration, "orders", record["run_id"])
        item = self.f.table.get_item(TableName="control", Key=key)["Item"]
        stored = json.loads(item["record"]["S"])
        daily = stored["tasks"]["daily"]
        daily.update(skipped="NO_AFFECTED_PARTITIONS", epoch=None, release_id="release_previous", attempts=[])
        from colvenna_isle.control import pipelines

        stored["integrity_sha256"] = pipelines.digest({name: member for name, member in stored.items()
                                                       if name != "integrity_sha256"})
        item["record"] = {"S": json.dumps(stored)}
        self.f.table.items[(key["PK"]["S"], key["SK"]["S"])] = item
        self.native.releases["release_previous"] = {"release_id": "release_previous", "status": "PUBLISHED",
                                                    "pipeline_id": "orders", "task_id": "daily", "run_id": "run_old"}
        nodes = {(node["kind"], node["id"]): node for node in self.trace(self.monitor, "run", record["run_id"],
                                                                        "orders")["nodes"]}
        job = nodes[("job", daily["job_id"])]
        self.assertEqual((job["status"], job["attributes"]["skipped"]), ("PUBLISHED", "NO_AFFECTED_PARTITIONS"))
        self.assertNotIn(("publication_job", daily["job_id"]), nodes)
        self.assertEqual(nodes[("release", "release_previous")]["authority"], "NATIVE_LEDGER")
        # A bound release the ledger attributes to another task is a conflict, not a confirmation.
        self.native.releases["release_previous"]["task_id"] = "clean"
        nodes = {(node["kind"], node["id"]): node for node in self.trace(self.monitor, "run", record["run_id"],
                                                                        "orders")["nodes"]}
        self.assertEqual(nodes[("job", daily["job_id"])]["status"], "UNKNOWN")

    def test_ingestion_trace_reads_the_archive_under_its_intake_name_and_native_diagnostics(self):
        from datetime import datetime, timezone

        configuration = self.f.registry._read("space_one")
        identity = "ing_" + "a" * 64

        class Ledger:
            def read(self, configuration, ingestion_id):
                return {"status": "COMMITTED", "dataset": "ods.orders", "commit_sequence": 7,
                        "committed_at": "2026-10-18T00:00:00Z", "error_code": None}

            def diagnostics(self, configuration, ingestion_id):
                return {"status": {"status": "COMMITTED"}, "attempts": [{"attempt": 1}], "dlq_reconciliations": []}

        trace = Trace(self.f.registry, self.f.keys, self.native, Ledger(), self.f.runs, S3(), ECS(), "test-bucket")
        key = f"spaces/space_one/ingest/{configuration['generation']}/orders/{identity}.json"
        metadata = {"ingestion-id": identity, "space-id": "space_one", "space-generation": configuration["generation"],
                    "dataset": "ods.orders", "raw-sha256": "b" * 64, "payload-sha256": "c" * 64}
        trace.s3.objects[(key, None)] = {"VersionId": "av1", "ContentLength": 99, "Metadata": metadata,
                                         "LastModified": datetime(2026, 10, 18, tzinfo=timezone.utc)}
        value = trace.get(self.monitor, "space_one", "ingestion", identity)
        nodes = {node["kind"]: node for node in value["nodes"]}
        uri = "s3://test-bucket/" + key
        self.assertEqual(nodes["ingestion"]["links"], [{"kind": "archive", "id": uri}])
        self.assertEqual((nodes["archive"]["id"], nodes["archive"]["status"], nodes["archive"]["attributes"]
                          ["object_version"]), (uri, "PRESENT", "av1"))
        self.assertEqual(nodes["ingestion_diagnostics"]["status"], "COMMITTED")
        # An archive whose stored identity differs is never accepted as this ingestion's.
        trace.s3.objects[(key, None)]["Metadata"]["ingestion-id"] = "ing_" + "d" * 64
        nodes = {node["kind"]: node for node in trace.get(self.monitor, "space_one", "ingestion", identity)["nodes"]}
        self.assertEqual((nodes["archive"]["status"], nodes["archive"]["attributes"]["reason"]),
                         ("UNKNOWN", "ARCHIVE_IDENTITY_MISMATCH"))

    def test_query_trace_shows_sql_only_to_audit_read_and_never_results(self):
        configuration = self.f.registry._read("space_one")
        trace = Trace(self.f.registry, self.f.keys, self.native, None, self.f.runs, S3(), ECS(), "test-bucket")
        s3 = trace.s3
        intent = {"query_id": "qry_1", "request": {"sql": "SELECT secret_column FROM dwd.orders"}}
        s3.objects[(f"spaces/space_one/query-intents/{configuration['generation']}/qry_1.json", "iv1")] = {
            "Body": json.dumps(intent).encode()}
        job = {"query_id": "qry_1", "space_id": "space_one", "generation": configuration["generation"],
               "status": "SUCCEEDED", "key_id": "key_owner", "input": {"version_id": "iv1"}, "created_unix": self.now,
               "result": {"key": "spaces/space_one/query-results/x/manifest.json", "bytes": 42, "sha256": "f" * 64}}
        self.f.table.items[(f"QUERY#space_one#{configuration['generation']}", "qry_1")] = {
            "PK": {"S": f"QUERY#space_one#{configuration['generation']}"}, "SK": {"S": "qry_1"},
            "record": {"S": json.dumps(job)}}
        monitor = trace.get(self.monitor, "space_one", "query", "qry_1")["nodes"][0]["attributes"]
        self.assertEqual(monitor["sql_visibility"], "AUDIT_READ_REQUIRED")
        self.assertNotIn("sql", monitor)
        self.assertEqual(monitor["sql_sha256"], hashlib.sha256(intent["request"]["sql"].encode()).hexdigest())
        audited = trace.get(self.auditor, "space_one", "query", "qry_1")["nodes"][0]["attributes"]
        self.assertEqual(audited["sql"], intent["request"]["sql"])
        self.assertEqual(audited["result_visibility"], "OWNER_ONLY")
        self.assertNotIn("rows", json.dumps(audited))

    def test_costs_attribute_measured_compute_exports_and_imports_to_workloads(self):
        self.f.writer.ingest(1)
        record = self.f.drive(self.f.start_run()["run_id"])
        report = Costs(self.f.registry, self.f.keys, self.f.runs, self.f.environments, ECS(),
                       clock=lambda: self.now + 60).report(self.monitor, "space_one")
        categories = {line["category"] for line in report["lines"]}
        self.assertTrue({"writer_compute", "ecs_compute", "lambda_compute", "s3_storage", "s3_requests"} <= categories)
        ecs = next(line for line in report["lines"] if line["category"] == "ecs_compute")
        self.assertEqual((ecs["unit"], ecs["usage"]), ("task-s", 60))  # Fargate one-minute minimum
        self.assertEqual(ecs["workload"]["id"].split("/")[0], record["run_id"])
        exports = [line for line in report["lines"] if line["workload"]["kind"] == "snapshot_export"]
        self.assertTrue(exports)
        self.assertEqual(report["basis"], "MEASURED_USAGE_X_LIST_PRICE")
        self.assertAlmostEqual(sum(report["totals"].values()), sum(line["estimated_usd"] for line in report["lines"]),
                               places=6)
        self.assertEqual((report["status"], report["unmeasured"]), ("COMPLETE", []))
        # An ECS attempt whose task definition cannot be sized is named, never priced at zero.
        class Unsized(ECS):
            def describe_task_definition(self, taskDefinition):
                raise ClientError({"Error": {"Code": "AccessDenied"}}, "DescribeTaskDefinition")
        partial = Costs(self.f.registry, self.f.keys, self.f.runs, self.f.environments, Unsized(),
                        clock=lambda: self.now + 60).report(self.monitor, "space_one")
        self.assertEqual(partial["status"], "PARTIAL")
        self.assertEqual({(gap["kind"], gap["reason"]) for gap in partial["unmeasured"]},
                         {("attempt", "TASK_DEFINITION_UNAVAILABLE")})
        self.assertTrue(all(gap["id"].startswith(record["run_id"] + "/") for gap in partial["unmeasured"]))
        with self.assertRaisesRegex(ControlError, "INVALID_COST_WINDOW"):
            Costs(self.f.registry, self.f.keys, self.f.runs, None, ECS()).report(
                self.monitor, "space_one", "2026-01-01T00:00:00Z", "2026-03-01T00:00:00Z")

    # -- routes -------------------------------------------------------------------------------

    def test_admin_routes_check_scope_before_services(self):
        class Recorder:
            def __init__(self):
                self.calls = []

            def __getattr__(self, name):
                def call(*args, **kwargs):
                    self.calls.append(name)
                    return {"ok": True}
                return call
        services = {name: Recorder() for name in ["audit", "overview", "trace", "retention", "costs"]}

        def request(principal, method, path, query=None, body=None):
            event = {"rawPath": path, "queryStringParameters": query, "headers": {},
                     "requestContext": {"requestId": "r", "http": {"method": method},
                                        "authorizer": {"lambda": {
                                            "principal_id": principal.principal_id, "key_id": principal.key_id,
                                            "spaces": json.dumps(sorted(principal.spaces)),
                                            "scopes": json.dumps(sorted(principal.scopes)),
                                            "generation": principal.generation, "expires_at": None}}}}
            if body is not None:
                event["body"] = json.dumps(body)
            return handle(event, self.f.registry, None, None, self.f.environments, self.f.pipelines, self.f.runs,
                          services)

        self.assertEqual(request(self.monitor, "GET", "/v1/admin/spaces/space_one/status")["statusCode"], 200)
        self.assertEqual(request(self.monitor, "GET", "/v1/admin/spaces/space_one/audit")["statusCode"], 403)
        self.assertEqual(request(self.monitor, "GET", "/v1/admin/spaces/space_one/trace",
                                 {"kind": "run", "id": "run_1", "pipeline_id": "orders"})["statusCode"], 200)
        self.assertEqual(request(self.monitor, "GET", "/v1/admin/spaces/space_one/trace")["statusCode"], 400)
        self.assertEqual(request(self.monitor, "PUT", "/v1/admin/spaces/space_one/retention",
                                 body={"state": "ENABLED", "policy": {}})["statusCode"], 403)
        self.assertEqual(request(self.monitor, "GET", "/v1/admin/spaces/other/status")["statusCode"], 403)
        self.assertEqual(request(self.monitor, "GET", "/v1/admin/spaces/space_one/metrics",
                                 {"window_seconds": "60,120"})["statusCode"], 400)
        self.assertEqual(services["overview"].calls, ["status"])
        self.assertEqual(services["trace"].calls, ["get"])
        self.assertEqual(services["retention"].calls, [])
        listed = request(self.monitor, "GET", "/v1/spaces/space_one/pipelines")
        self.assertEqual((listed["statusCode"], json.loads(listed["body"])["pipelines"][0]["pipeline_id"]),
                         (200, "orders"))


def load_tests(loader, tests, pattern):
    return loader.loadTestsFromTestCase(ConsoleTests)


if __name__ == "__main__":
    unittest.main()
