"""Public contract seams: known good/bad documents, types, states and gate decisions."""

import copy
import json
import subprocess
import sys
import tempfile
import unittest
from datetime import timezone
from pathlib import Path

from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
from colvenna_isle.contracts.model import ROUTES, artifacts, schemas
from colvenna_isle.contracts.validation import (
    ContractError, load_document, validate, validate_change,
    validate_record, validate_transition, validate_value, wire_schema,
)

HASH = "a" * 64
STAMP = "2026-10-15T08:00:00Z"
PREVIOUS = "r_previous"


def context():
    columns = [{"name": "order_id", "type": "BIGINT", "nullable": False},
               {"name": "amount", "type": "DECIMAL(18,2)", "nullable": False}]
    return {
        "contract_version": 2, "space_id": "space_1", "pipeline_id": "orders",
        "pipeline_version": "v1", "run_id": "run_1", "task_id": "score_orders",
        "job_id": "job_1", "attempt_id": "attempt_1", "execution_id": "exec_1",
        "input_mode": "local_files", "parameters": {"high_value_threshold": "500.00"},
        "inputs": {"orders": {
            "dataset": "dwd.orders", "release_id": "r1", "manifest_sha256": HASH,
            "watermark": {"ods_sequence": 42}, "selection": {"mode": "snapshot", "partitions": []},
            "schema": columns, "files": [{
                "s3_uri": "s3://fixture-bucket/spaces/space_1/inputs/r1/part.parquet",
                "object_version": "fixture-version", "sha256": HASH, "rows": 2, "bytes": 256,
                "local_path": "/tmp/job_1/attempt_1/exec_1/input/part.parquet",
            }],
        }},
        "output": {
            "name": "result", "dataset": "dwd.scores", "directory": "/tmp/job_1/attempt_1/exec_1/output",
            "write_mode": "versioned_replace", "expected_previous_release": PREVIOUS,
            "key_columns": ["order_id"], "allow_empty": True, "replace_partitions": [],
            "schema": columns,
        },
    }


def manifest(ctx):
    return {
        "contract_version": 2, "job_id": ctx["job_id"], "attempt_id": ctx["attempt_id"],
        "execution_id": ctx["execution_id"], "output_name": "result", "format": "parquet",
        "schema": copy.deepcopy(ctx["output"]["schema"]), "row_count": 2,
        "files": [{"path": "part.parquet", "rows": 2, "bytes": 256,
                   "sha256": HASH, "partition_values": {}}],
    }


class ContractTests(unittest.TestCase):
    def assertReject(self, operation):
        with self.assertRaises(ContractError):
            operation()

    def test_registered_complete_state_image_requires_pipeline_owner_and_exact_business_keys(self):
        definition = {"dataset": "dwd.result", "schema_version": 1, "write_owner": "pipeline",
            "schema": [{"name": "id", "type": "BIGINT", "nullable": False}],
            "key_columns": ["id"], "partition_columns": [],
            "publication_state": {"protocol": "complete_image_v1",
                "schema": [{"name": "id", "type": "BIGINT", "nullable": False},
                           {"name": "deleted", "type": "BOOLEAN", "nullable": False}],
                "key_columns": ["id"], "allow_empty": False}}
        validate("dataset_registration", definition)
        changes = [
            lambda value: value.update(write_owner="ingestion"),
            lambda value: value["publication_state"].update(protocol="unregistered_protocol"),
            lambda value: value["publication_state"].update(key_columns=["deleted"]),
            lambda value: value["publication_state"]["schema"][0].update(type="VARCHAR"),
            lambda value: value["publication_state"]["schema"][0].update(nullable=True),
            lambda value: value["publication_state"]["schema"].append({"name": "ID", "type": "BIGINT", "nullable": False}),
        ]
        for change in changes:
            changed = copy.deepcopy(definition)
            change(changed)
            with self.subTest(change=change), self.assertRaises(ContractError):
                validate("dataset_registration", changed)

    def test_artifact_generation_and_every_api_reference(self):
        Draft202012Validator.check_schema(schemas())
        generated = artifacts()
        folder = ROOT / "src/colvenna_isle/contracts/v2"
        for name, value in generated.items():
            self.assertEqual(json.loads((folder / name).read_text()), value)
        operations = []
        for path, methods in generated["openapi.json"]["paths"].items():
            for method, operation in methods.items():
                operations.append(operation["operationId"])
                self.assertEqual(operation["security"], [{"BearerApiKey": []}])
                self.assertIn("space_id", [parameter["name"] for parameter in operation["parameters"]])
                for response in operation["responses"].values():
                    schema = response["content"]["application/json"]["schema"]
                    alternatives = schema.get("oneOf", [schema])
                    self.assertTrue(alternatives)
                    for alternative in alternatives:
                        self.assertIn(alternative["$ref"].rsplit("/", 1)[1], schemas()["$defs"])
        self.assertEqual(len(operations), len(set(operations)))
        self.assertEqual(len(operations), 55)

    def test_all_request_bodies_reject_execution_resource_overrides(self):
        pipeline = load_document(ROOT / "examples/orders/pipeline.yaml")
        samples = {
            "dataset_registration": {
                "dataset": "ods.orders", "schema_version": 1,
                "schema": [{"name": "order_id", "type": "BIGINT", "nullable": False}],
                "key_columns": ["order_id"], "partition_columns": [],
            },
            "ingestion_request": {"dataset": "orders", "schema_version": 1, "operation": "upsert",
                                  "records": [{"order_id": 1, "amount": "12.30"}]},
            "write_request": {"dataset": "dwd.orders", "schema_version": 1, "operation": "insert",
                              "records": [{"order_id": 1, "amount": "12.30"}]},
            "query_request": {"sql": "SELECT 1", "client_request_id": "original",
                              "max_rows": 1000, "timeout_ms": 30000},
            "key_request": {"name": "fixture", "scopes": ["query:read"], "expires_at": "2027-01-01T00:00:00Z"},
            "draft_request": {"definition": pipeline, "artifacts": [
                {"path": task["entrypoint"], "upload_id": f"upload_{i}", "sha256": HASH, "bytes": 128}
                for i, task in enumerate(pipeline["tasks"])
            ]},
            "upload_request": {"path": "sql/main.sql", "sha256": HASH, "bytes": 128},
            "validate_request": {"draft_revision": "r1"},
            "version_request": {"draft_revision": "r1"},
            "activate_request": {"pipeline_version": "v1"},
            "schedule_request": {"owner": "colvenna", "expression": "rate(1 minute)", "timezone": "UTC", "state": "ENABLED"},
            "run_request": {"pipeline_version": "v1", "inputs": {"clean_orders.events": {"release_id": "r1"}},
                            "parameters": {"high_value_threshold": "500.00"}, "mode": "snapshot"},
            "environment_request": {
                "environment_id": "pandas", "version": "v1", "python_version": "3.12",
                "platform": "linux_arm64", "build_profile": "binary_wheels",
                "dependency_source": {"format": "requirements", "content": "pandas==2.2.3"},
            },
            "empty_request": {},
            "artifact_inline_request": {"path": "sql/main.sql", "sha256": HASH, "content": "SELECT 1"},
            "trial_request": {"pipeline_version": "v1", "task_id": "clean_orders", "sample_rows": 100},
            "retention_request": {"state": "ENABLED", "policy": {"staging_seconds": 3600}},
            "hold_request": {"target": {"kind": "release", "id": "release_1", "dataset": "dwd.orders"},
                             "reason": "backup", "expires_at": "2027-01-01T00:00:00Z"},
            "collection_request": {"dry_run": True},
        }
        expected = {route[2] for route in ROUTES if route[2]}
        self.assertEqual(set(samples), expected)
        for name, sample in samples.items():
            with self.subTest(contract=name):
                validate(name, sample, definition=pipeline if name == "run_request" else None)
                self.assertReject(lambda: validate(name, {**sample, "role_arn": "arn:fixture:unauthorized"}))
        self.assertReject(lambda: validate("query_request", {**samples["query_request"], "consistency": "after_ingestion"}))
        self.assertReject(lambda: validate("query_request", {**samples["query_request"], "timeout_ms": 300001}))
        self.assertReject(lambda: validate("query_request", {**samples["query_request"], "sql": " "}))
        self.assertReject(lambda: validate("key_request", {**samples["key_request"], "scopes": ["superuser"]}))
        self.assertReject(lambda: validate("key_request", {**samples["key_request"], "expires_at": "2026-02-30T00:00:00Z"}))
        self.assertReject(lambda: validate("key_request", {**samples["key_request"], "expires_at": "2027-01-01T00:00:00"}))
        self.assertReject(lambda: validate("schedule_request", {**samples["schedule_request"], "expression": "rate(1 second)"}))
        self.assertReject(lambda: validate("schedule_request", {**samples["schedule_request"], "timezone": "unknown/zone"}))
        validate("schedule_request", {**samples["schedule_request"], "expression": "cron(0/5 0-23 ? 1,12 1-5 2026)"})
        for expression in ["cron(60 0 ? * 1 2026)", "cron(0 24 ? * 1 2026)",
                           "cron(0 0 ? * 8 2026)", "cron(0 0 * * * 2026)",
                           "cron(*/0 0 ? * 1 2026)", "cron(0 0 L * ? 2026)"]:
            self.assertReject(lambda: validate("schedule_request", {**samples["schedule_request"], "expression": expression}))
        key = {"key_id": "key1", "name": "n", "scopes": ["query:read"], "expires_at": STAMP, "status": "ACTIVE"}
        validate("key_list", {"keys": [key]})
        self.assertReject(lambda: validate("key_list", {"keys": [{**key, "token": "must_not_leak"}]}))

    def test_declared_wire_types_calendar_precision_and_integer_limits(self):
        accepted = [
            ("BIGINT", "9223372036854775807"), ("BIGINT", "-9223372036854775808"),
            ("BIGINT", 9007199254740991), ("DECIMAL(18,2)", "1234567890123456.78"),
            ("DECIMAL(38,2)", "123456789012345678901234567890123456.78"),
            ("BOOLEAN", False), ("DATE", "2024-02-29"), ("VARCHAR[]", ["a", "b"]),
            ("TIMESTAMP", "2026-10-15T00:00:00.000001"),
            ("TIMESTAMPTZ", "2026-10-15T08:00:00+08:00"),
            ("JSON", {"items": [1, None]}), ("BLOB", "3q0="),
        ]
        rejected = [
            ("BIGINT", True), ("BIGINT", 9007199254740992), ("BIGINT", "9223372036854775808"),
            ("BIGINT", "01"), ("BIGINT", "1١"), ("DECIMAL(18,2)", 12.3),
            ("DECIMAL(18,2)", "1e2"), ("DECIMAL(18,2)", "NaN"), ("DECIMAL(18,2)", "1.234"),
            ("DECIMAL(18,2)", "12345678901234567.00"), ("DECIMAL(39,2)", "1.00"),
            ("DECIMAL(2,3)", "0.001"), ("DATE", "2025-02-29"),
            ("TIMESTAMPTZ", "2026-10-15T08:00:00"), ("TIMESTAMP", "2026-10-15T00:00:00Z"),
            ("TIMESTAMPTZ", "2026-10-15T00:00:00.0000001Z"), ("BOOLEAN", 1),
            ("VARCHAR[]", ["a", 1]), ("BLOB", "not base64"),
        ]
        for kind, value in accepted:
            with self.subTest(kind=kind, accepted=value):
                validate_value(kind, value)
        for kind, value in rejected:
            with self.subTest(kind=kind, rejected=value):
                self.assertReject(lambda: validate_value(kind, value))
        self.assertEqual(validate_value("TIMESTAMPTZ", "2026-10-15T08:00:00+08:00"),
                         validate_value("TIMESTAMPTZ", "2026-10-15T00:00:00Z"))
        self.assertEqual(validate_value("TIMESTAMPTZ", STAMP).tzinfo, timezone.utc)
        columns = context()["output"]["schema"]
        validate_record(columns, {"order_id": 1, "amount": "12.30"}, key_columns=["order_id"])
        self.assertReject(lambda: validate_record(columns, {"order_id": 1, "amount": 12.3}))
        self.assertReject(lambda: validate_record(columns, {"order_id": None, "amount": "12.30"}))
        validate_record(columns, {"order_id": 1}, "delete", ["order_id"])

    def test_http_envelope_bearer_conditions_and_internal_header_refusal(self):
        request = {"method": "POST", "path": "/v1/spaces/space_1/queries",
                   "headers": {"Authorization": "Bearer "+("a"*43), "Content-Type": "application/json"},
                   "body": {"sql": "SELECT 1", "client_request_id": "original"}}
        validate("http_request", request)
        for headers in [
            {"Content-Type": "application/json"},
            {**request["headers"], "Authorization": "Basic "+("a"*43)},
            {**request["headers"], "authorization": "Bearer "+("b"*43)},
            {**request["headers"], "X-Colvenna-Scopes": "keys:manage"},
            {**request["headers"], "X-Trace": "a\r\nb"},
        ]:
            self.assertReject(lambda: validate("http_request", {**request, "headers": headers}))
        req = {**request, "method": "GET", "path": "/v1/spaces/space_1/queries/q1",
               "body": None, "query": {"page_token": "page1", "page_size": 100}}
        validate("http_request", req)
        export = {**req, "path": "/v1/spaces/space_1/queries/q1/export", "query": {"token": "original"}}
        validate("http_request", export)
        self.assertReject(lambda: validate("http_request", {**export, "query": {}}))
        self.assertReject(lambda: validate("http_request", {**req, "query": {"token": "wrong-purpose"}}))
        pipeline = load_document(ROOT / "examples/orders/pipeline.yaml")
        version = {**request, "path": "/v1/spaces/space_1/pipelines/orders/versions",
                   "body": {"draft_revision": "r1"}}
        self.assertReject(lambda: validate("http_request", version))
        version["headers"] = {**request["headers"], "Idempotency-Key": "fixture-1"}
        validate("http_request", version)
        activation = {**request, "path": "/v1/spaces/space_1/pipelines/orders/activate",
                      "body": {"pipeline_version": "v1"}}
        activation["headers"] = {**request["headers"], "If-Match": "*"}
        self.assertReject(lambda: validate("http_request", activation))
        activation["headers"]["If-Match"] = '"r1"'
        validate("http_request", activation)
        cancel = {**request, "path": "/v1/spaces/space_1/queries/q1/cancel"}
        del cancel["body"]
        validate("http_request", cancel)

    def test_query_consistency_conditions_are_exclusive_and_duplicate_receipts_keep_actual_state(self):
        base = {"sql": "SELECT 1", "client_request_id": "original"}
        for request in [
            base, {**base, "consistency": "current"},
            {**base, "consistency": "published", "release_id": "r1"},
            {**base, "consistency": "after_ingestion", "ingestion_id": "ing1"},
            {**base, "consistency": "after_write", "write_id": "write1"},
        ]:
            validate("query_request", request)
        for request in [
            {"sql": "SELECT 1"}, {**base, "consistency": "current", "release_id": "r1"},
            {**base, "consistency": "after_write", "write_id": "write1", "release_id": "r1"},
            {**base, "consistency": "after_ingestion", "ingestion_id": "ing1", "write_id": "write1"},
        ]:
            self.assertReject(lambda: validate("query_request", request))
        for status in ["QUEUED", "RUNNING", "CANCELLING", "SUCCEEDED", "FAILED", "CANCELLED", "UNKNOWN"]:
            validate("query_receipt", {"query_id": "q1", "status": status,
                                      "status_url": "/v1/spaces/space_1/queries/q1"})

    def test_dag_modes_executor_and_root_input_bindings(self):
        base = load_document(ROOT / "examples/orders/pipeline.yaml")
        validate("pipeline", base)
        required_param = copy.deepcopy(base)
        del required_param["parameters"]["high_value_threshold"]["default"]
        validate("pipeline", required_param)
        self.assertReject(lambda: validate("run_request", {"pipeline_version": "v1", "mode": "snapshot"},
                                          definition=required_param))
        for mutate in [
            lambda p: p["tasks"][0].update(depends_on=["dashboard"]),
            lambda p: p["tasks"][0].update(executor="lambda"),
            lambda p: p["tasks"][0].update(runtime_environment="unexpected"),
            lambda p: p["tasks"][1].update(executor="writer", resource_profile="writer_short"),
            lambda p: p["tasks"][1].update(timeout_seconds=901),
            lambda p: p["tasks"][0].update(entrypoint="../escape.sql"),
            lambda p: p["tasks"][0].update(task_id="ambiguous.id"),
            lambda p: p["tasks"][0]["output"].update(dataset="ods.orders"),
            lambda p: p["tasks"][1]["output"].update(write_mode="merge_keys"),
            lambda p: p["tasks"][0]["output"]["schema"][0].update(nullable=True),
        ]:
            changed = copy.deepcopy(base)
            mutate(changed)
            self.assertReject(lambda: validate("pipeline", changed))
        task = copy.deepcopy(base["tasks"][0])
        task["processing"] = {"mode": "incremental", "bootstrap": "full", "on_missing_state": "fail"}
        task["inputs"] = {"changes": {"dataset": "ods.orders", "selection": "changes"},
                          "key_state": {"state": "target_key_state", "selection": "affected_keys"}}
        task["output"]["write_mode"] = "merge_keys"
        task["output"]["change_metadata"] = [
            {"name": "_colvenna_version", "type": "BIGINT", "nullable": False},
            {"name": "_colvenna_sequence", "type": "BIGINT", "nullable": False},
        ]
        incremental = {**base, "tasks": [task]}
        validate("pipeline", incremental)
        incomplete_state = copy.deepcopy(incremental)
        incomplete_state["tasks"][0]["output"]["write_mode"] = "versioned_replace"
        self.assertReject(lambda: validate("pipeline", incomplete_state))
        validate_change(task["output"], {"_colvenna_op": "DELETE", "order_id": 1, "_colvenna_version": 2, "_colvenna_sequence": 42})
        self.assertReject(lambda: validate_change(task["output"], {"_colvenna_op": "UPSERT", "order_id": 1,
                                                                   "_colvenna_version": 2, "_colvenna_sequence": 42}))
        self.assertReject(lambda: validate_change(task["output"], {"_colvenna_op": "DELETE", "order_id": 1,
                                                                   "_colvenna_version": -1, "_colvenna_sequence": 42}))
        without_state = copy.deepcopy(incremental)
        del without_state["tasks"][0]["inputs"]["key_state"]
        self.assertReject(lambda: validate("pipeline", without_state))
        partition = copy.deepcopy(base)
        partition["tasks"] = [partition["tasks"][0]]
        task = partition["tasks"][0]
        task["processing"] = {"mode": "partition_recompute", "partition_columns": ["order_date"], "on_unbounded_impact": "fail"}
        task["inputs"]["events"]["selection"] = "affected_partitions"
        task["output"].update(write_mode="replace_partitions", partition_columns=["order_date"])
        validate("pipeline", partition)
        run = {"pipeline_version": "v1", "mode": "snapshot", "inputs": {"score_orders.orders": {"release_id": "r1"}}}
        self.assertReject(lambda: validate("run_request", run, definition=base))
        self.assertReject(lambda: validate("run_request", {"pipeline_version": "v1", "mode": "backfill"}, definition=base))
        run = {"pipeline_version": "v1", "mode": "backfill", "partitions": [{"order_date": "2026-10-15"}]}
        validate("run_request", run, definition=partition)

    def test_context_manifest_fixed_identity_paths_noop_and_empty_partition(self):
        ctx = context()
        result = manifest(ctx)
        validate("context", ctx)
        validate("output_manifest", result, context=ctx)
        for mutate in [
            lambda m: m.update(execution_id="stale_exec"),
            lambda m: m.update(row_count=3),
            lambda m: m["files"][0].update(path="../escape.parquet"),
            lambda m: m["files"].append(copy.deepcopy(m["files"][0])),
            lambda m: m["files"][0].update(sha256="fake"),
        ]:
            changed = copy.deepcopy(result)
            mutate(changed)
            self.assertReject(lambda: validate("output_manifest", changed, context=ctx))
        empty = {**result, "row_count": 0, "files": []}
        validate("output_manifest", empty, context=ctx)
        disallow = copy.deepcopy(ctx)
        disallow["output"]["allow_empty"] = False
        self.assertReject(lambda: validate("output_manifest", empty, context=disallow))
        cloud = copy.deepcopy(ctx)
        cloud["input_mode"] = "s3_files"
        self.assertReject(lambda: validate("context", cloud))
        del cloud["inputs"]["orders"]["files"][0]["local_path"]
        validate("context", cloud)
        inc = copy.deepcopy(ctx)
        output = inc["output"]
        metadata = [{"name": "_colvenna_version", "type": "BIGINT", "nullable": False},
                    {"name": "_colvenna_sequence", "type": "BIGINT", "nullable": False}]
        output.update(write_mode="merge_keys", allow_empty=False, target_schema=copy.deepcopy(output["schema"]))
        output["schema"] = wire_schema({"schema": output["target_schema"], "key_columns": ["order_id"],
                                        "change_metadata": metadata})
        inc["inputs"]["orders"]["selection"] = {"mode": "changes"}
        inc["processing"] = {"mode": "incremental", "from_watermark": {"ods_sequence": 40},
                             "to_watermark": {"ods_sequence": 42}, "state_release_id": PREVIOUS}
        validate("context", inc)
        noop = {**manifest(inc), "row_count": 0, "files": []}
        validate("output_manifest", noop, context=inc)  # zero delta is not target clearing.
        partition = copy.deepcopy(ctx)
        date_column = {"name": "order_date", "type": "DATE", "nullable": False}
        partition["output"]["schema"].append(date_column)
        parts = [{"order_date": "2026-10-14"}, {"order_date": "2026-10-15"}]
        partition["output"].update(write_mode="replace_partitions", partition_columns=["order_date"], replace_partitions=parts)
        partition["processing"] = {"mode": "partition_recompute", "affected_partitions": parts}
        partition["inputs"]["orders"]["selection"] = {"mode": "affected_partitions", "partitions": parts}
        validate("context", partition)
        empty_parts = {**manifest(partition), "row_count": 0, "files": [], "replace_partitions": parts}
        validate("output_manifest", empty_parts, context=partition)
        empty_parts["replace_partitions"] = parts[:1]
        self.assertReject(lambda: validate("output_manifest", empty_parts, context=partition))

    def test_parser_duplicate_fields_and_nonfinite_values(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "request.json"
            for text in ['{"a":1,"a":2}', '{"n":NaN}', '{"n":1e999}']:
                path.write_text(text)
                self.assertReject(lambda: load_document(path))
            path = Path(folder) / "pipeline.yaml"
            path.write_text("spec_version: 1\nspec_version: 2\n")
            self.assertReject(lambda: load_document(path))
            path.write_text("value: !!python/object/apply:os.system ['never execute']\n")
            self.assertReject(lambda: load_document(path))
            path.write_text("value: &a [*a]\n")
            self.assertReject(lambda: load_document(path))

    def test_state_reconciliation_cancellation_and_immutable_retries(self):
        validate_transition("task", "IMPORTING", "PUBLISHED")
        validate_transition("task", "RUNNING", "VALIDATING", {"executor": "writer"})
        self.assertReject(lambda: validate_transition("task", "RUNNING", "VALIDATING"))
        self.assertReject(lambda: validate_transition("task", "PUBLISHED", "CANCELLED"))
        self.assertReject(lambda: validate_transition("run", "UNKNOWN", "QUEUED"))
        validate_transition("run", "UNKNOWN", "PUBLISHED", {"reconciled": True, "authoritative_state": "PUBLISHED"})
        self.assertReject(lambda: validate_transition("task", "CANCELLING", "CANCELLED"))
        validate_transition("task", "CANCELLING", "CANCELLED", {"writer_eligibility_revoked": True})
        self.assertReject(lambda: validate_transition("task", "FAILED", "RETRY_WAIT"))
        validate_transition("task", "FAILED", "RETRY_WAIT", {"new_attempt": True, "same_immutable_bindings": True})
        self.assertReject(lambda: validate_transition("ingestion", "FAILED", "COMMITTED"))
        validate_transition("ingestion", "FAILED", "COMMITTED", {
            "new_attempt": True, "same_immutable_bindings": True,
            "reconciled": True, "authoritative_state": "COMMITTED"})
        self.assertReject(lambda: validate_transition("environment", "READY", "BUILDING"))
        self.assertReject(lambda: validate("run_status", {
            "run_id": "r1", "pipeline_version": "v1", "status": "PUBLISHED", "observed_at": STAMP,
            "projection_version": 1, "tasks": [{"task_id": "t1", "job_id": "j1", "status": "RUNNING"}],
        }))
        self.assertReject(lambda: validate("environment_metadata", {"environment_id": "e", "version": "v1", "status": "READY"}))
        self.assertReject(lambda: validate("query_status", {"query_id": "q1", "status": "SUCCEEDED"}))
        self.assertReject(lambda: validate("ingestion_status", {
            "ingestion_id": "i1", "status": "COMMITTED", "observed_at": STAMP, "projection_version": 1,
        }))

    def test_cli_exit_codes_and_explicit_scope(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "query.json"
            for value, expected in [({"sql": "SELECT 1", "client_request_id": "original"}, 0),
                                    ({"sql": "SELECT 1", "client_request_id": "original", "role": "override"}, 2)]:
                path.write_text(json.dumps(value))
                env = __import__("os").environ.copy()
                env["PYTHONPATH"] = str(ROOT / "src")
                result = subprocess.run([sys.executable, "-m", "colvenna_isle.contracts", "validate",
                                         "query_request", str(path)], env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, expected)
                if expected == 0:
                    self.assertEqual(json.loads(result.stdout)["validation_scope"], "OFFLINE_CONTRACT_ONLY")


if __name__ == "__main__":
    unittest.main()
