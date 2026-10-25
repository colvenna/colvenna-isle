"""Controlled SQL in real subprocesses over the exact-version object-store double."""

import copy
import hashlib
import io
import json
import tempfile
import unittest
import zipfile
from datetime import date
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from colvenna_isle.control.registry import ControlError, canonical
from colvenna_isle.pipeline.parquet import write
from colvenna_isle.pipeline.task_adapter import TaskAdapter
from colvenna_isle.pipeline.task_descriptor import digest
from colvenna_isle.pipeline.task_objects import TaskObjects
from colvenna_isle.writer.ingestion import wire
from test_task_adapter import PYTHON_PATH, VersionStore

EVENTS = [{"name": "order_id", "type": "BIGINT", "nullable": False},
          {"name": "order_date", "type": "DATE", "nullable": False},
          {"name": "amount", "type": "DECIMAL(18,2)", "nullable": True},
          {"name": "source_version", "type": "BIGINT", "nullable": False},
          {"name": "_colvenna_operation", "type": "VARCHAR", "nullable": False},
          {"name": "_colvenna_commit_sequence", "type": "BIGINT", "nullable": False}]
DAILY = [{"name": "order_date", "type": "DATE", "nullable": False},
         {"name": "revenue", "type": "DECIMAL(38,2)", "nullable": False},
         {"name": "order_count", "type": "BIGINT", "nullable": False}]
ROWS = [[1, date(2026, 10, 1), Decimal("10.10"), 1, "upsert", 1],
        [2, date(2026, 10, 1), Decimal("999999999999999.99"), 1, "upsert", 2],
        [3, date(2026, 10, 2), Decimal("5.00"), 1, "upsert", 3],
        [1, date(2026, 10, 1), Decimal("11.10"), 2, "upsert", 4],
        [3, date(2026, 10, 2), None, 2, "delete", 5]]
CLEAN = """
WITH ranked AS (
  SELECT *, row_number() OVER (PARTITION BY order_id
                               ORDER BY source_version DESC, _colvenna_commit_sequence DESC) AS rank
  FROM events)
SELECT order_date, CAST(sum(amount) AS DECIMAL(38,2)) AS revenue, CAST(count(*) AS BIGINT) AS order_count
FROM ranked WHERE rank = 1 AND _colvenna_operation <> 'delete' GROUP BY order_date
"""


class SqlTasks(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.store = VersionStore()
        path = self.root / "events.parquet"
        file = write(path, EVENTS, [ROWS], maximum_bytes=1048576, maximum_rows=10)
        self.input = {**self.store.seed("spaces/owned/inputs/gen_owned/input_one/exec_one/events/part-00000.parquet",
                                      path.read_bytes()), "rows": file["rows"]}

    def tearDown(self):
        self.temporary.cleanup()

    def task(self, executor, *, mode="full", write_mode="versioned_replace", partitions=None):
        profile = {"writer": "writer_short", "lambda": "sql_lambda_standard", "ecs_task": "sql_ecs_medium"}[executor]
        task = {"task_id": "daily", "language": "sql", "executor": executor, "resource_profile": profile,
                "entrypoint": "sql/daily.sql", "processing": {"mode": mode}, "timeout_seconds": 20,
                "max_attempts": 1, "inputs": {"events": {"dataset": "ods.orders", "selection": "snapshot"}},
                "output": {"dataset": "dws.daily", "write_mode": write_mode, "schema": copy.deepcopy(DAILY),
                           "key_columns": ["order_date"], "allow_empty": True}}
        if executor != "writer":
            task.update(runtime_environment={"environment_id": "sql", "version": "v1"}, input_mode="local_files")
        if partitions is not None:
            task["processing"] = {"mode": "partition_recompute", "partition_columns": ["order_date"],
                                  "on_unbounded_impact": "full"}
            task["output"]["write_mode"] = "replace_partitions"
            task["inputs"]["events"]["selection"] = "affected_partitions"
            task["output"]["partition_columns"] = ["order_date"]
        return task

    def adapter(self, executor):
        if executor == "writer":
            environment = {"bucket": "test-bucket", "space_id": "owned", "generation": "gen_owned",
                           "definition_epoch": "e" * 64, "executor": "writer", "source_commit": "f" * 40}
        else:
            environment = {"bucket": "test-bucket", "space_id": "owned", "generation": "gen_owned",
                "environment_id": "sql", "version": "v1", "source_commit": "a" * 40, "adapter_sha256": "b" * 64,
                "dependency_lock_sha256": "c" * 64, "specification_sha256": "d" * 64,
                "definition_epoch": "e" * 64, "executor": executor,
                "profiles": ["python_lambda_standard", "sql_lambda_standard"] if executor == "lambda"
                else ["python_ecs_medium", "sql_ecs_medium"]}
        self.environment = environment
        return TaskAdapter(self.store, environment, workspace_root=self.root / ("executions-" + executor),
                           python_path=PYTHON_PATH)

    def event(self, executor, sql, *, partitions=None, **task_options):
        task = self.task(executor, partitions=partitions, **task_options)
        code = sql.encode()
        if executor == "writer":
            environment = {"executor_binding": {"executor": "writer", "profile": "writer_short",
                                                "source_commit": "f" * 40}}
            environments, executors = {}, {"daily": environment["executor_binding"]}
        else:
            binding = ({"executor": "lambda", "profile": task["resource_profile"], "lambda_mode": "standard",
                        "function_arn": "arn:aws:lambda:us-west-2:123456789012:function:owned:1"}
                       if executor == "lambda" else
                       {"executor": "ecs_task", "profile": task["resource_profile"],
                        "task_definition_arn": "arn:aws:ecs:us-west-2:123456789012:task-definition/owned:1",
                        "image": "123456789012.dkr.ecr.us-west-2.amazonaws.com/env@sha256:" + "1" * 64})
            environment = {"source_commit": "a" * 40, "adapter_sha256": "b" * 64,
                           "specification_sha256": "d" * 64, "dependency_lock_sha256": "c" * 64,
                           "reference": task["runtime_environment"], "executor_binding": binding}
            environments = {"daily": {"reference": environment["reference"],
                "specification_sha256": "d" * 64, "executor_binding": binding,
                "ready": {"runtime": {"source_commit": "a" * 40, "adapter_sha256": "b" * 64},
                          "dependency_lock": {"sha256": "c" * 64}}}}
            executors = {"daily": binding}
        selection = {"mode": "snapshot"}
        processing = {"mode": "full"}
        replace = []
        if partitions is not None:
            selection = {"mode": "affected_partitions", "partitions": partitions}
            processing = {"mode": "partition_recompute", "affected_partitions": partitions}
            replace = partitions
        descriptor = {"contract_version": 2, "space_id": "owned", "generation": "gen_owned",
            "pipeline_id": "demo", "pipeline_version": "v1", "definition_epoch": "e" * 64,
            "run_id": "run_one", "job_id": "job_" + executor, "attempt_id": "attempt_one",
            "task": task, "environment": environment, "parameters": {}, "processing": processing,
            "output_state": {"expected_previous_release": None, "replace_partitions": replace},
            "inputs": {"events": {"dataset": "ods.orders", "release_id": "input_one",
                "watermark": {"ods_sequence": 5}, "schema": copy.deepcopy(EVENTS),
                "files": [copy.deepcopy(self.input)], "selection": selection}}}
        binding = descriptor["inputs"]["events"]
        binding["manifest_sha256"] = digest({"contract_version": 2,
            **{name: binding[name] for name in ["dataset", "release_id", "watermark", "schema", "files"]}})
        definition = {"spec_version": 1, "pipeline_id": "demo", "name": "SQL", "schedule": {"owner": "none"},
                      "max_active_runs": 1, "coalesce_pending": True, "tasks": [task]}
        snapshot = {**{name: descriptor[name] for name in [
            "space_id", "generation", "pipeline_id", "pipeline_version", "definition_epoch"]},
            "definition": definition, "environments": environments, "executors": executors,
            "artifacts": [{"path": "sql/daily.sql", "sha256": hashlib.sha256(code).hexdigest(), "bytes": len(code)}]}
        descriptor["snapshot_sha256"] = digest(snapshot)
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            for name, raw in [("sql/daily.sql", code), (".colvenna/manifest.json", canonical(snapshot).encode()),
                              (".colvenna/definition.json", canonical(definition).encode())]:
                info = zipfile.ZipInfo(name)
                info.external_attr = 0o100644 << 16
                archive.writestr(info, raw)
        descriptor["package"] = self.store.seed(
            "spaces/owned/pipeline-code/gen_owned/demo/versions/v1/package.zip", buffer.getvalue())
        reference = self.store.seed(
            f"spaces/owned/task-contexts/gen_owned/run_one/{descriptor['job_id']}/attempt_one/descriptor.json",
            canonical(descriptor).encode())
        return {"operation": "task_execute", "descriptor": reference}

    def rows(self, result):
        objects = TaskObjects(self.store, "test-bucket", "owned", "gen_owned")
        completion = json.loads(objects.read(result["completion"], "staging", maximum_bytes=1048576))
        values = []
        for file in completion["files"]:
            raw = objects.read({name: file[name] for name in ["s3_uri", "object_version", "sha256", "bytes"]},
                               "staging", maximum_bytes=1048576)
            values += pq.read_table(pa.BufferReader(raw)).to_pylist()
        return completion, values

    def test_writer_and_independent_executors_stage_identical_typed_results(self):
        results = {}
        for executor in ["writer", "lambda", "ecs_task"]:
            result = self.adapter(executor).execute(self.event(executor, CLEAN),
                                                    invocation={"executor": executor, "request_id": "r1"})
            self.assertEqual((result["status"], result["row_count"]), ("STAGED", 1))
            completion, values = self.rows(result)
            self.assertEqual(completion["invocation"]["executor"], executor)
            self.assertEqual(completion["manifest"]["schema"], DAILY)
            results[executor] = sorted(values, key=lambda row: row["order_date"])
        self.assertEqual(results["writer"], [
            {"order_date": date(2026, 10, 1), "revenue": Decimal("1000000000000011.09"), "order_count": 2}])
        self.assertEqual(results["writer"], results["lambda"])
        self.assertEqual(results["lambda"], results["ecs_task"])

    def test_non_select_external_access_and_schema_mismatch_are_rejected(self):
        cases = [
            ("INSERT INTO events SELECT * FROM events", "SQL_READ_ONLY_REQUIRED"),
            ("SELECT 1; SELECT 2", "SQL_READ_ONLY_REQUIRED"),
            ("ATTACH '/tmp/other.duckdb' AS other", "SQL_READ_ONLY_REQUIRED"),
            ("COPY events TO '/tmp/leak.csv'", "SQL_READ_ONLY_REQUIRED"),
            ("SELECT * FROM read_csv('/etc/passwd')", "SQL_EXTERNAL_ACCESS_FORBIDDEN"),
            ("SELECT order_date, CAST(sum(amount) AS DOUBLE) AS revenue, count(*) AS order_count "
             "FROM events GROUP BY order_date", "SQL_SCHEMA_MISMATCH"),
            ("SELECT order_date AS day, CAST(sum(amount) AS DECIMAL(38,2)) AS revenue, "
             "CAST(count(*) AS BIGINT) AS order_count FROM events GROUP BY 1", "SQL_SCHEMA_MISMATCH"),
            ("SELECT * FROM missing_table", "SQL_INVALID"),
        ]
        for sql, code in cases:
            with self.subTest(sql=sql), self.assertRaisesRegex(ControlError, code):
                self.adapter("writer").execute(self.event("writer", sql))
        self.assertFalse(Path("/tmp/leak.csv").exists())

    def test_timeout_budget_stops_the_original_process(self):
        sql = ("SELECT DATE '2026-10-01' AS order_date, CAST(sum(a.range) AS DECIMAL(38,2)) AS revenue, "
               "CAST(count(*) AS BIGINT) AS order_count FROM range(100000000) a, range(1000000) b")
        event = self.event("writer", sql)
        descriptor = json.loads(self.store.objects[event["descriptor"]["s3_uri"].removeprefix("s3://test-bucket/")][0][1])
        self.assertEqual(descriptor["task"]["timeout_seconds"], 20)
        with self.assertRaisesRegex(ControlError, "TASK_EXECUTION_TIMEOUT|SQL_MEMORY_BUDGET"):
            self.adapter("writer").execute(event, remaining_seconds=23)

    def test_partition_replacement_declares_empty_partitions_and_rejects_outside_rows(self):
        partitions = [{"order_date": "2026-10-01"}, {"order_date": "2026-10-02"}]
        result = self.adapter("lambda").execute(self.event("lambda", CLEAN, partitions=partitions))
        completion, values = self.rows(result)
        manifest = completion["manifest"]
        self.assertEqual(manifest["replace_partitions"], partitions)
        # 2026-10-02 became empty after the delete; it is still replaced.
        self.assertEqual([file["partition_values"] for file in manifest["files"]], [{"order_date": "2026-10-01"}])
        self.assertEqual(len(values), 1)
        with self.assertRaisesRegex(ControlError, "SQL_PARTITION_SCOPE"):
            self.adapter("lambda").execute(self.event("lambda", CLEAN, partitions=[{"order_date": "2026-10-02"}]))

    def test_trailing_semicolon_and_comment_keep_the_original_statement(self):
        result = self.adapter("writer").execute(self.event("writer", CLEAN + ";  -- end of daily\n"))
        self.assertEqual(result["row_count"], 1)

    def test_partition_ordering_keeps_a_trailing_semicolon_and_comment(self):
        partitions = [{"order_date": "2026-10-01"}, {"order_date": "2026-10-02"}]
        result = self.adapter("lambda").execute(
            self.event("lambda", CLEAN + ";  -- end of daily\n", partitions=partitions))
        completion, values = self.rows(result)
        self.assertEqual([file["partition_values"] for file in completion["manifest"]["files"]],
                         [{"order_date": "2026-10-01"}])
        self.assertEqual(values, [
            {"order_date": date(2026, 10, 1), "revenue": Decimal("1000000000000011.09"), "order_count": 2}])

    def test_empty_full_result_has_no_file_and_counts_zero_rows(self):
        sql = CLEAN.replace("rank = 1", "rank = 99")
        result = self.adapter("writer").execute(self.event("writer", sql))
        completion, values = self.rows(result)
        self.assertEqual((completion["manifest"]["row_count"], completion["manifest"]["files"], values), (0, [], []))

    def test_writer_environment_requires_the_exact_writer_source(self):
        event = self.event("writer", CLEAN)
        adapter = self.adapter("writer")
        adapter.environment["source_commit"] = "0" * 40
        with self.assertRaisesRegex(ControlError, "TASK_INSTALLED_ENVIRONMENT_MISMATCH"):
            adapter.execute(event)


if __name__ == "__main__":
    unittest.main()
