"""Real pandas subprocesses and Parquet over an exact-version object-store double."""

import copy
import hashlib
import io
import json
import os
import tempfile
import time
import unittest
import zipfile
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError, ReadTimeoutError

from colvenna_isle.contracts.validation import ContractError
from colvenna_isle.control.registry import ControlError, canonical
from colvenna_isle.pipeline.parquet import write
from colvenna_isle.pipeline.task_adapter import TaskAdapter, ExecutionFailure
from colvenna_isle.pipeline.task_descriptor import digest
from colvenna_isle.pipeline.task_objects import TaskObjects
from colvenna_isle.pipeline.environment_adapter import execute_task
from colvenna_isle.pipeline.versioned_parquet import input_batches
from colvenna_isle.pipeline.workspace import Workspace

ROOT = Path(__file__).resolve().parents[2]
PYTHON_PATH = os.pathsep.join(str(Path(path).resolve()) for path in
                            os.environ.get("PYTHONPATH", str(ROOT / "src")).split(os.pathsep))
COLUMNS = [{"name": "id", "type": "BIGINT", "nullable": False},
           {"name": "amount", "type": "DECIMAL(38,2)", "nullable": False}]
ROW = [9007199254740993, Decimal("999999999999999999999999999999999999.99")]
SCRIPT = '''import argparse, copy, hashlib, json
from pathlib import Path
from decimal import Decimal
import pandas as pd
import pyarrow.parquet as pq
from colvenna_isle.pipeline.parquet import write
p=argparse.ArgumentParser()
p.add_argument("--context")
p.add_argument("--output-dir")
a=p.parse_args()
context=json.loads(Path(a.context).read_text())
binding=context["inputs"]["source"]
if context["input_mode"] == "local_files":
    rows=pq.read_table(binding["files"][0]["local_path"]).to_pylist()
else:
    assert all("local_path" not in file for file in binding["files"])
    rows=[{"id": 9007199254740993, "amount": Decimal("999999999999999999999999999999999999.99")}]
frame=pd.DataFrame(rows)
assert frame["id"].dtype.name == "int64"
assert isinstance(frame.iloc[0]["amount"], Decimal)
output=Path(a.output_dir)
{before}
file=write(output/"part.parquet", context["output"]["schema"],
           [[[int(row.id), row.amount] for row in frame.itertuples()]],
           maximum_bytes=1048576, maximum_rows=10)
manifest={{"contract_version":2, **{{name:context[name] for name in ["job_id","attempt_id","execution_id"]}},
           "output_name":"result","format":"parquet","schema":context["output"]["schema"],
           "row_count":len(frame),"files":[{{**file,"path":"part.parquet","partition_values":{{}}}}]}}
{after}
(output/"output-manifest.json").write_text(json.dumps(manifest))
'''


class VersionStore:
    def __init__(self):
        self.objects, self.reads, self.writes = {}, [], []
        self.lose_staging = False
        self.lose_suffix = None
        self.corrupt_readback = False

    def seed(self, key, raw):
        values = self.objects.setdefault(key, [])
        version = "v_" + str(sum(len(value) for value in self.objects.values()) + 1)
        values.append((version, raw))
        return {"s3_uri": "s3://test-bucket/" + key, "object_version": version,
                "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}

    def get_object(self, **request):
        assert request["Bucket"] == "test-bucket"
        assert request["ExpectedBucketOwner"] == "123456789012"
        assert request["VersionId"] not in {"", "null"}  # Never latest, including readback.
        self.reads.append(copy.deepcopy(request))
        key, version = request["Key"], request["VersionId"]
        result = next((raw for identity, raw in self.objects.get(key, []) if identity == version), None)
        if result is None:
            raise ClientError({"Error": {"Code": "NoSuchVersion"}}, "GetObject")
        if self.corrupt_readback and "/staging/" in key:
            result += b"corrupt"
        return {"VersionId": version, "Body": io.BytesIO(result)}

    def put_object(self, **request):
        assert request["IfNoneMatch"] == "*"
        assert request["ExpectedBucketOwner"] == "123456789012"
        if request["Key"] in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.writes.append(copy.deepcopy(request))
        result = self.seed(request["Key"], request["Body"])
        if (self.lose_staging and "/staging/" in request["Key"]
                or self.lose_suffix and request["Key"].endswith(self.lose_suffix)):
            self.lose_staging = False
            self.lose_suffix = None
            raise ReadTimeoutError(endpoint_url="controlled-original-staging")
        return {"VersionId": result["object_version"]}


class PythonAdapter(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.store = VersionStore()
        self.environment = {"bucket": "test-bucket", "space_id": "owned", "generation": "gen_owned",
            "environment_id": "analysis", "version": "v1", "source_commit": "a" * 40,
            "adapter_sha256": "b" * 64, "dependency_lock_sha256": "c" * 64,
            "specification_sha256": "d" * 64, "definition_epoch": "e" * 64,
            "executor": "lambda", "profile": "python_lambda_standard"}
        self.task = {"task_id": "transform", "language": "python", "executor": "lambda",
            "resource_profile": "python_lambda_standard", "entrypoint": "transform.py",
            "runtime_environment": {"environment_id": "analysis", "version": "v1"},
            "input_mode": "local_files", "processing": {"mode": "full"},
            "timeout_seconds": 30, "max_attempts": 1,
            "inputs": {"source": {"dataset": "ods.source", "selection": "snapshot"}},
            "output": {"dataset": "dwd.result", "write_mode": "versioned_replace",
                       "schema": copy.deepcopy(COLUMNS), "key_columns": ["id"], "allow_empty": True}}
        self.adapter = TaskAdapter(self.store, self.environment, workspace_root=self.root / "executions",
                                   python_path=PYTHON_PATH)
        path = self.root / "source.parquet"
        file = write(path, COLUMNS, [[ROW]], maximum_bytes=1048576, maximum_rows=10)
        self.input = {**self.store.seed("spaces/owned/inputs/gen_owned/input_one/exec_one/part.parquet",
                                      path.read_bytes()), "rows": 1}
        self.descriptor = {"contract_version": 2, "space_id": "owned", "generation": "gen_owned",
            "pipeline_id": "demo", "pipeline_version": "v1", "definition_epoch": "e" * 64,
            "run_id": "run_one", "job_id": "job_one", "attempt_id": "attempt_one",
            "task": self.task, "environment": {
                **{name: self.environment[name] for name in ["source_commit", "adapter_sha256",
                    "specification_sha256", "dependency_lock_sha256"]},
                "reference": self.task["runtime_environment"],
                "executor_binding": {"executor": "lambda", "profile": "python_lambda_standard",
                    "lambda_mode": "standard",
                    "function_arn": "arn:aws:lambda:us-west-2:123456789012:function:owned:1"}},
            "parameters": {}, "processing": {"mode": "full"},
            "output_state": {"expected_previous_release": None, "replace_partitions": []},
            "inputs": {"source": {"dataset": "ods.source", "release_id": "input_one",
                "watermark": {"ods_sequence": 1}, "schema": copy.deepcopy(COLUMNS),
                "files": [self.input], "selection": {"mode": "snapshot"}}}}
        self.bind_manifest_hash()

    def tearDown(self):
        self.temporary.cleanup()

    def bind_manifest_hash(self):
        binding = self.descriptor["inputs"]["source"]
        binding["manifest_sha256"] = digest({"contract_version": 2,
            **{name: binding[name] for name in ["dataset", "release_id", "watermark", "schema", "files"]}})

    def event(self, *, before="pass", after="pass"):
        code = SCRIPT.replace("{before}", before).replace("{after}", after)
        # The script uses format-literal doubled braces to keep its own dictionaries.
        code = code.replace("{{", "{").replace("}}", "}").encode()
        definition = {"spec_version": 1, "pipeline_id": "demo", "name": "Synthetic",
            "schedule": {"owner": "none"}, "max_active_runs": 1, "coalesce_pending": True, "tasks": [self.task]}
        environment = self.descriptor["environment"]
        snapshot = {**{name: self.descriptor[name] for name in [
            "space_id", "generation", "pipeline_id", "pipeline_version", "definition_epoch"]},
            "definition": definition, "environments": {"transform": {
                "reference": environment["reference"],
                "specification_sha256": environment["specification_sha256"],
                "executor_binding": environment["executor_binding"],
                "ready": {"runtime": {name: environment[name] for name in ["source_commit", "adapter_sha256"]},
                          "dependency_lock": {"sha256": environment["dependency_lock_sha256"]}}}},
            "artifacts": [{"path": "transform.py", "sha256": hashlib.sha256(code).hexdigest(), "bytes": len(code)}]}
        self.descriptor["snapshot_sha256"] = digest(snapshot)
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            for name, raw in [("transform.py", code), (".colvenna/manifest.json", canonical(snapshot).encode()),
                              (".colvenna/definition.json", canonical(definition).encode())]:
                info = zipfile.ZipInfo(name)
                info.external_attr = 0o100644 << 16
                archive.writestr(info, raw)
        self.descriptor["package"] = self.store.seed(
            "spaces/owned/pipeline-code/gen_owned/demo/versions/v1/package.zip", buffer.getvalue())
        reference = self.store.seed(
            "spaces/owned/task-contexts/gen_owned/run_one/job_one/attempt_one/descriptor.json",
            canonical(self.descriptor).encode())
        return {"operation": "task_execute", "descriptor": reference}

    def read_completion(self, result):
        objects = TaskObjects(self.store, "test-bucket", "owned", "gen_owned")
        return json.loads(objects.read(result["completion"], "staging", maximum_bytes=1048576))

    def test_actual_pandas_subprocess_stages_typed_decimal_and_complete_versioned_manifest(self):
        result = self.adapter.execute(self.event())
        self.assertEqual((result["status"], result["publication_status"], result["row_count"]),
                         ("STAGED", "NOT_PUBLISHED", 1))
        self.assertEqual(result["workspace_cleanup"], "CONFIRMED")
        completion = self.read_completion(result)
        objects = TaskObjects(self.store, "test-bucket", "owned", "gen_owned")
        table = pq.read_table(pa.BufferReader(objects.read(
            {name: completion["files"][0][name] for name in ["s3_uri", "object_version", "sha256", "bytes"]},
            "staging", maximum_bytes=1048576)))
        self.assertEqual(table.to_pylist(), [{"id": ROW[0], "amount": ROW[1]}])
        manifest_raw = objects.read(completion["output_manifest"], "staging", maximum_bytes=1048576)
        self.assertEqual(json.loads(manifest_raw), completion["manifest"])
        self.assertEqual(hashlib.sha256(manifest_raw).hexdigest(), completion["manifest_sha256"])
        self.assertEqual(self.store.writes[-1]["Key"].rsplit("/", 1)[1], "completion.json")
        self.assertFalse(list((self.root / "executions").rglob("context.json")))

    def test_fixed_old_input_and_code_versions_survive_a_different_latest_version(self):
        event = self.event()
        code_key = self.descriptor["package"]["s3_uri"].removeprefix("s3://test-bucket/")
        self.store.seed(code_key, b"new unapproved code")
        self.store.seed(self.input["s3_uri"].removeprefix("s3://test-bucket/"), b"new wrong input")
        result = self.adapter.execute(event)
        self.assertEqual(result["row_count"], 1)
        self.assertIn(self.input["object_version"], [request["VersionId"] for request in self.store.reads])

    def test_s3_context_has_no_local_paths_and_adapter_verifies_fixed_objects(self):
        self.task["input_mode"] = "s3_files"
        result = self.adapter.execute(self.event())
        self.assertEqual(self.read_completion(result)["output"]["dataset"], "dwd.result")
        self.assertTrue(any(request["VersionId"] == self.input["object_version"] for request in self.store.reads))

    def test_two_invocations_of_one_attempt_have_distinct_execution_and_staging_custody(self):
        event = self.event()
        first, second = self.adapter.execute(event), self.adapter.execute(event)
        self.assertNotEqual(first["execution_id"], second["execution_id"])
        self.assertNotEqual(first["completion"]["s3_uri"], second["completion"]["s3_uri"])
        self.assertEqual(self.read_completion(first)["attempt_id"], self.read_completion(second)["attempt_id"])

    def test_script_editing_context_cannot_change_trusted_target_or_approve_publication(self):
        event = self.event(before='context["output"]["dataset"]="ads.forbidden"; '
                          'Path(a.context).write_text(json.dumps(context))')
        result = self.adapter.execute(event)
        completion = self.read_completion(result)
        self.assertEqual(completion["output"]["dataset"], "dwd.result")
        self.assertEqual(completion["publication_status"], "NOT_PUBLISHED")
        self.assertNotIn("directory", completion["output"])

    def test_wrong_hash_duplicate_keys_escape_and_partial_files_are_independently_rejected(self):
        cases = [
            ('manifest["files"][0]["sha256"]="0"*64', "PARQUET_FILE_HASH"),
            ('(output/"partial.tmp").write_bytes(b"partial")', "OUTPUT_FILES_INCOMPLETE"),
            ('manifest["files"][0]["path"]="../outside.parquet"', None),
            ('manifest["execution_id"]="exec_foreign"', None),
            ('manifest["schema"][0]["type"]="VARCHAR"', None),
        ]
        for after, code in cases:
            with self.subTest(after=after):
                event = self.event(after=after)
                with self.assertRaises((ControlError, ContractError)) as failure:
                    self.adapter.execute(event)
                if code:
                    self.assertEqual(failure.exception.code, code)
                self.assertFalse(self.store.writes)
        event = self.event(before="frame=pd.concat([frame,frame],ignore_index=True)")
        with self.assertRaisesRegex(ControlError, "OUTPUT_DUPLICATE_KEY"):
            self.adapter.execute(event)
        self.assertFalse(self.store.writes)

    def test_foreign_object_and_installed_environment_mismatch_reject_before_script(self):
        self.input["s3_uri"] = self.input["s3_uri"].replace("/owned/", "/foreign/")
        self.bind_manifest_hash()
        with self.assertRaisesRegex(ControlError, "TASK_OBJECT_SCOPE"):
            self.adapter.execute(self.event())
        self.assertFalse(self.store.writes)
        self.input["s3_uri"] = self.input["s3_uri"].replace("/foreign/", "/owned/")
        self.bind_manifest_hash()
        for field, value in [("generation", "gen_other"), ("source_commit", "f" * 40),
                             ("executor", "ecs_task"), ("definition_epoch", "f" * 64)]:
            with self.subTest(field=field):
                installed = {**self.environment, field: value}
                adapter = TaskAdapter(self.store, installed, workspace_root=self.root / "rejected",
                                      python_path=PYTHON_PATH)
                with self.assertRaises(ControlError):
                    adapter.execute(self.event())
        self.assertFalse((self.root / "rejected").exists())

    def test_lost_original_upload_reply_keeps_execution_and_key_unknown_without_completion(self):
        self.store.lose_staging = True
        with self.assertRaises(ExecutionFailure) as failure:
            self.adapter.execute(self.event())
        receipt = failure.exception.receipt
        self.assertEqual(receipt["status"], "UNKNOWN")
        self.assertEqual(failure.exception.code, "TASK_UPLOAD_ORIGINAL_UNKNOWN")
        self.assertIn(receipt["execution_id"], receipt["original_upload"]["s3_uri"])
        self.assertEqual(len(self.store.writes), 1)
        self.assertFalse(any(key.endswith("completion.json") for key in self.store.objects))
        self.assertNotIn("object_version", receipt["original_upload"])

    def test_corrupt_exact_upload_readback_keeps_known_original_version_unknown(self):
        self.store.corrupt_readback = True
        with self.assertRaises(ExecutionFailure) as failure:
            self.adapter.execute(self.event())
        self.assertEqual(failure.exception.receipt["status"], "UNKNOWN")
        self.assertIn("object_version", failure.exception.receipt["original_upload"])
        self.assertFalse(any(key.endswith("completion.json") for key in self.store.objects))

    def test_lost_completion_reply_can_leave_original_complete_object_and_still_returns_unknown(self):
        self.store.lose_suffix = "completion.json"
        with self.assertRaises(ExecutionFailure) as failure:
            self.adapter.execute(self.event())
        receipt = failure.exception.receipt
        self.assertEqual(receipt["status"], "UNKNOWN")
        key = receipt["original_upload"]["s3_uri"].removeprefix("s3://test-bucket/")
        version, raw = self.store.objects[key][0]
        self.assertEqual(hashlib.sha256(raw).hexdigest(), receipt["original_upload"]["sha256"])
        self.assertEqual(json.loads(raw)["execution_id"], receipt["execution_id"])
        self.assertEqual(len(self.store.writes), 3)  # File, original script manifest, completion; no replay.

    def test_cleanup_failure_does_not_hide_confirmed_staging_or_original_upload_unknown(self):
        with patch.object(Workspace, "cleanup", side_effect=OSError("synthetic-cleanup-failure")):
            result = self.adapter.execute(self.event())
        self.assertEqual((result["status"], result["workspace_cleanup"]), ("STAGED", "UNKNOWN"))
        self.assertEqual(self.read_completion(result)["execution_id"], result["execution_id"])
        self.store.lose_staging = True
        with patch.object(Workspace, "cleanup", side_effect=OSError("synthetic-cleanup-failure")):
            with self.assertRaises(ExecutionFailure) as failure:
                self.adapter.execute(self.event())
        self.assertEqual(failure.exception.receipt["status"], "UNKNOWN")
        self.assertEqual(failure.exception.receipt["workspace_cleanup"], "UNKNOWN")
        self.assertIn("original_upload", failure.exception.receipt)

    def test_approved_s3_reader_uses_bound_old_version_and_preserves_decimal(self):
        self.store.seed(self.input["s3_uri"].removeprefix("s3://test-bucket/"), b"wrong latest")
        with patch("boto3.client", return_value=self.store), patch(
                "colvenna_isle.pipeline.versioned_parquet.Path.read_text",
                return_value=json.dumps(self.environment)):
            batches = list(input_batches(self.input))
        self.assertEqual(batches[0].to_pylist(), [{"id": ROW[0], "amount": ROW[1]}])
        self.assertEqual(self.store.reads[-1]["VersionId"], self.input["object_version"])

    def test_public_entrypoint_preserves_unknown_original_and_never_reports_success(self):
        self.store.lose_staging = True
        with patch("colvenna_isle.pipeline.environment_adapter.installed_environment",
                   return_value=self.environment), patch("boto3.client", return_value=self.store), patch(
                "colvenna_isle.pipeline.task_adapter.TaskAdapter", return_value=self.adapter):
            result = execute_task(self.event(), "lambda", remaining_seconds=60)
        self.assertEqual((result["status"], result["code"]), ("UNKNOWN", "TASK_UPLOAD_ORIGINAL_UNKNOWN"))
        self.assertEqual(result["publication_status"], "NOT_PUBLISHED")
        self.assertIn(result["execution_id"], result["staging_prefix"])
        self.assertEqual(len(self.store.writes), 1)

    def test_replaced_input_hash_and_unconfirmed_version_reject_before_output(self):
        for change in [{"sha256": "0" * 64}, {"object_version": "unconfirmed-version"}]:
            original = copy.deepcopy(self.input)
            self.input.update(change)
            self.bind_manifest_hash()
            with self.subTest(change=change), self.assertRaisesRegex(ControlError, "TASK_OBJECT_UNCONFIRMED"):
                self.adapter.execute(self.event())
            self.assertFalse(self.store.writes)
            self.input.clear()
            self.input.update(original)
            self.bind_manifest_hash()

    def test_preflight_budget_and_installed_lambda_deadline_reject_without_script(self):
        self.input["bytes"] = 33554433
        self.bind_manifest_hash()
        with self.assertRaisesRegex(ControlError, "TASK_INPUT_WORKSPACE_BUDGET"):
            self.adapter.execute(self.event())
        self.assertFalse(self.store.writes)
        self.input["bytes"] = self.root.joinpath("source.parquet").stat().st_size
        self.bind_manifest_hash()
        with self.assertRaisesRegex(ControlError, "TASK_DISPATCH_DEADLINE_EXHAUSTED"):
            self.adapter.execute(self.event(), remaining_seconds=9)
        self.assertFalse(self.store.writes)

    def test_descriptor_io_consumes_original_task_deadline(self):
        self.task["timeout_seconds"] = 1
        event = self.event()
        original = self.store.get_object

        def delayed_original(**request):
            time.sleep(1.05)
            return original(**request)

        with patch.object(self.store, "get_object", side_effect=delayed_original):
            with self.assertRaisesRegex(ControlError, "TASK_DISPATCH_DEADLINE_EXHAUSTED"):
                self.adapter.execute(event)
        self.assertFalse((self.root / "executions").exists())
        self.assertFalse(self.store.writes)


if __name__ == "__main__":
    unittest.main()
