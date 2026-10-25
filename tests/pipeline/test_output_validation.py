"""Real multipart Parquet outputs, independent of script success or metadata."""

import copy
import hashlib
import json
import tempfile
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from colvenna_isle.contracts.validation import ContractError
from colvenna_isle.control.registry import ControlError
from colvenna_isle.pipeline.output_validation import OutputValidator
from colvenna_isle.pipeline.parquet import write


COLUMNS = [{"name": "id", "type": "BIGINT", "nullable": False},
           {"name": "amount", "type": "DECIMAL(38,2)", "nullable": False},
           {"name": "day", "type": "DATE", "nullable": False}]
ROW = [9007199254740993, Decimal("999999999999999999999999999999999999.99"), date(2026, 10, 16)]


class OutputChecks(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.workspace = Path(self.temporary.name) / "job_one" / "attempt_one" / "exec_one"
        self.directory = self.workspace / "output" / "result"
        self.directory.mkdir(parents=True)
        self.context = {"contract_version": 2, "space_id": "space_one", "pipeline_id": "demo",
                        "pipeline_version": "v1", "run_id": "run_one", "task_id": "transform",
                        "job_id": "job_one", "attempt_id": "attempt_one", "execution_id": "exec_one",
                        "input_mode": "local_files", "parameters": {},
                        "inputs": {"source": {"dataset": "ods.source", "release_id": "input_one",
                            "manifest_sha256": "a" * 64, "watermark": {"ods_sequence": 1},
                            "selection": {"mode": "snapshot"}, "schema": copy.deepcopy(COLUMNS), "files": []}},
                        "output": {"name": "result", "dataset": "dwd.result",
                            "directory": str(self.directory), "write_mode": "versioned_replace",
                            "expected_previous_release": None, "key_columns": ["id"],
                            "allow_empty": True, "replace_partitions": [], "schema": copy.deepcopy(COLUMNS)}}
        self.manifest = {"contract_version": 2, "job_id": "job_one", "attempt_id": "attempt_one",
                         "execution_id": "exec_one", "output_name": "result", "format": "parquet",
                         "schema": copy.deepcopy(COLUMNS), "row_count": 0, "files": []}
        self.validator = OutputValidator(maximum_bytes=1048576, maximum_rows=1000)

    def tearDown(self):
        self.temporary.cleanup()

    def part(self, name, rows, partition=None):
        value = write(self.directory / name, self.context["output"]["schema"], [rows],
                      maximum_bytes=1048576, maximum_rows=1000)
        self.manifest["files"].append({**value, "path": name, "partition_values": partition or {}})
        self.manifest["row_count"] += len(rows)
        return self.manifest["files"][-1]

    def save(self):
        (self.directory / "output-manifest.json").write_text(json.dumps(self.manifest))

    def verify(self):
        self.save()
        return self.validator.verify(self.context, workspace=self.workspace)

    def test_complete_typed_multipart_outputs_preserve_large_integer_and_decimal(self):
        self.part("part-a.parquet", [ROW])
        self.part("part-b.parquet", [[9007199254740995, Decimal("500.00"), date(2026, 10, 16)]])
        result = self.verify()
        self.assertEqual(result["row_count"], 2)
        self.assertEqual(pq.read_table(self.directory / "part-a.parquet")["amount"][0].as_py(), ROW[1])
        self.assertEqual(result["manifest_sha256"],
                         hashlib.sha256((self.directory / "output-manifest.json").read_bytes()).hexdigest())

    def test_same_business_key_in_different_files_is_rejected(self):
        self.part("part-a.parquet", [ROW])
        self.part("part-b.parquet", [ROW])
        with self.assertRaisesRegex(ControlError, "OUTPUT_DUPLICATE_KEY"):
            self.verify()

    def test_hash_actual_type_nullability_and_row_count_are_independent(self):
        for change, expected in [
            (lambda: self.manifest["files"][0].update(sha256="0" * 64), "PARQUET_FILE_HASH"),
            (lambda: self.manifest.update(row_count=2), "Manifest total rows mismatch"),
            (lambda: self.context["output"]["schema"][0].update(type="VARCHAR"),
             "Result wire schema differs"),
        ]:
            with self.subTest(expected=expected):
                self.part("part-a.parquet", [ROW])
                change()
                with self.assertRaises((ControlError, ContractError)):
                    self.verify()
                (self.directory / "part-a.parquet").unlink()
                self.manifest["files"] = []
                self.manifest["row_count"] = 0
                self.context["output"]["schema"] = copy.deepcopy(COLUMNS)

    def test_script_claimed_schema_does_not_hide_wrong_physical_types(self):
        self.part("part-a.parquet", [ROW])
        path = self.directory / "part-a.parquet"
        table = pa.table({"id": [1.0], "amount": [ROW[1]], "day": [ROW[2]]})
        pq.write_table(table, path)
        self.manifest["files"][0].update(bytes=path.stat().st_size,
                                         sha256=hashlib.sha256(path.read_bytes()).hexdigest())
        with self.assertRaisesRegex(ControlError, "PARQUET_COLUMN_TYPE"):
            self.verify()

    def test_nonnullable_data_is_checked_even_if_parquet_marks_column_optional(self):
        self.part("part-a.parquet", [ROW])
        path = self.directory / "part-a.parquet"
        table = pq.read_table(path)
        table = table.set_column(0, pa.field("id", pa.int64(), nullable=True), pa.array([None], type=pa.int64()))
        pq.write_table(table, path)
        self.manifest["files"][0].update(bytes=path.stat().st_size,
                                         sha256=hashlib.sha256(path.read_bytes()).hexdigest())
        with self.assertRaisesRegex(ControlError, "PARQUET_NULLABILITY"):
            self.verify()

    def test_missing_manifest_or_missing_part_is_incomplete(self):
        with self.assertRaisesRegex(ControlError, "OUTPUT_MANIFEST_INCOMPLETE"):
            self.validator.verify(self.context, workspace=self.workspace)
        self.part("part-a.parquet", [ROW])
        (self.directory / "part-a.parquet").unlink()
        with self.assertRaisesRegex(ControlError, "OUTPUT_FILES_INCOMPLETE"):
            self.verify()

    def test_undeclared_partial_file_and_symlink_cannot_become_complete_outputs(self):
        self.part("part-a.parquet", [ROW])
        extra = self.directory / "unfinished.parquet.tmp"
        extra.write_bytes(b"partial")
        with self.assertRaisesRegex(ControlError, "OUTPUT_FILES_INCOMPLETE"):
            self.verify()
        extra.unlink()
        extra.symlink_to(self.directory / "part-a.parquet")
        with self.assertRaisesRegex(ControlError, "OUTPUT_FILE_SCOPE"):
            self.verify()

    def test_manifest_escape_and_identity_changes_are_rejected(self):
        self.part("part-a.parquet", [ROW])
        self.manifest["files"][0]["path"] = "../part-a.parquet"
        with self.assertRaises(ContractError):
            self.verify()
        self.manifest["files"][0]["path"] = "part-a.parquet"
        self.manifest["execution_id"] = "other_execution"
        with self.assertRaisesRegex(ContractError, "Result identity differs"):
            self.verify()

    def test_empty_replacement_requires_explicit_allow_empty(self):
        self.assertEqual(self.verify()["row_count"], 0)
        self.context["output"]["allow_empty"] = False
        with self.assertRaisesRegex(ContractError, "Empty replacement is forbidden"):
            self.verify()

    def test_partition_metadata_and_actual_column_values_must_agree(self):
        output = self.context["output"]
        output.update(write_mode="replace_partitions", partition_columns=["day"],
                      replace_partitions=[{"day": "2026-10-16"}, {"day": "2026-10-17"}])
        self.context["processing"] = {"mode": "partition_recompute",
                                     "affected_partitions": output["replace_partitions"]}
        self.context["inputs"]["source"]["selection"] = {"mode": "affected_partitions"}
        self.manifest["replace_partitions"] = copy.deepcopy(output["replace_partitions"])
        self.part("part-a.parquet", [ROW], {"day": "2026-10-16"})
        self.assertEqual(self.verify()["row_count"], 1)  # Empty second partition remains explicit.
        self.manifest["files"][0]["partition_values"] = {"day": "2026-10-17"}
        with self.assertRaisesRegex(ControlError, "OUTPUT_PARTITION_VALUE"):
            self.verify()

    def test_output_byte_row_and_file_budgets_reject_before_success(self):
        self.part("part-a.parquet", [ROW])
        self.part("part-b.parquet", [[ROW[0] + 1, ROW[1], ROW[2]]])
        self.save()
        for options in [{"maximum_bytes": 1, "maximum_rows": 100},
                        {"maximum_bytes": 1048576, "maximum_rows": 1},
                        {"maximum_bytes": 1048576, "maximum_rows": 100, "maximum_files": 1}]:
            with self.subTest(options=options), self.assertRaisesRegex(ControlError, "OUTPUT_BUDGET_EXCEEDED"):
                OutputValidator(**options).verify(self.context, workspace=self.workspace)


if __name__ == "__main__":
    unittest.main()
