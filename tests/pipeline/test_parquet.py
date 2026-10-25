"""Real PyArrow files; no native transaction, custody or AWS acceptance claim."""

import hashlib
import tempfile
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from colvenna_isle.control.registry import ControlError
from colvenna_isle.pipeline.parquet import describe, write


COLUMNS = [{"name": name, "type": kind, "nullable": False} for name, kind in [
    ("id", "BIGINT"), ("amount", "DECIMAL(30,2)"), ("day", "DATE"),
    ("observed", "TIMESTAMPTZ"), ("local", "TIMESTAMP"), ("flag", "BOOLEAN"),
    ("tags", "VARCHAR[]"), ("binary", "BLOB"), ("attributes", "JSON")]]
ROW = [9007199254740993, Decimal("900719925474099.01"), date(2026, 10, 16),
    datetime(2026, 10, 16, tzinfo=timezone.utc), datetime(2026, 10, 16), True,
    ["alpha", "beta"], b"\x00\xff", '{"n":1}']


class TypedFiles(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "part-00000.parquet"

    def tearDown(self):
        self.temporary.cleanup()

    def write(self, batches=None, **kwargs):
        return write(self.path, COLUMNS, batches if batches is not None else [[ROW]],
            maximum_bytes=kwargs.get("maximum_bytes", 1048576),
            maximum_rows=kwargs.get("maximum_rows", 100), guard=kwargs.get("guard", lambda: None))

    def test_real_files_preserve_all_types_precision_and_content_hash(self):
        metadata = self.write()
        self.assertEqual(metadata["rows"], 1)
        self.assertEqual(metadata["sha256"], hashlib.sha256(self.path.read_bytes()).hexdigest())
        actual = pq.read_table(self.path).to_pydict()
        self.assertEqual([actual[column["name"]][0] for column in COLUMNS], ROW)
        self.assertEqual(describe(self.path, COLUMNS, maximum_bytes=1048576, expected=metadata), metadata)

    def test_empty_file_has_actual_schema_and_zero_rows(self):
        metadata = self.write([])
        self.assertEqual(metadata["rows"], 0)
        self.assertEqual(pq.read_table(self.path).schema.field("amount").type, pa.decimal128(30, 2))

    def test_native_values_never_coerce_float_bool_decimal_or_timezone(self):
        for index, value in [(0, float(9007199254740993)), (0, True), (1, 1.23),
                (1, Decimal("1.001")), (1, Decimal("NaN")), (3, datetime(2026, 10, 16)),
                (4, datetime(2026, 10, 16, tzinfo=timezone.utc)), (6, ["a", 2]),
                (7, "AA=="), (8, '{"n":NaN}'), (0, None)]:
            with self.subTest(index=index, value=value):
                row = list(ROW)
                row[index] = value
                with self.assertRaises(ControlError):
                    self.write([[row]])
                self.path.unlink(missing_ok=True)

    def test_decimal_precision_boundary_rejects_overflow_without_float(self):
        row = list(ROW)
        row[1] = Decimal("10000000000000000000000000000.00")
        with self.assertRaisesRegex(ControlError, "PARQUET_DECIMAL_RANGE"):
            self.write([[row]])

    def test_decimal38_preserves_full_coefficient_beyond_default_context_precision(self):
        columns = [{"name": "value", "type": "DECIMAL(38,2)", "nullable": False}]
        value = Decimal("999999999999999999999999999999999999.99")
        metadata = write(self.path, columns, [[[value]]], maximum_bytes=1048576,
            maximum_rows=1)
        self.assertEqual(metadata["rows"], 1)
        self.assertEqual(pq.read_table(self.path)["value"][0].as_py(), value)

    def test_original_file_cannot_be_overwritten_and_symlink_is_rejected(self):
        metadata = self.write()
        with self.assertRaisesRegex(ControlError, "PARQUET_ORIGINAL_REQUIRED"):
            self.write()
        self.assertEqual(metadata["sha256"], hashlib.sha256(self.path.read_bytes()).hexdigest())
        alias = self.path.with_name("alias.parquet")
        alias.symlink_to(self.path)
        with self.assertRaisesRegex(ControlError, "PARQUET_FILE_SCOPE"):
            describe(alias, COLUMNS, maximum_bytes=1048576)

    def test_hash_rows_and_schema_are_checked_independently(self):
        metadata = self.write()
        with self.assertRaisesRegex(ControlError, "PARQUET_FILE_HASH"):
            describe(self.path, COLUMNS, maximum_bytes=1048576, expected={**metadata, "sha256": "0" * 64})
        with self.assertRaisesRegex(ControlError, "PARQUET_ROW_COUNT"):
            describe(self.path, COLUMNS, maximum_bytes=1048576, expected={**metadata, "rows": 2})
        changed = [dict(column) for column in COLUMNS]
        changed[0]["type"] = "VARCHAR"
        with self.assertRaisesRegex(ControlError, "PARQUET_COLUMN_TYPE"):
            describe(self.path, changed, maximum_bytes=1048576)

    def test_rows_bytes_and_batch_budgets_leave_no_success_descriptor(self):
        for args, batches, code in [({"maximum_rows": 0}, [[ROW]], "PARQUET_ROWS_QUOTA"),
                ({"maximum_bytes": 1}, [[ROW]], "PARQUET_BYTES_QUOTA"),
                ({}, [[ROW] * 513], "PARQUET_BATCH_QUOTA")]:
            with self.subTest(code=code):
                with self.assertRaisesRegex(ControlError, code):
                    self.write(batches, **args)
                self.path.unlink(missing_ok=True)

    def test_partial_file_cannot_exceed_reserved_bytes_even_during_footer_close(self):
        with self.assertRaisesRegex(ControlError, "PARQUET_BYTES_QUOTA"):
            self.write(maximum_bytes=50)
        self.assertLessEqual(self.path.stat().st_size, 50)

    def test_compressible_large_values_are_rejected_before_arrow_conversion(self):
        columns = [{"name": "value", "type": "BLOB", "nullable": False}]
        with self.assertRaisesRegex(ControlError, "PARQUET_BATCH_BYTES_QUOTA"):
            write(self.path, columns, [[[b"x" * 8388609]]],
                  maximum_bytes=33554432, maximum_rows=1)
        self.assertLessEqual(self.path.stat().st_size, 33554432)

    def test_permission_loss_during_write_prevents_success_descriptor(self):
        calls = []

        def guard():
            calls.append(1)
            if len(calls) == 2:
                raise ControlError(403, "FORBIDDEN")

        with self.assertRaisesRegex(ControlError, "FORBIDDEN"):
            self.write(guard=guard)

    def test_same_size_change_during_independent_read_is_rejected(self):
        self.write()
        calls = []

        def guard():
            calls.append(1)
            if len(calls) == 3:
                raw = bytearray(self.path.read_bytes())
                raw[4] ^= 1
                self.path.write_bytes(raw)

        with self.assertRaisesRegex(ControlError, "PARQUET_FILE_CHANGED"):
            describe(self.path, COLUMNS, maximum_bytes=1048576, guard=guard)


if __name__ == "__main__":
    unittest.main()
