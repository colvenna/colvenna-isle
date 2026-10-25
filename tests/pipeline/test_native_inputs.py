"""Actual DuckDB 1.5.6 MVCC and Parquet; no object-storage/writer-role claim."""

import copy
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from colvenna_isle.control.registry import ControlError
from colvenna_isle.pipeline.native_inputs import capture
from colvenna_isle.writer.ingestion import IngestionStore, digest
from colvenna_isle.writer.writes import WriteStore, write_id


class NativeExports(unittest.TestCase):
    def setUp(self):
        self.assertEqual(duckdb.__version__, "1.5.6")
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.path = self.directory / "native.duckdb"
        self.writer = duckdb.connect()
        self.writer.execute("ATTACH '" + str(self.path) + "' AS warehouse")
        for name in ["ctl", "ods", "dwd", "dws", "ads"]:
            self.writer.execute("CREATE SCHEMA warehouse." + name)
        self.configuration = {"space_id": "fixture", "generation": "gen_fixture", "datasets": {}}
        for name in ["dwd.orders", "dwd.other"]:
            definition = {"dataset": name, "schema_version": 1, "write_owner": "direct",
                "schema": [{"name": "id", "type": "BIGINT", "nullable": False},
                    {"name": "amount", "type": "DECIMAL(30,2)", "nullable": False}],
                "key_columns": ["id"], "partition_columns": [],
                "constraints": {"primary_key": ["id"]}}
            self.configuration["datasets"][name] = {"definition": definition,
                "definition_sha256": digest(definition)}
        self.store = WriteStore(IngestionStore(self.writer, self.configuration, lambda: {"epoch": 1}))
        for index, name in enumerate(self.configuration["datasets"]):
            self.mutate(name, "insert", "initial_" + str(index), [{"id": 1, "amount": "12.30"}])
        # A second cursor uses the same native database instance. DuckDB rejects
        # attaching one local file to two independently created instances.
        self.reader = self.writer.cursor()
        self.bindings = {"orders": {"dataset": "dwd.orders", "selection": {"mode": "snapshot"}},
            "other": {"dataset": "dwd.other", "selection": {"mode": "snapshot"}}}
        self.index = 0

    def tearDown(self):
        self.reader.close()
        self.writer.close()
        self.temporary.cleanup()

    def mutate(self, dataset, operation, nonce, records):
        self.store.commit(write_id("fixture", "gen_fixture", dataset, nonce), "owner",
            {"dataset": dataset, "schema_version": 1, "operation": operation, "records": records},
            self.configuration["datasets"][dataset], lambda: None)

    def capture(self, *, guard=lambda: None, configuration=None, bindings=None, maximum_bytes=1048576):
        self.index += 1
        return capture(self.reader, configuration or self.configuration, bindings or self.bindings,
            self.directory / ("export_" + str(self.index)), maximum_bytes=maximum_bytes,
            maximum_rows=100, guard=guard)

    def test_stable_multi_input_vector_survives_concurrent_commit_and_repeated_file_reads(self):
        calls = []

        def guard():
            calls.append(1)
            if len(calls) == 3:
                self.mutate("dwd.orders", "upsert", "new_original", [{"id": 1, "amount": "99.99"}])

        original = self.capture(guard=guard)
        self.assertEqual(original["status"], "EXPORTED_LOCAL")
        self.assertEqual(original["publication_status"], "NOT_PUBLISHED")
        self.assertEqual(original["metrics"]["copied_rows"], 2)
        inputs = original["inputs"]
        self.assertEqual([inputs[name]["native_version"]["snapshot_sequence"] for name in ["orders", "other"]],
            [2, 2])
        self.assertEqual([inputs[name]["native_version"]["commit_sequence"] for name in ["orders", "other"]],
            [1, 2])
        file = Path(inputs["orders"]["file"]["local_path"])
        raw = file.read_bytes()
        self.assertEqual(pq.read_table(file)["amount"][0].as_py(), Decimal("12.30"))
        newer = self.capture()
        self.assertEqual(newer["inputs"]["orders"]["native_version"]["snapshot_sequence"], 3)
        self.assertEqual(pq.read_table(newer["inputs"]["orders"]["file"]["local_path"])["amount"][0].as_py(),
            Decimal("99.99"))
        self.assertEqual(file.read_bytes(), raw)
        self.assertEqual(pq.read_table(file)["amount"][0].as_py(), Decimal("12.30"))

    def test_approved_subset_exports_no_other_dataset_and_missing_input_is_denied(self):
        value = self.capture(bindings={"orders": self.bindings["orders"]})
        self.assertEqual(set(value["inputs"]), {"orders"})
        with self.assertRaisesRegex(ControlError, "INPUT_DATASET_FORBIDDEN"):
            self.capture(bindings={"secret": {"dataset": "ods.secret", "selection": {"mode": "snapshot"}}})

    def test_generation_schema_and_unpublished_pipeline_inputs_fail_closed(self):
        changed = copy.deepcopy(self.configuration)
        changed["generation"] = "gen_other"
        with self.assertRaisesRegex(ControlError, "NATIVE_SPACE_IDENTITY_MISMATCH"):
            self.capture(configuration=changed)
        changed = copy.deepcopy(self.configuration)
        changed["datasets"]["dwd.orders"]["definition"]["schema"][1]["type"] = "VARCHAR"
        with self.assertRaisesRegex(ControlError, "NATIVE_SCHEMA_IDENTITY_MISMATCH"):
            self.capture(configuration=changed)
        changed = copy.deepcopy(self.configuration)
        pipeline = changed["datasets"]["dwd.orders"]["definition"]
        pipeline["write_owner"] = "pipeline"
        pipeline.pop("constraints")
        changed["datasets"]["dwd.orders"]["definition_sha256"] = digest(pipeline)
        with self.assertRaisesRegex(ControlError, "INPUT_RELEASE_NOT_PUBLISHED"):
            self.capture(configuration=changed)

    def test_partial_export_revocation_rolls_back_read_and_returns_no_complete_vector(self):
        calls = []

        def guard():
            calls.append(1)
            if len(calls) == 4:
                raise ControlError(403, "FORBIDDEN")

        with self.assertRaisesRegex(ControlError, "FORBIDDEN"):
            self.capture(guard=guard)
        self.reader.execute("BEGIN")
        self.reader.execute("ROLLBACK")
        self.assertEqual(self.reader.execute("SELECT count(*) FROM warehouse.dwd.orders").fetchone(), (1,))
        with self.assertRaisesRegex(ControlError, "PARQUET_BYTES_QUOTA"):
            self.capture(maximum_bytes=1)

    def test_changes_and_state_never_silently_read_full_snapshot(self):
        # Without an exact interval, partition set or key-state source a
        # selection is malformed; it never degrades to the current snapshot.
        for mode in ["changes", "affected_partitions", "affected_keys"]:
            with self.subTest(mode=mode), self.assertRaisesRegex(ControlError, "INVALID_INPUT_EXPORT"):
                self.capture(bindings={"orders": {"dataset": "dwd.orders", "selection": {"mode": mode}}})
        # Direct tables have no committed interval or partition release.
        with self.assertRaisesRegex(ControlError, "INVALID_INPUT_EXPORT"):
            self.capture(bindings={"orders": {"dataset": "dwd.orders", "selection": {"mode": "changes"},
                                              "from_sequence": 0, "to_sequence": 1}})
        with self.assertRaisesRegex(ControlError, "INPUT_SELECTION_UNSUPPORTED"):
            self.capture(bindings={"orders": {"dataset": "dwd.orders", "release_id": "release_x",
                                              "selection": {"mode": "snapshot"}}})


if __name__ == "__main__":
    unittest.main()
