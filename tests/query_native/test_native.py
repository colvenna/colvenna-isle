"""Trusted staging from actual writer ledgers into a separate read-only snapshot."""

import copy
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from colvenna_isle.control.registry import ControlError
from colvenna_isle.query import native
from colvenna_isle.query.native import describe, stage
from colvenna_isle.query.sql import Session, references
from colvenna_isle.writer.ingestion import IngestionStore, digest
from colvenna_isle.writer.writes import WriteStore, write_id
from test_sql import definition


class Recording:
    """The native read connection, recording each statement and its parameters."""

    def __init__(self, connection):
        self.connection, self.statements = connection, []

    def execute(self, sql, *args):
        self.statements.append((sql, args))
        return self.connection.execute(sql, *args)


class NativeStaging(unittest.TestCase):
    def setUp(self):
        self.assertEqual(duckdb.__version__, "1.5.6", "Use the fixed native image")
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "source.duckdb"
        self.writer = duckdb.connect()
        self.writer.execute("ATTACH '" + str(self.path).replace("'", "''") + "' AS warehouse")
        for schema in ["ctl", "ods", "dwd", "dws", "ads"]:
            self.writer.execute("CREATE SCHEMA warehouse." + schema)
        registered = {"definition": definition(), "definition_sha256": digest(definition())}
        self.configuration = {"space_id": "fixture", "generation": "gen_fixture",
                              "datasets": {"ods.orders": registered}}
        self.native = IngestionStore(self.writer, self.configuration, lambda: {"epoch": 1})
        self.writes = WriteStore(self.native)
        self.writes.commit(write_id("fixture", "gen_fixture", "ods.orders", "one"), "owner",
            {"dataset": "ods.orders", "schema_version": 1, "operation": "insert",
             "records": [{"id": 1, "amount": "12.30", "at": "2026-10-16T00:00:00Z"}]},
            registered, lambda: None)
        self.snapshot = Session(Path(self.temporary.name) / "snapshot")
        self.reader = None

    def tearDown(self):
        self.snapshot.close()
        if self.reader is not None:
            self.reader.close()
        if self.writer is not None:
            self.writer.close()
        self.temporary.cleanup()

    def open_reader(self):
        self.writer.execute("CHECKPOINT warehouse")
        self.writer.close()
        self.writer = None
        self.reader = duckdb.connect()
        self.reader.execute("ATTACH '" + str(self.path).replace("'", "''") + "' AS warehouse (READ_ONLY)")
        return self.reader

    def test_actual_values_versions_and_catalog_exclude_ledgers_and_candidates(self):
        hidden = copy.deepcopy(definition())
        hidden.update(dataset="dwd.candidate", write_owner="pipeline")
        hidden.pop("constraints")
        self.configuration["datasets"]["dwd.candidate"] = {
            "definition": hidden, "definition_sha256": digest(hidden)}
        self.writer.execute("CREATE TABLE warehouse.dwd.candidate AS SELECT 'unpublished' AS secret")
        catalog = stage(self.open_reader(), self.configuration, self.snapshot, lambda: None)
        self.assertEqual([item["dataset"] for item in catalog], ["ods.orders"])
        result = self.snapshot.execute("SELECT id,amount FROM ods.orders", 10)
        self.assertEqual(result["rows"], [[1, "12.30"]])
        self.assertEqual(result["data_versions"], [{"dataset": "ods.orders", "release_id": None,
            "commit_sequence": 1, "snapshot_sequence": 1, "generation": "gen_fixture"}])
        for sql in ["SELECT * FROM ctl.writes", "SELECT * FROM dwd.candidate"]:
            with self.assertRaises(ControlError):
                self.snapshot.execute(sql, 10)

    def test_a_registered_dataset_never_written_is_empty_at_sequence_zero(self):
        never = copy.deepcopy(definition())
        never.update(dataset="dwd.never")
        self.configuration["datasets"]["dwd.never"] = {"definition": never, "definition_sha256": digest(never)}
        catalog = stage(self.open_reader(), self.configuration, self.snapshot, lambda: None, datasets={"dwd.never"})
        self.assertEqual(catalog, [{"dataset": "dwd.never", "schema": never["schema"]}])
        result = self.snapshot.execute("SELECT count(*) FROM dwd.never", 10)
        self.assertEqual(result["rows"], [[0]])
        self.assertEqual(result["data_versions"], [{"dataset": "dwd.never", "release_id": None,
            "commit_sequence": 0, "snapshot_sequence": 1, "generation": "gen_fixture"}])

    def test_other_generation_and_changed_physical_schema_are_rejected(self):
        self.writer.execute("ALTER TABLE warehouse.ods.orders ADD COLUMN unregistered BIGINT")
        reader = self.open_reader()
        with self.assertRaisesRegex(ControlError, "NATIVE_SCHEMA_IDENTITY_MISMATCH"):
            stage(reader, self.configuration, self.snapshot, lambda: None)
        changed = copy.deepcopy(self.configuration)
        changed["generation"] = "gen_other"
        with self.assertRaisesRegex(ControlError, "NATIVE_SPACE_IDENTITY_MISMATCH"):
            stage(reader, changed, self.snapshot, lambda: None)
        self.assertFalse(self.snapshot.sealed)

    def test_metadata_only_catalog_reads_actual_identity_schema_and_watermarks_without_copy(self):
        catalog, versions = describe(self.open_reader(), self.configuration, lambda: None)
        self.assertEqual(catalog, [{"dataset": "ods.orders", "schema": definition()["schema"]}])
        self.assertEqual(versions, [{"dataset": "ods.orders", "release_id": None, "commit_sequence": 1,
                                    "snapshot_sequence": 1, "generation": "gen_fixture"}])
        self.assertFalse(self.snapshot.sealed)
        self.assertEqual(self.snapshot.catalog, [])

    def test_revocation_during_copy_cannot_produce_a_usable_snapshot(self):
        calls = []

        def guard():
            calls.append(1)
            if len(calls) == 3:
                raise ControlError(403, "FORBIDDEN")

        with self.assertRaisesRegex(ControlError, "FORBIDDEN"):
            stage(self.open_reader(), self.configuration, self.snapshot, guard)
        self.assertFalse(self.snapshot.sealed)
        self.assertEqual(self.reader.execute("SELECT count(*) FROM warehouse.ods.orders").fetchone(), (1,))

    def test_ods_staging_preserves_sparse_events_without_internal_columns(self):
        ods = {"dataset": "ods.events", "schema_version": 1, "write_owner": "ingestion",
            "schema": [{"name": name, "type": kind, "nullable": False} for name, kind in [
                ("event_id", "VARCHAR"), ("id", "BIGINT"), ("version", "BIGINT"),
                ("amount", "DECIMAL(12,2)")]],
            "key_columns": ["id"], "partition_columns": [],
            "ingestion": {"operations": ["upsert", "delete"], "event_id_column": "event_id",
                          "source_version_column": "version"}}
        registered = {"definition": ods, "definition_sha256": digest(ods)}
        self.configuration["datasets"]["ods.events"] = registered
        for index, operation in [(1, "upsert"), (2, "delete")]:
            row = {"event_id": "event_" + str(index), "id": 7, "version": index}
            if operation == "upsert":
                row["amount"] = "45.60"
            payload = {"dataset": "events", "schema_version": 1, "operation": operation, "records": [row]}
            message = {"ingestion_id": "ing_" + digest(index), "space_id": "fixture",
                "generation": "gen_fixture", "dataset": "ods.events", "principal_id": "owner",
                "payload_sha256": digest(payload), "source": {"bucket": "test", "key": str(index),
                    "version_id": "fixed", "sha256": digest(payload), "bytes": 1}}
            self.native.commit(message, payload, registered, lambda: None)
        stage(self.open_reader(), self.configuration, self.snapshot, lambda: None)
        result = self.snapshot.execute("SELECT id,amount FROM ods.events ORDER BY version", 10)
        self.assertEqual(result["rows"], [[7, "45.60"], [7, None]])
        self.assertEqual(result["data_versions"][0]["commit_sequence"], 3)
        columns = next(item["schema"] for item in self.snapshot.catalog if item["dataset"] == "ods.events")
        self.assertTrue(next(item["nullable"] for item in columns if item["name"] == "amount"))
        with self.assertRaises(ControlError):
            self.snapshot.execute("SELECT _colvenna_operation FROM ods.events", 10)

    def direct(self, dataset, records, schema=None):
        value = copy.deepcopy(definition())
        value["dataset"] = dataset
        if schema is not None:
            value["schema"] = schema
        registered = {"definition": value, "definition_sha256": digest(value)}
        self.configuration["datasets"][dataset] = registered
        self.writes.commit(write_id("fixture", "gen_fixture", dataset, "first"), "owner",
            {"dataset": dataset, "schema_version": 1, "operation": "insert", "records": records},
            registered, lambda: None)

    def wide_space(self):
        """Many registered datasets; dwd.wide_0 holds 200,000 rows."""
        for index in range(12):
            self.direct("dwd.wide_" + str(index), [{"id": 1, "amount": "1.00", "at": "2026-10-16T00:00:00Z"}])
        self.writer.execute("INSERT INTO warehouse.dwd.wide_0 SELECT i + 2, 1.00, "
                            "TIMESTAMPTZ '2026-10-16 00:00:00+00' FROM range(199999) t(i)")

    def test_query_stages_only_the_named_datasets_through_a_private_parquet_copy(self):
        self.wide_space()
        reader = Recording(self.open_reader())
        named = references(["SELECT count(*) FROM ODS.Orders"], self.configuration["datasets"])
        self.assertEqual(named, ["ods.orders"])
        with patch.object(Session, "load", side_effect=AssertionError("row-by-row staging")):
            catalog = stage(reader, self.configuration, self.snapshot, lambda: None, datasets=named)
        self.assertEqual([item["dataset"] for item in catalog], ["ods.orders"])
        self.assertNotIn("wide_", repr(reader.statements))
        self.assertEqual(list((self.snapshot.directory / "staging").iterdir()), [])
        result = self.snapshot.execute("SELECT count(*) FROM ods.orders", 10)
        self.assertEqual(result["rows"], [[1]])
        self.assertEqual(result["data_versions"], [{"dataset": "ods.orders", "release_id": None,
            "commit_sequence": 1, "snapshot_sequence": 13, "generation": "gen_fixture"}])
        with self.assertRaisesRegex(ControlError, "INVALID_OR_UNAUTHORIZED_SQL"):
            self.snapshot.execute("SELECT count(*) FROM dwd.wide_0", 10)

    def test_select_one_reads_no_dataset_and_a_named_large_dataset_copies_in_bulk(self):
        self.wide_space()
        reader = Recording(self.open_reader())
        catalog = stage(reader, self.configuration, self.snapshot, lambda: None,
                        datasets=references(["SELECT 1"], self.configuration["datasets"]))
        self.assertEqual(catalog, [])
        result = self.snapshot.execute("SELECT 1", 10)
        self.assertEqual((result["rows"], result["data_versions"]), ([[1]], []))
        self.assertFalse([sql for sql, _ in reader.statements
                          if sql.startswith(("COPY", "SELECT count")) or "duckdb_columns" in sql])
        sql = "SELECT count(*) FROM dwd.wide_0 JOIN ods.orders USING (id)"
        large = Session(Path(self.temporary.name) / "large")
        try:
            stage(reader, self.configuration, large, lambda: None,
                  datasets=references([sql], self.configuration["datasets"]))
            result = large.execute(sql, 10)
            self.assertEqual(result["rows"], [[1]])
            self.assertEqual([value["dataset"] for value in result["data_versions"]],
                             ["dwd.wide_0", "ods.orders"])
            self.assertEqual(large.execute("SELECT count(*) FROM dwd.wide_0", 10)["rows"], [[200000]])
        finally:
            large.close()

    def test_bulk_copy_keeps_exact_types_and_nullability(self):
        schema = [{"name": name, "type": kind, "nullable": nullable} for name, kind, nullable in [
            ("id", "BIGINT", False), ("amount", "DECIMAL(38,2)", False), ("small", "DECIMAL(4,1)", True),
            ("flag", "BOOLEAN", True), ("txt", "VARCHAR", False), ("day", "DATE", True),
            ("ts", "TIMESTAMP", True), ("utc", "TIMESTAMPTZ", False), ("blob", "BLOB", True),
            ("j", "JSON", True), ("tags", "VARCHAR[]", True)]]
        full = {"id": "9007199254740993", "amount": "999999999999999999999999999999999999.99",
                "small": "-123.4", "flag": True, "txt": "雪 \"quoted\"", "day": "2026-10-16",
                "ts": "2026-10-16T12:34:56.123456", "utc": "2026-10-16T12:34:56.123456Z",
                "blob": "AAF//w==", "j": {"ok": [1, "雪"]}, "tags": ["a", "雪"]}
        sparse = {"id": 2, "amount": "-0.01", "txt": "", "utc": "1970-01-01T00:00:00Z",
                  **{name: None for name in ["small", "flag", "day", "ts", "blob", "j", "tags"]}}
        self.direct("dwd.types", [full, sparse], schema)
        reader = self.open_reader()
        stage(reader, self.configuration, self.snapshot, lambda: None, datasets=["dwd.types"])
        names = [column["name"] for column in schema]
        result = self.snapshot.execute(
            "SELECT " + ",".join('"' + name + '"' for name in names) + " FROM dwd.types ORDER BY id", 10)
        self.assertEqual(result["rows"], [[sparse[name] for name in names],
            [full[name] if name != "j" else '{"ok":[1,"雪"]}' for name in names]])
        columns = ("SELECT column_name,data_type,is_nullable FROM duckdb_columns() WHERE "
                   "schema_name='dwd' AND table_name='types' AND database_name=? ORDER BY column_index")
        self.assertEqual(self.snapshot.connection.execute(columns, ["snapshot"]).fetchall(),
                         reader.execute(columns, ["warehouse"]).fetchall())
        self.assertEqual([column[2] for column in reader.execute(columns, ["warehouse"]).fetchall()],
                         [column["nullable"] for column in schema])
        self.assertEqual(list((self.snapshot.directory / "staging").iterdir()), [])

    def test_staging_budget_fails_before_any_copy(self):
        self.direct("dwd.extra", [{"id": key, "amount": "1.00", "at": "2026-10-16T00:00:00Z"} for key in [1, 2]])
        reader = Recording(self.open_reader())
        with patch.object(native, "STAGING_MAX_ROWS", 2):
            with self.assertRaisesRegex(ControlError, "QUERY_STAGING_LIMIT") as caught:
                stage(reader, self.configuration, self.snapshot, lambda: None,
                      datasets=["dwd.extra", "ods.orders"])
            self.assertEqual(caught.exception.status, 413)
            self.assertFalse(self.snapshot.sealed)
            self.assertEqual(self.snapshot.catalog, [])
            self.assertFalse([sql for sql, _ in reader.statements if sql.startswith("COPY")])
            self.assertEqual(reader.statements[-1][0], "ROLLBACK")
            within = Session(Path(self.temporary.name) / "within")
            try:
                stage(reader, self.configuration, within, lambda: None, datasets=["dwd.extra"])
                self.assertEqual(within.execute("SELECT count(*) FROM dwd.extra", 10)["rows"], [[2]])
            finally:
                within.close()

    def test_published_queries_depend_only_on_the_datasets_they_name(self):
        reader = self.open_reader()
        stage(reader, self.configuration, self.snapshot, lambda: None, consistency="published", datasets=[])
        self.snapshot.publications_only(self.configuration["datasets"])
        result = self.snapshot.execute("SELECT 1", 10)
        self.assertEqual((result["rows"], result["data_versions"]), ([[1]], []))
        for sql in ["SELECT * FROM ods.orders", "SELECT * FROM ods.orders a JOIN ods.orders b USING (id)",
                    "EXPLAIN SELECT * FROM ODS.ORDERS", "SELECT * FROM query_table('ods.orders')"]:
            with self.assertRaisesRegex(ControlError, "PUBLICATION_NOT_AVAILABLE"):
                self.snapshot.execute(sql, 10)
        named = Session(Path(self.temporary.name) / "named")
        try:
            with self.assertRaisesRegex(ControlError, "PUBLICATION_NOT_AVAILABLE"):
                stage(reader, self.configuration, named, lambda: None, consistency="published",
                      datasets=["ods.orders"])
            self.assertFalse(named.sealed)
        finally:
            named.close()

    def test_worker_stages_what_the_request_sql_names(self):
        from colvenna_isle.query import worker

        self.direct("dwd.extra", [{"id": 5, "amount": "5.00", "at": "2026-10-16T00:00:00Z"}])
        self.writer.execute("CHECKPOINT warehouse")
        self.writer.close()
        self.writer = None
        path = self.path

        class LocalReader(native.Reader):
            def __init__(self, configuration, directory, guard):
                self.configuration, self.guard = configuration, guard
                self.connection = duckdb.connect()
                self.connection.execute("ATTACH '" + str(path) + "' AS warehouse (READ_ONLY)")

        for index, (sql, expected) in enumerate([("SELECT 1", []),
                                                 ("SELECT sum(amount) FROM dwd.extra", ["dwd.extra"])]):
            directory = Path(self.temporary.name) / ("worker_" + str(index))
            directory.mkdir()
            raw = json.dumps({"configuration": self.configuration, "statements": [{"sql": sql, "max_rows": 10}],
                "open_only": False, "catalog_only": False, "consistency": "current", "condition": {},
                "source": None}).encode()
            (directory / "input.json").write_bytes(raw)
            with patch.object(native, "Reader", LocalReader), patch.object(sys, "argv", [
                    "worker", str(directory / "input.json"), hashlib.sha256(raw).hexdigest(),
                    str(directory / "process")]):
                worker.main()
            value = json.loads((directory / "process" / "result.json").read_text())["results"][0]
            self.assertEqual([version["dataset"] for version in value["data_versions"]], expected)
            self.assertEqual(value["rows"], [["5.00"]] if expected else [[1]])
