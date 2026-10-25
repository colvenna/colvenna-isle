"""Actual DuckDB 1.5.6 native ODS, merge_keys and partition publication; no AWS claim.

Each incremental step uses the writer's own fixed exports (changes + key state)
and controlled SQL, then the native ledger. Results are compared with an
independent full recomputation from the same committed ODS watermark.
"""

import copy
import tempfile
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from colvenna_isle.contracts.model import KEY_STATE_METADATA
from colvenna_isle.contracts.validation import wire_schema
from colvenna_isle.control.pipelines import DEFINITION_EPOCH
from colvenna_isle.control.registry import ControlError, canonical
from colvenna_isle.pipeline.native_inputs import capture
from colvenna_isle.pipeline.publications import Publications, resolve_release
from colvenna_isle.pipeline.task_descriptor import descriptor_key
from colvenna_isle.pipeline.sql_task import connect, seal, statement
from colvenna_isle.writer.ingestion import IngestionStore, digest
from colvenna_isle.writer.pipeline_reads import impact, watermarks

ODS = {"dataset": "ods.orders", "schema_version": 1,
       "schema": [{"name": "event_id", "type": "VARCHAR", "nullable": False},
                  {"name": "order_id", "type": "BIGINT", "nullable": False},
                  {"name": "version", "type": "BIGINT", "nullable": False},
                  {"name": "order_date", "type": "DATE", "nullable": False},
                  {"name": "amount", "type": "DECIMAL(18,2)", "nullable": False}],
       "key_columns": ["order_id"], "partition_columns": [], "write_owner": "ingestion",
       "ingestion": {"operations": ["upsert", "delete"], "event_id_column": "event_id",
                     "source_version_column": "version"}}
CLEAN_SCHEMA = [{"name": "order_id", "type": "BIGINT", "nullable": False},
                {"name": "order_date", "type": "DATE", "nullable": False},
                {"name": "amount", "type": "DECIMAL(18,2)", "nullable": False}]
DWD = {"dataset": "dwd.orders", "schema_version": 1, "schema": CLEAN_SCHEMA, "key_columns": ["order_id"],
       "partition_columns": ["order_date"], "write_owner": "pipeline",
       "publication_state": {"protocol": "key_state_v1", "key_columns": ["order_id"], "allow_empty": True,
                             "schema": [CLEAN_SCHEMA[0]] + KEY_STATE_METADATA}}
DAILY_SCHEMA = [{"name": "order_date", "type": "DATE", "nullable": False},
                {"name": "revenue", "type": "DECIMAL(38,2)", "nullable": False},
                {"name": "order_count", "type": "BIGINT", "nullable": False}]
DWS = {"dataset": "dws.daily", "schema_version": 1, "schema": DAILY_SCHEMA, "key_columns": ["order_date"],
       "partition_columns": ["order_date"], "write_owner": "pipeline"}
FLAGS_SCHEMA = [{"name": "flag", "type": "VARCHAR", "nullable": False},
                {"name": "value", "type": "BIGINT", "nullable": False}]
FLAGS = {"dataset": "dws.flags", "schema_version": 1, "schema": FLAGS_SCHEMA, "key_columns": ["flag"],
         "partition_columns": [], "write_owner": "pipeline"}
METADATA = [{"name": "_colvenna_version", "type": "BIGINT", "nullable": False},
            {"name": "_colvenna_sequence", "type": "BIGINT", "nullable": False}]
CLEAN = """
WITH ranked AS (
  SELECT *, row_number() OVER (PARTITION BY order_id
                               ORDER BY version DESC, _colvenna_commit_sequence DESC) AS version_rank
  FROM changes)
SELECT CASE WHEN c._colvenna_operation = 'delete' THEN 'DELETE' ELSE 'UPSERT' END AS _colvenna_op,
       c.order_id,
       CASE WHEN c._colvenna_operation = 'delete' THEN NULL ELSE c.order_date END AS order_date,
       CASE WHEN c._colvenna_operation = 'delete' THEN NULL ELSE c.amount END AS amount,
       c.version AS _colvenna_version, c._colvenna_commit_sequence AS _colvenna_sequence
FROM ranked c LEFT JOIN key_state s USING (order_id)
WHERE c.version_rank = 1 AND (s.order_id IS NULL OR c.version > s._colvenna_version
      OR (c.version = s._colvenna_version AND c._colvenna_commit_sequence > s._colvenna_sequence))
"""
FULL = """
WITH ranked AS (
  SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY version DESC, _colvenna_commit_sequence DESC) AS r
  FROM events)
SELECT order_id, order_date, amount FROM ranked WHERE r = 1 AND _colvenna_operation <> 'delete'
"""
DAILY = """SELECT order_date, CAST(sum(amount) AS DECIMAL(38,2)) AS revenue, CAST(count(*) AS BIGINT) AS order_count
FROM orders GROUP BY order_date"""


def registered(definition):
    return {"definition": definition, "definition_sha256": digest(definition)}


class IncrementalPublications(unittest.TestCase):
    def setUp(self):
        self.assertEqual(duckdb.__version__, "1.5.6")
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.configuration = {"space_id": "owned", "generation": "gen_owned", "route_version": 1,
            "database_uri": "s3://test-bucket/spaces/owned/gen_owned/warehouse",
            "datasets": {value["dataset"]: registered(value) for value in [ODS, DWD, DWS, FLAGS]}}
        self.writer = duckdb.connect()
        self.writer.execute("ATTACH '" + str(self.root / "native.duckdb") + "' AS warehouse")
        for name in ["ctl", "ods", "dwd", "dws", "ads"]:
            self.writer.execute("CREATE SCHEMA warehouse." + name)
        self.native = IngestionStore(self.writer, self.configuration, lambda: {"epoch": 1})
        self.native.initialize()
        self.store = Publications(self.native, lambda value: None)
        self.store.initialize()
        self.reader = self.writer.cursor()
        self.counter = 0

    def tearDown(self):
        self.reader.close()
        self.writer.close()
        self.temporary.cleanup()

    def ingest(self, operation, records):
        self.counter += 1
        rows = []
        for event, order, version, day, amount in records:
            row = {"event_id": event, "order_id": order, "version": version}
            if operation != "delete":
                row.update(order_date=day, amount=amount)
            rows.append(row)
        value = {"dataset": "orders", "schema_version": 1, "operation": operation, "records": rows}
        key = "batch_" + str(self.counter)
        message = {"ingestion_id": "ing_" + digest(key), "space_id": "owned", "generation": "gen_owned",
                   "dataset": "ods.orders", "principal_id": "owner", "payload_sha256": digest(value),
                   "source": {"bucket": "test", "key": key, "version_id": "v", "sha256": digest(value), "bytes": 1}}
        return self.native.commit(message, value, self.configuration["datasets"]["ods.orders"], lambda: None)

    def export(self, bindings):
        self.counter += 1
        return capture(self.reader, self.configuration, bindings, self.root / ("export_" + str(self.counter)),
                       maximum_bytes=1048576, maximum_rows=10000, guard=lambda: None)

    def sql(self, exported, sql):
        """The controlled runner's binding/seal/statement path, in this process."""
        directory = self.root / ("sql_" + str(self.counter))
        directory.mkdir()
        connection = connect(directory, 256 * 1048576)
        for alias, value in exported["inputs"].items():
            columns = value["schema"]
            connection.execute("CREATE TABLE " + alias + " (" + ",".join(
                column["name"] + " " + column["type"] for column in columns) + ")")
            connection.execute("INSERT INTO " + alias + " SELECT * FROM read_parquet(?)", [value["file"]["local_path"]])
        seal(connection)
        rows = connection.execute(statement(connection, sql)).fetchall()
        connection.close()
        return rows

    def task(self, task_id, mode):
        if mode == "incremental":
            return {"task_id": task_id, "language": "sql", "executor": "writer", "resource_profile": "writer_short",
                    "entrypoint": "clean.sql", "timeout_seconds": 30, "max_attempts": 2,
                    "processing": {"mode": "incremental", "bootstrap": "full", "on_missing_state": "full"},
                    "inputs": {"changes": {"dataset": "ods.orders", "selection": "changes"},
                               "key_state": {"state": "target_key_state", "selection": "affected_keys"}},
                    "output": {"dataset": "dwd.orders", "write_mode": "merge_keys", "key_columns": ["order_id"],
                               "allow_empty": True, "schema": copy.deepcopy(CLEAN_SCHEMA),
                               "change_metadata": copy.deepcopy(METADATA)}}
        return {"task_id": task_id, "language": "sql", "executor": "writer", "resource_profile": "writer_short",
                "entrypoint": "daily.sql", "timeout_seconds": 30, "max_attempts": 2,
                "processing": {"mode": "partition_recompute", "partition_columns": ["order_date"],
                               "on_unbounded_impact": "full"},
                "inputs": {"orders": {"task": "clean", "selection": "affected_partitions"}},
                "output": {"dataset": "dws.daily", "write_mode": "replace_partitions", "key_columns": ["order_date"],
                           "partition_columns": ["order_date"], "allow_empty": True,
                           "schema": copy.deepcopy(DAILY_SCHEMA)}}

    def publish(self, task, job, exported, processing, rows, *, previous, replace=()):
        inputs = {}
        for alias, value in exported["inputs"].items():
            files = [{"s3_uri": f"s3://test-bucket/spaces/owned/inputs/gen_owned/{job}/exec/{alias}.parquet",
                      "object_version": "v1", "sha256": value["file"]["sha256"], "rows": value["file"]["rows"],
                      "bytes": value["file"]["bytes"]}]
            binding = {"dataset": value["dataset"], "release_id": "input_" + digest([job, alias]),
                       "watermark": {"ods_sequence": value["native_version"]["commit_sequence"]},
                       "schema": value["schema"], "files": files, "selection": value["selection"]}
            binding["manifest_sha256"] = digest({"contract_version": 2, **{name: binding[name] for name in [
                "dataset", "release_id", "watermark", "schema", "files"]}})
            inputs[alias] = binding
        descriptor = {"contract_version": 2, "space_id": "owned", "generation": "gen_owned",
            "pipeline_id": "demo", "pipeline_version": "v1", "definition_epoch": DEFINITION_EPOCH,
            "snapshot_sha256": "f" * 64, "run_id": "run_" + job, "job_id": job, "attempt_id": "attempt_1",
            "task": task, "package": {"s3_uri":
                "s3://test-bucket/spaces/owned/pipeline-code/gen_owned/demo/versions/v1/package.zip",
                "object_version": "code-v1", "sha256": "a" * 64, "bytes": 200},
            "environment": {"executor_binding": {"executor": "writer", "profile": "writer_short",
                                                 "source_commit": "a" * 40}},
            "parameters": {}, "processing": processing,
            "output_state": {"expected_previous_release": previous, "replace_partitions": list(replace)},
            "inputs": inputs}
        reference = {"s3_uri": "s3://test-bucket/" + descriptor_key(descriptor), "object_version": "d1",
                     "sha256": digest(descriptor), "bytes": len(canonical(descriptor).encode())}
        authority = {"principal_id": "owner", "input_export_id": "input_" + job,
                     "input_manifest": {"inputs": {alias: {"native_version": value["native_version"]}
                                                   for alias, value in exported["inputs"].items()}}}
        value = self.store.grant(reference, descriptor, authority)
        prefix = f"s3://test-bucket/spaces/owned/staging/gen_owned/demo/run_{job}/{job}/attempt_1/exec_1/"
        files = [{"s3_uri": prefix + "result/part-00000.parquet", "object_version": "f1", "sha256": "c" * 64,
                  "bytes": 100, "rows": len(rows)}] if rows else []
        value = self.store.select(job, value["epoch"], "attempt_1", "exec_1",
            {"s3_uri": prefix + "completion.json", "object_version": "c1", "sha256": "b" * 64, "bytes": 10}, files)
        for offset in range(0, len(rows), 512):
            self.store.chunk(job, value["epoch"], "attempt_1", "exec_1", 0, offset // 512,
                             [list(row) for row in rows[offset:offset + 512]])
        return self.store.publish(job, value["epoch"], "attempt_1", "exec_1"), (job, value["epoch"])

    def clean(self, job, *, bootstrap=False):
        """One incremental run from the authoritative previous release watermark."""
        current = resolve_release(self.reader, self.configuration, "dwd.orders")
        to = self.reader.execute("SELECT sequence FROM warehouse.ctl.commit_sequence").fetchone()[0]
        start = 0 if bootstrap or current is None else current["input_watermarks"]["changes"]["watermark"]["ods_sequence"]
        state = None if bootstrap or current is None else current["release_id"]
        exported = self.export({
            "changes": {"dataset": "ods.orders", "selection": {"mode": "changes"},
                        "from_sequence": start, "to_sequence": to},
            "key_state": {"dataset": "dwd.orders", "selection": {"mode": "affected_keys"},
                          "release_id": state, "keys_from": "changes"}})
        rows = self.sql(exported, CLEAN)
        processing = {"mode": "incremental", "from_watermark": {"ods_sequence": start},
                      "to_watermark": {"ods_sequence": to}, "state_release_id": state,
                      "bootstrap": bootstrap or current is None}
        return self.publish(self.task("clean", "incremental"), job, exported, processing, rows,
                            previous=current["release_id"] if current else None)[0]

    def expected_full(self, release):
        exported = self.export({"events": {"dataset": "ods.orders", "selection": {"mode": "snapshot"},
            "to_sequence": release["input_watermarks"]["changes"]["watermark"]["ods_sequence"]}})
        return sorted(self.sql(exported, FULL))

    def actual(self, release):
        table = "warehouse._colvenna_data.\"" + release["physical"]["table"] + "\""
        return sorted(self.reader.execute("SELECT * FROM " + table).fetchall())

    def test_multi_batch_incremental_equals_same_watermark_full_with_late_delete_and_restore(self):
        d1, d2 = "2026-10-01", "2026-10-02"
        self.ingest("upsert", [("e1", 1, 1, d1, "10.00"), ("e2", 2, 1, d1, "20.00"), ("e3", 3, 1, d2, "30.00")])
        first = self.clean("job_1")
        self.assertEqual(self.actual(first), self.expected_full(first))
        self.assertTrue(first["processing"]["bootstrap"])
        # Newer update, late older version (STALE in ODS), cross-date move and delete.
        self.ingest("upsert", [("e4", 1, 3, d2, "11.00")])
        self.ingest("upsert", [("e5", 1, 2, d1, "99.00")])
        self.ingest("delete", [("e6", 2, 2, None, None)])
        second = self.clean("job_2")
        self.assertEqual(self.actual(second), self.expected_full(second))
        self.assertEqual(second["change_summary"]["changes"], 2)
        # Deleted key: an older version arriving later cannot resurrect it.
        self.ingest("upsert", [("e7", 2, 1, d1, "20.00")])
        third = self.clean("job_3")
        self.assertEqual(self.actual(third), self.expected_full(third))
        self.assertNotIn(2, [row[0] for row in self.actual(third)])
        # A newer version restores it.
        self.ingest("upsert", [("e8", 2, 4, d1, "21.00")])
        fourth = self.clean("job_4")
        self.assertEqual(self.actual(fourth), self.expected_full(fourth))
        self.assertIn((2, date(2026, 10, 1), Decimal("21.00")), self.actual(fourth))
        state = self.reader.execute("SELECT * FROM warehouse._colvenna_data.\"" + fourth["state"]["physical"]["table"]
                                    + "\" ORDER BY order_id").fetchall()
        self.assertEqual(state, [(1, 3, 4, False), (2, 4, 11, False), (3, 1, 1, False)])
        # Empty delta preserves the complete target; watermark still advances.
        fifth = self.clean("job_5")
        self.assertEqual(self.actual(fifth), self.actual(fourth))
        self.assertEqual(fifth["change_summary"], {"changes": 0, "inserted_rows": 0, "deleted_rows": 0})
        # Old releases remain readable and unchanged.
        self.assertEqual(self.actual(resolve_release(self.reader, self.configuration, "dwd.orders",
                                                     second["release_id"])), self.actual(second))
        # Rule change: complete re-bootstrap from the full history equals full.
        rebuilt = self.clean("job_6", bootstrap=True)
        self.assertEqual(self.actual(rebuilt), self.expected_full(rebuilt))
        self.assertEqual(rebuilt["base"]["copied_rows"], 0)
        self.assertEqual(rebuilt["expected_previous_release"], fifth["release_id"])

    def test_target_becomes_empty_and_stale_or_outside_interval_changes_are_rejected(self):
        self.ingest("upsert", [("e1", 1, 1, "2026-10-01", "10.00")])
        first = self.clean("job_1")
        self.ingest("delete", [("e2", 1, 2, None, None)])
        emptied = self.clean("job_2")
        self.assertEqual((emptied["row_count"], self.actual(emptied)), (0, []))
        to = self.reader.execute("SELECT sequence FROM warehouse.ctl.commit_sequence").fetchone()[0]
        start = emptied["input_versions"]["changes"]["to_sequence"]

        def interval(lower):
            exported = self.export({
                "changes": {"dataset": "ods.orders", "selection": {"mode": "changes"}, "from_sequence": lower,
                            "to_sequence": to},
                "key_state": {"dataset": "dwd.orders", "selection": {"mode": "affected_keys"},
                              "release_id": emptied["release_id"], "keys_from": "changes"}})
            return exported, {"mode": "incremental", "from_watermark": {"ods_sequence": lower},
                              "to_watermark": {"ods_sequence": to}, "state_release_id": emptied["release_id"],
                              "bootstrap": False}
        # A gap after the prior interval could permanently skip changes.
        exported, processing = interval(start + 1)
        with self.assertRaisesRegex(ControlError, "PUBLICATION_INTERVAL_NOT_CONTINUOUS"):
            self.publish(self.task("clean", "incremental"), "job_gap", exported, processing, [],
                         previous=emptied["release_id"])
        exported, processing = interval(start)
        stale = [("UPSERT", 1, date(2026, 10, 1), Decimal("1.00"), 1, start)]
        with self.assertRaisesRegex(ControlError, "PUBLICATION_CHANGE_OUTSIDE_INTERVAL"):
            self.publish(self.task("clean", "incremental"), "job_x", exported, processing, stale,
                         previous=emptied["release_id"])
        self.assertEqual(resolve_release(self.reader, self.configuration, "dwd.orders")["release_id"],
                         emptied["release_id"])
        self.ingest("upsert", [("e3", 9, 1, "2026-10-01", "5.00")])
        to = self.reader.execute("SELECT sequence FROM warehouse.ctl.commit_sequence").fetchone()[0]
        exported, processing = interval(start)
        # Key 1's tombstone (version 2) blocks an older version 1 resurrection.
        resurrect = [("UPSERT", 1, date(2026, 10, 1), Decimal("1.00"), 1, to)]
        with self.assertRaisesRegex(ControlError, "PUBLICATION_STALE_CHANGE"):
            self.publish(self.task("clean", "incremental"), "job_y", exported, processing, resurrect,
                         previous=emptied["release_id"])
        self.assertIsNotNone(first)

    def daily(self, job, upstream):
        """Partition recompute from the full-row difference of two upstream releases."""
        current = resolve_release(self.reader, self.configuration, "dws.daily")
        before = current["input_versions"]["orders"]["release_id"] if current else None
        affected = impact(self.reader, self.configuration, "dwd.orders", before, upstream["release_id"],
                          ["order_date"])["partitions"]
        exported = self.export({"orders": {"dataset": "dwd.orders", "release_id": upstream["release_id"],
                                           "selection": {"mode": "affected_partitions", "partitions": affected}}})
        rows = self.sql(exported, DAILY)
        processing = {"mode": "partition_recompute", "affected_partitions": affected}
        return self.publish(self.task("daily", "partition"), job, exported, processing, rows,
                            previous=current["release_id"] if current else None, replace=affected)[0]

    def full_daily(self, upstream):
        exported = self.export({"orders": {"dataset": "dwd.orders", "release_id": upstream["release_id"],
                                           "selection": {"mode": "snapshot"}}})
        return sorted(self.sql(exported, DAILY))

    def test_partition_recompute_matches_full_aggregate_with_cross_date_move_and_emptied_day(self):
        d1, d2, d3 = "2026-10-01", "2026-10-02", "2026-10-03"
        self.ingest("upsert", [("e1", 1, 1, d1, "10.00"), ("e2", 2, 1, d2, "20.00"), ("e3", 3, 1, d3, "30.00")])
        clean_1 = self.clean("clean_1")
        daily_1 = self.daily("daily_1", clean_1)
        self.assertEqual(self.actual(daily_1), self.full_daily(clean_1))
        self.assertEqual({entry["partition"]["order_date"]: entry["replaced"] for entry in daily_1["partitions"]},
                         {d1: True, d2: True, d3: True})
        # Move order 1 from d1 to d2 (d1 becomes empty); d3 is unaffected.
        self.ingest("upsert", [("e4", 1, 2, d2, "12.00")])
        clean_2 = self.clean("clean_2")
        daily_2 = self.daily("daily_2", clean_2)
        self.assertEqual(self.actual(daily_2), self.full_daily(clean_2))
        partitions = {entry["partition"]["order_date"]: entry for entry in daily_2["partitions"]}
        self.assertEqual(sorted(partitions), [d1, d2, d3])
        self.assertEqual((partitions[d1]["rows"], partitions[d1]["replaced"]), (0, True))
        self.assertEqual(partitions[d3]["origin_release_id"], daily_1["release_id"])
        self.assertFalse(partitions[d3]["replaced"])
        self.assertNotIn(date(2026, 10, 1), [row[0] for row in self.actual(daily_2)])
        # The writer recomputes the impact itself: omitting a changed day fails.
        self.ingest("upsert", [("e5", 3, 2, d3, "31.00")])
        clean_3 = self.clean("clean_3")
        exported = self.export({"orders": {"dataset": "dwd.orders", "release_id": clean_3["release_id"],
                                           "selection": {"mode": "affected_partitions", "partitions": [{"order_date": d2}]}}})
        with self.assertRaisesRegex(ControlError, "PUBLICATION_IMPACT_INCOMPLETE"):
            self.publish(self.task("daily", "partition"), "daily_bad", exported,
                         {"mode": "partition_recompute", "affected_partitions": [{"order_date": d2}]},
                         self.sql(exported, DAILY), previous=daily_2["release_id"], replace=[{"order_date": d2}])
        # The previous daily release still reads its complete original rows.
        self.assertEqual(len(self.actual(daily_1)), 3)
        marks = watermarks(self.reader, self.configuration, "demo", ["ods.orders", "dwd.orders"], lambda: None)
        self.assertEqual(marks["tasks"]["daily"]["release_id"], daily_2["release_id"])
        self.assertEqual(marks["datasets"]["dwd.orders"]["release_id"], clean_3["release_id"])

    def test_republished_identical_dimension_keeps_unaffected_partitions(self):
        """A full upstream task republishes every run; identical rows are no impact, changed rows are."""
        d1, d2, d3 = "2026-10-01", "2026-10-02", "2026-10-03"
        flags_task = {"task_id": "flags", "language": "sql", "executor": "writer", "resource_profile": "writer_short",
                      "entrypoint": "flags.sql", "timeout_seconds": 30, "max_attempts": 2,
                      "processing": {"mode": "full"}, "depends_on": ["clean"],
                      "inputs": {"orders": {"task": "clean", "selection": "snapshot"}},
                      "output": {"dataset": "dws.flags", "write_mode": "versioned_replace", "key_columns": ["flag"],
                                 "allow_empty": True, "schema": copy.deepcopy(FLAGS_SCHEMA)}}
        daily_task = self.task("daily", "partition")
        daily_task["depends_on"] = ["clean", "flags"]
        daily_task["inputs"]["flags"] = {"task": "flags", "selection": "snapshot"}

        def flags(job, clean, value):
            exported = self.export({"orders": {"dataset": "dwd.orders", "release_id": clean["release_id"],
                                               "selection": {"mode": "snapshot"}}})
            current = resolve_release(self.reader, self.configuration, "dws.flags")
            return self.publish(flags_task, job, exported, {"mode": "full"}, [("threshold", value)],
                                previous=current["release_id"] if current else None)[0]

        def daily(job, clean, flag, affected):
            current = resolve_release(self.reader, self.configuration, "dws.daily")
            exported = self.export({
                "orders": {"dataset": "dwd.orders", "release_id": clean["release_id"],
                           "selection": {"mode": "affected_partitions", "partitions": affected}},
                "flags": {"dataset": "dws.flags", "release_id": flag["release_id"], "selection": {"mode": "snapshot"}}})
            return self.publish(daily_task, job, exported, {"mode": "partition_recompute", "affected_partitions": affected},
                                self.sql(exported, DAILY), previous=current["release_id"] if current else None,
                                replace=affected)[0]

        self.ingest("upsert", [("e1", 1, 1, d1, "10.00"), ("e2", 2, 1, d2, "20.00"), ("e3", 3, 1, d3, "30.00")])
        clean_1 = self.clean("clean_1")
        flags_1 = flags("flags_1", clean_1, 1)
        daily_1 = daily("daily_1", clean_1, flags_1, [{"order_date": day} for day in [d1, d2, d3]])
        self.ingest("upsert", [("e4", 2, 2, d2, "21.00")])
        clean_2 = self.clean("clean_2")
        flags_2 = flags("flags_2", clean_2, 1)
        self.assertNotEqual(flags_2["release_id"], flags_1["release_id"])
        self.assertEqual(impact(self.reader, self.configuration, "dws.flags", flags_1["release_id"],
                                flags_2["release_id"], [])["partitions"], [])
        affected = impact(self.reader, self.configuration, "dwd.orders", clean_1["release_id"],
                          clean_2["release_id"], ["order_date"])["partitions"]
        self.assertEqual(affected, [{"order_date": d2}])
        # The writer's own coverage check accepts the narrow impact: the dimension rows did not change.
        daily_2 = daily("daily_2", clean_2, flags_2, affected)
        partitions = {entry["partition"]["order_date"]: entry for entry in daily_2["partitions"]}
        self.assertEqual({day: entry["replaced"] for day, entry in partitions.items()}, {d1: False, d2: True, d3: False})
        self.assertEqual(partitions[d3]["origin_release_id"], daily_1["release_id"])
        # Changed dimension rows have no declared mapping: every partition must be replaced.
        self.ingest("upsert", [("e5", 3, 2, d3, "31.00")])
        clean_3 = self.clean("clean_3")
        flags_3 = flags("flags_3", clean_3, 2)
        self.assertEqual(impact(self.reader, self.configuration, "dws.flags", flags_2["release_id"],
                                flags_3["release_id"], [])["partitions"], [{}])
        with self.assertRaisesRegex(ControlError, "PUBLICATION_IMPACT_INCOMPLETE"):
            daily("daily_bad", clean_3, flags_3, [{"order_date": d3}])
        daily_3 = daily("daily_3", clean_3, flags_3, [{"order_date": day} for day in [d1, d2, d3]])
        self.assertTrue(all(entry["replaced"] for entry in daily_3["partitions"]))
        with self.assertRaisesRegex(ControlError, "PUBLICATION_RELEASE_NOT_FOUND"):
            impact(self.reader, self.configuration, "dws.flags", "release_missing", flags_3["release_id"], [])

    def test_exported_selections_are_typed_and_fixed(self):
        self.ingest("upsert", [("e1", 1, 1, "2026-10-01", "10.00")])
        self.ingest("upsert", [("e2", 2, 1, "2026-10-02", "20.00")])
        changes = self.export({"changes": {"dataset": "ods.orders", "selection": {"mode": "changes"},
                                           "from_sequence": 1, "to_sequence": 2}})["inputs"]["changes"]
        self.assertEqual(changes["native_version"]["from_sequence"], 1)
        self.assertEqual(changes["native_version"]["to_sequence"], 2)
        self.assertEqual([column["name"] for column in changes["schema"]][-2:],
                         ["_colvenna_operation", "_colvenna_commit_sequence"])
        table = pq.read_table(changes["file"]["local_path"]).to_pylist()
        self.assertEqual([(row["order_id"], row["_colvenna_commit_sequence"], row["_colvenna_operation"]) for row in table],
                         [(2, 2, "upsert")])
        with self.assertRaisesRegex(ControlError, "INPUT_WATERMARK_NOT_COMMITTED"):
            self.export({"events": {"dataset": "ods.orders", "selection": {"mode": "snapshot"}, "to_sequence": 99}})
        with self.assertRaisesRegex(ControlError, "INPUT_SELECTION_UNSUPPORTED"):
            self.export({"x": {"dataset": "ods.orders", "release_id": "release_x",
                               "selection": {"mode": "affected_partitions", "partitions": []}}})
        with self.assertRaisesRegex(ControlError, "INVALID_INPUT_EXPORT"):
            self.export({"x": {"dataset": "ods.orders", "selection": {"mode": "changes"}}})
        with self.assertRaisesRegex(ControlError, "INPUT_RELEASE_NOT_PUBLISHED|PUBLICATION_RELEASE_NOT_FOUND"):
            self.export({"x": {"dataset": "dwd.orders", "selection": {"mode": "snapshot"}}})


if __name__ == "__main__":
    unittest.main()
