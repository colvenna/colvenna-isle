"""Historical ODS business events replay into a new native generation."""

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from colvenna_isle.contracts.validation import ContractError
from colvenna_isle.migration import backup_page, prepare
from colvenna_isle.writer.ingestion import CommitUnknown, IngestionStore, digest, quoted
import test_transactions as native_fixture
from test_transactions import DEFINITION, REGISTERED, message


ROOT = Path(__file__).resolve().parents[2]
HISTORY = json.loads((ROOT / "docs/history/v1.1.0-identifiers.json").read_text())
PREFIX = HISTORY["system_prefix"]


def export_page():
    columns = [{"name": column["name"], "type": column["type"], "encoding": "native_json"}
               for column in DEFINITION["schema"]]
    columns += [{"name": PREFIX + suffix, "type": kind, "encoding": "native_json"} for suffix, kind in [
        ("ingestion_id", "VARCHAR"), ("commit_sequence", "BIGINT"), ("operation", "VARCHAR"),
        ("disposition", "VARCHAR"), ("received_at", "TIMESTAMPTZ")]]
    return {"columns": columns, "rows": [
        ["created", "1", "7", "1234567890.12", "old_1", "1", "upsert", "CURRENT", "2026-10-20T00:00:00Z"],
        ["deleted", "1", "9", None, "old_2", "2", "delete", "CURRENT", "2026-10-20T00:00:01Z"],
        ["late", "1", "8", "3.00", "old_3", "3", "upsert", "STALE", "2026-10-20T00:00:02Z"]],
        "truncated": False, "row_count": 3, "next_page_token": None}


class MigrationValidation(unittest.TestCase):
    def test_replay_plan_preserves_exact_business_values_and_stable_retry_keys(self):
        plan = prepare(DEFINITION, export_page(), PREFIX)
        self.assertEqual(plan, prepare(DEFINITION, export_page(), PREFIX))
        self.assertEqual([batch["body"]["operation"] for batch in plan["batches"]], ["upsert", "delete", "upsert"])
        self.assertEqual(plan["batches"][0]["body"]["records"][0]["amount"], "1234567890.12")
        self.assertEqual(plan["batches"][1]["body"]["records"][0]["amount"], None)
        self.assertEqual(len({batch["idempotency_key"] for batch in plan["batches"]}), 3)
        self.assertTrue(all(not any(name.startswith(PREFIX) for name in batch["body"]["records"][0])
                            for batch in plan["batches"]))

    def test_incomplete_reordered_or_inexact_export_is_rejected(self):
        for change in [
            lambda page: page.update(truncated=True),
            lambda page: page.update(row_count=4),
            lambda page: page.update(next_page_token="remaining"),
            lambda page: page["rows"].reverse(),
            lambda page: page["rows"][0].__setitem__(3, 1234567890.12),
            lambda page: page["columns"][0].update(name=page["columns"][1]["name"]),
            lambda page: page["columns"][3].update(type="DOUBLE"),
        ]:
            page = export_page()
            change(page)
            with self.subTest(change=change), self.assertRaises(ContractError):
                prepare(DEFINITION, page, PREFIX)


class NativeMigration(unittest.TestCase):
    setUp = native_fixture.Transactions.setUp
    open = native_fixture.Transactions.open
    tearDown = native_fixture.Transactions.tearDown
    def test_replay_rebuilds_tombstone_and_exact_values_with_new_system_columns(self):
        plan = prepare(DEFINITION, export_page(), PREFIX)
        for batch in plan["batches"]:
            receipt = self.store.commit(message(batch["body"], batch["idempotency_key"]), batch["body"],
                                        REGISTERED, lambda: None)
            self.assertEqual(self.store.commit(message(batch["body"], batch["idempotency_key"]), batch["body"],
                                              REGISTERED, lambda: None), receipt)
        probe = self.store.probe("ods.orders")
        self.assertEqual(probe["keys"], [("ods.orders", 9, "deleted", True, 2)])
        self.assertEqual(str(probe["rows"][0][3]), "1234567890.12")
        self.assertEqual(len(probe["rows"]), 3)
        columns = self.connection.execute("DESCRIBE warehouse.ods.orders").fetchall()
        self.assertIn("_colvenna_operation", [column[0] for column in columns])
        self.assertFalse(any(column[0].startswith(PREFIX) for column in columns))

    def test_old_native_format_is_rejected_without_updating_identity(self):
        self.store.initialize()
        self.connection.execute("UPDATE warehouse.ctl.space_identity SET format_version=?",
                                [HISTORY["native_format_version"]])
        reopened = IngestionStore(self.connection, self.store.configuration, lambda: {"epoch": 1})
        with self.assertRaisesRegex(CommitUnknown, "NATIVE_SPACE_IDENTITY_MISMATCH"):
            reopened.initialize()
        self.assertEqual(self.connection.execute("SELECT format_version FROM warehouse.ctl.space_identity")
                         .fetchone()[0], HISTORY["native_format_version"])


class HistoricalBackup(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.folder = Path(self.directory.name)
        self.definition = copy.deepcopy(DEFINITION)
        self.definition["schema"] += [
            {"name": "metadata", "type": "JSON", "nullable": False},
            {"name": "occurred_at", "type": "TIMESTAMPTZ", "nullable": False},
            {"name": "image", "type": "BLOB", "nullable": False}]
        self.connection = duckdb.connect()
        # A real historical-shaped table, exported by DuckDB exactly as the old backup COPY did.
        declaration = [(column["name"], column["type"]) for column in self.definition["schema"]]
        declaration += [(PREFIX + suffix, kind) for suffix, kind in [
            ("ingestion_id", "VARCHAR"), ("commit_sequence", "BIGINT"), ("operation", "VARCHAR"),
            ("disposition", "VARCHAR"), ("received_at", "TIMESTAMPTZ")]]
        self.connection.execute("CREATE TABLE old_orders (" +
                                ",".join(quoted(name) + " " + kind for name, kind in declaration) + ")")
        for index, metadata in reversed(list(enumerate(['{"items":[1,true],"name":"原值"}', '[1,"two"]', '"text"']))):
            self.connection.execute("INSERT INTO old_orders VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", [
                "event_" + str(index), index, 7, "1234567890.12", metadata, "2026-10-20T08:00:00+08:00",
                bytes([0, 255]), "old_" + str(index), index + 1, "upsert", "CURRENT", "2026-10-20T00:00:00Z"])
        path = self.folder / "part.parquet"
        self.connection.execute("COPY old_orders TO ? (FORMAT parquet)", [str(path)])
        sha = hashlib.sha256(path.read_bytes()).hexdigest()
        path.rename(self.folder / (sha + ".parquet"))
        columns = [[name, kind, False] for name, kind, *_ in
                   self.connection.execute("DESCRIBE old_orders").fetchall()]
        manifest = {"format_version": 1, "kind": PREFIX.strip("_") + "_native_backup",
                    "tables": [{"schema": "ods", "table": "orders", "columns": columns,
                                "digest": {"rows": 3},
                                "files": [{"sha256": sha, "bytes": path.with_name(sha + ".parquet").stat().st_size}]}]}
        self.manifest = self.folder / "native.json"
        self.manifest.write_text(json.dumps(manifest))
        self.sha = hashlib.sha256(self.manifest.read_bytes()).hexdigest()

    def tearDown(self):
        self.connection.close()
        self.directory.cleanup()

    def page(self, sha=None):
        return backup_page(self.definition, self.manifest, sha or self.sha, self.folder, PREFIX)

    def test_verified_parquet_decodes_json_timezone_blob_and_orders_commits(self):
        plan = prepare(self.definition, self.page(), PREFIX)
        records = [batch["body"]["records"][0] for batch in plan["batches"]]
        self.assertEqual([record["event_id"] for record in records], ["event_0", "event_1", "event_2"])
        self.assertEqual([record["metadata"] for record in records],
                         [{"items": [1, True], "name": "原值"}, [1, "two"], "text"])
        self.assertTrue(all(record["amount"] == "1234567890.12" for record in records))
        self.assertTrue(all(record["occurred_at"] == "2026-10-20T00:00:00+00:00" for record in records))
        self.assertTrue(all(record["image"] == "AP8=" for record in records))
        registered = {"definition": self.definition, "definition_sha256": digest(self.definition)}
        configuration = {"space_id": "test_space", "generation": "gen_test",
                         "datasets": {"ods.orders": registered}}
        self.connection.execute("ATTACH ':memory:' AS warehouse")
        for schema in ["ctl", "ods"]:
            self.connection.execute("CREATE SCHEMA warehouse." + schema)
        store = IngestionStore(self.connection, configuration, lambda: {"epoch": 1})
        for batch in plan["batches"]:
            store.commit(message(batch["body"], batch["idempotency_key"]), batch["body"], registered, lambda: None)
        actual = self.connection.execute("SELECT metadata,image FROM warehouse.ods.orders ORDER BY id").fetchall()
        self.assertEqual([json.loads(row[0]) for row in actual], [record["metadata"] for record in records])
        self.assertEqual([row[1] for row in actual], [bytes([0, 255])] * 3)

    def test_manifest_and_part_tampering_are_rejected(self):
        with self.assertRaisesRegex(ContractError, "manifest checksum"):
            self.page("0" * 64)
        part = next(self.folder.glob("*.parquet"))
        raw = bytearray(part.read_bytes())
        raw[-1] ^= 1
        part.write_bytes(raw)
        with self.assertRaisesRegex(ContractError, "part checksum"):
            self.page()
