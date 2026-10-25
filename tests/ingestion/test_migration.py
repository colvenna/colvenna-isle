"""Historical ODS business events replay into a new native generation."""

import json
import unittest
from pathlib import Path

from colvenna_isle.contracts.validation import ContractError
from colvenna_isle.migration import prepare
from colvenna_isle.writer.ingestion import CommitUnknown, IngestionStore
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
