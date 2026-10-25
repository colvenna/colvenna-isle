"""Transaction, business-version and uncertain-COMMIT behavior on DuckDB 1.5.6."""

import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

import duckdb

from colvenna_isle.writer.ingestion import CommitUnknown, IngestionError, IngestionStore, digest


DEFINITION = {
    "dataset": "ods.orders", "schema_version": 1,
    "schema": [{"name": name, "type": kind, "nullable": False} for name, kind in [
        ("event_id", "VARCHAR"), ("id", "BIGINT"), ("version", "BIGINT"), ("amount", "DECIMAL(12,2)")]],
    "key_columns": ["id"], "partition_columns": [],
    "ingestion": {"operations": ["append", "upsert", "delete"], "event_id_column": "event_id",
                  "source_version_column": "version"},
}
REGISTERED = {"definition": DEFINITION, "definition_sha256": digest(DEFINITION)}
CONFIGURATION = {"space_id": "test_space", "generation": "gen_test",
                 "datasets": {"ods.orders": REGISTERED}}


def payload(event="e1", row_id=1, version=1, operation="upsert", amount="1.00"):
    row = {"event_id": event, "id": row_id, "version": version}
    if operation != "delete":
        row["amount"] = amount
    return {"dataset": "orders", "schema_version": 1, "operation": operation, "records": [row]}


def message(value, key="one"):
    return {"ingestion_id": "ing_" + digest(key), "space_id": "test_space", "generation": "gen_test",
            "dataset": "ods.orders", "principal_id": "test_owner", "payload_sha256": digest(value),
            "source": {"bucket": "test", "key": key, "version_id": "fixed", "sha256": digest(value), "bytes": 1}}


class Transactions(unittest.TestCase):
    def setUp(self):
        self.assertEqual(duckdb.__version__, "1.5.6", "Run in the qualified native image")
        self.directory = tempfile.TemporaryDirectory()
        self.path = str(Path(self.directory.name) / "warehouse.db")
        self.open()

    def open(self):
        self.connection = duckdb.connect()
        self.connection.execute("ATTACH '" + self.path.replace("'", "''") + "' AS warehouse")
        self.connection.execute("CREATE SCHEMA IF NOT EXISTS warehouse.ctl")
        self.connection.execute("CREATE SCHEMA IF NOT EXISTS warehouse.ods")
        self.store = IngestionStore(self.connection, CONFIGURATION, lambda: {"epoch": 1})

    def tearDown(self):
        self.connection.close()
        self.directory.cleanup()

    def commit(self, value, key="one", **extra):
        return self.store.commit(message(value, key), value, REGISTERED, lambda: None, **extra)

    def test_duplicate_batch_survives_cold_reopen_and_keeps_receipt(self):
        value = payload()
        first = self.commit(value)
        self.connection.close()
        self.open()
        self.assertEqual(self.commit(value), first)
        result = self.store.probe("ods.orders")
        self.assertEqual((len(result["rows"]), len(result["ledger"]), result["commit_sequence"]), (1, 1, 1))
        changed = payload(amount="2.00")
        with self.assertRaisesRegex(IngestionError, "INGESTION_BINDING_CONFLICT"):
            self.commit(changed)
        self.store.reject(message(changed), "bad-duplicate", "INGESTION_BINDING_CONFLICT")
        self.assertEqual(self.store.status(message(value)["ingestion_id"]), first)

    def test_precommit_failure_rolls_back_data_events_sequence_and_receipt(self):
        value = payload()
        def fail():
            raise RuntimeError("before commit")
        with self.assertRaisesRegex(RuntimeError, "before commit"):
            self.commit(value, before_commit=fail)
        result = self.store.probe("ods.orders")
        self.assertEqual((result["rows"], result["events"], result["ledger"], result["commit_sequence"]),
                         ([], [], [], 0))
        self.assertEqual(self.commit(value)["commit_sequence"], 1)

    def test_native_observation_versions_survive_restart_and_failed_attempt_cannot_regress_commit(self):
        value = payload()
        envelope = message(value)
        self.store.reject(envelope, "original-message", "SOURCE_HASH_MISMATCH")
        failed = self.store.status(envelope["ingestion_id"])
        self.assertEqual((failed["status"], failed["projection_version"]), ("FAILED", 1))
        self.store.reject(envelope, "original-message", "SOURCE_HASH_MISMATCH")
        self.assertEqual(self.store.status(envelope["ingestion_id"]), failed)
        committed = self.commit(value)
        self.assertEqual((committed["status"], committed["commit_sequence"], committed["projection_version"]),
                         ("COMMITTED", 1, 2))
        self.connection.close()
        self.open()
        self.assertEqual(self.store.observations(0), [committed])
        self.store.reject(envelope, "later-rejection", "INGESTION_BINDING_CONFLICT")
        self.assertEqual(self.store.status(envelope["ingestion_id"]), committed)
        self.assertEqual(self.store.observations(2), [])

    def test_one_conflicting_event_rolls_back_the_whole_batch(self):
        self.commit(payload())
        batch = payload(event="new", row_id=2)
        batch["records"].append(payload(amount="3.00")["records"][0])
        with self.assertRaisesRegex(IngestionError, "EVENT_IDENTITY_CONFLICT"):
            self.commit(batch, "conflict")
        result = self.store.probe("ods.orders")
        self.assertEqual((len(result["rows"]), len(result["ledger"]), result["commit_sequence"]), (1, 1, 1))

    def test_old_versions_and_tombstones_are_retained_without_regressing_key_state(self):
        self.commit(payload(version=7))
        self.commit(payload(event="delete", version=9, operation="delete"), "delete")
        stale = self.commit(payload(event="late", version=8), "late")
        self.assertEqual(stale["stale_events"], 1)
        result = self.store.probe("ods.orders")
        self.assertEqual(len(result["rows"]), 3)
        self.assertEqual(result["keys"], [("ods.orders", 9, "delete", True, 2)])
        self.assertEqual(result["rows"][1][3], None)
        duplicate = self.commit(payload(event="late", version="8", amount="1.0"), "same-event-new-batch")
        self.assertEqual((duplicate["inserted_events"], duplicate["duplicate_events"]), (0, 1))
        with self.assertRaisesRegex(IngestionError, "BUSINESS_VERSION_CONFLICT"):
            self.commit(payload(event="version-collision", version=9), "collision")
        self.commit(payload(event="resurrect", version=10), "resurrect")
        self.assertEqual(self.store.probe("ods.orders")["keys"], [("ods.orders", 10, "resurrect", False, 5)])

    def test_events_of_one_batch_apply_in_order_against_each_other(self):
        self.commit(payload(event="before", row_id=3, version=1))
        records = []
        for event, row_id, version in [("e1", 1, 3), ("e2", 1, 1), ("e3", 1, 5), ("e4", 1, 4), ("e5", 2, 2),
                                       ("e1", 1, 3), ("e6", 3, 2)]:
            records.append(payload(event=event, row_id=row_id, version=version, amount="%d.00" % version)["records"][0])
        batch = {"dataset": "orders", "schema_version": 1, "operation": "upsert", "records": records}
        receipt = self.commit(batch, "ordered")
        self.assertEqual({name: receipt[name] for name in ["input_records", "inserted_events", "duplicate_events",
                                                          "stale_events", "tombstones"]},
                         {"input_records": 7, "inserted_events": 6, "duplicate_events": 1, "stale_events": 2,
                          "tombstones": 0})
        self.assertEqual(self.connection.execute(
            "SELECT event_id, _colvenna_disposition FROM warehouse.ods.orders WHERE _colvenna_commit_sequence = 2 "
            "ORDER BY event_id").fetchall(),
            [("e1", "CURRENT"), ("e2", "STALE"), ("e3", "CURRENT"), ("e4", "STALE"), ("e5", "CURRENT"),
             ("e6", "CURRENT")])
        self.assertEqual(self.store.probe("ods.orders")["keys"], [
            ("ods.orders", 5, "e3", False, 2), ("ods.orders", 2, "e5", False, 2), ("ods.orders", 2, "e6", False, 2)])

    def test_conflicts_within_one_batch_roll_back_everything(self):
        for key, records, code in [
                ("event", [("same", 1, 1, "1.00"), ("same", 2, 1, "2.00")], "EVENT_IDENTITY_CONFLICT"),
                ("version", [("first", 1, 4, "1.00"), ("second", 1, 4, "2.00")], "BUSINESS_VERSION_CONFLICT")]:
            batch = {"dataset": "orders", "schema_version": 1, "operation": "upsert", "records": [
                payload(event=event, row_id=row_id, version=version, amount=amount)["records"][0]
                for event, row_id, version, amount in records]}
            with self.assertRaisesRegex(IngestionError, code):
                self.commit(batch, key)
        result = self.store.probe("ods.orders")
        self.assertEqual((result["rows"], result["events"], result["keys"], result["commit_sequence"]),
                         ([], [], [], 0))

    def test_documented_ten_thousand_records_commit_within_the_writer_memory_limit(self):
        self.connection.execute("SET threads = 2")
        self.connection.execute("SET memory_limit = '512MB'")
        self.connection.execute("SET temp_directory = ?", [self.directory.name + "/spill"])
        records = [payload(event="bulk-%d" % index, row_id=index, version=1, amount="%d.25" % index)["records"][0]
                   for index in range(10000)]
        batch = {"dataset": "orders", "schema_version": 1, "operation": "upsert", "records": records}
        receipt = self.commit(batch, "bulk")
        self.assertEqual((receipt["inserted_events"], receipt["commit_sequence"]), (10000, 1))
        self.assertEqual(self.connection.execute("SELECT count(*), sum(amount) FROM warehouse.ods.orders").fetchone(),
                         (10000, sum(Decimal("%d.25" % index) for index in range(10000))))

    def test_native_space_identity_rejects_a_different_generation(self):
        self.commit(payload())
        changed = {**CONFIGURATION, "generation": "gen_other"}
        store = IngestionStore(self.connection, changed, lambda: {"epoch": 1})
        with self.assertRaisesRegex(CommitUnknown, "NATIVE_SPACE_IDENTITY_MISMATCH"):
            store.initialize()
        with self.assertRaises(CommitUnknown):
            store.status(message(payload())["ingestion_id"])
        self.assertEqual(self.store.probe("ods.orders")["commit_sequence"], 1)

    def test_lost_commit_reply_pauses_handle_and_reopen_reconciles_original(self):
        value = payload()
        connection = self.connection
        class LostReply:
            def execute(self, sql, *args):
                result = connection.execute(sql, *args)
                if sql == "COMMIT":
                    raise TimeoutError("reply lost")
                return result

            def __getattr__(self, name):
                return getattr(connection, name)
        self.store.initialize()
        self.store.connection = LostReply()
        with self.assertRaises(CommitUnknown):
            self.commit(value)
        with self.assertRaises(CommitUnknown):
            self.store.status(message(value)["ingestion_id"])
        self.connection.close()
        self.open()
        result = self.commit(value)
        self.assertEqual(result["commit_sequence"], 1)
        self.assertEqual(len(self.store.probe("ods.orders")["rows"]), 1)

    def test_registered_types_round_trip_without_numeric_or_timezone_loss(self):
        definition = {
            "dataset": "ods.typed", "schema_version": 1,
            "schema": [{"name": name, "type": kind, "nullable": False} for name, kind in [
                ("event", "VARCHAR"), ("id", "BIGINT"), ("amount", "DECIMAL(38,2)"), ("flag", "BOOLEAN"),
                ("tags", "VARCHAR[]"), ("day", "DATE"), ("clock", "TIMESTAMP"),
                ("instant", "TIMESTAMPTZ"), ("document", "JSON"), ("binary", "BLOB")]],
            "key_columns": ["id"], "partition_columns": [],
            "ingestion": {"operations": ["append"], "event_id_column": "event"},
        }
        registered = {"definition": definition, "definition_sha256": digest(definition)}
        self.store.configuration["datasets"]["ods.typed"] = registered
        value = {"dataset": "typed", "schema_version": 1, "operation": "append", "records": [{
            "event": "typed-1", "id": "9223372036854775807", "amount": "123456789012345678901234567890123456.12",
            "flag": True, "tags": ["one", "two"], "day": "2026-10-15",
            "clock": "2026-10-15T09:00:00.123456", "instant": "2026-10-15T11:00:00+02:00",
            "document": {"n": 1, "nested": ["ok", None]}, "binary": "AAH/"}]}
        envelope = {**message(value, "typed"), "dataset": "ods.typed"}
        receipt = self.store.commit(envelope, value, registered, lambda: None)
        row = self.store.probe("ods.typed")["rows"][0]
        self.assertEqual(row[:10], [
            "typed-1", 9223372036854775807, "123456789012345678901234567890123456.12", True, ["one", "two"],
            "2026-10-15", "2026-10-15T09:00:00.123456", "2026-10-15T09:00:00+00:00",
            '{"n":1,"nested":["ok",null]}', "AAH/"])
        self.assertEqual(receipt["inserted_events"], 1)

    def test_decimal_negative_zero_cannot_create_a_second_business_key(self):
        definition = {**DEFINITION, "dataset": "ods.zero", "key_columns": ["amount"]}
        registered = {"definition": definition, "definition_sha256": digest(definition)}
        self.store.configuration["datasets"]["ods.zero"] = registered
        for event, amount, version in [("negative", "-0.00", 5), ("positive", "0.00", 4)]:
            value = {**payload(event=event, amount=amount, version=version), "dataset": "zero"}
            envelope = {**message(value, event), "dataset": "ods.zero"}
            receipt = self.store.commit(envelope, value, registered, lambda: None)
        self.assertEqual(receipt["stale_events"], 1)
        result = self.store.probe("ods.zero")
        self.assertEqual(len(result["keys"]), 1)
        self.assertEqual(result["keys"][0][1:4], (5, "negative", False))


if __name__ == "__main__":
    unittest.main()
