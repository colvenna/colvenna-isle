"""Direct write invariants against the qualified DuckDB 1.5.6 engine."""

import copy
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

from colvenna_isle.control.registry import ControlError
from colvenna_isle.writer.ingestion import CommitUnknown, IngestionStore, digest
from colvenna_isle.writer.writes import WriteStore, business_binding, write_id
from test_transactions import CONFIGURATION as INGESTION_CONFIGURATION
from test_transactions import REGISTERED as INGESTION_REGISTERED
from test_transactions import message, payload


DEFINITION = {
    "dataset": "dwd.orders", "schema_version": 1, "write_owner": "direct",
    "schema": [
        {"name": "id", "type": "BIGINT", "nullable": False},
        {"name": "code", "type": "VARCHAR", "nullable": False},
        {"name": "amount", "type": "DECIMAL(12,2)", "nullable": False},
        {"name": "region", "type": "VARCHAR", "nullable": False},
    ],
    "key_columns": ["id"], "partition_columns": ["region"],
    "constraints": {"primary_key": ["id"], "unique": [["code"]],
                    "checks": [{"column": "amount", "operator": ">=", "value": "0.00"}]},
    "indexes": [["region", "amount"]],
}
REGISTERED = {"definition": DEFINITION, "definition_sha256": digest(DEFINITION)}


def request(operation="insert", **form):
    return {"dataset": "dwd.orders", "schema_version": 1, "operation": operation,
            **(form or {"records": [{"id": 1, "code": "one", "amount": "1.00", "region": "west"}]})}


class NativeWrites(unittest.TestCase):
    def setUp(self):
        self.assertEqual(duckdb.__version__, "1.5.6", "Run in the qualified native image")
        self.directory = tempfile.TemporaryDirectory()
        self.path = str(Path(self.directory.name) / "warehouse.db")
        self.configuration = copy.deepcopy(INGESTION_CONFIGURATION)
        self.configuration["datasets"]["dwd.orders"] = copy.deepcopy(REGISTERED)
        self.now = datetime(2026, 10, 15, tzinfo=timezone.utc)
        self.open()

    def open(self):
        self.connection = duckdb.connect()
        self.connection.execute("ATTACH '" + self.path.replace("'", "''") + "' AS warehouse")
        for schema in ["ctl", "ods", "dwd", "dws", "ads"]:
            self.connection.execute("CREATE SCHEMA IF NOT EXISTS warehouse." + schema)
        self.native = IngestionStore(self.connection, self.configuration, lambda: {"epoch": 1})
        self.store = WriteStore(self.native, lambda: self.now)

    def tearDown(self):
        self.connection.close()
        self.directory.cleanup()

    def identity(self, key, dataset="dwd.orders"):
        return write_id("test_space", "gen_test", dataset, key)

    def commit(self, value=None, key="one", principal="owner", **extra):
        value = value or request()
        registered = self.configuration["datasets"][value["dataset"]]
        return self.store.commit(self.identity(key, value["dataset"]), principal, value,
                                 registered, lambda: None, **extra)

    def test_all_operations_zero_rows_and_partition_changes_are_atomic(self):
        self.assertEqual(self.commit()["affected_rows"], 1)
        upsert = request("upsert", records=[{"id": 1, "code": "one", "amount": "2.00", "region": "east"},
                                          {"id": 2, "code": "two", "amount": "3.00", "region": "east"}])
        self.assertEqual(self.commit(upsert, "upsert")["affected_rows"], 2)
        update = request("update", updates=[{"key": {"id": 1}, "set": {"amount": "4.00", "region": "south"}},
                                           {"key": {"id": 99}, "set": {"amount": "5.00"}}])
        self.assertEqual(self.commit(update, "update")["affected_rows"], 1)
        self.assertEqual(self.commit(request("delete", keys=[{"id": 2}, {"id": 99}]), "delete")["affected_rows"], 1)
        self.assertEqual(self.commit(request("update", updates=[{"key": {"id": 99}, "set": {"region": "none"}}]),
                                     "zero-update")["affected_rows"], 0)
        zero = self.commit(request("delete", keys=[{"id": 99}]), "zero-delete")
        self.assertEqual((zero["commit_sequence"], zero["dataset_watermark"], zero["affected_rows"]), (6, 6, 0))
        observed = self.store.probe("dwd.orders")
        self.assertEqual(observed["rows"], [[1, "one", "4", "south"]])
        self.assertEqual(observed["watermarks"], [("dwd.orders", 6)])
        self.assertEqual(len(observed["receipts"]), 6)
        self.assertEqual(len(observed["changes"]), 5)
        changes = observed["changes"]
        self.assertEqual([row[3] for row in changes], ["INSERT", "UPDATE", "INSERT", "UPDATE", "DELETE"])
        self.assertEqual((json.loads(changes[1][6]), json.loads(changes[1][7])),
                         ({"region": "west"}, {"region": "east"}))
        self.assertEqual((json.loads(changes[3][6]), json.loads(changes[3][7])),
                         ({"region": "east"}, {"region": "south"}))
        self.assertEqual((json.loads(changes[4][6]), changes[4][7]), ({"region": "east"}, None))

    def test_native_constraints_reject_whole_batch_and_preserve_original_rejection(self):
        self.commit()
        for key, bad in [
            ("pk", {"id": 1, "code": "new", "amount": "2.00", "region": "east"}),
            ("unique", {"id": 3, "code": "one", "amount": "2.00", "region": "east"}),
            ("check", {"id": 3, "code": "three", "amount": "-1.00", "region": "east"}),
        ]:
            batch = request(records=[{"id": 2, "code": "two", "amount": "2.00", "region": "east"}, bad])
            rejected = self.commit(batch, key)
            self.assertEqual((rejected["status"], rejected["code"]), ("REJECTED", "NATIVE_CONSTRAINT"))
            self.assertNotIn("commit_sequence", rejected)
            self.assertEqual(self.commit(batch, key), rejected)
        observed = self.store.probe("dwd.orders")
        self.assertEqual((observed["rows"], observed["sequence"], len(observed["changes"])),
                         ([[1, "one", "1", "west"]], 1, 1))
        constraints = [row[0] for row in observed["physical"]["constraints"]]
        self.assertTrue({"PRIMARY KEY", "UNIQUE", "CHECK", "NOT NULL"}.issubset(constraints))
        self.assertEqual(len(observed["physical"]["indexes"]), 1)
        with self.assertRaises(duckdb.ConstraintException):
            self.native.transaction(lambda: self.connection.execute(
                "INSERT INTO warehouse.dwd.orders VALUES (2,'two',NULL,'east')"))
        self.assertEqual(self.store.probe("dwd.orders")["sequence"], 1)

    def test_foreign_key_is_installed_and_enforced_on_insert_update_delete(self):
        definition = {
            "dataset": "dwd.lines", "schema_version": 1, "write_owner": "direct",
            "schema": [{"name": "id", "type": "BIGINT", "nullable": False},
                       {"name": "order_id", "type": "BIGINT", "nullable": False}],
            "key_columns": ["id"], "partition_columns": [],
            "constraints": {"primary_key": ["id"], "foreign_keys": [
                {"columns": ["order_id"], "references": "dwd.orders", "referenced_columns": ["id"]}]},
        }
        self.configuration["datasets"]["dwd.lines"] = {"definition": definition, "definition_sha256": digest(definition)}
        child = {"dataset": "dwd.lines", "schema_version": 1, "operation": "insert",
                 "records": [{"id": 1, "order_id": 1}]}
        self.assertEqual(self.commit(child, "missing-parent")["status"], "REJECTED")
        self.commit()
        self.assertEqual(self.commit(child, "child")["status"], "COMMITTED")
        update = {"dataset": "dwd.lines", "schema_version": 1, "operation": "update",
                  "updates": [{"key": {"id": 1}, "set": {"order_id": 9}}]}
        self.assertEqual(self.commit(update, "bad-reference")["status"], "REJECTED")
        self.assertEqual(self.commit(request("delete", keys=[{"id": 1}]), "parent-delete")["status"], "REJECTED")
        self.assertEqual(self.store.probe("dwd.lines")["rows"], [[1, 1]])
        self.assertIn("FOREIGN KEY", [row[0] for row in self.store.physical("dwd.lines")["constraints"]])

    def test_referenced_row_assigns_only_changed_columns(self):
        # DuckDB 1.5.6 executes UPDATE ... RETURNING, or an UPDATE assigning an indexed or LIST column, as delete +
        # insert and checks the foreign key on that delete: a referenced row would reject even unchanged
        # assignments.
        parent = {
            "dataset": "dwd.parents", "schema_version": 1, "write_owner": "direct",
            "schema": [{"name": "id", "type": "BIGINT", "nullable": False},
                       {"name": "code", "type": "VARCHAR", "nullable": False},
                       {"name": "labels", "type": "VARCHAR[]", "nullable": False},
                       {"name": "note", "type": "VARCHAR", "nullable": True}],
            "key_columns": ["id"], "partition_columns": [],
            "constraints": {"primary_key": ["id"], "unique": [["code"]]},
        }
        child = {
            "dataset": "dwd.children", "schema_version": 1, "write_owner": "direct",
            "schema": [{"name": "id", "type": "BIGINT", "nullable": False},
                       {"name": "parent_id", "type": "BIGINT", "nullable": False}],
            "key_columns": ["id"], "partition_columns": [],
            "constraints": {"primary_key": ["id"], "foreign_keys": [
                {"columns": ["parent_id"], "references": "dwd.parents", "referenced_columns": ["id"]}]},
        }
        for definition in [parent, child]:
            self.configuration["datasets"][definition["dataset"]] = {"definition": definition,
                                                                     "definition_sha256": digest(definition)}

        def write(key, operation, **form):
            return self.commit({"dataset": "dwd.parents", "schema_version": 1, "operation": operation, **form}, key)

        def record(identity, code, note, labels=("x",)):
            return {"id": identity, "code": code, "labels": list(labels), "note": note}

        write("parent", "insert", records=[record(1, "p1", "a"), record(2, "p2", None)])
        self.assertEqual(self.commit({"dataset": "dwd.children", "schema_version": 1, "operation": "insert",
                                      "records": [{"id": 1, "parent_id": 1}]}, "child")["status"], "COMMITTED")
        upsert = write("upsert", "upsert", records=[record(1, "p1", "b")])
        self.assertEqual((upsert["status"], upsert["affected_rows"]), ("COMMITTED", 1))
        update = write("update", "update", updates=[{"key": {"id": 1}, "set": {"code": "p1", "note": "c"}}])
        self.assertEqual((update["status"], update["affected_rows"]), ("COMMITTED", 1))
        same = write("same", "upsert", records=[record(1, "p1", "c")])
        self.assertEqual((same["status"], same["affected_rows"]), ("COMMITTED", 1))
        missing = write("missing", "update", updates=[{"key": {"id": 9}, "set": {"note": "x"}}])
        self.assertEqual((missing["status"], missing["affected_rows"]), ("COMMITTED", 0))
        for key, change in [("rename", {"code": "p9"}), ("relabel", {"labels": ["y"]})]:
            rejected = write(key, "update", updates=[{"key": {"id": 1}, "set": change}])
            self.assertEqual((rejected["status"], rejected["code"]), ("REJECTED", "NATIVE_CONSTRAINT"))
        unreferenced = write("unreferenced", "upsert", records=[record(2, "p8", "d", ["y"]), record(3, "p3", None)])
        self.assertEqual(unreferenced["affected_rows"], 2)
        observed = self.store.probe("dwd.parents")
        self.assertEqual(observed["rows"], [[1, "p1", ["x"], "c"], [2, "p8", ["y"], "d"], [3, "p3", ["x"], None]])
        self.assertEqual([change[3] for change in observed["changes"] if change[2] == "dwd.parents"],
                         ["INSERT", "INSERT", "UPDATE", "UPDATE", "UPDATE", "UPDATE", "INSERT"])

    def test_success_survives_cold_reopen_with_exact_receipt_and_no_second_effect(self):
        first = self.commit()
        self.connection.close()
        self.open()
        self.assertEqual(self.commit(), first)
        self.assertEqual(self.store.status(self.identity("one"), "owner"), first)
        self.assertEqual(self.store.require_committed(self.identity("one"), "owner"), first)
        observed = self.store.probe("dwd.orders")
        self.assertEqual((len(observed["rows"]), len(observed["receipts"]), observed["sequence"]), (1, 1, 1))
        self.assertEqual(len(observed["physical"]["indexes"]), 1)
        self.assertEqual(self.store.status(self.identity("one"), "admin", admin=True), first)
        with self.assertRaises(ControlError) as owner:
            self.commit(principal="another")
        self.assertEqual(owner.exception.status, 403)
        changed = request(records=[{"id": 1, "code": "one", "amount": "2.00", "region": "west"}])
        with self.assertRaises(ControlError) as conflict:
            self.commit(changed)
        self.assertEqual(conflict.exception.code, "WRITE_BINDING_CONFLICT")

    def test_timeout_is_not_business_identity_and_typed_equivalents_match(self):
        value = request(records=[{"id": "1", "code": "one", "amount": "1.0", "region": "west"}])
        self.assertEqual(business_binding(value, REGISTERED), business_binding(request(), REGISTERED))
        first = self.commit({**value, "timeout_ms": 1})
        self.assertEqual(self.commit({**request(), "timeout_ms": 20000}), first)
        self.assertEqual(self.store.probe("dwd.orders")["sequence"], 1)

    def test_expired_completed_and_unresolved_identities_are_never_new_operations(self):
        first = self.commit()
        unknown = self.identity("unknown")
        self.store.register(unknown, "owner", request(), REGISTERED)
        self.now += timedelta(days=30)
        for key in ["one", "unknown"]:
            with self.assertRaises(ControlError) as expired:
                self.commit(key=key)
            self.assertEqual(expired.exception.code, "WRITE_IDENTITY_EXPIRED")
        self.assertEqual(self.store.status(self.identity("one"), "owner"), first)
        self.assertEqual(self.store.status(unknown, "owner")["status"], "UNKNOWN")
        self.assertEqual(self.store.probe("dwd.orders")["sequence"], 1)

    def test_precommit_failure_keeps_original_registration_but_no_partial_business(self):
        def fail():
            raise RuntimeError("before commit")
        with self.assertRaisesRegex(RuntimeError, "before commit"):
            self.commit(before_commit=fail)
        observed = self.store.probe("dwd.orders")
        self.assertEqual((observed["rows"], observed["receipts"], observed["changes"], observed["sequence"]),
                         ([], [], [], 0))
        self.assertIsNotNone(self.store.original(self.identity("one"), "owner"))
        self.connection.close()
        self.open()
        self.assertEqual(self.store.status(self.identity("one"), "owner")["status"], "UNKNOWN")
        self.assertEqual(self.commit()["commit_sequence"], 1)

    def test_lost_commit_reply_freezes_handle_then_cold_reconciles_same_original(self):
        self.store.register(self.identity("one"), "owner", request(), REGISTERED)
        connection = self.connection

        class LostReply:
            def execute(self, sql, *args):
                result = connection.execute(sql, *args)
                if sql == "COMMIT":
                    raise TimeoutError("lost durable commit reply")
                return result
        self.native.connection = LostReply()
        with self.assertRaises(CommitUnknown):
            self.commit()
        with self.assertRaises(CommitUnknown):
            self.store.status(self.identity("one"), "owner")
        self.connection.close()
        self.open()
        original = self.store.status(self.identity("one"), "owner")
        self.assertEqual((original["status"], original["commit_sequence"]), ("COMMITTED", 1))
        self.assertEqual(self.commit(), original)
        self.assertEqual(len(self.store.probe("dwd.orders")["rows"]), 1)

    def test_ingestion_and_direct_writes_share_one_sequence(self):
        self.native.commit(message(payload()), payload(), INGESTION_REGISTERED, lambda: None)
        self.assertEqual(self.commit()["commit_sequence"], 2)
        next_value = payload(event="two", row_id=2)
        self.assertEqual(self.native.commit(message(next_value, "two"), next_value, INGESTION_REGISTERED,
                                           lambda: None)["commit_sequence"], 3)
        self.assertEqual(self.store.probe("dwd.orders")["watermarks"], [("dwd.orders", 2)])

    def test_removed_physical_constraint_is_not_accepted_as_registered_metadata(self):
        self.commit()
        self.connection.execute("DROP INDEX warehouse.dwd." +
                                '"' + self.store.physical("dwd.orders")["indexes"][0][0] + '"')
        with self.assertRaises(ControlError) as changed:
            self.commit(request("delete", keys=[{"id": 1}]), "delete")
        self.assertEqual(changed.exception.code, "NATIVE_SCHEMA_IDENTITY_MISMATCH")
        self.assertEqual(self.store.probe("dwd.orders")["sequence"], 1)

    def test_insert_only_table_without_key_has_changes_but_cannot_upsert(self):
        definition = {
            "dataset": "ads.notes", "schema_version": 1, "write_owner": "direct",
            "schema": [{"name": "text", "type": "VARCHAR", "nullable": False}],
            "key_columns": [], "partition_columns": [],
        }
        self.configuration["datasets"]["ads.notes"] = {"definition": definition, "definition_sha256": digest(definition)}
        value = {"dataset": "ads.notes", "schema_version": 1, "operation": "insert",
                 "records": [{"text": "same"}, {"text": "same"}]}
        self.assertEqual(self.commit(value)["affected_rows"], 2)
        observed = self.store.probe("ads.notes")
        self.assertEqual(observed["rows"], [["same"], ["same"]])
        self.assertEqual(len(observed["changes"]), 2)
        self.assertEqual(observed["changes"][0][5:7], (None, None))


if __name__ == "__main__":
    unittest.main()
