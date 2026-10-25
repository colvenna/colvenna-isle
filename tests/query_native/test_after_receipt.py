"""Actual Native receipt reads and sealed SQL; separate from AWS acceptance."""

import unittest
import copy
import threading
import io
import json
import time
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from botocore.exceptions import ReadTimeoutError
from colvenna_isle.control.registry import ControlError, Principal
from colvenna_isle.query.sql import Session
from colvenna_isle.writer.writes import write_id
from colvenna_isle.writer.writes import WriteStore
from colvenna_isle.writer.ingestion import CommitUnknown, IngestionStore, digest
from colvenna_isle.writer.scheduler import Scheduler
import test_jobs as queue_fixture
import test_transactions as ingestion_fixture

# The referenced datasets of the original fixture SQL.
ORDERS = ["dwd.orders"]


class NativeFixture:
    """The locked query image provides native DuckDB and typed Arrow loading."""

    def setUp(self):
        import duckdb

        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.writer = duckdb.connect()
        self.writer.execute("ATTACH '" + str(self.directory / "native.duckdb") + "' AS warehouse")
        for name in ["ctl", "ods", "dwd", "dws", "ads"]:
            self.writer.execute("CREATE SCHEMA warehouse." + name)
        self.configuration = {"space_id": "fixture", "generation": "gen_fixture", "datasets": {}}
        for name in ["dwd.orders", "dwd.other"]:
            definition = {"dataset": name, "schema_version": 1, "write_owner": "direct",
                "schema": [{"name": "id", "type": "BIGINT", "nullable": False},
                           {"name": "amount", "type": "DECIMAL(30,2)", "nullable": False}],
                "key_columns": ["id"], "partition_columns": [], "constraints": {"primary_key": ["id"]}}
            self.configuration["datasets"][name] = {"definition": definition,
                                                    "definition_sha256": digest(definition)}
        self.store = WriteStore(IngestionStore(self.writer, self.configuration, lambda: {"epoch": 1}))
        for index, name in enumerate(self.configuration["datasets"]):
            self.mutate(name, "insert", "initial_" + str(index), [{"id": 1, "amount": "12.30"}])
        self.reader = self.writer.cursor()

    def mutate(self, dataset, operation, nonce, records):
        return self.store.commit(write_id("fixture", "gen_fixture", dataset, nonce), "owner",
            {"dataset": dataset, "schema_version": 1, "operation": operation, "records": records},
            self.configuration["datasets"][dataset], lambda: None)

    def tearDown(self):
        self.reader.close()
        self.writer.close()
        self.temporary.cleanup()


class AfterReceipt(unittest.TestCase):
    def setUp(self):
        import duckdb
        self.assertEqual(duckdb.__version__, "1.5.6")
        self.fixture = NativeFixture()
        self.fixture.setUp()
        self.actor = Principal("owner", frozenset(["fixture"]), frozenset(["query:read"]),
                               "key_rotated_query_only", "gen_fixture")

    def tearDown(self):
        self.fixture.tearDown()

    def test_original_write_reads_later_update_and_delete_instead_of_old_reader(self):
        from colvenna_isle.query.snapshots import capture, load

        fixture = self.fixture
        original = write_id("fixture", "gen_fixture", "dwd.orders", "initial_0")
        condition = {"consistency": "after_write", "write_id": original}
        fixture.reader.execute("BEGIN")
        self.assertEqual(fixture.reader.execute(
            "SELECT amount FROM warehouse.dwd.orders").fetchone()[0].to_eng_string(), "12.30")
        fixture.mutate("dwd.orders", "upsert", "later", [{"id": 1, "amount": "99.99"}])
        value = capture(fixture.writer, fixture.configuration, self.actor, condition, lambda: None, ORDERS)
        with_snapshot = Session(Path(fixture.directory) / "query_one")
        try:
            load(value, fixture.configuration, with_snapshot, lambda: None, ORDERS)
            result = with_snapshot.execute("SELECT id,amount FROM dwd.orders", 10)
            self.assertEqual(result["rows"], [[1, "99.99"]])
            versions = {entry["dataset"]: entry for entry in result["data_versions"]}
            self.assertEqual(versions["dwd.orders"]["commit_sequence"], 3)
            self.assertEqual(versions["dwd.orders"]["snapshot_sequence"], 3)
        finally:
            with_snapshot.close()
        self.assertEqual(fixture.reader.execute(
            "SELECT amount FROM warehouse.dwd.orders").fetchone()[0].to_eng_string(), "12.30")
        fixture.reader.execute("ROLLBACK")
        fixture.store.commit(write_id("fixture", "gen_fixture", "dwd.orders", "deleted"), "owner",
            {"dataset": "dwd.orders", "schema_version": 1, "operation": "delete", "keys": [{"id": 1}]},
            fixture.configuration["datasets"]["dwd.orders"], lambda: None)
        value = capture(fixture.writer, fixture.configuration, self.actor, condition, lambda: None, ORDERS)
        snapshot = Session(Path(fixture.directory) / "query_two")
        try:
            load(value, fixture.configuration, snapshot, lambda: None, ORDERS)
            result = snapshot.execute("SELECT * FROM dwd.orders", 10)
            self.assertEqual(result["rows"], [])
            self.assertEqual(result["data_versions"][0]["snapshot_sequence"], 4)
        finally:
            snapshot.close()

    def private_source(self):
        from colvenna_isle.query.jobs import Jobs
        from colvenna_isle.writer.query_reads import QueryReads

        storage = queue_fixture.Storage()
        storage.space("fixture")
        jobs = Jobs(storage, "table", storage, "bucket", "123456789012",
                    queue_fixture.SOURCE, queue_fixture.IMAGE)
        configuration = {**self.fixture.configuration, "revision": 1, "route_version": 1}
        request = {"sql": "SELECT amount FROM dwd.orders", "consistency": "after_write",
                   "write_id": write_id("fixture", "gen_fixture", "dwd.orders", "initial_0"),
                   "client_request_id": "original"}
        jobs.admit(self.actor, configuration, request, "original")
        job = jobs.claim(queue_fixture.EXECUTOR)
        identity = {"task_arn": "writer-original", "task_definition": "writer:1",
                    "source_commit": "c" * 40, "image_digest": "sha256:" + "d" * 64,
                    "pid": 1, "start_ticks": 2, "boot_id": "boot", "process_nonce": "writer",
                    "address": "10.0.0.2"}
        runtime = SimpleNamespace(identity=identity, lock=threading.Lock(),
            guard=lambda epoch: {"epoch": 1},
            registry=SimpleNamespace(_read=lambda space: copy.deepcopy(configuration)),
            direct=SimpleNamespace(keys=SimpleNamespace(authenticate_id=lambda key: self.actor)),
            consumer=SimpleNamespace(paused=False),
            database=lambda config: SimpleNamespace(connection=self.fixture.writer,
                                                     ingestions=self.fixture.store.native))
        runtime.scheduler = Scheduler(runtime.lock)
        reads = QueryReads(runtime, jobs)
        operation = {"operation": "query_snapshot", "identity": identity, "epoch": 1,
                     "space_id": "fixture", "generation": "gen_fixture", "key_id": self.actor.key_id,
                     "principal_id": "owner", "query_id": job["query_id"],
                     "execution_id": job["execution_id"], "datasets": ORDERS}
        return jobs, reads, operation, configuration, request

    def test_private_capture_requires_original_running_query_and_never_recaptures_it(self):
        jobs, reads, operation, configuration, request = self.private_source()
        job = jobs.get(self.actor, "fixture", "gen_fixture", operation["query_id"])
        with self.assertRaisesRegex(ControlError, "QUERY_EXECUTION_CUSTODY_UNKNOWN"):
            reads.execute({**operation, "execution_id": "forged_execution"})
        value = reads.execute(operation)
        snapshot = Session(self.fixture.directory / "private_query")
        try:
            from colvenna_isle.query.snapshots import load
            load(value, configuration, snapshot, lambda: None, ORDERS)
            self.assertEqual(snapshot.execute(request["sql"], 10)["rows"], [["12.30"]])
        finally:
            snapshot.close()
        self.fixture.mutate("dwd.orders", "upsert", "after_capture", [{"id": 1, "amount": "99.99"}])
        with self.assertRaisesRegex(ControlError, "QUERY_SOURCE_ALREADY_CAPTURED"):
            reads.execute(operation)
        self.assertEqual(jobs.get(self.actor, "fixture", "gen_fixture", job["query_id"])
                         ["source_read"]["state"], "CLOSED")

    def test_lost_private_reply_never_recaptures_or_executes_sql(self):
        from colvenna_isle.query.controlled import Reader

        jobs, reads, operation, configuration, request = self.private_source()
        producer = operation["identity"]
        calls = []

        def lost(**kwargs):
            import json
            calls.append(kwargs)
            reads.execute(json.loads(kwargs["Payload"]))
            raise ReadTimeoutError(endpoint_url="original-read-reply-lost")

        reader = Reader(jobs, reads.runtime.direct.keys, reads.runtime.registry,
            SimpleNamespace(read=lambda: {"state": "ACTIVE", "identity": producer, "epoch": 1}),
            SimpleNamespace(invoke=lost), "private-function", producer["source_commit"],
            producer["image_digest"])
        target = Session(self.fixture.directory / "lost_reply")
        try:
            origin = {name: operation[name] for name in [
                "space_id", "generation", "key_id", "principal_id", "query_id", "execution_id"]}
            with self.assertRaisesRegex(ControlError, "QUERY_SOURCE_UNCONFIRMED"):
                reader.stage(origin, configuration, target, lambda: None, ORDERS)
            self.assertEqual(len(calls), 1)
            self.assertFalse(target.sealed)
            self.assertEqual(target.catalog, [])
            self.assertNotIn("SELECT", calls[0]["Payload"].decode())
        finally:
            target.close()
        with self.assertRaisesRegex(ControlError, "QUERY_SOURCE_ALREADY_CAPTURED"):
            reads.execute(operation)

    def test_receipt_owner_and_administrator_need_query_permission(self):
        from colvenna_isle.query.snapshots import capture

        configuration = self.fixture.configuration
        condition = {"consistency": "after_write", "write_id":
                     write_id("fixture", "gen_fixture", "dwd.orders", "initial_0")}
        other = Principal("other", self.actor.spaces, frozenset(["query:read", "query:manage"]),
                          "key_other", "gen_fixture")
        with self.assertRaisesRegex(ControlError, "WRITE_OWNER_REQUIRED"):
            capture(self.fixture.writer, configuration, other, condition, lambda: None, ORDERS)
        administrator = Principal("administrator", self.actor.spaces,
            frozenset(["query:read", "datasets:manage"]), "key_admin", "gen_fixture")
        self.assertTrue(capture(self.fixture.writer, configuration, administrator, condition,
                                lambda: None, ORDERS)["snapshot"]["tables"])
        for actor in [
            Principal("owner", self.actor.spaces, frozenset(["data:write"]), "key_write", "gen_fixture"),
            Principal("administrator", self.actor.spaces, frozenset(["datasets:manage"]), "key_admin", "gen_fixture"),
            Principal("owner", self.actor.spaces, self.actor.scopes, "key_old", "gen_old"),
            Principal("owner", frozenset(["other_space"]), self.actor.scopes, "key_other", "gen_fixture"),
        ]:
            with self.subTest(actor=actor), self.assertRaises(ControlError):
                capture(self.fixture.writer, configuration, actor, condition, lambda: None, ORDERS)

    def test_unknown_rejected_and_missing_native_ledger_never_satisfy_commit(self):
        from colvenna_isle.query.snapshots import capture

        fixture = self.fixture
        with self.assertRaisesRegex(ControlError, "COMMIT_RECEIPT_UNCONFIRMED"):
            capture(fixture.writer, fixture.configuration, self.actor,
                {"consistency": "after_write", "write_id": "write_" + "f" * 64}, lambda: None, ORDERS)
        fixture.mutate("dwd.orders", "insert", "rejected", [{"id": 1, "amount": "2.00"}])
        rejected = write_id("fixture", "gen_fixture", "dwd.orders", "rejected")
        self.assertEqual(fixture.store.status(rejected, "owner")["status"], "REJECTED")
        with self.assertRaisesRegex(ControlError, "COMMIT_RECEIPT_NOT_COMMITTED"):
            capture(fixture.writer, fixture.configuration, self.actor,
                {"consistency": "after_write", "write_id": rejected}, lambda: None, ORDERS)
        fixture.writer.execute("ALTER TABLE warehouse.ctl.writes RENAME TO unavailable_writes")
        with self.assertRaisesRegex(ControlError, "COMMIT_RECEIPT_UNCONFIRMED"):
            capture(fixture.writer, fixture.configuration, self.actor,
                {"consistency": "after_write", "write_id":
                 write_id("fixture", "gen_fixture", "dwd.orders", "initial_0")}, lambda: None, ORDERS)

    def test_ingestion_uses_native_commit_and_original_owner_or_monitor_administrator(self):
        from colvenna_isle.query.snapshots import capture, load

        fixture = self.fixture
        registered = copy.deepcopy(ingestion_fixture.REGISTERED)
        fixture.configuration["datasets"]["ods.orders"] = registered
        payload = ingestion_fixture.payload(amount="9.87")
        message = {**ingestion_fixture.message(payload), "space_id": "fixture", "generation": "gen_fixture",
                   "principal_id": "owner"}
        committed = fixture.store.native.commit(message, payload, registered, lambda: None)
        self.assertEqual(committed["commit_sequence"], 3)
        condition = {"consistency": "after_ingestion", "ingestion_id": message["ingestion_id"]}
        value = capture(fixture.writer, fixture.configuration, self.actor, condition, lambda: None, ["ods.orders"])
        snapshot = Session(fixture.directory / "ingestion_query")
        try:
            load(value, fixture.configuration, snapshot, lambda: None, ["ods.orders"])
            result = snapshot.execute("SELECT id,amount FROM ods.orders", 10)
            self.assertEqual(result["rows"], [[1, "9.87"]])
            version = next(v for v in result["data_versions"] if v["dataset"] == "ods.orders")
            self.assertEqual((version["commit_sequence"], version["snapshot_sequence"]), (3, 3))
        finally:
            snapshot.close()
        other = Principal("other", self.actor.spaces, self.actor.scopes, "key_other", "gen_fixture")
        with self.assertRaisesRegex(ControlError, "INGESTION_OWNER_REQUIRED"):
            capture(fixture.writer, fixture.configuration, other, condition, lambda: None, ["ods.orders"])
        administrator = Principal("other", self.actor.spaces,
            frozenset(["query:read", "monitor:read"]), "key_admin", "gen_fixture")
        self.assertTrue(capture(fixture.writer, fixture.configuration, administrator, condition,
                                lambda: None, ["ods.orders"])["snapshot"]["tables"])

    def test_exact_bigint_decimal_blob_dates_json_and_arrays_reach_sealed_sql(self):
        from colvenna_isle.query.snapshots import capture, load
        from colvenna_isle.writer.ingestion import digest

        definition = {"dataset": "dwd.types", "schema_version": 1, "write_owner": "direct",
            "schema": [{"name": name, "type": kind, "nullable": False} for name, kind in [
                ("id", "BIGINT"), ("amount", "DECIMAL(38,2)"), ("flag", "BOOLEAN"), ("txt", "VARCHAR"),
                ("day", "DATE"), ("ts", "TIMESTAMP"), ("utc", "TIMESTAMPTZ"), ("blob", "BLOB"),
                ("j", "JSON"), ("tags", "VARCHAR[]")]], "key_columns": ["id"],
            "partition_columns": [], "constraints": {"primary_key": ["id"]}}
        registered = {"definition": definition, "definition_sha256": digest(definition)}
        fixture = self.fixture
        fixture.configuration["datasets"]["dwd.types"] = registered
        record = {"id": "9007199254740993", "amount": "999999999999999999999999999999999999.99",
                  "flag": True, "txt": "雪 \"quoted\"", "day": "2026-10-16",
                  "ts": "2026-10-16T12:34:56.123456", "utc": "2026-10-16T12:34:56.123456Z",
                  "blob": "AAF//w==", "j": {"ok": [1, "雪"]}, "tags": ["a", "雪"]}
        identity = write_id("fixture", "gen_fixture", "dwd.types", "typed")
        fixture.store.commit(identity, "owner", {"dataset": "dwd.types", "schema_version": 1,
            "operation": "insert", "records": [record]}, registered, lambda: None)
        value = capture(fixture.writer, fixture.configuration, self.actor,
            {"consistency": "after_write", "write_id": identity}, lambda: None, ["dwd.types"])
        snapshot = Session(fixture.directory / "typed_query")
        try:
            load(value, fixture.configuration, snapshot, lambda: None, ["dwd.types"])
            result = snapshot.execute("SELECT id,amount,flag,txt,day,ts,utc,blob,j,tags FROM dwd.types", 10)
            self.assertEqual(result["rows"], [[record[name] if name != "j" else '{"ok":[1,"雪"]}'
                for name in ["id", "amount", "flag", "txt", "day", "ts", "utc", "blob", "j", "tags"]]])
        finally:
            snapshot.close()

    def test_source_quota_rolls_back_and_subsequent_write_is_usable(self):
        from colvenna_isle.query.snapshots import capture

        fixture = self.fixture
        with patch("colvenna_isle.query.snapshots.MAX_BYTES", 100):
            with self.assertRaisesRegex(ControlError, "QUERY_SOURCE_QUOTA"):
                capture(fixture.writer, fixture.configuration, self.actor,
                    {"consistency": "after_write", "write_id":
                     write_id("fixture", "gen_fixture", "dwd.orders", "initial_0")}, lambda: None, ORDERS)
        fixture.mutate("dwd.orders", "upsert", "after_quota", [{"id": 1, "amount": "7.89"}])
        self.assertEqual(fixture.store.probe("dwd.orders")["rows"], [[1, "7.89"]])

    def test_ten_thousand_receipt_rows_load_within_five_seconds(self):
        from colvenna_isle.query.snapshots import capture, load

        fixture = self.fixture
        fixture.writer.execute("INSERT INTO warehouse.dwd.orders "
                               "SELECT i, 12.30 FROM range(2,10211) t(i)")
        condition = {"consistency": "after_write", "write_id":
                     write_id("fixture", "gen_fixture", "dwd.orders", "initial_0")}
        value = capture(fixture.writer, fixture.configuration, self.actor, condition, lambda: None, ORDERS)
        snapshot = Session(fixture.directory / "ten_thousand")
        try:
            started = time.monotonic()
            load(value, fixture.configuration, snapshot, lambda: None, ORDERS)
            self.assertLess(time.monotonic() - started, 5)
            self.assertEqual(snapshot.execute("SELECT count(*),sum(amount) FROM dwd.orders", 10)["rows"],
                             [[10210, "125583.00"]])
        finally:
            snapshot.close()

    def test_byte_quota_rejects_before_any_source_row_is_copied(self):
        from colvenna_isle.query.snapshots import capture

        fixture = self.fixture
        definition = {"dataset": "dwd.large", "schema_version": 1, "write_owner": "direct",
            "schema": [{"name": "id", "type": "BIGINT", "nullable": False},
                       {"name": "text", "type": "VARCHAR", "nullable": False}],
            "key_columns": ["id"], "partition_columns": [], "constraints": {"primary_key": ["id"]}}
        fixture.configuration["datasets"]["dwd.large"] = {
            "definition": definition, "definition_sha256": digest(definition)}
        fixture.mutate("dwd.large", "insert", "large", [{"id": 1, "text": "initial"}])
        fixture.writer.execute("UPDATE warehouse.dwd.large SET text=repeat('雪',400000)")
        # _Target.load is the row-copy boundary, after the native preflight.
        with patch("colvenna_isle.query.snapshots._Target.load",
                   side_effect=AssertionError("oversized source was copied")):
            with self.assertRaisesRegex(ControlError, "QUERY_SOURCE_QUOTA"):
                capture(fixture.writer, fixture.configuration, self.actor,
                    {"consistency": "after_write", "write_id":
                     write_id("fixture", "gen_fixture", "dwd.large", "large")},
                    lambda: None, ["dwd.large"])
        fixture.mutate("dwd.orders", "upsert", "after_large", [{"id": 1, "amount": "7.89"}])

    def test_hundred_thousand_rows_fit_both_capture_and_load_budgets(self):
        from colvenna_isle.query.snapshots import capture, load

        fixture = self.fixture
        fixture.writer.execute("INSERT INTO warehouse.dwd.orders "
                               "SELECT i, 0.01 FROM range(2,100001) t(i)")
        condition = {"consistency": "after_write", "write_id":
                     write_id("fixture", "gen_fixture", "dwd.orders", "initial_0")}
        started = time.monotonic()
        value = capture(fixture.writer, fixture.configuration, self.actor, condition, lambda: None, ORDERS)
        self.assertLess(time.monotonic() - started, 5)
        snapshot = Session(fixture.directory / "hundred_thousand")
        try:
            started = time.monotonic()
            load(value, fixture.configuration, snapshot, lambda: None, ORDERS)
            self.assertLess(time.monotonic() - started, 5)
            self.assertEqual(snapshot.execute("SELECT count(*),sum(amount) FROM dwd.orders", 10)["rows"],
                             [[100000, "1012.29"]])
        finally:
            snapshot.close()

    def test_receipt_version_is_confirmed_when_the_sql_names_none_of_its_rows(self):
        from colvenna_isle.query.snapshots import capture, load

        fixture = self.fixture
        condition = {"consistency": "after_write", "write_id":
                     write_id("fixture", "gen_fixture", "dwd.orders", "initial_0")}
        value = capture(fixture.writer, fixture.configuration, self.actor, condition, lambda: None, [])
        version = {"dataset": "dwd.orders", "release_id": None, "commit_sequence": 1,
                   "snapshot_sequence": 2, "generation": "gen_fixture"}
        self.assertEqual(value["snapshot"]["tables"], [{"version": version}])
        snapshot = Session(fixture.directory / "receipt_only")
        try:
            load(value, fixture.configuration, snapshot, lambda: None, [])
            result = snapshot.execute("SELECT 1", 10)
            self.assertEqual((result["rows"], result["data_versions"], snapshot.catalog), ([[1]], [version], []))
            with self.assertRaisesRegex(ControlError, "INVALID_OR_UNAUTHORIZED_SQL"):
                snapshot.execute("SELECT * FROM dwd.orders", 10)
        finally:
            snapshot.close()
        other = capture(fixture.writer, fixture.configuration, self.actor, condition, lambda: None,
                        ["dwd.other"])
        self.assertEqual([sorted(table) for table in other["snapshot"]["tables"]],
                         [["version"], ["definition", "rows", "version"]])
        unnamed = Session(fixture.directory / "rows_not_named")
        try:
            with self.assertRaisesRegex(ControlError, "QUERY_SOURCE_UNCONFIRMED"):
                load(other, fixture.configuration, unnamed, lambda: None, [])
            self.assertFalse(unnamed.sealed)
        finally:
            unnamed.close()
        fixture.writer.execute("DELETE FROM warehouse.ctl.dataset_watermarks WHERE dataset='dwd.orders'")
        with self.assertRaisesRegex(ControlError, "COMMIT_WATERMARK_UNCONFIRMED"):
            capture(fixture.writer, fixture.configuration, self.actor, condition, lambda: None, [])

    def test_source_quota_is_counted_before_any_row_is_copied(self):
        from colvenna_isle.query.snapshots import capture

        fixture = self.fixture
        fixture.mutate("dwd.orders", "insert", "second", [{"id": 2, "amount": "1.00"}])
        statements = []

        class Recording:
            def execute(self, sql, *args):
                statements.append(sql)
                return fixture.writer.execute(sql, *args)

        with patch("colvenna_isle.query.snapshots.MAX_ROWS", 1):
            with self.assertRaisesRegex(ControlError, "QUERY_SOURCE_QUOTA"):
                capture(Recording(), fixture.configuration, self.actor, {"consistency": "after_write",
                    "write_id": write_id("fixture", "gen_fixture", "dwd.orders", "initial_0")},
                    lambda: None, ORDERS)
        self.assertFalse([sql for sql in statements if sql.startswith('SELECT "id"')])
        self.assertEqual(statements[-1], "ROLLBACK")

    def test_writer_copies_exactly_the_datasets_the_original_sql_names(self):
        jobs, reads, operation, configuration, request = self.private_source()
        for datasets in [[], ["dwd.orders", "dwd.other"], ["dwd.other"], "dwd.orders"]:
            with self.subTest(datasets=datasets), self.assertRaisesRegex(
                    ControlError, "QUERY_EXECUTION_CUSTODY_UNKNOWN"):
                reads.execute({**operation, "datasets": datasets})
        self.assertIsNone(jobs.get(self.actor, "fixture", "gen_fixture", operation["query_id"])
                          .get("source_read"))
        with self.assertRaisesRegex(ControlError, "UNKNOWN_QUERY_SOURCE_OPERATION"):
            reads.execute({name: item for name, item in operation.items() if name != "datasets"})
        value = reads.execute(operation)
        self.assertEqual([table["version"]["dataset"] for table in value["snapshot"]["tables"]], ORDERS)

    def test_private_success_verifies_capture_custody_before_sql(self):
        from colvenna_isle.query.controlled import Reader

        jobs, reads, operation, configuration, request = self.private_source()
        producer = operation["identity"]

        def invoke(**kwargs):
            result = reads.execute(json.loads(kwargs["Payload"]))
            return {"Payload": io.BytesIO(json.dumps({"statusCode": 200, "body": {
                "identity": producer, "epoch": 1, "result": result}}).encode())}

        reader = Reader(jobs, reads.runtime.direct.keys, reads.runtime.registry,
            SimpleNamespace(read=lambda: {"state": "ACTIVE", "identity": producer, "epoch": 1}),
            SimpleNamespace(invoke=invoke), "private-function", producer["source_commit"],
            producer["image_digest"])
        snapshot = Session(self.fixture.directory / "client_success")
        origin = {name: operation[name] for name in [
            "space_id", "generation", "key_id", "principal_id", "query_id", "execution_id"]}
        try:
            reader.stage(origin, configuration, snapshot, lambda: None, ORDERS)
            self.assertTrue(snapshot.sealed)
            self.assertEqual(snapshot.execute(request["sql"], 10)["rows"], [["12.30"]])
        finally:
            snapshot.close()

    def test_writer_switch_after_capture_returns_no_sealed_snapshot(self):
        from colvenna_isle.query.controlled import Reader

        jobs, reads, operation, configuration, request = self.private_source()
        producer = operation["identity"]
        permit = {"state": "ACTIVE", "identity": producer, "epoch": 1}

        def invoke(**kwargs):
            result = reads.execute(json.loads(kwargs["Payload"]))
            permit["epoch"] = 2
            permit["identity"] = {**producer, "task_arn": "replacement", "process_nonce": "replacement",
                                  "address": "10.0.0.3"}
            return {"Payload": io.BytesIO(json.dumps({"statusCode": 200, "body": {
                "identity": producer, "epoch": 1, "result": result}}).encode())}

        reader = Reader(jobs, reads.runtime.direct.keys, reads.runtime.registry,
            SimpleNamespace(read=lambda: copy.deepcopy(permit)), SimpleNamespace(invoke=invoke),
            "private-function", producer["source_commit"], producer["image_digest"])
        snapshot = Session(self.fixture.directory / "switched")
        origin = {name: operation[name] for name in [
            "space_id", "generation", "key_id", "principal_id", "query_id", "execution_id"]}
        try:
            with self.assertRaisesRegex(ControlError, "QUERY_SOURCE_IDENTITY_CHANGED"):
                reader.stage(origin, configuration, snapshot, lambda: None, ORDERS)
            self.assertFalse(snapshot.sealed)
            self.assertEqual(snapshot.catalog, [])
        finally:
            snapshot.close()
        with self.assertRaisesRegex(ControlError, "WRITER_IDENTITY_CHANGED"):
            reads.execute({**operation, "identity": permit["identity"], "epoch": 2})

    def test_cancellation_after_capture_does_not_publish_or_execute_sql(self):
        from colvenna_isle.query.controlled import Reader

        jobs, reads, operation, configuration, request = self.private_source()
        producer = operation["identity"]

        def invoke(**kwargs):
            result = reads.execute(json.loads(kwargs["Payload"]))
            jobs.cancel(self.actor, "fixture", "gen_fixture", operation["query_id"])
            return {"Payload": io.BytesIO(json.dumps({"statusCode": 200, "body": {
                "identity": producer, "epoch": 1, "result": result}}).encode())}

        reader = Reader(jobs, reads.runtime.direct.keys, reads.runtime.registry,
            SimpleNamespace(read=lambda: {"state": "ACTIVE", "identity": producer, "epoch": 1}),
            SimpleNamespace(invoke=invoke), "private-function", producer["source_commit"],
            producer["image_digest"])
        snapshot = Session(self.fixture.directory / "cancelled")
        origin = {name: operation[name] for name in [
            "space_id", "generation", "key_id", "principal_id", "query_id", "execution_id"]}
        try:
            with self.assertRaisesRegex(ControlError, "QUERY_CANCELLED"):
                reader.stage(origin, configuration, snapshot, lambda: None, ORDERS)
            self.assertFalse(snapshot.sealed)
            self.assertEqual(snapshot.catalog, [])
        finally:
            snapshot.close()
        self.assertEqual(jobs.source_reads(), [])

    def test_unconfirmed_original_source_holds_quota_until_exact_producer_stop(self):
        jobs, reads, operation, configuration, request = self.private_source()
        now = [time.time()]
        jobs.clock = lambda: now[0]
        claimed = []
        for index in range(4):
            job = jobs.get(self.actor, "fixture", "gen_fixture", operation["query_id"]) if index == 0 else \
                jobs.claim(queue_fixture.EXECUTOR)
            captured = jobs.source_admit(job, {**operation["identity"], "epoch": 1})
            finished = jobs.finish(captured, {"state": "NOT_STARTED", "outcome": "LAUNCH_FAILED",
                "pid": None, "exit_code": None}, failure_code="QUERY_SOURCE_UNCONFIRMED")
            claimed.append(finished)
            nonce = "next_" + str(index)
            jobs.admit(self.actor, configuration, {**request, "client_request_id": nonce}, nonce)
        fifth = jobs.claim(queue_fixture.EXECUTOR)
        with self.assertRaisesRegex(ControlError, "QUERY_SOURCE_RETENTION_QUOTA"):
            jobs.source_admit(fifth, {**operation["identity"], "epoch": 1})
        now[0] += 90000
        with self.assertRaisesRegex(ControlError, "QUERY_HISTORY_CUSTODY_UNKNOWN"):
            jobs.collect_history(claimed[0])
        with self.assertRaisesRegex(ControlError, "QUERY_SOURCE_UNCONFIRMED"):
            jobs.source_stopped(claimed[0], {"taskArn": "wrong",
                "taskDefinitionArn": "writer:1", "lastStatus": "STOPPED"})
        stopped = jobs.source_stopped(claimed[0], {"taskArn": "writer-original",
            "taskDefinitionArn": "writer:1", "lastStatus": "STOPPED"})
        self.assertEqual(stopped["source_read"]["state"], "STOPPED")
        self.assertEqual(len(jobs.source_reads()), 3)
        jobs.collect_history(stopped)
        original = jobs.get(self.actor, "fixture", "gen_fixture", operation["query_id"])
        self.assertEqual(original["status"], "FAILED")
        self.assertIn("input_collected_unix", original)

    def test_timeout_interrupts_original_native_read_and_releases_lane(self):
        jobs, reads, operation, configuration, request = self.private_source()
        original = self.fixture.writer

        def slow(connection, *args, **kwargs):
            connection.execute("BEGIN")
            try:
                connection.execute("SELECT sum(i * 1.0) FROM range(1000000000000) t(i)").fetchone()
            except BaseException:
                connection.execute("ROLLBACK")
                raise

        with patch("colvenna_isle.writer.query_reads.CAPTURE_SECONDS", 0.1), \
                patch("colvenna_isle.writer.query_reads.capture", slow):
            with self.assertRaisesRegex(ControlError, "QUERY_SOURCE_TIMEOUT"):
                reads.execute(operation)
        source = jobs.get(self.actor, "fixture", "gen_fixture", operation["query_id"])["source_read"]
        self.assertEqual((source["state"], source["proof"]["outcome"]), ("CLOSED", "FAILED"))
        self.fixture.mutate("dwd.orders", "upsert", "after_timeout", [{"id": 1, "amount": "8.90"}])
        self.assertEqual(original.execute("SELECT amount FROM warehouse.dwd.orders").fetchone()[0]
                         .to_eng_string(), "8.90")

    def test_unconfirmed_rollback_fences_actual_writes_and_later_capture(self):
        jobs, reads, operation, configuration, request = self.private_source()
        original = self.fixture.writer
        statements = []

        class UnconfirmedRollback:
            def execute(self, sql, *args):
                statements.append(sql)
                if sql.startswith('SELECT "id","amount" FROM warehouse."dwd"."orders"'):
                    raise RuntimeError("original Native copy failed")
                if sql == "ROLLBACK":
                    raise RuntimeError("original rollback reply unavailable")
                return original.execute(sql, *args)

            def interrupt(self):
                original.interrupt()

        database = SimpleNamespace(connection=UnconfirmedRollback(), ingestions=self.fixture.store.native)
        reads.runtime.database = lambda config: database
        with self.assertRaisesRegex(ControlError, "NATIVE_READ_CLOSURE_UNKNOWN"):
            reads.execute(operation)
        source = jobs.get(self.actor, "fixture", "gen_fixture", operation["query_id"])["source_read"]
        self.assertEqual(source["state"], "CAPTURING")
        self.assertEqual(len(jobs.source_reads()), 1)
        self.assertIn("BEGIN", statements)
        self.assertIn("ROLLBACK", statements)
        with self.assertRaisesRegex(CommitUnknown, "NATIVE_RESULT_UNKNOWN"):
            self.fixture.mutate("dwd.orders", "upsert", "after_unknown",
                                [{"id": 1, "amount": "98.76"}])

        # The SQL child was never launched in this fixture. Its terminal
        # proof releases the query lease, while the Native capture stays held.
        jobs.finish(jobs.get(self.actor, "fixture", "gen_fixture", operation["query_id"]),
                    {"state": "NOT_STARTED", "outcome": "LAUNCH_FAILED", "pid": None, "exit_code": None},
                    failure_code="NATIVE_READ_CLOSURE_UNKNOWN")
        jobs.admit(self.actor, configuration, {**request, "client_request_id": "later"}, "later")
        later = jobs.claim(queue_fixture.EXECUTOR)
        self.assertIsNotNone(later)
        before = list(statements)
        with self.assertRaisesRegex(ControlError, "NATIVE_RESULT_UNKNOWN"):
            reads.execute({**operation, "query_id": later["query_id"],
                           "execution_id": later["execution_id"]})
        self.assertEqual(statements, before)
        self.assertTrue(reads.runtime.consumer.paused)
        self.assertEqual(len(jobs.source_reads()), 1)
        later_source = jobs.get(self.actor, "fixture", "gen_fixture", later["query_id"])["source_read"]
        self.assertEqual((later_source["state"], later_source["proof"]["outcome"]), ("CLOSED", "FAILED"))

    def test_controlled_read_rejects_a_handle_fenced_by_other_native_work(self):
        jobs, reads, operation, configuration, request = self.private_source()
        self.fixture.store.native.unknown = True
        with self.assertRaisesRegex(ControlError, "NATIVE_RESULT_UNKNOWN"):
            reads.execute(operation)
        source = jobs.get(self.actor, "fixture", "gen_fixture", operation["query_id"])["source_read"]
        self.assertEqual((source["state"], source["proof"]["outcome"]), ("CLOSED", "FAILED"))
        self.assertEqual(jobs.source_reads(), [])
        # The admission was refused before BEGIN; the fixture still has no
        # transaction to roll back, even though its authority remains fenced.
        self.fixture.writer.execute("BEGIN")
        self.fixture.writer.execute("ROLLBACK")


if __name__ == "__main__":
    unittest.main()
