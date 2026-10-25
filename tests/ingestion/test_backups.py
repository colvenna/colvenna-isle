"""Consistent native backup and isolated, same-version restore on DuckDB 1.5.6."""

import base64
import copy
import hashlib
import io
import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

import duckdb
from botocore.exceptions import ClientError

from colvenna_isle.control.registry import ControlError, Principal, canonical
from colvenna_isle.writer.backups import Backups
from colvenna_isle.writer.ingestion import IngestionStore
from colvenna_isle.writer.native_schema import physical
from colvenna_isle.writer.scheduler import Scheduler
from colvenna_isle.writer.writes import WriteStore, write_id
from test_transactions import CONFIGURATION as INGESTION_CONFIGURATION
from test_transactions import message, payload
from test_writes import REGISTERED, request

ACCOUNT = "123456789012"
IDENTITY = {name: name for name in ["task_arn", "task_definition", "image_digest", "source_commit", "process_nonce",
                                    "pid", "start_ticks", "boot_id", "address"]}


class Vault:
    """A versioned bucket double: conditional creation, exact versions, SHA-256 checksums."""

    def __init__(self):
        self.objects, self.lock, self.corrupt = {}, threading.Lock(), set()

    def put_object(self, **value):
        body = value["Body"]
        raw = body if isinstance(body, bytes) else body.read()
        if base64.b64encode(hashlib.sha256(raw).digest()).decode() != value["ChecksumSHA256"]:
            raise ClientError({"Error": {"Code": "BadDigest"}}, "PutObject")
        with self.lock:
            if value["Key"] in self.objects:
                raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
            version = "v%04d" % (len(self.objects) + 1)
            self.objects[value["Key"]] = {"raw": raw, "version": version, "checksum": value["ChecksumSHA256"]}
        return {"VersionId": version}

    def head_object(self, **value):
        with self.lock:
            stored = self.objects.get(value["Key"])
        if stored is None or value.get("VersionId", stored["version"]) != stored["version"]:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"VersionId": stored["version"], "ContentLength": len(stored["raw"]),
                "ChecksumSHA256": stored["checksum"]}

    def get_object(self, **value):
        stored = self.objects[value["Key"]]
        raw = stored["raw"] + (b"x" if value["Key"] in self.corrupt else b"")
        return {"Body": io.BytesIO(raw), "VersionId": stored["version"]}


class Keys:
    def __init__(self, scopes):
        self.principal = Principal("operator", frozenset(["test_space"]), frozenset(scopes), "key_operator",
                                   "gen_test", "2027-01-01T00:00:00Z")

    def authenticate_id(self, key_id):
        if key_id != self.principal.key_id:
            raise ControlError(403, "FORBIDDEN")
        return self.principal


class Native:
    def __init__(self, path, configuration):
        self.connection = duckdb.connect()
        self.connection.execute("ATTACH '" + str(path) + "' AS warehouse")
        for schema in ["ctl", "ods", "dwd", "dws", "ads"]:
            self.connection.execute("CREATE SCHEMA IF NOT EXISTS warehouse." + schema)
        self.ingestions = IngestionStore(self.connection, configuration, lambda: {"epoch": 1})
        self.writes = WriteStore(self.ingestions)


class BackupRestoreTests(unittest.TestCase):
    def setUp(self):
        self.assertEqual(duckdb.__version__, "1.5.6", "Run in the qualified native image")
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.configuration = copy.deepcopy(INGESTION_CONFIGURATION)
        self.configuration.update(route_version=1, datasets={**self.configuration["datasets"],
                                                             "dwd.orders": copy.deepcopy(REGISTERED)})
        self.vault = Vault()
        self.source = Native(self.root / "source.db", self.configuration)
        self.ingest(1)
        self.write("one", request())
        self.write("two", request("upsert", records=[
            {"id": 2, "code": "two", "amount": "12345.67", "region": "east"}]))

    def tearDown(self):
        self.directory.cleanup()

    def runtime(self, native, scopes=("backup:manage",)):
        lock = threading.Lock()
        runtime = SimpleNamespace(identity=IDENTITY, lock=lock, scheduler=Scheduler(lock),
                                  guard=lambda epoch=None: {"epoch": 1},
                                  registry=SimpleNamespace(_read=lambda space: self.configuration),
                                  database=lambda configuration: native,
                                  direct=SimpleNamespace(keys=Keys(scopes)), log=lambda *arguments: None)
        work = Path(tempfile.mkdtemp(prefix="work-", dir=self.root))
        return runtime, Backups(runtime, self.vault, "vault", ACCOUNT, work)

    def ingest(self, number):
        value = payload("e%d" % number, number, 1)
        return self.source.ingestions.commit(message(value, "k%d" % number), value,
                                             self.configuration["datasets"]["ods.orders"], lambda: None)

    def write(self, key, value):
        return self.source.writes.commit(write_id("test_space", "gen_test", "dwd.orders", key), "owner", value,
                                         self.configuration["datasets"]["dwd.orders"], lambda: None)

    def call(self, backups, operation, **fields):
        return backups.execute({"identity": IDENTITY, "epoch": 1, "space_id": "test_space", "generation": "gen_test",
                                "principal_id": "operator", "key_id": "key_operator", "operation": operation,
                                **fields})

    def wait(self, backups, operation, **fields):
        for _ in range(200):
            value = self.call(backups, operation, **fields)
            if value["state"] != "RUNNING":
                return value
            time.sleep(0.05)
        self.fail("job not terminal")

    def backup(self, backups, identity="backup_" + "1" * 32):
        self.assertEqual(self.call(backups, "backup_native", backup_id=identity)["state"], "RUNNING")
        return self.wait(backups, "backup_status", backup_id=identity)

    def contents(self, connection):
        tables = connection.execute("SELECT schema_name,table_name FROM duckdb_tables() WHERE "
                                    "database_name='warehouse' ORDER BY 1,2").fetchall()
        return {schema + "." + table: (sorted(map(repr, connection.execute(
                    'SELECT * FROM warehouse."%s"."%s"' % (schema, table)).fetchall())),
                physical(connection, schema + "." + table)) for schema, table in tables}

    def test_restore_reproduces_every_table_row_and_catalog_at_the_backup_point(self):
        _, backups = self.runtime(self.source)
        backup = self.backup(backups)
        self.assertEqual(backup["state"], "COMPLETED", backup)
        self.assertEqual(backup["point"]["commit_sequence"], 3)
        manifest = json.loads(self.vault.objects[backup["manifest"]["key"]]["raw"])
        self.assertEqual(manifest["point"]["commit_sequence"], 3)
        # Work committed after the point never enters this backup.
        self.ingest(4)
        target = Native(self.root / "target.db", self.configuration)
        _, restorer = self.runtime(target)
        self.call(restorer, "restore_native", restore_id="restore_" + "2" * 32, manifest={
            name: backup["manifest"][name] for name in ["bucket", "key", "version_id", "sha256"]})
        restored = self.wait(restorer, "restore_status", restore_id="restore_" + "2" * 32)
        self.assertEqual(restored["state"], "COMPLETED", restored)
        after = self.contents(target.connection)
        self.source.connection.execute("BEGIN")
        sequence = self.source.connection.execute(
            "SELECT sequence FROM warehouse.ctl.commit_sequence").fetchone()[0]
        self.source.connection.execute("ROLLBACK")
        self.assertEqual(sequence, 4)
        self.assertEqual(target.connection.execute("SELECT sequence FROM warehouse.ctl.commit_sequence").fetchone(),
                         (3,))
        self.assertEqual((len(after["ods.orders"][0]), len(after["dwd.orders"][0])), (1, 2))
        self.assertEqual(after["dwd.orders"][1], physical(self.source.connection, "dwd.orders"))
        # The restored ledger accepts the same original identities and the next sequence.
        restored_store = target.writes
        duplicate = restored_store.commit(write_id("test_space", "gen_test", "dwd.orders", "two"), "owner",
                                          request("upsert", records=[{"id": 2, "code": "two", "amount": "12345.67",
                                                                      "region": "east"}]),
                                          self.configuration["datasets"]["dwd.orders"], lambda: None)
        self.assertEqual(duplicate["commit_sequence"], 3)
        fresh = target.ingestions.commit(message(payload("e9", 9, 1), "k9"), payload("e9", 9, 1),
                                         self.configuration["datasets"]["ods.orders"], lambda: None)
        self.assertEqual(fresh["commit_sequence"], 4)

    def test_a_backup_snapshot_excludes_commits_queued_behind_it(self):
        runtime, backups = self.runtime(self.source)
        entered, release = threading.Event(), threading.Event()
        original = backups.snapshot

        def snapshot(*arguments):
            entered.set()
            self.assertTrue(release.wait(5))
            return original(*arguments)
        backups.snapshot = snapshot
        self.call(backups, "backup_native", backup_id="backup_" + "3" * 32)
        self.assertTrue(entered.wait(5))
        queued = runtime.scheduler.submit("ingestion", "later", lambda: self.ingest(5), "test_space")
        release.set()
        backup = self.wait(backups, "backup_status", backup_id="backup_" + "3" * 32)
        self.assertEqual(queued.future.result(timeout=5)["commit_sequence"], 4)
        self.assertEqual(backup["point"]["commit_sequence"], 3)

    def test_restore_refuses_changed_objects_other_runtimes_and_written_targets_without_effect(self):
        _, backups = self.runtime(self.source)
        references = []
        for digit in "345":
            backup = self.backup(backups, "backup_" + digit * 32)
            references.append({name: backup["manifest"][name] for name in ["bucket", "key", "version_id", "sha256"]})
        manifest = json.loads(self.vault.objects[references[0]["key"]]["raw"])
        self.vault.corrupt.add(manifest["tables"][0]["files"][0]["key"])
        cases = [("BACKUP_OBJECT_CHANGED", references[0], None),
                 ("BACKUP_RUNTIME_MISMATCH", self.republish(references[1], ["runtime", "duckdb_version"], "1.5.5"),
                  None),
                 ("RESTORE_VERIFICATION_FAILED", self.republish(references[2], ["tables", -1, "digest", "hash_xor"],
                                                                "1"), None),
                 ("RESTORE_TARGET_NOT_EMPTY", references[1], "written")]
        cases[3] = ("RESTORE_TARGET_NOT_EMPTY", references[0], "written")
        for number, (code, value, state) in enumerate(cases):
            if state == "written":
                self.vault.corrupt.clear()
            target = Native(self.root / ("refused-%d.db" % number), self.configuration)
            if state == "written":
                target.ingestions.commit(message(payload("x", 7, 1), "x"), payload("x", 7, 1),
                                         self.configuration["datasets"]["ods.orders"], lambda: None)
            _, restorer = self.runtime(target)
            identity = "restore_" + ("%032x" % (number + 16))
            self.call(restorer, "restore_native", restore_id=identity, manifest=value)
            result = self.wait(restorer, "restore_status", restore_id=identity)
            self.assertEqual((result["state"], result.get("code")), ("FAILED", code))
            tables = {row[0] for row in target.connection.execute(
                "SELECT schema_name||'.'||table_name FROM duckdb_tables() WHERE database_name='warehouse'").fetchall()}
            self.assertNotIn("dwd.orders", tables)

    def test_manifest_ddl_that_is_not_the_recorded_catalog_is_refused_before_any_effect(self):
        _, backups = self.runtime(self.source)
        backup = self.backup(backups, "backup_" + "6" * 32)
        reference = {name: backup["manifest"][name] for name in ["bucket", "key", "version_id", "sha256"]}
        manifest = json.loads(self.vault.objects[reference["key"]]["raw"])
        position = next(number for number, table in enumerate(manifest["tables"])
                        if (table["schema"], table["table"]) == ("dwd", "orders"))
        original = manifest["tables"][position]["sql"]
        leak = self.root / "leak.csv"
        replacements = [
            (["tables", position, "sql"], "CREATE TABLE dwd.orders AS SELECT 1 AS id"),
            (["tables", position, "sql"], original + " COPY (SELECT 1) TO '" + str(leak) + "';"),
            (["tables", position, "columns", 0, 1], "INTEGER) FROM read_csv('/etc/passwd') --"),
        ]
        published = dict(self.vault.objects[reference["key"]])
        for number, (path, replacement) in enumerate(replacements):
            self.vault.objects[reference["key"]] = dict(published)
            target = Native(self.root / ("tampered-%d.db" % number), self.configuration)
            _, restorer = self.runtime(target)
            identity = "restore_" + ("%032x" % (number + 32))
            self.call(restorer, "restore_native", restore_id=identity,
                      manifest=self.republish(reference, path, replacement))
            result = self.wait(restorer, "restore_status", restore_id=identity)
            self.assertEqual((result["state"], result.get("code")), ("FAILED", "RESTORE_MANIFEST_DDL_REJECTED"))
            tables = {row[0] for row in target.connection.execute(
                "SELECT schema_name||'.'||table_name FROM duckdb_tables() WHERE database_name='warehouse'").fetchall()}
            self.assertNotIn("dwd.orders", tables)
            self.assertFalse(leak.exists())

    def test_an_initialized_but_never_committed_target_is_replaced(self):
        _, backups = self.runtime(self.source)
        backup = self.backup(backups)
        target = Native(self.root / "initialized.db", self.configuration)
        target.ingestions.initialize()
        _, restorer = self.runtime(target)
        self.call(restorer, "restore_native", restore_id="restore_" + "6" * 32, manifest={
            name: backup["manifest"][name] for name in ["bucket", "key", "version_id", "sha256"]})
        self.assertEqual(self.wait(restorer, "restore_status", restore_id="restore_" + "6" * 32)["state"],
                         "COMPLETED")

    def test_backup_requires_the_backup_scope(self):
        _, backups = self.runtime(self.source, scopes=("monitor:read",))
        with self.assertRaises(ControlError) as raised:
            self.call(backups, "backup_native", backup_id="backup_" + "7" * 32)
        self.assertEqual(raised.exception.status, 403)

    def republish(self, reference, path, replacement):
        """Replace a published manifest's content (a new version under the same key)."""
        stored = self.vault.objects[reference["key"]]
        value = json.loads(stored["raw"])
        target = value
        for name in path[:-1]:
            target = target[name]
        target[path[-1]] = replacement
        raw = (canonical(value) + "\n").encode()
        stored.update(raw=raw, version=stored["version"] + "-replaced",
                      checksum=base64.b64encode(hashlib.sha256(raw).digest()).decode())
        return {**reference, "version_id": stored["version"], "sha256": hashlib.sha256(raw).hexdigest()}


if __name__ == "__main__":
    unittest.main()
