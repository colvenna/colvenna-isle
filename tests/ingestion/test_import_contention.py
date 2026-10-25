"""Actual Parquet chunks, FIFO transactions and direct writes share one sequence."""

import copy
import hashlib
import io
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

import duckdb

from colvenna_isle.control.registry import ControlError
from colvenna_isle.writer.ingestion import IngestionStore, digest
from colvenna_isle.writer.qualification_import import QualificationImports
from colvenna_isle.writer.scheduler import Scheduler
from colvenna_isle.writer.write_qualification import WriteQualification
from colvenna_isle.writer.writes import WriteStore, write_id
from test_transactions import REGISTERED as INGESTION_REGISTERED
from test_transactions import message, payload
from test_writes import REGISTERED, request


class NativeImportContention(unittest.TestCase):
    def setUp(self):
        self.assertEqual(duckdb.__version__, "1.5.6")
        self.directory = tempfile.TemporaryDirectory()
        self.work = Path(self.directory.name)
        self.connection = duckdb.connect()
        self.connection.execute("ATTACH '" + str(self.work / "warehouse.db") + "' AS warehouse")
        for schema in ["ctl", "ods", "dwd"]:
            self.connection.execute("CREATE SCHEMA warehouse." + schema)
        definition = {
            "dataset": "dwd.import_probe", "schema_version": 1, "write_owner": "pipeline",
            "schema": QualificationImports.SCHEMA, "key_columns": ["id"], "partition_columns": [],
        }
        self.configuration = {
            "space_id": "probe_contention_test", "generation": "gen_" + "a" * 32, "route_version": 1,
            "datasets": {"ods.orders": copy.deepcopy(INGESTION_REGISTERED), "dwd.orders": copy.deepcopy(REGISTERED),
                         "dwd.import_probe": {"definition": definition, "definition_sha256": digest(definition)}},
        }
        native = IngestionStore(self.connection, self.configuration, lambda: {"epoch": 1})
        self.database = SimpleNamespace(connection=self.connection, ingestions=native, work=self.work)
        self.database.writes = WriteStore(native)
        self.runtime = SimpleNamespace(
            identity={name: name for name in ["task_arn", "task_definition", "image_digest", "source_commit",
                                             "process_nonce", "pid", "start_ticks", "boot_id", "address"]},
            lock=threading.Lock(), guard=lambda epoch=None: {"epoch": 1},
            registry=SimpleNamespace(_read=lambda space: self.configuration),
            database=lambda configuration: self.database, log=lambda *args: None,
            probe_space="probe_contention_test", probe_generation=self.configuration["generation"],
        )
        self.runtime.scheduler = Scheduler(self.runtime.lock)
        self.runtime.write_qualification = WriteQualification(self.runtime)
        path = self.work / "source.parquet"
        reader = duckdb.connect()
        reader.execute("COPY (SELECT range::BIGINT AS id,'value'::VARCHAR AS value FROM range(1001)) TO '"
                       + str(path) + "' (FORMAT PARQUET)")
        reader.close()
        self.raw = path.read_bytes()
        self.runtime.consumer = SimpleNamespace(
            bucket="fixture-bucket", account="123456789012", s3=SimpleNamespace(get_object=self.get_object))
        self.imports = QualificationImports(self.runtime)
        self.identity = "import_" + "b" * 64

    def get_object(self, **options):
        self.assertEqual(options["VersionId"], "fixed")
        return {"VersionId": "fixed", "Body": io.BytesIO(self.raw)}

    def tearDown(self):
        self.connection.close()
        self.directory.cleanup()

    def rpc(self):
        space, generation = self.configuration["space_id"], self.configuration["generation"]
        return {
            "identity": self.runtime.identity, "epoch": 1, "space_id": space, "generation": generation,
            "operation": "probe_import", "phase": "start", "import_id": self.identity,
            "source": {"bucket": "fixture-bucket",
                       "key": f"spaces/{space}/qualification-import/{generation}/{self.identity}.parquet",
                       "version_id": "fixed", "sha256": hashlib.sha256(self.raw).hexdigest(),
                       "bytes": len(self.raw), "rows": 1001},
        }

    def wait_job(self):
        deadline = time.monotonic() + 10
        while self.imports.jobs[self.identity]["state"] not in {"COMMITTED", "UNKNOWN"}:
            if time.monotonic() > deadline:
                self.fail("import observer expired")
            time.sleep(0.01)
        return self.imports.jobs[self.identity]["state"]

    def test_actual_parquet_and_other_writes_advance_between_bounded_native_chunks(self):
        entered, release = threading.Event(), threading.Event()
        original = self.imports.chunk

        def chunk(database, identity, candidate, number, rows, recheck):
            if number == 0:
                entered.set()
                self.assertTrue(release.wait(5))
            return original(database, identity, candidate, number, rows, recheck)
        self.imports.chunk = chunk
        self.assertEqual(self.imports.execute(self.rpc())["state"], "PREPARING")
        self.assertTrue(entered.wait(5))
        space, generation = self.configuration["space_id"], self.configuration["generation"]
        direct = self.runtime.scheduler.submit("direct", "write", lambda: self.database.writes.commit(
            write_id(space, generation, "dwd.orders", "original"), "owner", request(), REGISTERED, lambda: None))
        value = payload()
        envelope = {**message(value), "space_id": space, "generation": generation}
        ingestion = self.runtime.scheduler.submit("ingestion", "fifo", lambda: self.database.ingestions.commit(
            envelope, value, INGESTION_REGISTERED, lambda: None))
        release.set()
        self.assertEqual(direct.future.result(timeout=5)["commit_sequence"], 2)
        self.assertEqual(ingestion.future.result(timeout=5)["commit_sequence"], 3)
        self.assertEqual(self.wait_job(), "COMMITTED")
        status = {**self.rpc(), "phase": "status"}
        observed = self.imports.execute(status)
        self.assertEqual(observed["observed"], (1001, 0, 1000, 500500))
        self.assertEqual(observed["chunks"], [(0, 1, 500), (1, 4, 500), (2, 5, 1)])
        self.assertEqual(self.database.writes.probe("dwd.orders")["sequence"], 5)
        self.assertEqual(len(self.database.ingestions.probe("ods.orders")["rows"]), 1)
        self.assertEqual(self.imports.execute(self.rpc())["state"], "COMMITTED")
        changed = self.rpc()
        changed["source"]["version_id"] = "replacement"
        with self.assertRaises(ControlError) as rejected:
            self.imports.execute(changed)
        self.assertEqual(rejected.exception.code, "QUALIFICATION_IMPORT_CONFLICT")

    def test_changed_hash_never_creates_native_candidate_or_committed_progress(self):
        changed = self.rpc()
        changed["source"]["sha256"] = "0" * 64
        self.imports.execute(changed)
        self.assertEqual(self.wait_job(), "UNKNOWN")
        self.assertEqual(self.connection.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name='warehouse' AND "
            "table_name LIKE '_colvenna_import_%'").fetchone()[0], 0)
        self.assertEqual(self.imports.execute({**changed, "phase": "status"})["state"], "UNKNOWN")

    def test_nonadmitted_generation_and_outside_staging_key_are_denied(self):
        changed = self.rpc()
        changed["generation"] = "gen_" + "c" * 32
        with self.assertRaises(ControlError):
            self.imports.execute(changed)
        changed = self.rpc()
        changed["source"]["key"] = "spaces/other/qualification-import/other/file.parquet"
        with self.assertRaises(ControlError):
            self.imports.execute(changed)
        self.assertEqual(self.imports.jobs, {})


if __name__ == "__main__":
    unittest.main()
