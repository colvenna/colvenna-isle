"""Writer input boundary with actual native transactions and versioned S3 double."""

import copy
import threading
import time
import unittest
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from colvenna_isle.control.registry import ControlError, Principal
from colvenna_isle.pipeline.input_exports import InputExports
from colvenna_isle.writer.ingestion import CommitUnknown
from colvenna_isle.writer.inputs import Inputs
from colvenna_isle.writer.scheduler import Scheduler
from test_input_objects import ObjectStore
import test_native_inputs as native_fixture


class WriterInputs(unittest.TestCase):
    def setUp(self):
        self.fixture = native_fixture.NativeExports()
        self.fixture.setUp()
        configuration = self.fixture.configuration
        configuration["route_version"] = 1
        for registered in configuration["datasets"].values():
            registered["registered_revision"] = 1
        self.actor = Principal("owner", frozenset(["fixture"]), frozenset(["pipeline:run"]),
            "key_owner", "gen_fixture")
        self.active, self.epoch = True, 1
        self.storage = ObjectStore()
        self.identity = {"task_arn": "original", "task_definition": "writer:1",
            "source_commit": "a" * 40, "image_digest": "sha256:" + "b" * 64,
            "pid": 1, "start_ticks": 2, "boot_id": "boot", "process_nonce": "original",
            "address": "10.0.0.2"}
        self.database = SimpleNamespace(ingestions=self.fixture.store.native,
            connection=self.fixture.writer, work=self.fixture.directory)
        self.runtime = SimpleNamespace(identity=self.identity, lock=threading.Lock(),
            registry=SimpleNamespace(_read=lambda space: copy.deepcopy(configuration)),
            direct=SimpleNamespace(keys=SimpleNamespace(authenticate_id=self.authenticate)),
            consumer=SimpleNamespace(s3=self.storage, bucket="bucket", account="123456789012"),
            guard=self.guard, database=lambda configuration: self.database)
        self.runtime.scheduler = Scheduler(self.runtime.lock)
        self.inputs = Inputs(self.runtime)
        self.request = {"operation": "input_prepare", "identity": self.identity, "epoch": self.epoch,
            "space_id": "fixture", "generation": "gen_fixture", "key_id": "key_owner",
            "principal_id": "owner", "client_request_id": "original", "retention_seconds": 600,
            "inputs": self.fixture.bindings}

    def tearDown(self):
        self.fixture.tearDown()

    def authenticate(self, identity):
        if not self.active or identity != self.actor.key_id:
            raise ControlError(403, "FORBIDDEN")
        return self.actor

    def guard(self, epoch):
        if epoch != self.epoch:
            raise ControlError(409, "WRITER_IDENTITY_CHANGED")
        return {"epoch": epoch}

    def operation(self, operation, export_id, **fields):
        return self.inputs.execute({**self.request, **fields, "operation": operation, "export_id": export_id})

    def test_unconfirmed_capture_close_blocks_reuse_of_the_shared_native_writer(self):
        self.request = {**self.request, "inputs": {"orders": self.fixture.bindings["orders"]}}

        class LostReadCommitReply:
            def __init__(self, connection):
                self.connection = connection

            def execute(self, statement, *args):
                value = self.connection.execute(statement, *args)
                if statement == "COMMIT":
                    raise OSError("original read commit reply lost")
                return value

        # Admission and business writes use the same real Native instance.
        # Only the export connection's successful read COMMIT loses its reply.
        self.database.connection = LostReadCommitReply(self.fixture.writer)
        with self.assertRaises(Exception):
            self.inputs.execute(self.request)
        self.assertEqual(self.storage.counter, 0)
        with self.assertRaises(CommitUnknown):
            self.fixture.mutate("dwd.orders", "upsert", "after_unknown_capture",
                [{"id": 1, "amount": "99.99"}])
        with self.assertRaises(CommitUnknown):
            self.inputs.execute(self.request)
        self.fixture.reader.close()
        self.fixture.writer.close()
        import duckdb

        with duckdb.connect() as reopened:
            reopened.execute("ATTACH '" + str(self.fixture.path).replace("'", "''") + "' AS warehouse")
            store = InputExports(native_fixture.IngestionStore(
                reopened, self.fixture.configuration, lambda: {"epoch": 2}))
            original, fresh = store.admit(self.actor, self.fixture.configuration,
                self.request["inputs"], self.request["client_request_id"],
                self.request["retention_seconds"], {**self.identity, "epoch": 2}, lambda: None)
            self.assertFalse(fresh)
            self.assertEqual(Inputs.summary(original)["state"], "UNKNOWN")
            self.assertEqual(store.retained_bytes(), 33554432)

    def test_unconfirmed_capture_begin_and_rollback_block_shared_native_writes(self):
        class LostBeginAndRollbackReplies:
            def __init__(self, connection):
                self.connection = connection

            def execute(self, statement, *args):
                value = self.connection.execute(statement, *args)
                if statement in {"BEGIN", "ROLLBACK"}:
                    raise OSError("original Native transaction reply lost")
                return value

        self.request = {**self.request, "inputs": {"orders": self.fixture.bindings["orders"]}}
        self.database.connection = LostBeginAndRollbackReplies(self.fixture.writer)
        with self.assertRaises(Exception):
            self.inputs.execute(self.request)
        self.assertEqual(self.storage.counter, 0)
        with self.assertRaises(CommitUnknown):
            self.fixture.mutate("dwd.orders", "upsert", "after_unknown_begin",
                [{"id": 1, "amount": "99.99"}])

    def test_concurrent_nonce_rebinding_still_reaches_original_native_binding_check(self):
        entered, release = threading.Event(), threading.Event()
        store = self.inputs.store(self.fixture.configuration)[1]
        original_admit = store.admit

        def blocked(*args, **kwargs):
            if not entered.is_set():
                entered.set()
                self.assertTrue(release.wait(5))
            return original_admit(*args, **kwargs)

        store.admit = blocked
        submitted = threading.Event()
        original_submit = self.runtime.scheduler.submit
        submissions = []

        def submit(*args, **kwargs):
            work = original_submit(*args, **kwargs)
            submissions.append(work)
            if len(submissions) == 2:
                submitted.set()
            return work

        self.runtime.scheduler.submit = submit
        with ThreadPoolExecutor(max_workers=2) as pool:
            original = pool.submit(self.inputs.execute, self.request)
            self.assertTrue(entered.wait(3))
            conflict = pool.submit(self.inputs.execute,
                {**self.request, "inputs": {"orders": self.fixture.bindings["orders"]}})
            try:
                self.assertTrue(submitted.wait(3))
            finally:
                release.set()
            self.assertEqual(original.result(timeout=5)["state"], "READY")
            with self.assertRaisesRegex(ControlError, "INPUT_EXPORT_BINDING_CONFLICT"):
                conflict.result(timeout=5)
        self.assertEqual(self.storage.counter, 3)

    def row(self, manifest, alias="orders"):
        file = manifest["inputs"][alias]["manifest"]["files"][0]
        key = file["s3_uri"].removeprefix("s3://bucket/")
        raw = next(value[1] for value in self.storage.objects[key] if value[0] == file["object_version"])
        return pq.read_table(pa.BufferReader(raw)).to_pylist()

    def test_original_ready_input_is_stable_while_new_export_observes_new_native_commit(self):
        original = self.inputs.execute(self.request)
        self.assertEqual(original["state"], "READY")
        self.assertEqual(self.row(original["manifest"])[0]["amount"].as_tuple().exponent, -2)
        original_manifest = copy.deepcopy(original["manifest"])
        self.fixture.mutate("dwd.orders", "upsert", "new_write", [{"id": 1, "amount": "99.99"}])
        repeat = self.inputs.execute(self.request)
        self.assertEqual(repeat["export_id"], original["export_id"])
        self.assertEqual(repeat["manifest"], original_manifest)
        self.assertEqual(str(self.row(repeat["manifest"])[0]["amount"]), "12.30")
        newer = self.inputs.execute({**self.request, "client_request_id": "new_original"})
        self.assertEqual(str(self.row(newer["manifest"])[0]["amount"]), "99.99")
        self.assertEqual(newer["manifest"]["inputs"]["orders"]["native_version"]["snapshot_sequence"], 3)

    def test_ingestion_lane_can_commit_while_original_upload_is_blocked(self):
        entered, release = threading.Event(), threading.Event()
        original_put = self.storage.put_object

        def blocked(**request):
            if request["Key"].endswith("part-00000.parquet") and not entered.is_set():
                entered.set()
                if not release.wait(5):
                    raise RuntimeError("Controlled upload not released")
            return original_put(**request)

        self.storage.put_object = blocked
        with ThreadPoolExecutor(max_workers=1) as pool:
            result = pool.submit(self.inputs.execute, self.request)
            self.assertTrue(entered.wait(3))
            work = self.runtime.scheduler.submit("ingestion", "new_native_commit",
                lambda: self.fixture.mutate("dwd.orders", "upsert", "during_upload",
                    [{"id": 1, "amount": "99.99"}]))
            try:
                work.future.result(timeout=3)
            finally:
                release.set()
            original = result.result(timeout=5)
        self.assertEqual(original["state"], "READY")
        self.assertEqual(str(self.row(original["manifest"])[0]["amount"]), "12.30")
        self.assertEqual(original["manifest"]["inputs"]["orders"]["native_version"]["snapshot_sequence"], 2)

    def test_writer_switch_recovers_uploaded_original_without_recapturing_source(self):
        original_put = self.storage.put_object

        def refuse_manifest(**request):
            if request["Key"].endswith("manifest.json"):
                self.storage.fail_key = request["Key"]
            return original_put(**request)

        self.storage.put_object = refuse_manifest
        with self.assertRaisesRegex(ControlError, "INPUT_OBJECT_UNCONFIRMED"):
            self.inputs.execute(self.request)
        captured = [value for values in self.storage.objects.values() for value in values]
        self.assertEqual(len(captured), 2)
        self.fixture.mutate("dwd.orders", "upsert", "after_unknown", [{"id": 1, "amount": "99.99"}])
        self.storage.put_object = original_put
        self.storage.fail_key = None
        self.runtime.identity = {**self.identity, "task_arn": "replacement",
            "process_nonce": "replacement", "address": "10.0.0.3"}
        self.epoch = 2
        self.database.work = self.fixture.directory / "replacement"
        self.database.work.mkdir()
        self.inputs = Inputs(self.runtime)
        with self.assertRaisesRegex(ControlError, "WRITER_IDENTITY_CHANGED"):
            self.inputs.execute(self.request)
        restored = self.inputs.execute({**self.request, "identity": self.runtime.identity, "epoch": 2})
        self.assertEqual(restored["state"], "READY")
        self.assertEqual(str(self.row(restored["manifest"])[0]["amount"]), "12.30")
        self.assertEqual(restored["manifest"]["inputs"]["orders"]["native_version"]["snapshot_sequence"], 2)
        self.assertEqual(self.storage.counter, 3)

    def test_retention_reference_keeps_original_available_after_requested_expiry(self):
        original = self.inputs.execute(self.request)
        store = next(iter(self.inputs.stores.values()))
        pinned = self.operation("input_retain", original["export_id"], consumer_id="run_original",
            retain_until_unix=time.time() + 900)
        self.assertEqual(pinned["state"], "READY")
        store.clock = lambda: time.time() + 1000
        value = self.operation("input_manifest", original["export_id"])
        self.assertEqual(value["manifest"], original["manifest"])
        self.assertEqual(self.storage.counter, 3)
        other = self.inputs.execute({**self.request, "client_request_id": "without_reference"})
        store.clock = lambda: time.time() + 2000
        with self.assertRaisesRegex(ControlError, "INPUT_RETENTION_EXPIRED"):
            self.operation("input_manifest", other["export_id"])

    def test_released_consumers_free_export_quota_but_retry_can_repin_before_expiry(self):
        original = self.inputs.execute(self.request)
        store = next(iter(self.inputs.stores.values()))
        self.operation("input_retain", original["export_id"], consumer_id="job_one",
                       retain_until_unix=time.time() + 900)
        record = store.get(self.actor, original["export_id"])
        self.assertTrue(store.live(record))
        self.assertEqual(store.retained_bytes(), record["retained_bytes"])
        self.operation("input_release", original["export_id"], consumer_id="job_one")
        released = store.get(self.actor, original["export_id"])
        # Every consumer ended: no capacity is reserved although retention has not expired.
        self.assertFalse(store.live(released))
        self.assertEqual(store.retained_bytes(), 0)
        with patch("colvenna_isle.pipeline.input_exports.MAX_EXPORTS", 1):
            self.inputs.execute({**self.request, "client_request_id": "next_original"})
        # A retry of the same job pins the original again while it is still retained.
        self.operation("input_retain", original["export_id"], consumer_id="job_one",
                       retain_until_unix=time.time() + 900)
        self.assertTrue(store.live(store.get(self.actor, original["export_id"])))
        self.assertEqual(self.operation("input_manifest", original["export_id"])["manifest"], original["manifest"])

    def test_expiry_during_upload_does_not_approve_an_unretained_manifest(self):
        store = self.inputs.store(self.fixture.configuration)[1]
        store.clock = lambda: 1000
        original_put = self.storage.put_object

        def expire(**request):
            result = original_put(**request)
            if request["Key"].endswith("manifest.json"):
                store.clock = lambda: 2000
            return result

        self.storage.put_object = expire
        with self.assertRaisesRegex(ControlError, "INPUT_RETENTION_EXPIRED"):
            self.inputs.execute(self.request)
        original = next(value for value in self.fixture.writer.execute(
            "SELECT record FROM warehouse.ctl.input_exports").fetchall())
        record = json.loads(original[0])
        self.assertEqual(record["state"], "STAGED")
        self.assertNotIn("manifest", record)
        self.assertEqual(self.storage.counter, 3)
        with self.assertRaisesRegex(ControlError, "INPUT_RETENTION_EXPIRED"):
            self.inputs.execute(self.request)
        self.assertEqual(self.storage.counter, 3)

    def test_unknown_original_admission_never_recaptures_native_tables(self):
        store = self.inputs.store(self.fixture.configuration)[1]
        record, fresh = store.admit(self.actor, self.fixture.configuration,
            self.fixture.bindings, self.request["client_request_id"], 600,
            {**self.identity, "epoch": 1}, lambda: self.guard(1))
        self.assertTrue(fresh)
        self.fixture.mutate("dwd.orders", "upsert", "after_unknown", [{"id": 1, "amount": "99.99"}])
        with self.assertRaisesRegex(ControlError, "INPUT_EXPORT_CUSTODY_UNKNOWN"):
            self.inputs.execute(self.request)
        status = self.operation("input_status", record["export_id"])
        self.assertEqual(status["state"], "UNKNOWN")
        self.assertEqual(self.storage.counter, 0)
        self.assertFalse((self.database.work / "inputs").exists())

    def test_failed_partial_exports_keep_reserved_bytes_and_quota_precedes_next_capture(self):
        from colvenna_isle.pipeline.input_objects import MAX_FILE_BYTES
        from colvenna_isle.pipeline.native_inputs import capture

        def partial(connection, configuration, bindings, directory, **kwargs):
            original_guard = kwargs["guard"]

            def fail_after_file_creation():
                original_guard()
                if list(Path(directory).glob("*.parquet")):
                    raise ControlError(503, "CONTROLLED_PARTIAL_EXPORT")

            return capture(connection, configuration, bindings, directory,
                           **{**kwargs, "guard": fail_after_file_creation})

        with patch("colvenna_isle.pipeline.input_exports.MAX_RETAINED_BYTES", MAX_FILE_BYTES):
            with patch("colvenna_isle.writer.inputs.capture", partial):
                with self.assertRaisesRegex(ControlError, "CONTROLLED_PARTIAL_EXPORT"):
                    self.inputs.execute(self.request)
            record = json.loads(self.fixture.writer.execute(
                "SELECT record FROM warehouse.ctl.input_exports").fetchone()[0])
            self.assertEqual(record["state"], "FAILED")
            self.assertEqual(record["retained_bytes"], MAX_FILE_BYTES)
            partial_files = list((self.database.work / "inputs").rglob("*.parquet"))
            self.assertTrue(partial_files)
            with self.assertRaisesRegex(ControlError, "INPUT_RETAINED_BYTES_QUOTA"):
                self.inputs.execute({**self.request, "client_request_id": "second_original"})
            self.assertEqual(list((self.database.work / "inputs").rglob("*.parquet")), partial_files)
            self.assertEqual(self.storage.counter, 0)

    def test_ready_files_charge_actual_bytes_after_initial_maximum_reservation(self):
        original = self.inputs.execute(self.request)
        store = next(iter(self.inputs.stores.values()))
        record = store.get(self.actor, original["export_id"])
        self.assertEqual(record["retained_bytes"], original["metrics"]["copied_bytes"])
        self.assertEqual(store.retained_bytes(), record["retained_bytes"])

    def test_whole_input_request_slots_bound_concurrent_uploads_and_consumer_reads(self):
        entered = threading.Barrier(5)
        release = threading.Event()
        original_prepare = self.inputs.objects.prepare

        def blocked(*args, **kwargs):
            entered.wait(timeout=5)
            self.assertTrue(release.wait(5))
            return original_prepare(*args, **kwargs)

        self.inputs.objects.prepare = blocked
        with ThreadPoolExecutor(max_workers=4) as pool:
            requests = [pool.submit(self.inputs.execute, self.request) for _ in range(4)]
            try:
                entered.wait(timeout=5)
                with self.assertRaisesRegex(ControlError, "INPUT_EXPORT_EXECUTOR_QUOTA"):
                    self.inputs.execute(self.request)
                with self.assertRaisesRegex(ControlError, "INPUT_EXPORT_EXECUTOR_QUOTA"):
                    self.operation("input_manifest", "input_queued")
            finally:
                release.set()
            results = [request.result(timeout=5) for request in requests]
        self.assertTrue(all(result["state"] == "READY" for result in results))
        self.assertTrue(all(result["manifest"] == results[0]["manifest"] for result in results))
        self.assertEqual(self.storage.counter, 3)
        self.assertEqual(self.operation("input_manifest", results[0]["export_id"])["state"], "READY")

    def test_nonce_rebinding_foreign_owner_and_forged_identity_do_not_read_or_export(self):
        original = self.inputs.execute(self.request)
        with self.assertRaisesRegex(ControlError, "INPUT_EXPORT_BINDING_CONFLICT"):
            self.inputs.execute({**self.request, "inputs": {"orders": self.fixture.bindings["orders"]}})
        for fields in [{"principal_id": "forged"}, {"space_id": "other"},
                {"generation": "gen_other"}, {"epoch": 2},
                {"identity": {**self.identity, "address": "10.0.0.9"}}]:
            with self.subTest(fields=fields), self.assertRaises(ControlError):
                self.operation("input_manifest", original["export_id"], **fields)
        self.actor = Principal("other", frozenset(["fixture"]), frozenset(["pipeline:run"]),
            "key_other", "gen_fixture")
        with self.assertRaisesRegex(ControlError, "INPUT_OWNER_REQUIRED"):
            self.operation("input_manifest", original["export_id"], principal_id="other", key_id="key_other")
        self.assertEqual(self.storage.counter, 3)

    def test_partial_and_missing_artifacts_never_pass_the_writer_boundary(self):
        original = self.inputs.execute(self.request)
        key = next(key for key in self.storage.objects if key.endswith("orders/part-00000.parquet"))
        self.storage.objects.pop(key)
        with self.assertRaisesRegex(ControlError, "INPUT_OBJECT_UNCONFIRMED"):
            self.operation("input_manifest", original["export_id"])
        self.active = False
        with self.assertRaisesRegex(ControlError, "FORBIDDEN"):
            self.inputs.execute(self.request)


if __name__ == "__main__":
    unittest.main()
