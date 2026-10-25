"""Concurrent public work reaches exactly one guarded DuckDB transaction."""

import copy
import io
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import duckdb
from botocore.exceptions import ClientError

from colvenna_isle.control.registry import ControlError, Principal
from colvenna_isle.writer.direct import DirectWrites
from colvenna_isle.writer.ingestion import IngestionStore
from colvenna_isle.writer.intents import Intents
from colvenna_isle.writer.scheduler import Scheduler
from colvenna_isle.writer.writes import WriteStore, write_id
from colvenna_isle.writer.write_qualification import WriteQualification
from test_writes import REGISTERED, request


class ObjectStore:
    def __init__(self):
        self.objects, self.lock = {}, threading.Lock()

    def put_object(self, **value):
        with self.lock:
            if value["Key"] in self.objects:
                raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
            self.objects[value["Key"]] = value
        return {"VersionId": "fixed"}

    def get_object(self, **value):
        with self.lock:
            original = self.objects.get(value["Key"])
            if original is None:
                raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
            return {"Body": io.BytesIO(original["Body"]), "Metadata": original["Metadata"], "VersionId": "fixed"}


class Keys:
    def __init__(self, principal):
        self.principal, self.active = principal, True

    def authenticate_id(self, key_id):
        if not self.active or key_id != self.principal.key_id:
            raise ControlError(403, "FORBIDDEN")
        return self.principal


class DirectTransactionTests(unittest.TestCase):
    def setUp(self):
        self.assertEqual(duckdb.__version__, "1.5.6")
        self.directory = tempfile.TemporaryDirectory()
        self.connection = duckdb.connect()
        self.connection.execute("ATTACH '" + str(Path(self.directory.name) / "warehouse.db") + "' AS warehouse")
        for schema in ["ctl", "dwd"]:
            self.connection.execute("CREATE SCHEMA warehouse." + schema)
        self.configuration = {
            "space_id": "fixture", "generation": "gen_fixture", "route_version": 1,
            "datasets": {"dwd.orders": copy.deepcopy(REGISTERED)},
        }
        self.native = IngestionStore(self.connection, self.configuration, lambda: {"epoch": 1})
        self.store = WriteStore(self.native)
        self.store.initialize()
        self.principal = Principal("owner", frozenset(["fixture"]), frozenset(["data:write"]),
                                   "key_fixture", "gen_fixture", "2027-01-01T00:00:00Z")
        self.keys, self.objects = Keys(self.principal), ObjectStore()
        identity = {name: name for name in [
            "task_arn", "task_definition", "image_digest", "source_commit", "process_nonce",
            "pid", "start_ticks", "boot_id", "address"]}
        self.runtime = SimpleNamespace(
            identity=identity, lock=threading.Lock(), guard=lambda epoch=None: {"epoch": 1},
            registry=SimpleNamespace(_read=lambda space: self.configuration),
            database=lambda configuration: SimpleNamespace(writes=self.store),
            consumer=SimpleNamespace(paused=False), log=lambda *args: None, last_commit=None,
        )
        self.runtime.scheduler = Scheduler(self.runtime.lock)
        self.runtime.write_qualification = WriteQualification(self.runtime)
        self.direct = DirectWrites(self.runtime, self.keys, Intents(self.objects, "bucket", "123456789012", "a" * 40))
        self.identity = write_id("fixture", "gen_fixture", "dwd.orders", "original")

    def tearDown(self):
        self.connection.close()
        self.directory.cleanup()

    def rpc(self, key="original", value=None, timeout=20000):
        return {
            "identity": self.runtime.identity, "epoch": 1, "space_id": "fixture", "generation": "gen_fixture",
            "key_id": "key_fixture", "principal_id": "owner", "operation": "write",
            "idempotency_key": key, "payload": value or request(), "deadline_unix_ms": int(time.time() * 1000) + timeout,
        }

    def test_four_concurrent_duplicates_share_one_native_effect_and_result(self):
        entered, release = threading.Event(), threading.Event()
        original = self.store.commit
        calls = []

        def commit(*args, **kwargs):
            calls.append(args[0])
            entered.set()
            self.assertTrue(release.wait(5))
            return original(*args, **kwargs)
        self.store.commit = commit
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(self.direct.execute, self.rpc()) for _ in range(4)]
            self.assertTrue(entered.wait(5))
            time.sleep(0.05)
            release.set()
            results = [future.result(timeout=5) for future in futures]
        # Each original caller has its own timing observation; the durable result is shared.
        shared = [{name: value for name, value in result.items() if name != "timing"} for result in results]
        self.assertTrue(all(result == shared[0] for result in shared))
        self.assertEqual(results[0]["status"], 200)
        self.assertEqual(calls, [self.identity])
        observed = self.store.probe("dwd.orders")
        self.assertEqual((len(observed["rows"]), len(observed["receipts"]), observed["sequence"]), (1, 1, 1))
        self.assertEqual(len(self.objects.objects), 1)

    def test_real_started_wait_timeout_can_commit_then_original_retry_reconciles(self):
        entered, release = threading.Event(), threading.Event()
        original = self.store.commit

        def commit(*args, **kwargs):
            entered.set()
            self.assertTrue(release.wait(5))
            return original(*args, **kwargs)
        self.store.commit = commit
        result = self.direct.execute(self.rpc(timeout=20))
        self.assertTrue(entered.wait(5))
        self.assertEqual((result["status"], result["result"]["status"]), (504, "UNKNOWN"))
        with self.runtime.scheduler.condition:
            work = next(iter(self.runtime.scheduler.originals.values()))
        release.set()
        committed = work.future.result(timeout=5)
        self.assertEqual(committed["status"], "COMMITTED")
        self.store.commit = original
        repeated = self.direct.execute(self.rpc())
        self.assertEqual(repeated["result"], committed)
        self.assertEqual(self.store.probe("dwd.orders")["sequence"], 1)

    def test_revoked_credential_at_final_commit_check_rolls_back_then_same_identity_resumes(self):
        original = self.store.commit

        def commit(*args, **kwargs):
            kwargs["before_commit"] = lambda: setattr(self.keys, "active", False)
            return original(*args, **kwargs)
        self.store.commit = commit
        with self.assertRaises(ControlError) as denied:
            self.direct.execute(self.rpc())
        self.assertEqual(denied.exception.status, 403)
        observed = self.store.probe("dwd.orders")
        self.assertEqual((observed["rows"], observed["receipts"], observed["sequence"]), ([], [], 0))
        self.assertEqual(self.store.status(self.identity, "owner")["status"], "UNKNOWN")
        self.store.commit, self.keys.active = original, True
        self.assertEqual(self.direct.execute(self.rpc())["result"]["commit_sequence"], 1)

    def test_fresh_full_queue_is_not_submitted_but_existing_original_is_unknown(self):
        entered, release = threading.Event(), threading.Event()
        self.runtime.scheduler.max_pending = 1
        active = self.runtime.scheduler.submit("import", "blocking", lambda: (entered.set(), release.wait(5)))
        self.assertTrue(entered.wait(5))
        first = self.direct.execute(self.rpc())
        self.assertEqual((first["status"], first["result"]["status"]), (429, "NOT_SUBMITTED"))
        repeated = self.direct.execute(self.rpc())
        self.assertEqual((repeated["status"], repeated["result"]["status"]), (503, "UNKNOWN"))
        self.assertIsNone(self.store.original(self.identity, "owner"))
        release.set()
        active.future.result(timeout=5)
        self.assertEqual(self.direct.execute(self.rpc())["result"]["commit_sequence"], 1)

    def test_changed_payload_during_inflight_cannot_coalesce_into_original(self):
        entered, release = threading.Event(), threading.Event()
        original = self.store.commit

        def commit(*args, **kwargs):
            entered.set()
            self.assertTrue(release.wait(5))
            return original(*args, **kwargs)
        self.store.commit = commit
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.direct.execute, self.rpc())
            self.assertTrue(entered.wait(5))
            changed = request(records=[{"id": 1, "code": "one", "amount": "2.00", "region": "west"}])
            with self.assertRaises(ControlError) as denied:
                self.direct.execute(self.rpc(value=changed))
            self.assertEqual(denied.exception.code, "WRITE_BINDING_CONFLICT")
            release.set()
            self.assertEqual(future.result(timeout=5)["status"], 200)
        self.assertEqual(self.store.probe("dwd.orders")["rows"], [[1, "one", "1", "west"]])

    def test_empty_scheduler_does_not_prove_an_original_rpc_cannot_still_execute(self):
        entered, release = threading.Event(), threading.Event()
        reserve = self.direct.intents.reserve

        def blocked_reservation(*args):
            entered.set()
            self.assertTrue(release.wait(5))
            return reserve(*args)
        self.direct.intents.reserve = blocked_reservation
        with ThreadPoolExecutor(max_workers=1) as pool:
            original_rpc = pool.submit(self.direct.execute, self.rpc())
            self.assertTrue(entered.wait(5))
            self.assertEqual(self.runtime.scheduler.observation()["pending"], 0)
            self.assertEqual(self.store.status(self.identity, "owner")["status"], "UNKNOWN")
            self.assertFalse(original_rpc.done())
            release.set()
            result = original_rpc.result(timeout=5)
        self.assertEqual((result["status"], result["result"]["commit_sequence"]), (200, 1))
        self.assertEqual(self.store.probe("dwd.orders")["rows"], [[1, "one", "1", "west"]])


if __name__ == "__main__":
    unittest.main()
