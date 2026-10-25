import threading
import unittest
from concurrent.futures import TimeoutError

from colvenna_isle.writer.scheduler import NotSubmitted, Scheduler


class SchedulingTests(unittest.TestCase):
    def test_round_robin_progress_and_duplicate_original(self):
        scheduler = Scheduler(threading.Lock())
        entered, release = threading.Event(), threading.Event()
        order = []
        def blocking():
            entered.set()
            self.assertTrue(release.wait(3))
            order.append("active")
        active = scheduler.submit("direct", "active", blocking)
        self.assertTrue(entered.wait(3))
        writes = [scheduler.submit("direct", str(index), lambda index=index: order.append("write" + str(index)))
                  for index in range(3)]
        self.assertIs(scheduler.submit("direct", "0", lambda: order.append("DUPLICATE")), writes[0])
        ingestion = scheduler.submit("ingestion", "fifo", lambda: order.append("fifo"))
        imported = scheduler.submit("import", "chunk", lambda: order.append("chunk"))
        release.set()
        for item in [active, *writes, ingestion, imported]:
            item.future.result(timeout=3)
        self.assertEqual(order, ["active", "fifo", "chunk", "write0", "write1", "write2"])

    def test_queue_limit_and_explicit_cancellation_cannot_execute_later(self):
        scheduler = Scheduler(threading.Lock(), max_pending=2)
        entered, release = threading.Event(), threading.Event()
        active = scheduler.submit("direct", "active", lambda: (entered.set(), release.wait(3)))
        self.assertTrue(entered.wait(3))
        effects = []
        pending = scheduler.submit("ingestion", "pending", lambda: effects.append("unwanted"))
        with self.assertRaises(NotSubmitted):
            scheduler.submit("import", "overload", lambda: None)
        self.assertFalse(scheduler.cancel_pending(active))
        self.assertTrue(scheduler.cancel_pending(pending))
        with self.assertRaises(NotSubmitted):
            pending.future.result()
        release.set()
        active.future.result(timeout=3)
        self.assertEqual(effects, [])

    def test_export_lane_shares_round_robin_with_ingestion_direct_and_import(self):
        scheduler = Scheduler(threading.Lock())
        entered, release = threading.Event(), threading.Event()
        active = scheduler.submit("direct", "active", lambda: (entered.set(), release.wait(3)))
        self.assertTrue(entered.wait(3))
        order = []
        pending = [scheduler.submit(kind, str(index), lambda kind=kind: order.append(kind))
            for index in range(2) for kind in scheduler.KINDS]
        release.set()
        for work in [active, *pending]:
            work.future.result(timeout=3)
        self.assertEqual(order, ["ingestion", "import", "export", "direct"] * 2)

    def test_wait_timeout_does_not_claim_rollback_of_started_work(self):
        scheduler = Scheduler(threading.Lock())
        entered, release = threading.Event(), threading.Event()
        effects = []
        def commit():
            entered.set()
            self.assertTrue(release.wait(3))
            effects.append("COMMITTED")
            return "original"
        item = scheduler.submit("direct", "original", commit)
        self.assertTrue(entered.wait(3))
        with self.assertRaises(TimeoutError):
            item.future.result(timeout=0.001)
        self.assertFalse(scheduler.cancel_pending(item))
        release.set()
        self.assertEqual(item.future.result(timeout=3), "original")
        self.assertEqual(effects, ["COMMITTED"])

    def test_spaces_take_turns_within_a_kind_and_waits_are_observed(self):
        scheduler = Scheduler(threading.Lock())
        entered, release = threading.Event(), threading.Event()
        order = []
        active = scheduler.submit("import", "blocker", lambda: (entered.set(), release.wait(3)), "space_a")
        self.assertTrue(entered.wait(3))
        # One space's long import queue cannot starve another space's import.
        work = [scheduler.submit("import", "a" + str(index), lambda index=index: order.append("a" + str(index)),
                                 "space_a") for index in range(3)]
        work.append(scheduler.submit("import", "b0", lambda: order.append("b0"), "space_b"))
        release.set()
        for item in [active, *work]:
            item.future.result(timeout=3)
        self.assertEqual(order, ["b0", "a0", "a1", "a2"])
        observed = scheduler.observation("space_a")
        self.assertEqual((observed["finished"]["import"], observed["space_pending"]), (5, 0))
        self.assertEqual(observed["wait_ms"]["import"]["samples"], 5)
        self.assertGreater(observed["wait_ms"]["import"]["max"], 0)
        self.assertEqual(observed["wait_ms"]["ingestion"], {"samples": 0, "p50": None, "p95": None, "max": None})

    def test_per_space_pending_cap_rejects_before_any_effect_and_is_counted(self):
        scheduler = Scheduler(threading.Lock(), max_pending=8, max_space=2)
        entered, release = threading.Event(), threading.Event()
        active = scheduler.submit("export", "blocker", lambda: (entered.set(), release.wait(3)), "busy")
        self.assertTrue(entered.wait(3))
        effects = []
        scheduler.submit("export", "second", lambda: effects.append("second"), "busy")
        with self.assertRaisesRegex(NotSubmitted, "WRITER_QUEUE_FULL"):
            scheduler.submit("export", "third", lambda: effects.append("third"), "busy")
        # Another space still has its own share of the queue.
        other = scheduler.submit("ingestion", "other", lambda: effects.append("other"), "quiet")
        release.set()
        active.future.result(timeout=3)
        other.future.result(timeout=3)
        self.assertNotIn("third", effects)
        self.assertEqual(scheduler.observation()["rejected"]["export"], 1)


if __name__ == "__main__":
    unittest.main()
