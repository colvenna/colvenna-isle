"""Bounded round-robin transaction admission to one native writer lock.

Work kinds take turns (direct, ingestion, import, export); within a kind,
spaces take turns, so one space's long import or export cannot starve another
space's work of the same kind. Wait times and rejections are observable.
"""

import math
import threading
import time
from collections import deque
from concurrent.futures import Future
from dataclasses import dataclass

WAIT_WINDOW_SECONDS = 300


class NotSubmitted(RuntimeError):
    """No native execution can continue for this rejected/cancelled queue item."""


@dataclass
class Work:
    kind: str
    key: str
    action: object
    future: Future
    queued_at: float
    started_at: float | None = None
    unexecuted_original: bool = False
    space: str | None = None


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, math.ceil(len(ordered) * fraction) - 1))] if ordered else None


class Scheduler:
    KINDS = ("direct", "ingestion", "import", "export")

    def __init__(self, native_lock, max_pending=32, max_direct=16, max_space=24):
        self.native_lock = native_lock
        self.max_pending, self.max_direct, self.max_space = max_pending, max_direct, max_space
        self.condition = threading.Condition()
        self.queues = {kind: deque() for kind in self.KINDS}
        self.originals = {}
        self.next_kind = 0
        self.last_space = {kind: None for kind in self.KINDS}
        self.finished = {kind: 0 for kind in self.KINDS}
        self.rejected = {kind: 0 for kind in self.KINDS}
        self.waits = {kind: deque(maxlen=512) for kind in self.KINDS}
        self.worker = threading.Thread(target=self.run, daemon=True, name="native-writer-scheduler")
        self.worker.start()

    def submit(self, kind, key, action, space=None):
        if kind not in self.queues:
            raise ValueError("Unknown writer work kind")
        with self.condition:
            prior = self.originals.get((kind, key))
            if prior:
                return prior
            if (len(self.originals) >= self.max_pending
                    or kind == "direct" and sum(item.kind == "direct" for item in self.originals.values()) >= self.max_direct
                    or space is not None and sum(item.space == space for item in self.originals.values())
                    >= self.max_space):
                # A full queue is a positive rejection before any native effect.
                self.rejected[kind] += 1
                raise NotSubmitted("WRITER_QUEUE_FULL")
            item = Work(kind, key, action, Future(), time.monotonic(), space=space)
            self.originals[(kind, key)] = item
            self.queues[kind].append(item)
            self.condition.notify()
            return item

    def cancel_pending(self, item):
        with self.condition:
            if item.started_at is not None or item.future.done():
                return False
            self.queues[item.kind].remove(item)
            self.originals.pop((item.kind, item.key))
            item.future.set_exception(NotSubmitted("WAIT_BUDGET_BEFORE_EXECUTION"))
            return True

    def observation(self, space=None):
        with self.condition:
            now = time.monotonic()
            waits = {}
            for kind in self.KINDS:
                recent = [wait for at, wait in self.waits[kind] if now - at <= WAIT_WINDOW_SECONDS]
                waits[kind] = {"samples": len(recent), "p50": percentile(recent, 0.5),
                               "p95": percentile(recent, 0.95), "max": max(recent) if recent else None}
            value = {"pending": len(self.originals), "queued": {kind: len(queue) for kind, queue in self.queues.items()},
                     "finished": dict(self.finished), "rejected": dict(self.rejected), "observed_unix": time.time(),
                     "max_pending": self.max_pending, "max_direct": self.max_direct, "max_space": self.max_space,
                     "wait_ms": waits, "wait_window_seconds": WAIT_WINDOW_SECONDS}
            if space is not None:
                value["space_pending"] = sum(item.space == space for item in self.originals.values())
            return value

    def take(self):
        with self.condition:
            while not any(self.queues.values()):
                self.condition.wait()
            for offset in range(len(self.KINDS)):
                index = (self.next_kind + offset) % len(self.KINDS)
                kind = self.KINDS[index]
                queue = self.queues[kind]
                if queue:
                    # Oldest item of a different space than this kind served last, if any.
                    item = next((value for value in queue if value.space != self.last_space[kind]), queue[0])
                    queue.remove(item)
                    item.started_at = time.monotonic()
                    self.last_space[kind] = item.space
                    self.waits[kind].append((item.started_at, round((item.started_at - item.queued_at) * 1000, 3)))
                    self.next_kind = (index + 1) % len(self.KINDS)
                    return item

    def run(self):
        while True:
            item = self.take()
            try:
                with self.native_lock:
                    result = item.action()
            except BaseException as error:
                item.future.set_exception(error)
            else:
                item.future.set_result(result)
            finally:
                with self.condition:
                    self.originals.pop((item.kind, item.key))
                    self.finished[item.kind] += 1
                    self.condition.notify_all()
