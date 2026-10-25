"""Monotonic per-request segment timing with one approved log line per original identity.

Each process measures only its own intervals with its own monotonic clock; no
segment is a difference of two hosts' wall clocks. A segment names its parent,
so nested intervals are never added twice and a parent's remainder is reported
as unattributed. A line carries only approved identities, stage names, counts
and durations: never keys, credentials, SQL, business values or results.
COLVENNA_TIMING=off disables recording, for measuring the instrumentation overhead.
"""

import json
import os
import time
from contextlib import contextmanager


def enabled():
    return os.environ.get("COLVENNA_TIMING", "on") != "off"


class Timing:
    def __init__(self, path, **identity):
        self.path, self.identity = path, dict(identity)
        self.active = enabled()
        self.started = time.perf_counter_ns()
        self.segments, self.counters = [], {}

    @contextmanager
    def segment(self, name, parent=None):
        start = time.perf_counter_ns()
        try:
            yield
        finally:
            if self.active:
                self.segments.append({"name": name, "parent": parent,
                                      "ms": round((time.perf_counter_ns() - start) / 1e6, 3)})

    def record(self, name, milliseconds, parent=None, source=None):
        """An interval measured elsewhere in this process, or by one service's own timestamps."""
        if self.active and milliseconds is not None:
            value = {"name": name, "parent": parent, "ms": round(float(milliseconds), 3)}
            if source:
                value["source"] = source
            self.segments.append(value)

    def count(self, name, value):
        if self.active:
            self.counters[name] = value

    def bind(self, **identity):
        self.identity.update(identity)

    def adopt(self, value, parent):
        """Nest another process's reported segments under the interval this process measured around it."""
        if not self.active or not isinstance(value, dict):
            return
        prefix = value.get("process", "remote")
        for segment in value.get("segments", []):
            self.segments.append({**segment, "name": prefix + "." + segment["name"],
                                  "parent": prefix + "." + segment["parent"] if segment.get("parent") else parent})
        self.segments.append({"name": prefix, "parent": parent, "ms": value.get("total_ms"), "reported": True})

    def value(self, process=None):
        total = round((time.perf_counter_ns() - self.started) / 1e6, 3)
        children = {}
        for segment in self.segments:
            if segment.get("ms") is not None and segment.get("source") is None:
                children[segment["parent"]] = children.get(segment["parent"], 0) + segment["ms"]
        parents = {segment["name"]: segment["ms"] for segment in self.segments if segment.get("ms") is not None}
        unattributed = {"total": round(total - children.get(None, 0), 3)}
        for name, measured in children.items():
            if name is not None and name in parents:
                unattributed[name] = round(parents[name] - measured, 3)
        return {"process": process or self.path, "total_ms": total, "segments": self.segments,
                "unattributed_ms": unattributed, "counters": self.counters}

    def emit(self, outcome, process=None):
        if not self.active:
            return None
        value = self.value(process)
        print(json.dumps({"event": "COLVENNA_TIMING", "path": self.path, **self.identity, "outcome": outcome,
                          **value}, separators=(",", ":"), default=str), flush=True)
        return value


class NotTimed:
    """Stands in where the caller supplied no timing; records nothing."""

    active = False

    @property
    def identity(self):
        return {}

    @contextmanager
    def segment(self, name, parent=None):
        yield

    def record(self, *arguments, **fields):
        pass

    def count(self, name, value):
        pass

    def bind(self, **identity):
        pass

    def adopt(self, value, parent):
        pass


NOT_TIMED = NotTimed()


def io_counters(connection):
    """Cumulative objfs S3 operation counters of one native instance (count, bytes, total latency)."""
    try:
        rows = connection.execute("SELECT operation,request_count,average_latency_ms,bytes "
                                  "FROM duckdb_objfs_io_stats()").fetchall()
    except Exception:
        return None
    return {operation: (count or 0, data or 0, (latency or 0) * (count or 0)) for operation, count, latency, data in rows}


def io_delta(before, after):
    if before is None or after is None:
        return None
    result = {}
    for operation, (count, data, latency) in after.items():
        prior = before.get(operation, (0, 0, 0))
        if count - prior[0]:
            result[operation] = {"requests": count - prior[0], "bytes": data - prior[1],
                                 "latency_ms": round(latency - prior[2], 3)}
    return result


def process_resources(connection=None):
    """Process and cgroup memory peaks, CPU time, and (given a native instance) DuckDB memory, spill and cache."""
    import resource
    from pathlib import Path

    value = {"rss_peak_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
             "cpu_seconds": round(sum(resource.getrusage(resource.RUSAGE_SELF)[:2]), 3)}
    for name, path in [("cgroup_memory_peak_bytes", "/sys/fs/cgroup/memory.peak"),
                       ("cgroup_memory_current_bytes", "/sys/fs/cgroup/memory.current")]:
        try:
            value[name] = int(Path(path).read_text().split()[0])
        except (OSError, ValueError, IndexError):
            value[name] = None
    if connection is not None:
        try:
            value["duckdb_memory"] = {tag: [memory, temporary] for tag, memory, temporary in connection.execute(
                "SELECT tag, memory_usage_bytes, temporary_storage_bytes FROM duckdb_memory() "
                "WHERE memory_usage_bytes > 0 OR temporary_storage_bytes > 0").fetchall()}
            value["objfs_cache"] = {cache: [hits, misses, size] for cache, hits, misses, _, _, size, _, _ in
                                    connection.execute("SELECT * FROM duckdb_objfs_cache_stats()").fetchall()}
            value["objfs_io"] = {operation: [count, data] for operation, count, _, _, data in
                                 connection.execute("SELECT * FROM duckdb_objfs_io_stats()").fetchall()}
        except Exception:
            value["native"] = "UNAVAILABLE"
    return value
