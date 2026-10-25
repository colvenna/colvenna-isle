"""Own and reap one query process even when a native call never returns.

Commands are supplied only by server code. SQL, credentials and command text
are absent from custody records. This is a process boundary, not a job store.
"""

import hashlib
import json
import math
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


def _birth(pid):
    stat = Path("/proc/" + str(pid) + "/stat").read_text()
    return stat[stat.rfind(")") + 2:].split()[19]


def _save(path, record):
    pending = path.with_suffix(".pending")
    with pending.open("x") as stream:
        os.chmod(pending, 0o600)
        stream.write(json.dumps(record, indent=2) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.link(pending, path)  # Publish a complete record without overwriting an original.
    pending.unlink()


def run(command, directory, *, timeout, cancel=lambda: False, guard=lambda: None,
        maximum_bytes=1073741824, terminate_grace=0.5, on_started=lambda identity: None):
    """Return a terminal only after wait(); never retry or replace the child.

    Each caller owns a new directory and a new process group. Cancellation,
    revocation, timeout and quota all stop that group before returning. The
    Linux child bootstrap also kills the child if its owning parent dies.
    """
    if (not command or any(not isinstance(value, str) for value in command)
            or type(timeout) not in {int, float} or not math.isfinite(timeout)
            or not 0 < timeout <= 3600 or type(maximum_bytes) is not int or maximum_bytes < 1
            or not 0 <= terminate_grace <= 5):
        raise ValueError("Invalid server execution limits")
    guard()
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=False, mode=0o700)
    record = {"directory": str(directory), "started_unix": time.time(), "pid": None,
        "command_sha256": hashlib.sha256(json.dumps(command).encode()).hexdigest(),
        "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
        "timeout_seconds": timeout, "maximum_bytes": maximum_bytes}
    if cancel():
        record.update(state="NOT_STARTED", outcome="CANCELLED", exit_code=None,
                      finished_unix=time.time())
        _save(directory / "terminal.json", record)
        return record
    try:
        process = subprocess.Popen(
            [sys.executable, "-m", "colvenna_isle.query.process_child", str(os.getpid()),
             str(maximum_bytes), *command], stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    except OSError:
        record.update(state="NOT_STARTED", outcome="LAUNCH_FAILED", exit_code=None,
                      error_code="QUERY_PROCESS_START_FAILED", finished_unix=time.time())
        _save(directory / "terminal.json", record)
        return record
    pidfd = None
    record.update(pid=process.pid, state="RUNNING")
    try:
        record["start_ticks"] = _birth(process.pid)
    except OSError:
        # This unreaped Popen child still owns the PID. No other thread waits it.
        os.killpg(process.pid, signal.SIGKILL)
        record.update(state="REAPED", outcome="SUPERVISOR_FAILED",
                      exit_code=process.wait(timeout=5), finished_unix=time.time())
        _save(directory / "terminal.json", record)
        raise

    def exited():
        # Observe without reaping: preserve the original PID until signalling
        # its group is finished, including any descendants of the worker.
        return os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is not None

    def signal_group(value):
        if _birth(process.pid) != record["start_ticks"]:
            raise RuntimeError("QUERY_PROCESS_CUSTODY_UNKNOWN")
        try:
            os.killpg(process.pid, value)
        except ProcessLookupError:
            pass  # An exited leader with no remaining descendants has no group.

    def stop():
        if not exited():
            signal_group(signal.SIGTERM)
            deadline = time.monotonic() + terminate_grace
            while not exited() and time.monotonic() < deadline:
                time.sleep(0.01)
        signal_group(signal.SIGKILL)

    try:
        pidfd = os.pidfd_open(process.pid)
        _save(directory / "identity.json", record)
        deadline = time.monotonic() + timeout
        on_started(dict(record))
        outcome = None
        while not exited():
            try:
                guard()
            except Exception as error:
                record["error_code"] = getattr(error, "code", "AUTHORIZATION_UNAVAILABLE")
                outcome = "REVOKED"
            if outcome is None and cancel():
                outcome = "CANCELLED"
            if outcome is None and time.monotonic() >= deadline:
                outcome = "TIMED_OUT"
            if outcome is None:
                total = 0
                for path in directory.rglob("*"):
                    try:
                        if path.is_file():
                            total += path.stat().st_size
                    except FileNotFoundError:
                        pass  # Native spill files can disappear during the walk.
                if total > maximum_bytes:
                    outcome = "QUOTA_EXCEEDED"
            if outcome:
                stop()
                break
            time.sleep(0.025)
        # Server workers must not leave descendants after success/failure.
        signal_group(signal.SIGKILL)
        code = process.wait(timeout=5)
        record.update(state="REAPED", outcome=outcome or ("SUCCEEDED" if code == 0 else "FAILED"),
                      exit_code=code, finished_unix=time.time())
        _save(directory / "terminal.json", record)
        return record
    except BaseException:
        try:
            if process.returncode is None:
                stop()
            record.update(state="REAPED", outcome="SUPERVISOR_FAILED",
                          exit_code=process.wait(timeout=5), finished_unix=time.time())
        except BaseException:
            record.update(state="UNKNOWN", outcome="CUSTODY_UNKNOWN", exit_code=None,
                          observed_unix=time.time())
        if not (directory / "terminal.json").exists():
            _save(directory / "terminal.json", record)
        raise
    finally:
        if pidfd is not None:
            os.close(pidfd)
