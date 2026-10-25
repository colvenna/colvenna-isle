"""Fresh execution directories and bounded child-process supervision."""

import hashlib
import math
import os
import selectors
import signal
import shutil
import stat
import subprocess
import time
from pathlib import Path

from colvenna_isle.contracts.validation import validate
from colvenna_isle.control.registry import ControlError


class Workspace:
    """Filesystem budgets are checked throughout IO, execution and validation.

    This is a process/budget boundary for trusted published code, not an OS or
    per-attempt IAM sandbox. AWS profile limits remain the outer resource bound.
    """

    def __init__(self, root, run, attempt, execution, *, maximum_bytes, maximum_nodes=12000):
        for identity in [run, attempt, execution]:
            validate("identifier", identity)
        if (type(maximum_bytes) is not int or maximum_bytes < 1
                or type(maximum_nodes) is not int or maximum_nodes < 1):
            raise ValueError("Positive workspace budgets required")
        root = Path(root)
        root.mkdir(parents=True, exist_ok=True)
        if root.is_symlink():
            raise ControlError(422, "WORKSPACE_ROOT_SCOPE")
        self.root = root.resolve(strict=True)
        self.path = self.root / run / attempt / execution
        parent = self.root
        for identity in [run, attempt]:
            parent = parent / identity
            if parent.is_symlink():
                raise ControlError(422, "WORKSPACE_ROOT_SCOPE")
            parent.mkdir(mode=0o700, exist_ok=True)
            if self.root not in parent.resolve(strict=True).parents:
                raise ControlError(422, "WORKSPACE_ROOT_SCOPE")
        if self.root not in self.path.parent.resolve(strict=True).parents:
            raise ControlError(422, "WORKSPACE_ROOT_SCOPE")
        if any(parent.is_symlink() for parent in self.path.parents):
            raise ControlError(422, "WORKSPACE_ROOT_SCOPE")
        try:
            self.path.mkdir(mode=0o700)
        except FileExistsError:
            raise ControlError(409, "EXECUTION_DIRECTORY_EXISTS") from None
        self.maximum_bytes, self.maximum_nodes = maximum_bytes, maximum_nodes
        self.peak_bytes = 0

    def guard(self):
        size, nodes = 0, 0
        for path in self.path.rglob("*"):
            try:
                info = path.lstat()
            except FileNotFoundError:
                continue  # Atomic script rename; the final scan still checks scope.
            nodes += 1
            if (stat.S_ISLNK(info.st_mode) or not (
                    stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode))):
                raise ControlError(422, "WORKSPACE_FILE_SCOPE")
            if stat.S_ISREG(info.st_mode):
                if info.st_nlink != 1:
                    raise ControlError(422, "WORKSPACE_FILE_SCOPE")
                size += info.st_size
            if size > self.maximum_bytes or nodes > self.maximum_nodes:
                raise ControlError(413, "WORKSPACE_BUDGET_EXCEEDED")
        self.peak_bytes = max(self.peak_bytes, size)
        return size

    def run(self, command, *, timeout_seconds, environment, maximum_log_bytes=65536):
        """Drain bounded pipes; any failure kills/reaps the original process group."""
        if (not isinstance(command, list) or not command or type(timeout_seconds) not in {int, float}
                or not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 86400
                or maximum_log_bytes < 1):
            raise ValueError("Bounded command required")
        self.guard()
        deadline = time.monotonic() + timeout_seconds
        process = subprocess.Popen(
            command, cwd=self.path, env=environment, start_new_session=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.DEVNULL)
        selector = selectors.DefaultSelector()
        logs = {name: {"bytes": 0, "digest": hashlib.sha256()} for name in ["stdout", "stderr"]}
        for name in logs:
            pipe = getattr(process, name)
            os.set_blocking(pipe.fileno(), False)
            selector.register(pipe, selectors.EVENT_READ, name)
        failure = None
        try:
            while selector.get_map() or process.poll() is None:
                if time.monotonic() >= deadline:
                    raise ControlError(422, "TASK_EXECUTION_TIMEOUT")
                self.guard()
                for entry, _ in selector.select(timeout=0.02):
                    chunk = os.read(entry.fd, 8192)
                    if not chunk:
                        selector.unregister(entry.fileobj)
                        continue
                    log = logs[entry.data]
                    log["bytes"] += len(chunk)
                    log["digest"].update(chunk)
                    if sum(value["bytes"] for value in logs.values()) > maximum_log_bytes:
                        raise ControlError(413, "TASK_LOG_BUDGET_EXCEEDED")
                # A successful parent with a retained descendant pipe is not
                # complete. Terminate the original group before inspecting output.
                if process.poll() is not None:
                    self._kill(process)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ControlError(422, "TASK_EXECUTION_TIMEOUT")
            result = process.wait(timeout=remaining)
            if result != 0:
                raise ControlError(422, "TASK_SCRIPT_FAILED")
            self.guard()
        except BaseException as error:
            failure = error
            raise
        finally:
            self._kill(process)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                if failure is None:
                    raise ControlError(503, "TASK_PROCESS_TERMINATION_UNKNOWN") from None
            selector.close()
            process.stdout.close()
            process.stderr.close()
        return {"exit_code": result, "peak_workspace_bytes": self.peak_bytes,
                "logs": {name: {"bytes": value["bytes"], "sha256": value["digest"].hexdigest()}
                         for name, value in logs.items()}}

    def _kill(self, process):
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    def cleanup(self):
        """Remove only this terminal execution; never a sibling invocation."""
        shutil.rmtree(self.path)
        for parent in [self.path.parent, self.path.parent.parent]:
            try:
                parent.rmdir()
            except OSError:
                break  # A concurrent invocation still owns this parent.
