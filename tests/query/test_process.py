"""Real child processes verify timeout, cancellation and terminal custody."""

import concurrent.futures
import json
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from colvenna_isle.control.registry import ControlError
from colvenna_isle.query.process import run


class ProcessBoundary(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def execute(self, code, name="execution", **options):
        return run([sys.executable, "-c", code], self.root / name,
                   timeout=options.pop("timeout", 5), **options)

    def assert_reaped(self, terminal):
        self.assertIsNotNone(terminal["exit_code"])
        self.assertEqual(terminal["state"], "REAPED")
        self.assertFalse(Path("/proc/" + str(terminal["pid"])).exists())
        self.assertEqual(terminal, json.loads(
            (Path(terminal["directory"]) / "terminal.json").read_text()))

    def test_success_and_original_failure_have_distinct_terminal_receipts(self):
        success = self.execute("print('private SQL and secret')")
        failure = self.execute("raise SystemExit(7)", name="failure")
        self.assertEqual(success["outcome"], "SUCCEEDED")
        self.assertEqual(failure["outcome"], "FAILED")
        self.assertEqual(failure["exit_code"], 7)
        for terminal in [success, failure]:
            self.assert_reaped(terminal)
            self.assertNotIn("private SQL", json.dumps(terminal))
        with self.assertRaises(FileExistsError):
            self.execute("raise SystemExit(0)")

    def test_unresponsive_native_pattern_is_killed_and_reaped_at_deadline(self):
        start = time.monotonic()
        terminal = self.execute(
            "import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(60)",
            timeout=0.2, terminate_grace=0.05)
        self.assertEqual(terminal["outcome"], "TIMED_OUT")
        self.assertEqual(terminal["exit_code"], -signal.SIGKILL)
        self.assertLess(time.monotonic() - start, 2)
        self.assert_reaped(terminal)

    def test_running_cancel_does_not_stop_another_process(self):
        cancellation = threading.Event()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            running = pool.submit(self.execute, "import time; time.sleep(60)",
                                  cancel=cancellation.is_set)
            other = pool.submit(self.execute, "import time; time.sleep(.2)", name="other")
            time.sleep(0.1)
            cancellation.set()
            cancelled, completed = running.result(), other.result()
        self.assertEqual(cancelled["outcome"], "CANCELLED")
        self.assertEqual(completed["outcome"], "SUCCEEDED")
        for terminal in [cancelled, completed]:
            self.assert_reaped(terminal)

    def test_revocation_and_directory_quota_reap_original_before_return(self):
        calls = []

        def guard():
            calls.append(1)
            if len(calls) > 2:
                raise ControlError(403, "FORBIDDEN")

        revoked = self.execute("import time; time.sleep(60)", guard=guard)
        path = self.root / "quota"
        quota = self.execute(
            "from pathlib import Path; import time; p=Path(" + repr(str(path))
            + "); [(p/str(i)).write_bytes(b'x'*900) for i in range(4)]; time.sleep(60)",
            name="quota", maximum_bytes=1024)
        self.assertEqual(revoked["outcome"], "REVOKED")
        self.assertEqual(revoked["error_code"], "FORBIDDEN")
        self.assertEqual(quota["outcome"], "QUOTA_EXCEEDED")
        for terminal in [revoked, quota]:
            self.assert_reaped(terminal)

    def test_already_cancelled_does_not_launch_a_child(self):
        terminal = self.execute("raise SystemExit(99)", cancel=lambda: True)
        self.assertEqual(terminal["outcome"], "CANCELLED")
        self.assertEqual(terminal["state"], "NOT_STARTED")
        self.assertIsNone(terminal["pid"])

    def test_parent_death_kills_the_original_native_child(self):
        directory = self.root / "owned"
        code = ("from colvenna_isle.query.process import run; import sys; run("
                "[sys.executable,'-c','import time; time.sleep(60)'],"
                + repr(str(directory)) + ",timeout=30)")
        parent = subprocess.Popen([sys.executable, "-c", code])
        try:
            deadline = time.monotonic() + 3
            while not (directory / "identity.json").exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            child = json.loads((directory / "identity.json").read_text())["pid"]
            # The bootstrap completes before exec; wait until its target command
            # is running so this test covers parent death after native startup.
            while time.monotonic() < deadline:
                arguments = Path("/proc/" + str(child) + "/cmdline").read_bytes()
                if b"import time; time.sleep(60)" in arguments and b"process_child" not in arguments:
                    break
                time.sleep(0.01)
            else:
                self.fail("Original native child never started")
            parent.kill()
            parent.wait(timeout=3)
            while time.monotonic() < deadline:
                try:
                    stat = Path("/proc/" + str(child) + "/stat").read_text()
                except (FileNotFoundError, ProcessLookupError):
                    # Reaped before open, or between open and read (ESRCH).
                    break
                if stat[stat.rfind(")") + 2:].split()[0] == "Z":
                    break
                time.sleep(0.01)
            else:
                self.fail("Native child survived its owner")
        finally:
            if parent.poll() is None:
                parent.kill()
                parent.wait(timeout=3)


if __name__ == "__main__":
    unittest.main()
