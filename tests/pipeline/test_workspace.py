"""Actual child execution, directory isolation, quotas and termination."""

import hashlib
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path

from colvenna_isle.control.registry import ControlError
from colvenna_isle.pipeline.workspace import Workspace


class ExecutionWorkspace(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "colvenna"
        self.workspace = Workspace(self.root, "run_one", "attempt_one", "exec_one",
                                   maximum_bytes=1048576)

    def tearDown(self):
        self.temporary.cleanup()

    def run_script(self, content, **options):
        return self.workspace.run([sys.executable, "-c", content],
                                  timeout_seconds=options.get("timeout_seconds", 5),
                                  environment=dict(os.environ),
                                  maximum_log_bytes=options.get("maximum_log_bytes", 65536))

    def test_actual_process_uses_fresh_directory_and_returns_log_hashes(self):
        result = self.run_script("from pathlib import Path; Path('result.txt').write_text('typed'); print('synthetic-secret')")
        self.assertEqual((self.workspace.path / "result.txt").read_text(), "typed")
        self.assertEqual(result["exit_code"], 0)
        self.assertEqual(result["logs"]["stdout"]["sha256"], hashlib.sha256(b"synthetic-secret\n").hexdigest())
        self.assertNotIn("synthetic-secret", repr(result))

    def test_repeated_execution_cannot_overwrite_original_directory(self):
        (self.workspace.path / "original").write_bytes(b"original")
        with self.assertRaisesRegex(ControlError, "EXECUTION_DIRECTORY_EXISTS"):
            Workspace(self.root, "run_one", "attempt_one", "exec_one", maximum_bytes=1048576)
        self.assertEqual((self.workspace.path / "original").read_bytes(), b"original")

    def test_distinct_invocations_have_distinct_directories_and_outputs(self):
        second = Workspace(self.root, "run_one", "attempt_one", "exec_two", maximum_bytes=1048576)
        self.run_script("from pathlib import Path; Path('result.txt').write_text('first')")
        second.run([sys.executable, "-c", "from pathlib import Path; Path('result.txt').write_text('second')"],
                   timeout_seconds=5, environment=dict(os.environ))
        self.assertEqual((self.workspace.path / "result.txt").read_text(), "first")
        self.assertEqual((second.path / "result.txt").read_text(), "second")

    def test_timeout_kills_original_child(self):
        with self.assertRaisesRegex(ControlError, "TASK_EXECUTION_TIMEOUT"):
            self.run_script("import time; time.sleep(30)", timeout_seconds=1)

    def check_descendant_terminated(self, *, closed_pipes=False):
        output = ",stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL" if closed_pipes else ""
        self.run_script("import subprocess,sys; from pathlib import Path; "
                        "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']" +
                        output + "); "
                        "value=Path('/proc/'+str(child.pid)+'/stat').read_text(); "
                        "import json; Path('child.json').write_text(json.dumps({'pid':child.pid,"
                        "'start_ticks':value[value.rfind(\")\")+2:].split()[19]}))")
        original = json.loads((self.workspace.path / "child.json").read_text())
        identity = original["pid"]
        process = Path(f"/proc/{identity}/stat")
        # SIGKILL delivery is asynchronous. Confirm the original birth becomes
        # dead/absent within a bound, even when inherited pipes provide no wait.
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            try:
                value = process.read_text()
            except (FileNotFoundError, ProcessLookupError):
                return  # Reaped before open, or between open and read (ESRCH).
            fields = value[value.rfind(')') + 2:].split()
            if fields[19] != original["start_ticks"] or fields[0] == "Z":
                return  # Original is dead; a reused PID is another process.
            time.sleep(0.001)
        self.fail("Original descendant remains live after process-group termination")

    def test_successful_parent_cannot_leave_ordinary_descendant_running(self):
        self.check_descendant_terminated()

    def test_successful_parent_cannot_leave_descendant_with_closed_pipes_running(self):
        self.check_descendant_terminated(closed_pipes=True)

    def test_closed_log_pipes_cannot_disable_disk_budget_monitor(self):
        self.workspace.maximum_bytes = 4096
        with self.assertRaisesRegex(ControlError, "WORKSPACE_BUDGET_EXCEEDED"):
            self.run_script("import os,time; os.close(1); os.close(2); open('oversized','wb').write(b'x'*8192); time.sleep(30)")

    def test_log_and_failure_results_cannot_be_treated_as_success(self):
        with self.assertRaisesRegex(ControlError, "TASK_LOG_BUDGET_EXCEEDED"):
            self.run_script("print('x'*8192)", maximum_log_bytes=1024)
        with self.assertRaisesRegex(ControlError, "TASK_SCRIPT_FAILED"):
            self.run_script("raise SystemExit(1)")

    def test_parent_symlink_is_rejected_before_creating_directories_outside_root(self):
        outside = Path(self.temporary.name) / "outside"
        outside.mkdir()
        (self.root / "run_foreign").symlink_to(outside, target_is_directory=True)
        with self.assertRaisesRegex(ControlError, "WORKSPACE_ROOT_SCOPE"):
            Workspace(self.root, "run_foreign", "attempt_foreign", "exec_foreign", maximum_bytes=1048576)
        self.assertFalse((outside / "attempt_foreign").exists())

    def test_file_nodes_and_hardlinks_are_counted_independently_of_bytes(self):
        self.workspace.maximum_nodes = 1
        (self.workspace.path / "first").write_bytes(b"")
        (self.workspace.path / "second").write_bytes(b"")
        with self.assertRaisesRegex(ControlError, "WORKSPACE_BUDGET_EXCEEDED"):
            self.workspace.guard()
        self.workspace.maximum_nodes = 100
        (self.workspace.path / "second").unlink()
        os.link(self.workspace.path / "first", self.workspace.path / "alias")
        with self.assertRaisesRegex(ControlError, "WORKSPACE_FILE_SCOPE"):
            self.workspace.guard()


if __name__ == "__main__":
    unittest.main()
