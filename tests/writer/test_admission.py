import os
import importlib.util
import re
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from colvenna_isle.control.registry import ControlError

# The operator tools name their stacks from an explicit environment.
os.environ.setdefault("COLVENNA_ENVIRONMENT", "qa")

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("writer_shared_control", ROOT / "scripts/aws/control.py")
CONTROL = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CONTROL)


class ExistingAdmissionTests(unittest.TestCase):
    def test_cloudformation_accepts_qualification_probe_and_rejects_other_spaces(self):
        pattern = CONTROL.baseline_template()["Parameters"]["WriterProbeSpace"]["AllowedPattern"]
        self.assertIsNotNone(re.fullmatch(pattern, "probe_20261015t132238z_1ac7e1ff"))
        self.assertIsNotNone(re.fullmatch(pattern, "probe_uat_1ac7e1ff"))
        for invalid in ("orders", "probe_", "probe_Test", "probe-uat"):
            with self.subTest(space=invalid):
                self.assertIsNone(re.fullmatch(pattern, invalid))

    def client(self):
        current = Mock()
        current.client.return_value.describe_services.return_value = {
            "services": [
                {"serviceName": "writer", "desiredCount": 0, "pendingCount": 0, "runningCount": 0},
                {"serviceName": "query", "desiredCount": 0, "pendingCount": 0, "runningCount": 0},
            ], "failures": []}
        return current

    def test_counts_zero_cannot_bypass_owner_or_terminal_verification(self):
        with patch.object(CONTROL, "Controller") as controller:
            controller.return_value.idle.side_effect = ControlError(503, "OLD_WRITER_TERMINATION_UNKNOWN")
            with self.assertRaises(ControlError):
                CONTROL.stopped(self.client(), {
                    "Cluster": "cluster", "WriterService": "writer", "QueryService": "query"})

    def test_existing_callers_receive_exact_terminal_evidence(self):
        with patch.object(CONTROL, "Controller") as controller:
            controller.return_value.idle.return_value = [{"taskArn": "old", "lastStatus": "STOPPED"}]
            controller.return_value.ownership.read.return_value = {"state": "STOPPED", "epoch": 1}
            result = CONTROL.stopped(self.client(), {
                "Cluster": "cluster", "WriterService": "writer", "QueryService": "query"})
            self.assertEqual(result["colvenna_task_terminal_receipts"][0]["taskArn"], "old")
            self.assertEqual(result["colvenna_writer_ownership"]["state"], "STOPPED")


if __name__ == "__main__":
    unittest.main()
