"""The bridge bounds read-only observations so public status answers inside the API integration limit."""

import json
import unittest
from unittest.mock import patch
from urllib.error import URLError

from colvenna_isle.control.registry import ControlError
from colvenna_isle.writer import bridge

TARGET = {"task_arn": "arn:aws:ecs:us-west-2:123456789012:task/c/1", "task_definition": "writer:1",
          "image_digest": "sha256:" + "a" * 64, "source_commit": "a" * 40, "process_nonce": "n", "pid": 1,
          "start_ticks": 1, "boot_id": "b", "address": "10.0.0.9"}


class Ownership:
    def __init__(self, ddb, table):
        pass

    def read(self):
        return {"state": "ACTIVE", "epoch": 3, "identity": TARGET}


class BridgeTimeoutTests(unittest.TestCase):
    def call(self, operation):
        timeouts = []

        def unreachable(request, timeout):
            timeouts.append(timeout)
            raise URLError("timed out")

        with patch.object(bridge, "clients", return_value=(None, None)), \
                patch.object(bridge, "Ownership", Ownership), patch.object(bridge, "urlopen", unreachable), \
                patch.dict("os.environ", {"COLVENNA_CONTROL_TABLE": "table"}):
            result = bridge.handler({"identity": TARGET, "epoch": 3, "operation": operation}, None)
        return result, timeouts

    def test_status_observations_give_up_early_and_report_unknown(self):
        for operation in sorted(bridge.OBSERVATIONS):
            result, timeouts = self.call(operation)
            self.assertEqual(timeouts, [bridge.OBSERVATION_TIMEOUT])
            self.assertIn("WRITER_RESULT_UNKNOWN", json.dumps(result))

    def test_state_changing_operations_keep_the_full_wait(self):
        _, timeouts = self.call("publication_import")
        self.assertEqual(timeouts, [bridge.OPERATION_TIMEOUT])
