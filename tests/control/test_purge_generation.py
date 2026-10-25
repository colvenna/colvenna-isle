"""Old-generation purge after a rebuild: the current generation and one still opened by a live writer are refused."""

import os
import contextlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

# The operator tools name their stacks from an explicit environment.
os.environ.setdefault("COLVENNA_ENVIRONMENT", "qa")

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/aws"))
spec = importlib.util.spec_from_file_location("control_operator", ROOT / "scripts/aws/control.py")
operator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(operator)

OLD, NEW = "gen_" + "a" * 32, "gen_" + "b" * 32


class Client:
    def __init__(self, permit_state):
        self.permit_state = permit_state

    def query(self, **request):
        return {"Items": []}

    def list_object_versions(self, **request):
        return {"Versions": [{"Key": request["Prefix"] + "x", "VersionId": "v1", "Size": 3}]
                if request["Prefix"] == f"spaces/space_one/{OLD}/" else [], "IsTruncated": False}


class Ownership:
    state = "STOPPED"

    def __init__(self, client, table):
        pass

    def read(self):
        return {"state": Ownership.state, "identity": {"task_arn": "arn:task/old"}}

    def registration(self, task_arn):
        # The last writer had opened the old generation before the rebuild.
        return {"opened_databases": ["space_one/" + OLD]}


class Registry:
    def __init__(self, *arguments):
        pass

    def _read(self, space):
        return {"space_id": space, "generation": NEW}


class Session:
    def __init__(self, client):
        self.value = client

    def client(self, name):
        return self.value


class PurgeTests(unittest.TestCase):
    def plan(self, state, generation=OLD):
        Ownership.state = state
        with tempfile.TemporaryDirectory() as directory, contextlib.redirect_stdout(io.StringIO()), \
                patch("colvenna_isle.writer.ownership.Ownership", Ownership), patch.object(operator, "Registry", Registry):
            operator.purge_generation(Session(Client(state)), {"ControlTable": "control", "DataBucket": "bucket"},
                                      "space_one", generation, False, {"Arn": "arn:operator"}, Path(directory))
            return json.loads((Path(directory) / "plan.json").read_text())

    def test_a_stopped_writer_last_registration_is_not_a_live_reference(self):
        plan = self.plan("STOPPED")
        self.assertEqual((plan["generation"], plan["objects"]), (OLD, 1))
        self.assertIn(f"spaces/space_one/{OLD}/", plan["prefixes"])

    def test_a_live_writer_holding_the_old_generation_or_the_current_generation_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "still referenced"):
            self.plan("ACTIVE")
        with self.assertRaisesRegex(RuntimeError, "current generation is never purged"):
            self.plan("STOPPED", NEW)


if __name__ == "__main__":
    unittest.main()
