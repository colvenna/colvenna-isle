import unittest
import json
from unittest.mock import Mock

from colvenna_isle.control.registry import ControlError
from colvenna_isle.writer.ownership import Ownership, matches, terminal_task


IDENTITY = {
    "task_arn": "arn:aws:ecs:us-west-2:123:task/cluster/old",
    "task_definition": "arn:aws:ecs:us-west-2:123:task-definition/writer:1",
    "image_digest": "sha256:" + "a" * 64, "source_commit": "a" * 40,
    "process_nonce": "a" * 32, "pid": 1, "start_ticks": 100, "boot_id": "boot", "address": "10.87.1.1",
}


def task():
    return {"taskArn": IDENTITY["task_arn"], "taskDefinitionArn": IDENTITY["task_definition"],
            "lastStatus": "STOPPED", "desiredStatus": "STOPPED", "stoppedAt": "2026-10-15T08:00:00Z",
            "stopCode": "EssentialContainerExited", "containers": [{
                "name": "baseline", "lastStatus": "STOPPED", "imageDigest": IDENTITY["image_digest"],
                "exitCode": 137}]}


class OwnershipTests(unittest.TestCase):
    def test_missing_current_after_grant_is_unknown_not_bootstrap(self):
        client = Mock()
        client.get_item.side_effect = [{}, {"Item": {"value": {"S": json.dumps({"event": "GRANT"})}}}]
        with self.assertRaises(ControlError) as failure:
            Ownership(client, "table").read()
        self.assertEqual(failure.exception.code, "WRITER_OWNERSHIP_UNKNOWN")

    def test_empty_environment_can_bootstrap_without_grant_history(self):
        client = Mock()
        client.get_item.return_value = {}
        self.assertEqual(Ownership(client, "table").read(),
                         {"revision": 0, "epoch": 0, "state": "STOPPED", "identity": None})

    def test_identity_includes_process_birth_and_address(self):
        self.assertTrue(matches(IDENTITY, IDENTITY))
        for field in IDENTITY:
            self.assertFalse(matches(IDENTITY, {**IDENTITY, field: "different"}), field)
            self.assertFalse(matches(IDENTITY, {k: v for k, v in IDENTITY.items() if k != field}), field)

    def test_desired_stop_and_heartbeat_expiry_do_not_prove_death(self):
        for change in [{"lastStatus": "STOPPING"}, {"lastStatus": "RUNNING"},
                       {"stoppedAt": None}, {"stopCode": None}, {"taskArn": "other"}]:
            with self.assertRaises(ControlError):
                terminal_task({**task(), **change}, IDENTITY)
        missing_exit = task()
        del missing_exit["containers"][0]["exitCode"]
        with self.assertRaises(ControlError):
            terminal_task(missing_exit, IDENTITY)
        self.assertEqual(terminal_task(task(), IDENTITY)["containers"][0]["exitCode"], 137)

    def test_stale_image_or_process_cannot_reuse_stopped_receipt(self):
        with self.assertRaises(ControlError):
            terminal_task(task(), {**IDENTITY, "image_digest": "sha256:" + "b" * 64})
        with self.assertRaises(ControlError):
            terminal_task(task(), {**IDENTITY, "task_definition": "new"})

    def test_permit_revocation_and_epoch_are_independent_of_heartbeat(self):
        store = Ownership(Mock(), "table")
        store.read = Mock(return_value={
            "state": "ACTIVE", "identity": IDENTITY, "epoch": 1,
            "heartbeat": 0, "lease_expires_at": 0})
        self.assertEqual(store.require(IDENTITY, 1)["epoch"], 1)
        with self.assertRaises(ControlError):
            store.require(IDENTITY, 2)
        for state in ["REVOKED", "STOPPED"]:
            store.read.return_value["state"] = state
            with self.assertRaises(ControlError):
                store.require(IDENTITY, 1)

    def test_takeover_rejects_missing_predecessor_terminal_without_mutation(self):
        client = Mock()
        store = Ownership(client, "table")
        for previous in [
            {"state": "ACTIVE", "identity": IDENTITY},
            {"state": "REVOKED", "identity": IDENTITY},
            {"state": "STOPPED", "identity": IDENTITY},
            {"state": "STOPPED", "identity": IDENTITY, "terminal_task": {**task(), "lastStatus": "STOPPING"}},
        ]:
            with self.assertRaises(ControlError):
                store.grant({**IDENTITY, "task_arn": "new"}, previous)
        client.transact_write_items.assert_not_called()


if __name__ == "__main__":
    unittest.main()
