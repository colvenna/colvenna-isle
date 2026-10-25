"""The public seam never turns response uncertainty into another business request."""

import copy
import io
import json
import time
import unittest
from unittest.mock import Mock
from urllib.error import URLError

from colvenna_isle.control.registry import ControlError, Principal
from colvenna_isle.sync.handler import handle
from colvenna_isle.sync.service import Proxy, WriteUnknown
from colvenna_isle.writer.writes import digest, write_id


SPACE, GENERATION, SOURCE = "fixture", "gen_" + "a" * 32, "a" * 40
PRINCIPAL = Principal("owner", frozenset([SPACE]), frozenset(["data:write"]), "key_fixture",
                      GENERATION, "2027-01-01T00:00:00Z")
TARGET = {
    "task_arn": "arn:aws:ecs:us-west-2:123456789012:task/fixture/" + "a" * 32,
    "task_definition": "arn:aws:ecs:us-west-2:123456789012:task-definition/fixture:1",
    "image_digest": "sha256:" + "b" * 64, "source_commit": SOURCE,
    "process_nonce": "a" * 32, "pid": 1, "start_ticks": 1, "boot_id": "a" * 36, "address": "10.87.11.10",
}
DEFINITION = {
    "dataset": "dwd.orders", "schema_version": 1, "write_owner": "direct",
    "schema": [{"name": "id", "type": "BIGINT", "nullable": False}],
    "key_columns": ["id"], "partition_columns": [], "constraints": {"primary_key": ["id"]},
}
PAYLOAD = {"dataset": "dwd.orders", "schema_version": 1, "operation": "insert", "records": [{"id": 1}]}
IDENTITY = write_id(SPACE, GENERATION, "dwd.orders", "original")
RECEIPT = {
    "write_id": IDENTITY, "space_id": SPACE, "generation": GENERATION, "dataset": "dwd.orders",
    "schema_version": 1, "status": "COMMITTED", "commit_sequence": 1, "dataset_watermark": 1,
    "affected_rows": 1, "committed_at": "2026-10-15T00:00:00Z", "retry_until": "2026-11-14T00:00:00Z",
}


class SyncProxyTests(unittest.TestCase):
    def setUp(self):
        self.configuration = {"space_id": SPACE, "generation": GENERATION}
        self.registered = {"definition": copy.deepcopy(DEFINITION), "definition_sha256": digest(DEFINITION)}
        self.registry = Mock()
        self.registry.dataset.return_value = self.configuration, self.registered
        self.registry._read.return_value = self.configuration
        self.ownership = Mock()
        self.ownership.read.return_value = {"state": "ACTIVE", "identity": TARGET, "epoch": 1}
        self.connect = Mock()
        self.set_response(RECEIPT)
        self.proxy = Proxy(self.registry, self.ownership, SOURCE, self.connect)

    def set_response(self, result, target=TARGET, epoch=1, status=200):
        content = json.dumps({"identity": target, "epoch": epoch, "result": result}).encode()
        value = io.BytesIO(content)
        value.status = status
        self.connect.return_value = value

    def submit(self, payload=PAYLOAD, started=None):
        return self.proxy.submit(PRINCIPAL, SPACE, payload, "original", started or int(time.time() * 1000))

    def test_durable_success_uses_fixed_private_writer_and_sends_no_credential(self):
        status, result = self.submit()
        self.assertEqual((status, result), (200, RECEIPT))
        request = self.connect.call_args.args[0]
        self.assertEqual(request.full_url, "http://10.87.11.10:8082/writes")
        sent = json.loads(request.data)
        self.assertEqual((sent["key_id"], sent["principal_id"], sent["identity"]), ("key_fixture", "owner", TARGET))
        self.assertNotIn("authorization", sent)
        self.assertLessEqual(self.connect.call_args.kwargs["timeout"], 10)

    def test_lost_response_and_malformed_receipt_are_original_unknown(self):
        self.connect.side_effect = URLError("lost original response")
        with self.assertRaises(WriteUnknown) as lost:
            self.submit()
        self.assertEqual(lost.exception.write_id, IDENTITY)
        self.assertEqual(self.connect.call_count, 1)
        self.connect.side_effect = None
        partial = copy.deepcopy(RECEIPT)
        partial.pop("affected_rows")
        self.set_response(partial)
        with self.assertRaises(WriteUnknown):
            self.submit()
        self.assertEqual(self.connect.call_count, 2)  # No transparent RPC retry.

    def test_stale_writer_response_cannot_claim_success(self):
        for target, epoch in [({**TARGET, "process_nonce": "b" * 32}, 1), (TARGET, 2)]:
            self.set_response(RECEIPT, target, epoch)
            with self.assertRaises(WriteUnknown):
                self.submit()

    def test_missing_owner_and_stale_schema_do_not_contact_writer(self):
        for owner in [None, "pipeline", "ingestion"]:
            self.registered["definition"]["write_owner"] = owner
            with self.assertRaises(ControlError) as denied:
                self.submit()
            self.assertEqual(denied.exception.status, 403)
        self.registered["definition"]["write_owner"] = "direct"
        with self.assertRaises(ControlError) as stale:
            self.submit({**PAYLOAD, "schema_version": 2})
        self.assertEqual(stale.exception.code, "SCHEMA_VERSION_MISMATCH")
        self.connect.assert_not_called()

    def test_expired_wait_budget_and_waiting_permit_are_unknown_without_dispatch(self):
        with self.assertRaises(WriteUnknown) as expired:
            self.submit(started=int(time.time() * 1000) - 10001)
        self.assertEqual(expired.exception.status, 504)
        self.ownership.read.return_value = {"state": "STOPPED", "identity": None, "epoch": 2}
        with self.assertRaises(WriteUnknown):
            self.submit()
        self.connect.assert_not_called()

    def test_gateway_identity_wrong_scope_and_injected_internal_identity_are_rejected(self):
        def event(principal=PRINCIPAL):
            return {
                "rawPath": "/v1/spaces/fixture/writes",
                "requestContext": {"requestId": "test", "http": {"method": "POST"}, "authorizer": {"lambda": {
                    "principal_id": principal.principal_id, "key_id": principal.key_id,
                    "generation": principal.generation, "expires_at": principal.expires_at,
                    "spaces": json.dumps(list(principal.spaces)), "scopes": json.dumps(list(principal.scopes)),
                }}}, "headers": {"authorization": "Bearer " + "x" * 48,
                    "content-type": "application/json", "idempotency-key": "original"}, "body": json.dumps(PAYLOAD),
            }
        keys = Mock()
        keys.authenticate.return_value = PRINCIPAL
        forged = event()
        forged["requestContext"]["authorizer"]["lambda"]["principal_id"] = "another"
        self.assertEqual(handle(forged, self.proxy, keys)["statusCode"], 403)
        wrong_scope = Principal("owner", frozenset([SPACE]), frozenset(["ingest:write"]),
                                PRINCIPAL.key_id, GENERATION, PRINCIPAL.expires_at)
        keys.authenticate.return_value = wrong_scope
        self.assertEqual(handle(event(wrong_scope), self.proxy, keys)["statusCode"], 403)
        keys.authenticate.return_value = PRINCIPAL
        internal = event()
        internal["headers"]["x-colvenna-principal"] = "another"
        self.assertEqual(handle(internal, self.proxy, keys)["statusCode"], 400)
        self.connect.assert_not_called()

    def test_full_queue_only_accepts_confirmed_not_submitted_shape(self):
        self.set_response({"write_id": IDENTITY, "status": "NOT_SUBMITTED", "code": "WRITER_QUEUE_FULL",
                           "retry_after_ms": 1000}, status=429)
        self.assertEqual(self.submit()[0], 429)
        self.set_response({"write_id": IDENTITY, "status": "NOT_SUBMITTED", "code": "fake"}, status=200)
        with self.assertRaises(WriteUnknown):
            self.submit()


if __name__ == "__main__":
    unittest.main()
