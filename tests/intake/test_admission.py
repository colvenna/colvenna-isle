"""Public admission behavior under lost replies and changing authority."""

import copy
import hashlib
import io
import json
import unittest

from botocore.exceptions import ClientError, ReadTimeoutError

from colvenna_isle.contracts.validation import ContractError, validate
from colvenna_isle.control.registry import ControlError, Principal, canonical
from colvenna_isle.intake.handler import handle
from colvenna_isle.intake.service import Intake, IntakeError


SPACE, GENERATION = "test_space", "gen_" + "a" * 32
PRINCIPAL = Principal("test_owner", frozenset([SPACE]), frozenset(["ingest:write"]),
                      "key_test", GENERATION, "2027-01-01T00:00:00Z")
DEFINITION = {
    "dataset": "ods.orders", "schema_version": 1,
    "schema": [{"name": "order_id", "type": "BIGINT", "nullable": False},
               {"name": "amount", "type": "DECIMAL(18,2)", "nullable": False},
               {"name": "event_id", "type": "VARCHAR", "nullable": False},
               {"name": "source_version", "type": "BIGINT", "nullable": False}],
    "key_columns": ["order_id"], "partition_columns": [],
    "ingestion": {"operations": ["append", "upsert", "delete"],
                  "event_id_column": "event_id", "source_version_column": "source_version"},
}
PAYLOAD = {"dataset": "orders", "schema_version": 1, "operation": "upsert",
           "records": [{"order_id": 1, "amount": "42.00", "event_id": "event-1", "source_version": 7}]}


class Registry:
    def __init__(self):
        self.reads, self.rebuild = 0, False

    def dataset(self, principal, space, dataset, scope):
        principal.authorize(space, scope)
        self.reads += 1
        return {"generation": "gen_" + "b" * 32 if self.rebuild and self.reads > 1 else GENERATION,
                "route_version": 1}, {
                    "definition": DEFINITION,
                    "definition_sha256": hashlib.sha256(canonical(DEFINITION).encode()).hexdigest()}


class ObjectStore:
    def __init__(self):
        self.objects, self.lose_reply = {}, False

    def put_object(self, **request):
        key = request["Key"]
        if key in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.objects[key] = request
        if self.lose_reply:
            raise ReadTimeoutError(endpoint_url="fixture")
        return {"VersionId": "first-version"}

    def get_object(self, **request):
        value = self.objects[request["Key"]]
        return {"Metadata": value.get("Metadata", {}), "Body": io.BytesIO(value["Body"]), "VersionId": "first-version"}


class Queue:
    def __init__(self, objects):
        self.objects, self.messages, self.lose_reply = objects, [], False

    def send_message(self, **request):
        message = json.loads(request["MessageBody"])
        assert self.objects.objects[message["source"]["key"]]["Body"]
        self.messages.append(request)
        if self.lose_reply:
            raise ReadTimeoutError(endpoint_url="fixture")
        return {"MessageId": "fixture", "SequenceNumber": "1", "ResponseMetadata": {"HTTPStatusCode": 200},
                "MD5OfMessageBody": hashlib.md5(request["MessageBody"].encode(), usedforsecurity=False).hexdigest()}


class Credentials:
    def authenticate(self, value):
        return PRINCIPAL


def event(payload=PAYLOAD):
    return {"rawPath": f"/v1/spaces/{SPACE}/ingestions",
            "requestContext": {"requestId": "gateway+opaque=",
                               "http": {"method": "POST"}, "authorizer": {"lambda": {
                                   "principal_id": PRINCIPAL.principal_id, "key_id": PRINCIPAL.key_id,
                                   "generation": GENERATION, "expires_at": PRINCIPAL.expires_at,
                                   "spaces": json.dumps([SPACE]), "scopes": json.dumps(["ingest:write"])}}},
            "headers": {"authorization": "Bearer " + "x" * 48, "content-type": "application/json",
                        "idempotency-key": "stable-client-key"}, "body": json.dumps(payload)}


class AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.registry, self.objects = Registry(), ObjectStore()
        self.queue = Queue(self.objects)
        self.intake = Intake(self.registry, self.objects, self.queue, "fixture-bucket",
                             "https://fixture/queue.fifo", "123456789012", "a" * 40)

    def submit(self, payload=PAYLOAD, raw=None):
        return self.intake.submit(PRINCIPAL, SPACE, payload, raw or json.dumps(payload).encode(), "stable-client-key")

    def test_confirmed_send_and_same_semantics_keep_first_raw_object(self):
        first = self.submit()
        repeated = self.submit(raw=canonical(PAYLOAD).encode())
        self.assertEqual(first, repeated)
        self.assertEqual(len(self.objects.objects), 1)
        self.assertEqual(self.queue.messages[0], self.queue.messages[1])
        message = json.loads(self.queue.messages[0]["MessageBody"])
        self.assertEqual(self.objects.objects[message["source"]["key"]]["Body"], json.dumps(PAYLOAD).encode())
        self.assertEqual(message["space_id"], self.queue.messages[0]["MessageGroupId"])
        self.assertEqual(first["ingestion_id"], self.queue.messages[0]["MessageDeduplicationId"])

    def test_different_content_cannot_overwrite_or_enqueue(self):
        self.submit()
        changed = copy.deepcopy(PAYLOAD)
        changed["records"][0]["amount"] = "43.00"
        with self.assertRaises(ControlError) as conflict:
            self.submit(changed)
        self.assertEqual(conflict.exception.status, 409)
        self.assertEqual(len(self.queue.messages), 1)

    def test_lost_archive_reply_reconciles_original_object_before_send(self):
        self.objects.lose_reply = True
        result = self.submit()
        self.assertEqual(result["status"], "ACCEPTED")
        self.assertEqual(len(self.objects.objects), 1)
        self.assertEqual(len(self.queue.messages), 1)

    def test_lost_queue_reply_is_unknown_and_retry_keeps_exact_identity(self):
        self.queue.lose_reply = True
        response = handle(event(), self.intake, Credentials())
        uncertain = json.loads(response["body"])
        self.assertEqual(response["statusCode"], 503)
        self.assertEqual(uncertain["status"], "UNKNOWN")
        validate("error", uncertain)
        self.queue.lose_reply = False
        recovered = self.submit()
        self.assertEqual(recovered["ingestion_id"], uncertain["ingestion_id"])
        self.assertEqual(len(self.objects.objects), 1)
        self.assertEqual(self.queue.messages[0], self.queue.messages[1])

    def test_delete_requires_event_and_version_even_when_nonkey_columns_are_omitted(self):
        payload = {**PAYLOAD, "operation": "delete", "records": [{"order_id": 1}]}
        with self.assertRaises(ContractError):
            self.submit(payload)
        payload["records"][0].update(event_id="delete-1", source_version=8)
        self.assertEqual(self.submit(payload)["status"], "ACCEPTED")

    def test_generation_change_prevents_dispatch_of_saved_old_object(self):
        self.registry.rebuild = True
        with self.assertRaises(ControlError) as changed:
            self.submit()
        self.assertEqual(changed.exception.status, 409)
        self.assertEqual(len(self.objects.objects), 1)
        self.assertEqual(self.queue.messages, [])

    def test_forged_gateway_identity_is_rejected_before_archive(self):
        request = event()
        request["requestContext"]["authorizer"]["lambda"]["principal_id"] = "forged_owner"
        self.assertEqual(handle(request, self.intake, Credentials())["statusCode"], 403)
        self.assertEqual(self.objects.objects, {})


if __name__ == "__main__":
    unittest.main()
