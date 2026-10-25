"""Immutable-original boundaries, including real conditional-write ambiguity."""

import copy
import io
import threading
import unittest
from datetime import datetime, timedelta, timezone

from botocore.exceptions import ClientError, ReadTimeoutError

from colvenna_isle.control.registry import ControlError
from colvenna_isle.writer.intents import IntentUnknown, Intents
from colvenna_isle.writer.writes import digest


DEFINITION = {
    "dataset": "dwd.orders", "schema_version": 1, "write_owner": "direct",
    "schema": [{"name": "id", "type": "BIGINT", "nullable": False},
               {"name": "amount", "type": "DECIMAL(12,2)", "nullable": False}],
    "key_columns": ["id"], "partition_columns": [], "constraints": {"primary_key": ["id"]},
}
REGISTERED = {"definition": DEFINITION, "definition_sha256": digest(DEFINITION)}
CONFIGURATION = {"space_id": "fixture", "generation": "gen_fixture"}
REQUEST = {"dataset": "dwd.orders", "schema_version": 1, "operation": "insert",
           "records": [{"id": 1, "amount": "1.00"}]}


class ObjectStore:
    def __init__(self, missing="NoSuchKey"):
        self.objects, self.lock, self.lose_reply, self.missing = {}, threading.Lock(), False, missing

    def put_object(self, **request):
        with self.lock:
            if request["Key"] in self.objects:
                raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
            assert request["IfNoneMatch"] == "*"
            self.objects[request["Key"]] = request
        if self.lose_reply:
            raise ReadTimeoutError(endpoint_url="qualification-reply-lost")
        return {"VersionId": "fixed-version"}

    def get_object(self, **request):
        with self.lock:
            if request["Key"] not in self.objects:
                raise ClientError({"Error": {"Code": self.missing}}, "GetObject")
            value = self.objects[request["Key"]]
            return {"Body": io.BytesIO(value["Body"]), "Metadata": value["Metadata"], "VersionId": "fixed-version"}


class ImmutableIntentTests(unittest.TestCase):
    def setUp(self):
        self.s3 = ObjectStore()
        self.now = datetime(2026, 10, 15, tzinfo=timezone.utc)
        self.intents = Intents(self.s3, "fixture-bucket", "123456789012", "a" * 40, lambda: self.now)

    def reserve(self, request=REQUEST, principal="owner"):
        return self.intents.reserve(CONFIGURATION, principal, request, REGISTERED, "original")

    def test_absent_intent_answering_access_denied_is_absent_not_unknown(self):
        # Without s3:ListBucket a missing key answers 403; after an isolated restore the original's intent lives
        # only in the source environment, while its native receipt is restored.
        self.s3.missing = "AccessDenied"
        self.assertIsNone(self.intents.read("fixture", "gen_fixture", "write_" + "0" * 64))
        self.intents.authorize_status("fixture", "gen_fixture", "write_" + "0" * 64, "anyone", False)
        with self.assertRaises(IntentUnknown):
            self.intents.read("fixture", "gen_fixture", "write_" + "0" * 64, "exact-version")

    def test_duplicate_semantics_keep_original_content_version_and_retry_window(self):
        first = self.reserve()
        self.now += timedelta(hours=1)
        equivalent = {**REQUEST, "records": [{"id": "1", "amount": "1.0"}], "timeout_ms": 1}
        second = self.reserve(equivalent)
        self.assertEqual(first[:3], second[:3])
        self.assertEqual((first[3], second[3]), (True, False))
        self.assertEqual(len(self.s3.objects), 1)
        self.assertNotIn("timeout_ms", second[1]["request"])

    def test_lost_put_reply_reads_same_fixed_original_without_replacement(self):
        self.s3.lose_reply = True
        identity, original, reference, fresh = self.reserve()
        self.assertEqual((original["write_id"], reference["version_id"]), (identity, "fixed-version"))
        self.assertFalse(fresh)
        self.assertEqual(len(self.s3.objects), 1)

    def test_changed_payload_and_other_owner_cannot_replace_original(self):
        self.reserve()
        changed = copy.deepcopy(REQUEST)
        changed["records"][0]["amount"] = "2.00"
        for request, principal, code in [
            (changed, "owner", "WRITE_BINDING_CONFLICT"), (REQUEST, "another", "WRITE_OWNER_REQUIRED"),
        ]:
            with self.assertRaises(ControlError) as denied:
                self.reserve(request, principal)
            self.assertEqual(denied.exception.code, code)
        self.assertEqual(len(self.s3.objects), 1)

    def test_expired_original_survives_and_never_becomes_new(self):
        first = self.reserve()
        self.now += timedelta(days=30)
        with self.assertRaises(ControlError) as expired:
            self.reserve()
        self.assertEqual(expired.exception.code, "WRITE_IDENTITY_EXPIRED")
        stored = self.intents.read(CONFIGURATION["space_id"], CONFIGURATION["generation"], first[0])
        self.assertEqual(stored[0], first[1])
        self.assertEqual(len(self.s3.objects), 1)

    def test_corruption_is_unknown_and_other_owner_cannot_observe_unregistered_original(self):
        identity = self.reserve()[0]
        with self.assertRaises(ControlError):
            self.intents.authorize_status("fixture", "gen_fixture", identity, "another", False)
        self.intents.authorize_status("fixture", "gen_fixture", identity, "admin", True)
        value = next(iter(self.s3.objects.values()))
        value["Body"] = value["Body"] + b" "
        with self.assertRaises(IntentUnknown):
            self.reserve()


if __name__ == "__main__":
    unittest.main()
