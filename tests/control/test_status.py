"""Public status authorization/reconciliation; real CAS and outage gates use AWS."""

import copy
import json
import unittest

from botocore.exceptions import BotoCoreError

from colvenna_isle.control.handler import handle
from colvenna_isle.control.registry import ControlError, Registry
from colvenna_isle.control.status import Projection, Status
from colvenna_isle.contracts.validation import validate


INGESTION = "ing_" + "a" * 64
GENERATION = "gen_" + "1" * 32
NATIVE = {
    "ingestion_id": INGESTION, "space_id": "test_space", "generation": GENERATION,
    "principal_id": "test_owner", "dataset": "ods.orders", "status": "COMMITTED",
    "commit_sequence": 1, "projection_version": 2, "observed_at": "2026-10-15T10:00:00+00:00",
    "committed_at": "2026-10-15T10:00:00+00:00",
    "publication": {"status": "NOT_PUBLISHED"},
    "layers": {"ods": {"status": "COMMITTED", "watermark": INGESTION},
               "dwd": {"status": "NOT_IMPLEMENTED"}, "dws": {"status": "NOT_IMPLEMENTED"},
               "ads": {"status": "NOT_IMPLEMENTED"}},
}


class DdbReplies:
    def __init__(self):
        self.item = None
        self.unavailable = False

    def get_item(self, **kwargs):
        if self.unavailable:
            raise BotoCoreError()
        return {"Item": copy.deepcopy(self.item)} if self.item else {}

    def put_item(self, **kwargs):
        if self.unavailable:
            raise BotoCoreError()
        self.item = copy.deepcopy(kwargs["Item"])


class RegistryReplies:
    _authorize_generation = Registry._authorize_generation

    def _read(self, space):
        return {"space_id": space, "generation": GENERATION,
                "datasets": {"ods.orders": {"definition_sha256": "b" * 64}}}


class LedgerReplies:
    def __init__(self):
        self.value, self.calls = copy.deepcopy(NATIVE), 0

    def read(self, configuration, ingestion_id):
        self.calls += 1
        if self.value is None:
            raise ControlError(503, "AUTHORITY_UNAVAILABLE")
        return copy.deepcopy(self.value)


class ArchiveReplies:
    def head_object(self, **kwargs):
        return {"Metadata": {
            "ingestion-id": INGESTION, "space-id": "test_space", "space-generation": GENERATION,
            "dataset": "ods.orders", "dataset-definition-sha256": "b" * 64, "principal-id": "test_owner"}}


def event(principal="test_owner", scopes=("ingest:write",), space="test_space"):
    return {
        "rawPath": f"/v1/spaces/{space}/ingestions/{INGESTION}",
        "requestContext": {"http": {"method": "GET"}, "authorizer": {"lambda": {
            "principal_id": principal, "spaces": '["test_space"]', "scopes": json.dumps(list(scopes)),
            "generation": GENERATION}}},
    }


class StatusApi(unittest.TestCase):
    def setUp(self):
        self.ddb, self.ledger = DdbReplies(), LedgerReplies()
        self.projection = Projection(self.ddb, "table")
        self.registry = RegistryReplies()
        self.status = Status(self.registry, self.projection, self.ledger, ArchiveReplies(), "bucket", "account")

    def request(self, request=None):
        response = handle(request or event(), self.registry, statuses=self.status)
        value = json.loads(response["body"])
        if response["statusCode"] == 200:
            validate("ingestion_status", value)
        return response["statusCode"], value

    def test_missing_projection_rebuilds_native_commit_with_version_and_observed_at(self):
        code, value = self.request()
        self.assertEqual(code, 200)
        self.assertEqual((value["status"], value["commit_sequence"], value["projection_version"]),
                         ("COMMITTED", 1, 2))
        self.assertEqual(value["observed_at"], "2026-10-15T10:00:00+00:00")
        self.assertEqual(value["projection_state"], "REBUILT")
        self.assertEqual(value["publication"], {"status": "NOT_PUBLISHED"})
        self.assertEqual(value["layers"]["ads"]["status"], "NOT_IMPLEMENTED")
        self.assertNotIn("principal_id", value)
        self.assertEqual(self.projection.decode(self.ddb.item, "test_space", GENERATION, INGESTION), NATIVE)

    def test_other_owner_and_wrong_space_or_scope_cannot_read(self):
        for request in [event(principal="other_owner"), event(space="other_space"),
                        event(scopes=("query:read",))]:
            with self.subTest(request=request):
                self.assertEqual(self.request(request)[0], 403)
        self.assertEqual(self.request(event(principal="admin", scopes=("monitor:read",)))[0], 200)

    def test_projection_read_and_write_outage_does_not_hide_confirmed_native_commit(self):
        self.ddb.unavailable = True
        code, value = self.request()
        self.assertEqual(code, 200)
        self.assertEqual((value["status"], value["projection_version"], value["projection_state"]),
                         ("COMMITTED", 2, "UNAVAILABLE"))
        self.assertTrue(value["reconciling"])

    def test_corrupt_projection_is_replaced_from_native_instead_of_reported_as_failed(self):
        self.ddb.item = self.projection.item(NATIVE)
        self.ddb.item["observation"]["S"] = '{"status":"FAILED"}'
        code, value = self.request()
        self.assertEqual((code, value["status"], value["projection_state"]), (200, "COMMITTED", "REBUILT"))

    def test_no_projection_or_authority_returns_unknown_for_archived_owner(self):
        self.ledger.value = None
        code, value = self.request()
        self.assertEqual((code, value["status"], value["projection_version"]), (200, "UNKNOWN", 0))
        self.assertTrue(value["reconciling"])
        self.assertEqual(self.request(event(principal="other_owner"))[0], 403)

    def test_known_native_absence_cannot_turn_a_conflicting_projection_into_a_commit(self):
        self.ddb.item = self.projection.item(NATIVE)
        self.ledger.value = {"ingestion_id": INGESTION, "status": "UNKNOWN"}
        self.assertEqual(self.request()[1]["status"], "UNKNOWN")
        self.ledger.value = None
        self.assertEqual(self.request()[1]["status"], "UNKNOWN")

    def test_cached_projection_cannot_forge_an_owner_when_native_is_unavailable(self):
        forged = {**NATIVE, "principal_id": "other_owner"}
        self.ddb.item = self.projection.item(forged)
        self.ledger.value = None
        self.assertEqual(self.request(event(principal="other_owner"))[0], 403)


if __name__ == "__main__":
    unittest.main()
