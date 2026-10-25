"""Authentication/context boundaries; atomic storage and lifecycle use live acceptance."""

import json
import unittest

from botocore.exceptions import ClientError

from colvenna_isle.control.authorizer import authorize
from colvenna_isle.control.handler import handle
from colvenna_isle.control.keys import KeyStore
from colvenna_isle.control.registry import ControlError, Principal


IDENTITY = Principal("owner", frozenset(["space_a"]), frozenset(["datasets:manage"]),
                     "key_a", "gen_" + "a" * 32, "2027-01-01T00:00:00Z")


class Credentials:
    def __init__(self, principal=IDENTITY, error=None):
        self.principal = principal
        self.error = error

    def authenticate(self, value):
        if self.error:
            raise self.error
        return self.principal


class NoAccess:
    def __getattr__(self, name):
        raise AssertionError("Denied request reached data storage")


class KeyBoundaryTests(unittest.TestCase):
    def test_authorizer_returns_only_verified_context(self):
        result = authorize({"headers": {"Authorization": "Bearer " + "x" * 40}}, Credentials())
        self.assertTrue(result["isAuthorized"])
        self.assertEqual(result["context"]["principal_id"], "owner")
        self.assertEqual(json.loads(result["context"]["spaces"]), ["space_a"])
        self.assertNotIn("x" * 40, json.dumps(result))

    def test_bad_key_and_unavailable_auth_read_fail_closed(self):
        for error in [
            ControlError(403, "FORBIDDEN"),
            ClientError({"Error": {"Code": "AccessDeniedException", "Message": "secret-must-not-leak"}}, "GetItem"),
        ]:
            result = authorize({"headers": {"authorization": "Bearer secret-must-not-leak"}},
                               Credentials(error=error))
            self.assertEqual(result, {"isAuthorized": False})

    def test_service_rejects_context_that_disagrees_with_credential(self):
        context = authorize({}, Credentials())["context"]
        context["principal_id"] = "forged_owner"
        request = {"rawPath": "/v1/admin/spaces/space_a/configuration",
                   "requestContext": {"requestId": "test", "http": {"method": "GET"},
                                      "authorizer": {"lambda": context}}}
        response = handle(request, NoAccess(), Credentials())
        self.assertEqual(response["statusCode"], 403)

    def test_expired_context_cannot_authorize_operation(self):
        expired = Principal("owner", frozenset(["space_a"]), frozenset(["datasets:manage"]),
                            expires_at="2020-01-01T00:00:00Z")
        with self.assertRaises(ControlError):
            expired.authorize("space_a", "datasets:manage")

    def test_wrong_scope_cannot_reach_key_store(self):
        keys = KeyStore(NoAccess(), "unit_table")
        with self.assertRaises(ControlError) as rejected:
            keys.create(IDENTITY, "space_a", {
                "name": "forbidden", "scopes": ["query:read"], "expires_at": "2027-01-01T00:00:00Z"})
        self.assertEqual(rejected.exception.status, 403)


if __name__ == "__main__":
    unittest.main()


class BootstrapTable:
    """A control-table double with one provisioned space; transactions apply their puts unless told to lose a race."""

    def __init__(self, race=False):
        configuration = {"space_id": "space_a", "generation": "gen_" + "a" * 32}
        self.items = {("SPACE#space_a", "META"): {"configuration": {"S": json.dumps(configuration)},
                                                  "generation": {"S": configuration["generation"]}}}
        self.race, self.transactions = race, 0

    def get_item(self, TableName, Key, ConsistentRead):
        return {"Item": self.items[(Key["PK"]["S"], Key["SK"]["S"])]} if (
            Key["PK"]["S"], Key["SK"]["S"]) in self.items else {}

    def transact_write_items(self, TransactItems, ClientRequestToken):
        self.transactions += 1
        if self.race:
            # Another bootstrap of the same generation committed first.
            self.items[("SPACE#space_a", "BOOTSTRAP#gen_" + "a" * 32)] = {"key_id": {"S": "key_other"}}
            raise ClientError({"Error": {"Code": "TransactionCanceledException", "Message": "conditional"}},
                              "TransactWriteItems")
        for item in TransactItems:
            if "Put" in item:
                put = item["Put"]["Item"]
                self.items[(put["PK"]["S"], put["SK"]["S"])] = put


class BootstrapTests(unittest.TestCase):
    def test_a_generation_issues_its_first_administrator_key_once(self):
        table = BootstrapTable()
        store = KeyStore(table, "table")
        first = store.bootstrap("space_a", "2099-01-01T00:00:00Z")
        self.assertTrue(first["token"])
        with self.assertRaises(ControlError) as refused:
            store.bootstrap("space_a", "2099-01-01T00:00:00Z")
        self.assertEqual((refused.exception.status, refused.exception.code), (409, "BOOTSTRAP_ALREADY_ISSUED"))
        self.assertEqual(table.transactions, 1)

    def test_a_lost_bootstrap_race_reports_the_issued_key_not_a_generic_conflict(self):
        with self.assertRaises(ControlError) as refused:
            KeyStore(BootstrapTable(race=True), "table").bootstrap("space_a", "2099-01-01T00:00:00Z")
        self.assertEqual(refused.exception.code, "BOOTSTRAP_ALREADY_ISSUED")
