"""Contract and HTTP authorization boundaries; live storage is checked separately."""

import copy
import json
import unittest

from colvenna_isle.contracts.validation import ContractError, validate
from colvenna_isle.control.handler import handle, parse_body


def definition(dataset="ods.orders"):
    return {
        "dataset": dataset, "schema_version": 1,
        "schema": [{"name": "order_id", "type": "BIGINT", "nullable": False},
                   {"name": "amount", "type": "DECIMAL(18,2)", "nullable": False},
                   {"name": "event_id", "type": "VARCHAR", "nullable": False},
                   {"name": "source_version", "type": "BIGINT", "nullable": False}],
        "key_columns": ["order_id"], "partition_columns": [],
        "ingestion": {"operations": ["append", "upsert", "delete"],
                      "event_id_column": "event_id", "source_version_column": "source_version"},
    }


def event(space="test_space", scopes=("datasets:manage",), authorized_space="test_space"):
    return {
        "rawPath": f"/v1/admin/spaces/{space}/datasets",
        "requestContext": {"requestId": "test_request", "http": {"method": "POST"},
                           "authorizer": {"lambda": {"principal_id": "test_owner",
                               "spaces": json.dumps([authorized_space]), "scopes": json.dumps(list(scopes))}}},
        "headers": {"content-type": "application/json"}, "body": json.dumps(definition()),
    }


class NoStorage:
    def register(self, *args):
        raise AssertionError("Forbidden request reached storage")


class RegistryContractTests(unittest.TestCase):
    def test_valid_ods_and_analytics_registration(self):
        validate("dataset_registration", definition())
        analytics = definition("ads.orders")
        del analytics["ingestion"]
        validate("dataset_registration", analytics)

    def test_illegal_types_columns_keys_and_event_metadata(self):
        for change in [
            lambda d: d["schema"][1].update(type="DECIMAL(39,2)"),
            lambda d: d["schema"][1].update(type="INTEGER"),
            lambda d: d["schema"].append(copy.deepcopy(d["schema"][0])),
            lambda d: d["schema"].append({"name": "ORDER_ID", "type": "BIGINT", "nullable": False}),
            lambda d: d["schema"].append({"name": "_CoLvEnNa_operation", "type": "VARCHAR", "nullable": True}),
            lambda d: d.update(dataset="ods.Orders"),
            lambda d: d.update(key_columns=["missing"]),
            lambda d: d["schema"][0].update(nullable=True),
            lambda d: d.update(dataset="ctl.ingestions"),
            lambda d: d.update(dataset="ads.orders"),
            lambda d: d["ingestion"].update(source_version_column="amount"),
            lambda d: d.update(unknown=True),
        ]:
            value = definition()
            change(value)
            with self.subTest(definition=value), self.assertRaises(ContractError):
                validate("dataset_registration", value)

    def test_caller_cannot_supply_identity_in_headers_or_body(self):
        request = event()
        del request["requestContext"]["authorizer"]
        request["headers"]["x-colvenna-principal"] = "administrator"
        request["body"] = json.dumps({"principal_id": "administrator", "scopes": ["datasets:manage"]})
        self.assertEqual(handle(request, NoStorage())["statusCode"], 403)

    def test_cross_space_and_wrong_scope_rejected_before_storage(self):
        self.assertEqual(handle(event(space="other_space"), NoStorage())["statusCode"], 403)
        self.assertEqual(handle(event(scopes=("ingest:write",)), NoStorage())["statusCode"], 403)

    def test_malformed_authorizer_context_is_denied(self):
        for value in [None, "not-json", "null", '"test_space"', '{"test_space": true}']:
            request = event()
            request["requestContext"]["authorizer"]["lambda"]["spaces"] = value
            self.assertEqual(handle(request, NoStorage())["statusCode"], 403)

    def test_registration_requires_explicit_generation(self):
        request = event()
        self.assertEqual(handle(request, NoStorage())["statusCode"], 428)
        request["headers"]["if-match"] = '"rev_1"'
        self.assertEqual(handle(request, NoStorage())["statusCode"], 428)

    def test_duplicate_json_keys_nonfinite_values_and_unknown_fields_rejected(self):
        request = event()
        for body in ['{"dataset":"ods.a","dataset":"ods.b"}', '{"value":NaN}']:
            request["body"] = body
            with self.assertRaises(ContractError):
                parse_body(request)


if __name__ == "__main__":
    unittest.main()


class RestoreRegistryTests(unittest.TestCase):
    def test_restore_keeps_space_generation_and_datasets_and_rebinds_only_the_bucket(self):
        from botocore.exceptions import ClientError
        from colvenna_isle.control.registry import Registry

        class Table:
            def __init__(self):
                self.items = {}

            def transact_write_items(self, TransactItems):
                puts = [operation["Put"] for operation in TransactItems]
                keys = [(put["Item"]["PK"]["S"], put["Item"]["SK"]["S"]) for put in puts]
                if any(key in self.items for key in keys):
                    raise ClientError({"Error": {"Code": "TransactionCanceledException"}}, "TransactWriteItems")
                self.items.update({key: put["Item"] for key, put in zip(keys, puts)})

            def get_item(self, TableName, Key, ConsistentRead):
                item = self.items.get((Key["PK"]["S"], Key["SK"]["S"]))
                return {"Item": item} if item else {}

        source = Registry(Table(), "control", "source-bucket")._new_space("restored")
        source.update(revision=4, route_version=2, datasets={})
        table = Table()
        target = Registry(table, "control", "target-bucket")
        operator = {"kind": "IAM_OPERATOR", "principal_id": "iam_" + "0" * 32, "key_id": None, "fingerprint": None}
        details = {"backup_id": "backup_" + "1" * 32, "commit_sequence": 7}
        value = target.restore(source, operator, details)
        self.assertEqual({name: value[name] for name in ["space_id", "generation", "revision", "route_version"]},
                         {name: source[name] for name in ["space_id", "generation", "revision", "route_version"]})
        self.assertEqual(value["database_uri"],
                         f"s3://target-bucket/spaces/restored/{source['generation']}/warehouse")
        self.assertEqual(target._read("restored"), value)
        audit = [item for key, item in table.items.items() if key[0].startswith("AUDIT#")]
        self.assertEqual([item["action"]["S"] for item in audit], ["SPACE_RESTORED"])
        with self.assertRaises(Exception) as raised:
            target.restore(source, operator, details)
        self.assertEqual(raised.exception.code, "RESTORE_TARGET_SPACE_EXISTS")
