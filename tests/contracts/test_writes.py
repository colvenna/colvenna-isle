import copy
import unittest

from colvenna_isle.contracts.validation import ContractError, validate


DEFINITION = {
    "dataset": "dwd.orders", "schema_version": 1, "write_owner": "direct",
    "schema": [
        {"name": "id", "type": "BIGINT", "nullable": False},
        {"name": "amount", "type": "DECIMAL(12,2)", "nullable": False},
    ],
    "key_columns": ["id"], "partition_columns": [],
    "constraints": {"primary_key": ["id"], "checks": [
        {"column": "amount", "operator": ">=", "value": "0.00"}]},
}


class WriteContractTests(unittest.TestCase):
    def request(self, operation, **payload):
        return {"dataset": "dwd.orders", "schema_version": 1, "operation": operation, **payload}

    def test_four_forms_types_and_mutual_exclusion(self):
        forms = [
            self.request("insert", records=[{"id": 1, "amount": "2.00"}]),
            self.request("upsert", records=[{"id": 1, "amount": "2.00"}]),
            self.request("update", updates=[{"key": {"id": 1}, "set": {"amount": "3.00"}}]),
            self.request("delete", keys=[{"id": 1}]),
        ]
        for request in forms:
            validate("write_request", request, definition=DEFINITION)
            with self.assertRaises(ContractError):
                validate("write_request", {**request, "keys": [{"id": 1}], "records": [{"id": 1}]})
        for operation, payload in [
            ("insert", {"records": []}), ("upsert", {"records": [{"id": 1}]}),
            ("update", {"updates": [{"key": {"id": 1}, "set": {"id": 2}}]}),
            ("delete", {"keys": [{"id": 1, "amount": "3"}]}),
        ]:
            with self.assertRaises(ContractError):
                validate("write_request", self.request(operation, **payload), definition=DEFINITION)

    def test_typed_duplicate_key_and_stale_owner_schema(self):
        with self.assertRaises(ContractError):
            validate("write_request", self.request("delete", keys=[{"id": 1}, {"id": "1"}]),
                     definition=DEFINITION)
        for changed in [{**DEFINITION, "write_owner": "pipeline"}, {**DEFINITION, "schema_version": 2}]:
            with self.assertRaises(ContractError):
                validate("write_request", self.request("delete", keys=[{"id": 1}]), definition=changed)

    def test_metadata_key_cannot_substitute_for_native_constraint(self):
        definition = copy.deepcopy(DEFINITION)
        definition.pop("constraints")
        with self.assertRaises(ContractError):
            validate("dataset_registration", definition)
        definition["key_columns"] = []
        validate("dataset_registration", definition)  # Insert-only target is valid.
        with self.assertRaises(ContractError):
            validate("write_request", self.request("delete", keys=[{"id": 1}]), definition=definition)

    def test_budget_and_payload_bounds(self):
        request = self.request("insert", records=[{"id": 1, "amount": "2.00"}])
        for timeout in [0, 20001]:
            with self.assertRaises(ContractError):
                validate("write_request", {**request, "timeout_ms": timeout})
        with self.assertRaises(ContractError):
            validate("write_request", {**request, "records": request["records"] * 1001})

    def test_receipt_requires_real_commit_fields_and_rejection_has_no_sequence(self):
        receipt = {
            "write_id": "write_" + "a" * 64, "space_id": "fixture", "generation": "gen_fixture",
            "dataset": "dwd.orders", "schema_version": 1, "status": "COMMITTED",
            "retry_until": "2026-11-15T00:00:00Z",
        }
        with self.assertRaises(ContractError):
            validate("write_receipt", receipt)
        validate("write_receipt", {**receipt, "commit_sequence": 1, "dataset_watermark": 1,
                                  "affected_rows": 0, "committed_at": "2026-10-15T00:00:00Z"})
        with self.assertRaises(ContractError):
            validate("write_receipt", {**receipt, "status": "REJECTED", "commit_sequence": 1,
                "code": "NATIVE_CONSTRAINT", "rejected_at": "2026-10-15T00:00:00Z"})


if __name__ == "__main__":
    unittest.main()
