"""The machine contract distinguishes administration and positive terminal custody."""

import unittest

from colvenna_isle.contracts.validation import ContractError, validate, validate_transition


class QueryJobContract(unittest.TestCase):
    def test_explicit_management_scope_can_be_granted_without_expanding_query_data_access(self):
        validate("key_request", {"name": "query-administrator",
            "scopes": ["query:read", "query:manage"], "expires_at": "2099-01-01T00:00:00Z"})

    def test_queued_cancel_requires_positive_no_execution_and_running_terminals_require_reaping(self):
        with self.assertRaises(ContractError):
            validate_transition("query", "QUEUED", "CANCELLED")
        validate_transition("query", "QUEUED", "CANCELLED", {"execution_never_started": True})
        validate_transition("query", "CANCELLING", "CANCELLED", {"execution_never_started": True})
        for before, after in [("RUNNING", "SUCCEEDED"), ("RUNNING", "FAILED"),
                              ("CANCELLING", "CANCELLED")]:
            with self.assertRaises(ContractError):
                validate_transition("query", before, after)
            validate_transition("query", before, after, {"execution_reaped": True})
        with self.assertRaises(ContractError):
            validate_transition("query", "RUNNING", "SUCCEEDED", {"execution_never_started": True})
        with self.assertRaises(ContractError):
            validate_transition("query", "CANCELLING", "SUCCEEDED",
                                {"execution_reaped": True, "completion_confirmed": True})


if __name__ == "__main__":
    unittest.main()
