"""Current names and explicit rejection of historical public interfaces."""

import json
import os
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from colvenna_isle import account
from colvenna_isle.contracts.validation import ContractError, validate
from colvenna_isle.query.service import IDENTITY_HEADERS, verified_principal
from colvenna_isle.control.registry import ControlError


ROOT = Path(__file__).resolve().parents[2]
HISTORY = json.loads((ROOT / "docs/history/v1.1.0-identifiers.json").read_text())


class BrandMigration(unittest.TestCase):
    def test_new_package_installed_old_import_unavailable(self):
        result = subprocess.run([sys.executable, "-c",
            "import importlib.util,sys; import colvenna_isle; "
            "assert importlib.util.find_spec(sys.argv[1]) is None", HISTORY["package"]],
            capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_account_uses_only_current_variable(self):
        with patch.dict(os.environ, {"COLVENNA_ACCOUNT": "123456789012",
                                     HISTORY["account_variable"]: "invalid"}, clear=True):
            with patch.object(account, "_resolved", None):
                self.assertEqual(account.account(), "123456789012")
        with patch.dict(os.environ, {HISTORY["account_variable"]: "invalid"}, clear=True):
            with patch.object(account, "_resolved", None), patch("colvenna_isle.account.region", return_value="us-west-2"):
                with patch("boto3.client") as sdk:
                    sdk.return_value.get_caller_identity.return_value = {"Account": "123456789012"}
                    self.assertEqual(account.account(), "123456789012")
                    sdk.return_value.get_caller_identity.assert_called_once()

    def test_old_gateway_identity_header_does_not_authorize(self):
        headers = {value: "fixture" for value in IDENTITY_HEADERS.values()}
        del headers[IDENTITY_HEADERS["principal_id"]]
        headers[HISTORY["principal_header"]] = "fixture"
        with self.assertRaises(ControlError) as error:
            verified_principal(headers, None)
        self.assertEqual(error.exception.code, "FORBIDDEN")

    def test_old_contract_revision_is_rejected(self):
        manifest = {"contract_version": 2, "dataset": "ods.orders", "release_id": "release_1",
                    "watermark": {"ods_sequence": 1},
                    "schema": [{"name": "id", "type": "BIGINT", "nullable": False}], "files": []}
        validate("input_manifest", manifest)
        manifest["contract_version"] = HISTORY["contract_version"]
        with self.assertRaises(ContractError):
            validate("input_manifest", manifest)
