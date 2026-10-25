"""Qualification operations cannot escape their generation or alter a receipt."""

import copy
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import duckdb

from colvenna_isle.control.registry import ControlError
from colvenna_isle.writer.ingestion import IngestionStore
from colvenna_isle.writer.write_qualification import LoseReply, WriteQualification
from colvenna_isle.writer.writes import WriteStore, write_id
from test_transactions import CONFIGURATION
from test_writes import REGISTERED, request


class NativeQualification(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.connection = duckdb.connect()
        path = str(Path(self.directory.name) / "warehouse.db")
        self.connection.execute("ATTACH '" + path.replace("'", "''") + "' AS warehouse")
        for schema in ["ctl", "dwd"]:
            self.connection.execute("CREATE SCHEMA warehouse." + schema)
        self.configuration = copy.deepcopy(CONFIGURATION)
        self.configuration["space_id"] = "probe_writes_local"
        self.configuration["datasets"]["dwd.orders"] = REGISTERED
        self.native = IngestionStore(self.connection, self.configuration, lambda: {"epoch": 1})
        self.store = WriteStore(self.native)
        self.identity = write_id(self.configuration["space_id"], self.configuration["generation"], "dwd.orders", "one")
        self.receipt = self.store.commit(self.identity, "owner", request(), REGISTERED, lambda: None)
        original = {"dataset": "dwd.orders", "principal_id": "owner", "request": request()}
        self.runtime = SimpleNamespace(
            probe_space=self.configuration["space_id"], probe_generation=self.configuration["generation"],
            direct=SimpleNamespace(intents=SimpleNamespace(read=lambda *args: (original, {}))),
            log=lambda *args: None)
        self.probe = WriteQualification(self.runtime)
        self.database = SimpleNamespace(writes=self.store)

    def tearDown(self):
        self.connection.close()
        self.directory.cleanup()

    def test_real_not_null_failure_rolls_back_and_original_receipt_is_unchanged(self):
        result = self.probe.execute("probe_write_not_null", self.configuration,
                                    {"write_id": self.identity, "column": "region"}, self.database)
        self.assertEqual(result, {"write_id": self.identity, "constraint": "NOT NULL", "rejected": True,
                                  "original_receipt_unchanged": True})
        self.assertEqual(self.store.probe("dwd.orders")["rows"], [[1, "one", "1", "west"]])
        self.assertEqual(self.store.status(self.identity, "owner"), self.receipt)

    def test_simulated_expiry_keeps_original_result_and_refuses_new_execution(self):
        result = self.probe.execute("probe_write_expiry", self.configuration,
                                    {"write_id": self.identity}, self.database)
        self.assertEqual(result["mode"], "SIMULATED_CLOCK_BOUNDARY")
        with self.assertRaises(ControlError) as rejected:
            self.store.commit(self.identity, "owner", request(), REGISTERED, lambda: None)
        self.assertEqual(rejected.exception.code, "WRITE_IDENTITY_EXPIRED")
        self.assertEqual(self.store.status(self.identity, "owner"), self.receipt)

    def test_wrong_generation_and_unadmitted_space_cannot_arm_a_fault(self):
        for configuration in [{**self.configuration, "generation": "gen_other"},
                              {**self.configuration, "space_id": "orders"}]:
            with self.assertRaises(ControlError):
                self.probe.execute("probe_write_fault", configuration,
                    {"write_id": self.identity, "fault": "after_commit_disconnect"}, None)
        self.assertEqual(self.probe.faults, {})
        self.probe.execute("probe_write_fault", self.configuration,
                           {"write_id": self.identity, "fault": "after_commit_disconnect"}, None)
        with self.assertRaises(LoseReply):
            self.probe.after_commit(self.identity)
        self.probe.after_commit(self.identity)  # Fault cannot affect a later retry.
        self.assertEqual(self.store.status(self.identity, "owner"), self.receipt)


if __name__ == "__main__":
    unittest.main()
