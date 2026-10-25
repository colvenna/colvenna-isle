"""Admission/ACK boundary using real native SQL and deterministic SDK replies."""

import hashlib
import io
import json
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

import duckdb

from colvenna_isle.control.registry import canonical
from colvenna_isle.writer.consumer import Consumer
from colvenna_isle.writer.ingestion import IngestionStore, digest
from test_transactions import DEFINITION, REGISTERED, payload


class Consumption(unittest.TestCase):
    def setUp(self):
        self.assertEqual(duckdb.__version__, "1.5.6")
        self.connection = duckdb.connect()
        self.connection.execute("ATTACH ':memory:' AS warehouse")
        for schema in ["ctl", "ods"]:
            self.connection.execute("CREATE SCHEMA warehouse." + schema)
        self.configuration = {"space_id": "test_space", "generation": "gen_" + "1" * 32,
                              "route_version": 1, "datasets": {"ods.orders": REGISTERED}}
        self.store = IngestionStore(self.connection, self.configuration, lambda: {"epoch": 1})
        self.runtime = Mock()
        self.runtime.registry._read.return_value = self.configuration
        self.runtime.database.return_value = SimpleNamespace(ingestions=self.store)
        self.s3, self.sqs = Mock(), Mock()
        self.sqs.delete_message.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
        self.consumer = Consumer(self.runtime, self.s3, self.sqs, "test-bucket", "queue", "123456789012", "a" * 40)

    def tearDown(self):
        self.connection.close()

    def delivery(self, value=None, key="one", modify=None):
        value = value or payload()
        raw = canonical(value).encode()
        ingestion_id = "ing_" + digest(key)
        message = {
            "contract_version": 2, "ingestion_id": ingestion_id, "space_id": "test_space",
            "generation": self.configuration["generation"], "route_version": 1,
            "dataset": "ods.orders", "schema_version": 1, "dataset_definition_sha256": digest(DEFINITION),
            "principal_id": "test_owner", "payload_sha256": digest(value),
            "source": {"bucket": "test-bucket",
                       "key": f"spaces/test_space/ingest/{self.configuration['generation']}/orders/{ingestion_id}.json",
                       "version_id": "fixed-version", "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()},
            "producer_source_commit": "a" * 40,
        }
        metadata = {
            "ingestion-id": ingestion_id, "space-id": "test_space",
            "space-generation": message["generation"], "dataset": "ods.orders", "schema-version": "1",
            "dataset-definition-sha256": digest(DEFINITION), "principal-id": "test_owner",
            "payload-sha256": digest(value), "raw-sha256": message["source"]["sha256"],
        }
        self.s3.get_object.side_effect = lambda **_: {
            "Body": io.BytesIO(raw), "VersionId": "fixed-version", "Metadata": metadata}
        self.s3.head_object.side_effect = lambda **_: {"VersionId": "fixed-version", "Metadata": metadata}
        if modify:
            modify(message)
        body = canonical(message)
        return {
            "Body": body, "MD5OfBody": hashlib.md5(body.encode(), usedforsecurity=False).hexdigest(),
            "MessageId": "message-" + key, "ReceiptHandle": "memory-only-receipt",
            "Attributes": {"MessageGroupId": "test_space", "MessageDeduplicationId": ingestion_id,
                           "ApproximateReceiveCount": "1"},
        }

    def test_fixed_source_commit_then_duplicate_acks_without_fetching_archive(self):
        delivery = self.delivery()
        first = self.consumer.process(delivery)
        self.s3.get_object.assert_called_once_with(
            Bucket="test-bucket", Key=json.loads(delivery["Body"])["source"]["key"],
            VersionId="fixed-version", ExpectedBucketOwner="123456789012")
        self.s3.get_object.side_effect = AssertionError("committed original needs no object reread")
        self.assertEqual(self.consumer.process(delivery), first)
        self.assertEqual(self.sqs.delete_message.call_count, 2)
        self.assertTrue(self.consumer.last_delivery["duplicate_batch"])
        self.assertEqual(self.store.probe("ods.orders")["commit_sequence"], 1)

    def test_bad_hash_and_schema_remain_queued_and_have_native_rejection_traces(self):
        for key, modify, code in [
            ("hash", lambda m: m["source"].update(sha256="b" * 64), "SOURCE_HASH_MISMATCH"),
            ("schema", lambda m: m.update(schema_version=2), "SCHEMA_IDENTITY_MISMATCH"),
        ]:
            delivery = self.delivery(key=key, modify=modify)
            self.assertIsNone(self.consumer.process(delivery))
            message = json.loads(delivery["Body"])
            self.assertEqual(self.store.status(message["ingestion_id"])["error_code"], code)
            self.assertEqual(self.store.diagnostics(message["ingestion_id"])["attempts"][0]["error_code"], code)
        self.sqs.delete_message.assert_not_called()
        self.assertEqual(self.sqs.change_message_visibility.call_count, 2)
        self.assertEqual(self.store.probe("ods.orders")["rows"], [])

    def test_wrong_group_producer_or_owner_metadata_cannot_commit_or_ack(self):
        delivery = self.delivery()
        delivery["Attributes"]["MessageGroupId"] = "other"
        self.consumer.process(delivery)
        self.consumer.process(self.delivery(key="producer", modify=lambda m: m.update(producer_source_commit="b" * 40)))
        delivery = self.delivery(key="principal", modify=lambda m: m.update(principal_id="other"))
        self.consumer.process(delivery)
        self.sqs.delete_message.assert_not_called()
        result = self.store.probe("ods.orders")
        self.assertEqual((result["rows"], result["ledger"], result["commit_sequence"]), ([], [], 0))

    def test_revocation_at_commit_rolls_back_all_data_and_does_not_ack(self):
        delivery = self.delivery()
        calls = 0
        def guard():
            nonlocal calls
            calls += 1
            # initialize uses 2 guards, existing and commit use additional guards.
            if calls >= 5:
                raise RuntimeError("revoked")
        self.store.guard = guard
        with self.assertRaisesRegex(RuntimeError, "revoked"):
            self.consumer.process(delivery)
        self.store.guard = lambda: None
        self.sqs.delete_message.assert_not_called()
        self.assertEqual(self.store.probe("ods.orders")["commit_sequence"], 0)

    def test_precommit_qualification_failure_and_redelivery_do_not_duplicate(self):
        delivery = self.delivery()
        self.consumer.fault = (json.loads(delivery["Body"])["ingestion_id"], "before_commit")
        with self.assertRaisesRegex(RuntimeError, "QUALIFICATION_BEFORE_COMMIT"):
            self.consumer.process(delivery)
        self.assertTrue(self.consumer.paused)
        self.assertEqual(self.store.probe("ods.orders")["rows"], [])
        self.sqs.delete_message.assert_not_called()
        self.sqs.change_message_visibility.assert_not_called()
        self.consumer.paused = False
        self.consumer.process(delivery)
        self.assertEqual(len(self.store.probe("ods.orders")["rows"]), 1)

    def test_capacity_uses_confirmed_new_commits_and_never_counts_duplicate_receipts(self):
        delivery = self.delivery()
        self.assertEqual(self.consumer.capacity()["state"], "UNKNOWN")
        self.consumer.process(delivery)
        self.consumer.process(delivery)
        result = self.consumer.capacity()
        self.assertEqual((result["state"], result["samples"]), ("MEASURED", 1))
        self.assertGreater(result["batches_per_second"], 0)
        self.consumer.paused = True
        self.assertEqual(self.consumer.capacity()["state"], "UNKNOWN")

    def test_capacity_includes_wait_for_shared_writer_resources(self):
        import time
        self.consumer.process(self.delivery(), queued_at=time.monotonic() - 3)
        result = self.consumer.capacity()
        self.assertEqual(result["samples"], 1)
        self.assertLessEqual(result["batches_per_second"], 0.25)


if __name__ == "__main__":
    unittest.main()
