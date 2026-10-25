"""Native commit and durable diagnostic gates precede actual dead-letter ACK."""

import json
import threading
import unittest

import test_consumer as fixture_module
from colvenna_isle.writer.dlq import DeadLetters


class ReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixture_module.Consumption()
        self.fixture.setUp()
        self.consumer = self.fixture.consumer
        self.runtime = self.fixture.runtime
        self.runtime.lock = threading.Lock()
        self.runtime.databases = {
            "test_space/" + self.fixture.configuration["generation"]: self.runtime.database.return_value}
        self.dead_letters = DeadLetters(self.runtime, self.fixture.s3, self.fixture.sqs,
                                        "test-bucket", "dead-letter-queue", "123456789012")

    def tearDown(self):
        self.fixture.tearDown()

    def delivery(self):
        original = self.fixture.delivery()
        message = json.loads(original["Body"])
        head = self.fixture.s3.head_object()
        head["ContentLength"] = message["source"]["bytes"]
        self.fixture.s3.head_object.return_value = head
        self.fixture.s3.head_object.side_effect = None
        return original, message

    def test_uncommitted_failed_or_unopened_original_is_retained(self):
        delivery, message = self.delivery()
        self.assertIsNone(self.dead_letters.reconcile(delivery))
        self.fixture.store.reject(message, delivery["MessageId"], "KNOWN_POISON")
        self.assertIsNone(self.dead_letters.reconcile(delivery))
        self.runtime.databases = {}
        self.assertIsNone(self.dead_letters.reconcile(delivery))
        self.fixture.sqs.delete_message.assert_not_called()

    def test_confirmed_original_records_poison_hash_before_idempotent_dlq_delete(self):
        delivery, message = self.delivery()
        self.consumer.process(delivery)
        self.fixture.sqs.delete_message.reset_mock()
        poison = json.loads(delivery["Body"])
        poison["source"]["sha256"] = "f" * 64
        delivery["Body"] = json.dumps(poison)
        first = self.dead_letters.reconcile(delivery)
        repeated = self.dead_letters.reconcile(delivery)
        self.assertEqual(first, repeated)
        self.assertEqual(first["commit_sequence"], 1)
        diagnostics = self.fixture.store.diagnostics(message["ingestion_id"])
        self.assertEqual(diagnostics["dlq_reconciliations"], [first])
        self.assertEqual(self.fixture.sqs.delete_message.call_count, 2)
        self.assertEqual(len(self.fixture.store.probe("ods.orders")["rows"]), 1)

    def test_native_diagnostic_write_or_ack_uncertainty_never_becomes_a_new_batch(self):
        delivery, message = self.delivery()
        self.consumer.process(delivery)
        self.fixture.sqs.delete_message.reset_mock()
        original = self.fixture.store.reconcile_dead_letter
        self.fixture.store.reconcile_dead_letter = lambda _: (_ for _ in ()).throw(RuntimeError("native unconfirmed"))
        with self.assertRaises(RuntimeError):
            self.dead_letters.reconcile(delivery)
        self.fixture.sqs.delete_message.assert_not_called()
        self.fixture.store.reconcile_dead_letter = original
        self.fixture.sqs.delete_message.return_value = {"ResponseMetadata": {"HTTPStatusCode": 500}}
        with self.assertRaisesRegex(RuntimeError, "DLQ_DELETE_UNKNOWN"):
            self.dead_letters.reconcile(delivery)
        self.assertEqual(len(self.fixture.store.diagnostics(message["ingestion_id"])["dlq_reconciliations"]), 1)
        self.assertEqual(self.fixture.store.probe("ods.orders")["commit_sequence"], 1)


if __name__ == "__main__":
    unittest.main()
