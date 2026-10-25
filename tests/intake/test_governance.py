"""Admission and replay public seams, including original identity and uncertainty."""

import io
import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

from botocore.exceptions import ReadTimeoutError

from colvenna_isle.control.registry import ControlError, Principal
from colvenna_isle.intake.backpressure import Admission, Backpressure, DEFAULT_POLICY, retention_decision
from colvenna_isle.intake.handler import handle
from colvenna_isle.intake.replay import Replay
import test_admission as fixture_module
from test_admission import GENERATION, PAYLOAD, PRINCIPAL, SPACE, event


class PressureTests(unittest.TestCase):
    def setUp(self):
        self.sqs, self.cloudwatch, self.ledger = Mock(), Mock(), Mock()
        self.admission = Admission(self.sqs, self.cloudwatch, self.ledger, "https://queue/main.fifo", "dlq")
        self.configuration = {"ingestion_policy": dict(DEFAULT_POLICY)}
        self.ledger.capacity.return_value = {"state": "MEASURED", "batches_per_second": 1}
        self.pending(0)

    def pending(self, count, age=0, missing=False):
        self.sqs.get_queue_attributes.return_value = {"Attributes": {
            "ApproximateNumberOfMessages": str(count), "ApproximateNumberOfMessagesNotVisible": "0",
            "ApproximateNumberOfMessagesDelayed": "0"}}
        self.cloudwatch.get_metric_statistics.return_value = {
            "Datapoints": [] if missing else [{"Timestamp": datetime.now(timezone.utc), "Maximum": age}]}

    def test_each_backpressure_dimension_and_missing_observation(self):
        for count, age, rate, policy, code in [
            (1000, 0, 1, {}, "QUEUE_BACKLOG"),
            (1, 3600, 1, {}, "QUEUE_AGE"),
            (2, 0, .001, {}, "WRITER_DRAIN_BUDGET"),
            (20, 0, None, {}, "WRITER_CAPACITY_UNAVAILABLE"),
            (0, 0, 1, {"max_batch_records": 1}, "SPACE_BATCH_QUOTA"),
        ]:
            with self.subTest(code=code):
                self.pending(count, age)
                self.ledger.capacity.return_value = {"state": "MEASURED", "batches_per_second": rate}
                self.configuration["ingestion_policy"] = {**DEFAULT_POLICY, **policy}
                with self.assertRaises(Backpressure) as rejected:
                    self.admission.check(self.configuration, 2, 256)
                self.assertEqual(rejected.exception.code, code)
        self.pending(1, missing=True)
        self.configuration["ingestion_policy"] = dict(DEFAULT_POLICY)
        with self.assertRaises(ControlError) as unknown:
            self.admission.check(self.configuration, 1, 256)
        self.assertEqual(unknown.exception.status, 503)

    def test_bounded_buffer_allows_recovery_without_claiming_healthy_writer(self):
        self.ledger.capacity.side_effect = ControlError(503, "AUTHORITY_UNAVAILABLE")
        value = self.admission.check(self.configuration, 1, 256)
        self.assertEqual(value["capacity"]["state"], "UNKNOWN")
        self.assertEqual(value["pending_messages"], 0)

    def test_archive_and_retry_windows_and_unreconciled_originals(self):
        original = datetime(2026, 1, 1, tzinfo=timezone.utc)
        replay = original + timedelta(days=44)
        def decision(status="COMMITTED", queue=True, dlq=True, day=57):
            return retention_decision(original.isoformat(), replay.isoformat(), status, queue, dlq,
                                      original + timedelta(days=day))
        self.assertFalse(decision()["cleanup_eligible"])
        self.assertTrue(decision(day=58)["cleanup_eligible"])
        for status, queue, dlq in [("UNKNOWN", True, True), ("FAILED", True, True),
                                   ("COMMITTED", False, True), ("COMMITTED", True, False)]:
            self.assertFalse(decision(status, queue, dlq, 100)["cleanup_eligible"])


class AuditTrail:
    """The control audit seam: records exactly once per original replay intent."""

    def __init__(self):
        from colvenna_isle.control.audit import Audit
        self.items = {}
        self.client, self.table = self, "unit_table"
        self.writer = Audit(self, "unit_table")

    def put(self, *args, **fields):
        return self.writer.put(*args, **fields)

    def transact_write_items(self, TransactItems):
        from botocore.exceptions import ClientError
        keys = [(entry["Put"]["Item"]["PK"]["S"], entry["Put"]["Item"]["SK"]["S"]) for entry in TransactItems]
        if keys[0] in self.items:
            raise ClientError({"Error": {"Code": "TransactionCanceledException"},
                               "CancellationReasons": [{"Code": "ConditionalCheckFailed"}]}, "TransactWriteItems")
        for key, entry in zip(keys, TransactItems):
            self.items[key] = entry["Put"]["Item"]

    def records(self):
        return [json.loads(item["record"]["S"]) for key, item in self.items.items() if key[0].startswith("AUDIT#")]


class ReplayTests(unittest.TestCase):
    def setUp(self):
        fixture = fixture_module.AdmissionTests()
        fixture.setUp()
        self.intake, self.objects, self.queue, self.registry = (
            fixture.intake, fixture.objects, fixture.queue, fixture.registry)
        self.accepted = fixture.submit()
        self.message = json.loads(self.queue.messages[0]["MessageBody"])
        self.key = self.message["source"]["key"]
        self.registry._read = Mock(return_value={
            "space_id": SPACE, "generation": GENERATION, "route_version": 1,
            "datasets": {"ods.orders": self.registry.dataset(PRINCIPAL, SPACE, "ods.orders", "ingest:write")[1]}})
        self.registry._authorize_generation = Mock()
        self.ledger = Mock()
        self.ledger.read.return_value = {"status": "UNKNOWN"}
        self.intake.admission = Mock()
        self.owner = Principal(PRINCIPAL.principal_id, PRINCIPAL.spaces,
                               frozenset(["ingest:write", "ingest:replay"]), PRINCIPAL.key_id,
                               GENERATION, PRINCIPAL.expires_at)
        self.audit = AuditTrail()
        self.replay = Replay(self.intake, self.ledger, self.audit)
        source = self.objects.objects[self.key]
        self.head = {"Metadata": source["Metadata"], "VersionId": "first-version",
                     "ContentLength": len(source["Body"]), "LastModified": datetime.now(timezone.utc)}
        self.objects.head_object = Mock(return_value=self.head)
        self.original_get = self.objects.get_object

    def test_original_payload_and_binding_are_replayed_without_caller_content(self):
        status, value = self.replay.submit(self.owner, SPACE, self.accepted["ingestion_id"], "request1")
        self.assertEqual(status, 202)
        self.assertEqual(value["ingestion_id"], self.accepted["ingestion_id"])
        self.assertEqual(self.queue.messages[0], self.queue.messages[1])
        self.assertEqual(len(self.objects.objects), 2)  # original + immutable audit
        self.assertEqual(self.objects.objects[self.key]["Body"], json.dumps(PAYLOAD).encode())
        records = self.audit.records()
        self.assertEqual([record["action"] for record in records], ["INGESTION_REPLAY_INTENT"])
        self.assertEqual((records[0]["outcome"], records[0]["actor"]["principal_id"], records[0]["target"]),
                         ("INTENT", self.owner.principal_id, {"kind": "ingestion", "id": self.accepted["ingestion_id"]}))
        self.assertNotIn(json.dumps(PAYLOAD), json.dumps(records))

    def test_retried_original_replay_records_one_audit_intent(self):
        self.replay.submit(self.owner, SPACE, self.accepted["ingestion_id"], "request1")
        self.replay.submit(self.owner, SPACE, self.accepted["ingestion_id"], "request1")
        self.assertEqual(len(self.audit.records()), 1)
        self.replay.submit(self.owner, SPACE, self.accepted["ingestion_id"], "request2")
        self.assertEqual(len(self.audit.records()), 2)

    def test_native_commit_reconciles_without_queue_or_admission(self):
        self.ledger.read.return_value = {
            "status": "COMMITTED", "principal_id": self.owner.principal_id,
            "generation": GENERATION, "commit_sequence": 7,
            "ingestion_id": self.accepted["ingestion_id"], "space_id": SPACE,
            "dataset": self.message["dataset"], "payload_sha256": self.message["payload_sha256"],
            "source": self.message["source"]}
        status, result = self.replay.submit(self.owner, SPACE, self.accepted["ingestion_id"], "request1")
        self.assertEqual((status, result["commit_sequence"]), (200, 7))
        self.assertEqual(len(self.queue.messages), 1)
        self.intake.admission.check.assert_not_called()

    def test_unknown_authority_never_dispatches_or_audits(self):
        self.ledger.read.side_effect = ControlError(503, "AUTHORITY_UNAVAILABLE")
        with self.assertRaises(ControlError):
            self.replay.submit(self.owner, SPACE, self.accepted["ingestion_id"], "request1")
        self.assertEqual(len(self.queue.messages), 1)
        self.assertEqual(len(self.objects.objects), 1)

    def test_other_owner_missing_scope_and_expired_archive_are_denied(self):
        for principal in [
            PRINCIPAL,
            Principal("other", self.owner.spaces, self.owner.scopes),
            Principal(self.owner.principal_id, frozenset(["other"]), self.owner.scopes),
        ]:
            with self.assertRaises(ControlError):
                self.replay.submit(principal, SPACE, self.accepted["ingestion_id"], "request1")
        self.head["LastModified"] -= timedelta(days=30)
        with self.assertRaises(ControlError) as expired:
            self.replay.submit(self.owner, SPACE, self.accepted["ingestion_id"], "request1")
        self.assertEqual(expired.exception.code, "REPLAY_WINDOW_EXPIRED")
        self.assertEqual(len(self.queue.messages), 1)

    def test_modified_fixed_archive_and_unconfirmed_audit_do_not_dispatch(self):
        self.objects.get_object = Mock(return_value={
            "Metadata": self.head["Metadata"], "VersionId": "first-version", "Body": io.BytesIO(b"tampered")})
        with self.assertRaises(ControlError):
            self.replay.submit(self.owner, SPACE, self.accepted["ingestion_id"], "request1")
        self.objects.get_object = self.original_get
        self.objects.put_object = Mock(side_effect=ReadTimeoutError(endpoint_url="fixture"))
        self.objects.get_object = Mock(side_effect=ReadTimeoutError(endpoint_url="fixture"))
        with self.assertRaises(Exception):
            self.replay.submit(self.owner, SPACE, self.accepted["ingestion_id"], "request1")
        self.assertEqual(len(self.queue.messages), 1)

    def test_public_backpressure_preserves_existing_confirmed_work(self):
        self.intake.admission.check.side_effect = Backpressure("QUEUE_BACKLOG")
        response = handle(event(), self.intake, Mock(authenticate=Mock(return_value=PRINCIPAL)))
        self.assertEqual(response["statusCode"], 429)
        self.assertEqual(response["headers"]["retry-after"], "5")
        self.assertEqual(len(self.queue.messages), 1)
        self.assertEqual(len(self.objects.objects), 1)


if __name__ == "__main__":
    unittest.main()
