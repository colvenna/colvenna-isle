"""Bounded admission from queue observations and measured sole-writer capacity."""

import math
from datetime import datetime, timedelta, timezone

from botocore.exceptions import BotoCoreError, ClientError

from colvenna_isle.control.registry import ControlError


DEFAULT_POLICY = {
    "max_pending_messages": 1000, "max_oldest_seconds": 3600,
    "max_drain_seconds": 1800, "unavailable_writer_buffer": 20,
    "max_batch_records": 10000, "max_batch_bytes": 1048576,
}
ARCHIVE_RETENTION_SECONDS = 45 * 86400
REPLAY_WINDOW_SECONDS = 30 * 86400
RETRY_RETENTION_SECONDS = 14 * 86400


class Backpressure(ControlError):
    def __init__(self, code):
        super().__init__(429, code, "Retry the same Idempotency-Key after backpressure clears")
        self.retry_after_ms = 5000


class Admission:
    def __init__(self, sqs, cloudwatch, ledger, queue, dlq):
        self.sqs, self.cloudwatch, self.ledger = sqs, cloudwatch, ledger
        self.queue, self.dlq = queue, dlq

    def observe(self, configuration):
        now = datetime.now(timezone.utc)
        names = ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible",
                 "ApproximateNumberOfMessagesDelayed"]
        attributes = self.sqs.get_queue_attributes(
            QueueUrl=self.queue, AttributeNames=names)["Attributes"]
        pending = sum(int(attributes[name]) for name in names)
        dlq = self.sqs.get_queue_attributes(
            QueueUrl=self.dlq, AttributeNames=names)["Attributes"]
        oldest, metric_time = 0 if pending == 0 else None, None
        if pending:
            result = self.cloudwatch.get_metric_statistics(
                Namespace="AWS/SQS", MetricName="ApproximateAgeOfOldestMessage",
                Dimensions=[{"Name": "QueueName", "Value": self.queue.rsplit("/", 1)[-1]}],
                StartTime=now - timedelta(minutes=5), EndTime=now, Period=60, Statistics=["Maximum"])
            points = [point for point in result["Datapoints"]
                      if 0 <= (now - point["Timestamp"]).total_seconds() <= 180
                      and math.isfinite(point["Maximum"]) and point["Maximum"] >= 0]
            if points:
                point = max(points, key=lambda entry: entry["Timestamp"])
                oldest, metric_time = point["Maximum"], point["Timestamp"].isoformat()
        capacity = {"state": "UNKNOWN", "batches_per_second": None}
        try:
            capacity = self.ledger.capacity(configuration)
        except (ClientError, BotoCoreError, ControlError):
            pass
        return {"observed_at": now.isoformat(), "pending_messages": pending,
                "oldest_seconds": oldest, "oldest_observed_at": metric_time,
                "dlq_messages": sum(int(dlq[name]) for name in names),
                "capacity": capacity, "queue_scope": "SHARED_QUEUE_CONSERVATIVE_BOUND"}

    def check(self, configuration, records, size):
        policy = configuration.get("ingestion_policy", DEFAULT_POLICY)
        if records > policy["max_batch_records"] or size > policy["max_batch_bytes"]:
            raise Backpressure("SPACE_BATCH_QUOTA")
        value = self.observe(configuration)
        pending, capacity = value["pending_messages"], value["capacity"]
        if pending >= policy["max_pending_messages"]:
            raise Backpressure("QUEUE_BACKLOG")
        if value["oldest_seconds"] is None:
            raise ControlError(503, "QUEUE_AGE_UNKNOWN", "Retry the same Idempotency-Key")
        if value["oldest_seconds"] >= policy["max_oldest_seconds"]:
            raise Backpressure("QUEUE_AGE")
        rate = capacity.get("batches_per_second")
        if (capacity.get("state") != "MEASURED" or type(rate) not in {int, float}
                or not math.isfinite(rate) or rate <= 0):
            if pending >= policy["unavailable_writer_buffer"]:
                raise Backpressure("WRITER_CAPACITY_UNAVAILABLE")
        elif (pending + 1) / rate > policy["max_drain_seconds"]:
            raise Backpressure("WRITER_DRAIN_BUDGET")
        return value


def retention_decision(created_at, last_replay_at, native_status, queue_empty, dlq_empty, now=None):
    """An eligibility report, never a deletion or an implicit UNKNOWN retry."""
    now = now or datetime.now(timezone.utc)
    original = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    replay = datetime.fromisoformat(last_replay_at.replace("Z", "+00:00")) if last_replay_at else original
    if not original.tzinfo or not replay.tzinfo:
        raise ValueError("Retention requires timezone-qualified observations")
    until = max(original + timedelta(seconds=ARCHIVE_RETENTION_SECONDS),
                replay + timedelta(seconds=RETRY_RETENTION_SECONDS))
    eligible = (now >= until and native_status == "COMMITTED" and queue_empty and dlq_empty)
    return {"retain_until": until.isoformat(), "cleanup_eligible": eligible,
            "native_identity_must_remain": True,
            "reason": "ELIGIBLE_FOR_OPERATOR_REVIEW" if eligible else "RETAIN_OR_RECONCILE"}
