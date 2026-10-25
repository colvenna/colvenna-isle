"""Validate fixed S3 references; ACK only a confirmed native transaction."""

import hashlib
import math
import time
from collections import deque

from colvenna_isle.contracts.model import LIMITS
from colvenna_isle.contracts.validation import ContractError, validate
from colvenna_isle.control.http import decode_json
from colvenna_isle.control.registry import ControlError
from colvenna_isle.writer.ingestion import CommitUnknown, IngestionError, digest
from colvenna_isle.timing import NOT_TIMED, Timing


def archived_ingestion(s3, bucket, account, space, generation, datasets, ingestion_id):
    """Whether this generation's archive holds the original (qualification fault probes only).

    The writer has no s3:ListBucket on ingest/, so HEAD of an absent key answers 403 rather than 404; either
    means "not this dataset". Only an archive whose metadata names this space, generation and ingestion counts."""
    for dataset in datasets:
        key = f"spaces/{space}/ingest/{generation}/{dataset.split('.', 1)[1]}/{ingestion_id}.json"
        try:
            metadata = s3.head_object(Bucket=bucket, Key=key, ExpectedBucketOwner=account)["Metadata"]
        except s3.exceptions.ClientError as error:
            if error.response["Error"]["Code"] in {"403", "404", "NoSuchKey"}:
                continue
            raise
        if (metadata.get("space-id") == space and metadata.get("space-generation") == generation
                and metadata.get("ingestion-id") == ingestion_id):
            return True
    return False


class Consumer:
    def __init__(self, runtime, s3, sqs, bucket, queue, account, producer):
        self.runtime, self.s3, self.sqs = runtime, s3, sqs
        self.bucket, self.queue, self.account, self.producer = bucket, queue, account, producer
        self.paused = False
        self.fault = None
        self.last_delivery = None
        self.completions = deque(maxlen=64)
        # Per space: (monotonic time, queue send -> native commit milliseconds).
        self.latencies = {}

    def capacity(self):
        now = time.monotonic()
        recent = [duration for at, duration in list(self.completions) if now - at <= 120]
        return {
            "state": "MEASURED" if recent and not self.paused else "UNKNOWN",
            "batches_per_second": (1 / (sum(recent) / len(recent) + 1)) if recent and not self.paused else None,
            "samples": len(recent), "sample_window_seconds": 120, "consumer_paused": self.paused,
            "observed_unix": time.time(),
        }

    def latency(self, space, window=3600):
        """Accepted-to-committed latency of recent new commits in one space (not a promise)."""
        now = time.monotonic()
        values = sorted(value for at, value in list(self.latencies.get(space, ())) if now - at <= window)
        if not values:
            return {"samples": 0, "window_seconds": window, "p50_ms": None, "p95_ms": None, "max_ms": None}

        def rank(fraction):
            return values[min(len(values) - 1, max(0, math.ceil(len(values) * fraction) - 1))]
        return {"samples": len(values), "window_seconds": window, "p50_ms": rank(0.5), "p95_ms": rank(0.95),
                "max_ms": values[-1]}

    def configuration(self, message):
        configuration = self.runtime.registry._read(message["space_id"])
        registered = configuration["datasets"].get(message["dataset"])
        if (configuration["generation"] != message["generation"]
                or configuration["route_version"] != message["route_version"]):
            raise IngestionError("SPACE_CONFIGURATION_CHANGED")
        if (not registered or registered["definition_sha256"] != message["dataset_definition_sha256"]
                or registered["definition"]["schema_version"] != message["schema_version"]):
            raise IngestionError("SCHEMA_IDENTITY_MISMATCH")
        return configuration, registered

    def payload(self, message, registered):
        source = message["source"]
        result = self.s3.get_object(
            Bucket=self.bucket, Key=source["key"], VersionId=source["version_id"],
            ExpectedBucketOwner=self.account)
        with result["Body"] as stream:
            raw = stream.read(LIMITS["api_body_bytes"] + 1)
        if (result.get("VersionId") != source["version_id"] or len(raw) != source["bytes"]
                or len(raw) > LIMITS["api_body_bytes"]
                or hashlib.sha256(raw).hexdigest() != source["sha256"]):
            raise IngestionError("SOURCE_HASH_MISMATCH")
        expected = {
            "ingestion-id": message["ingestion_id"], "space-id": message["space_id"],
            "space-generation": message["generation"], "dataset": message["dataset"],
            "schema-version": str(message["schema_version"]),
            "dataset-definition-sha256": message["dataset_definition_sha256"],
            "principal-id": message["principal_id"], "payload-sha256": message["payload_sha256"],
            "raw-sha256": source["sha256"],
        }
        if any(result.get("Metadata", {}).get(key) != value for key, value in expected.items()):
            raise IngestionError("SOURCE_METADATA_MISMATCH")
        try:
            payload = decode_json(raw)
        except (ValueError, UnicodeError, RecursionError):
            raise IngestionError("INVALID_ARCHIVE_JSON") from None
        validate("ingestion_request", payload)
        if (digest(payload) != message["payload_sha256"] or "ods." + payload["dataset"] != message["dataset"]
                or payload["schema_version"] != message["schema_version"]
                or payload["operation"] not in registered["definition"].get("ingestion", {}).get("operations", [])):
            raise IngestionError("PAYLOAD_SCHEMA_MISMATCH")
        return payload

    def process(self, delivery, queued_at=None, timing=NOT_TIMED):
        """Caller holds the writer lock. Invalid work stays queued for DLQ."""
        self.runtime.guard()
        started = time.monotonic() if queued_at is None else queued_at
        message, store = None, None
        try:
            body = delivery["Body"]
            message = decode_json(body.encode())
            validate("ingestion_message", message)
            timing.bind(ingestion_id=message["ingestion_id"], space_id=message["space_id"],
                        generation=message["generation"], dataset=message["dataset"])
            with timing.segment("configuration", "unit"):
                configuration = self.runtime.registry._read(message["space_id"])
                if configuration["generation"] != message["generation"]:
                    raise IngestionError("SPACE_CONFIGURATION_CHANGED")
            with timing.segment("database", "unit"):
                database = self.runtime.database(configuration)
            store = database.ingestions
            with timing.segment("recheck", "unit"):
                configuration, registered = self.configuration(message)
            attributes = delivery.get("Attributes", {})
            if (message["source"]["bucket"] != self.bucket
                    or message["producer_source_commit"] != self.producer
                    or attributes.get("MessageGroupId") != message["space_id"]
                    or attributes.get("MessageDeduplicationId") != message["ingestion_id"]
                    or delivery.get("MD5OfBody") != hashlib.md5(body.encode(), usedforsecurity=False).hexdigest()):
                raise IngestionError("QUEUE_REFERENCE_MISMATCH")
            with timing.segment("ledger_lookup", "unit"):
                receipt = store.existing(message)
            duplicate = receipt is not None
            if receipt is None:
                with timing.segment("archive_read", "unit"):
                    payload = self.payload(message, registered)
                timing.count("records", len(payload["records"]))

                def recheck():
                    self.configuration(message)

                def before_commit():
                    if self.fault and self.fault == (message["ingestion_id"], "before_commit"):
                        self.fault = None
                        self.paused = True
                        raise RuntimeError("QUALIFICATION_BEFORE_COMMIT")
                receipt = store.commit(message, payload, registered, recheck, before_commit, timing)
            self.runtime.last_commit = {
                "ingestion_id": message["ingestion_id"], "space_id": message["space_id"],
                "generation": message["generation"], "commit_sequence": receipt["commit_sequence"]}
            self.last_delivery = {
                **self.runtime.last_commit, "message_id": delivery["MessageId"],
                "receive_count": attributes.get("ApproximateReceiveCount"), "duplicate_batch": duplicate,
                "committed": True, "deleted": False,
            }
            if self.fault and self.fault == (message["ingestion_id"], "after_commit"):
                # Real abrupt process death; receipt/sequence/data are already committed.
                import os
                os._exit(137)
            self.runtime.guard()
            with timing.segment("ack", "unit"):
                result = self.sqs.delete_message(QueueUrl=self.queue, ReceiptHandle=delivery["ReceiptHandle"])
            if result.get("ResponseMetadata", {}).get("HTTPStatusCode") != 200:
                raise RuntimeError("MESSAGE_DELETE_UNKNOWN")
            self.last_delivery["deleted"] = True
            if not duplicate:
                self.completions.append((time.monotonic(), max(time.monotonic() - started, 0.001)))
                sent = attributes.get("SentTimestamp")
                if isinstance(sent, str) and sent.isdigit():
                    self.latencies.setdefault(message["space_id"], deque(maxlen=256)).append(
                        (time.monotonic(), max(0, round(time.time() * 1000) - int(sent))))
            self.runtime.log("INGESTION_COMMITTED", self.last_delivery)
            return receipt
        except CommitUnknown:
            self.paused = True
            self.runtime.log("NATIVE_RESULT_UNKNOWN")
            raise
        except (IngestionError, ContractError) as error:
            code = error.code if isinstance(error, IngestionError) else "INVALID_INGESTION_CONTRACT"
            if store and message:
                source = message["source"]
                if source["bucket"] == self.bucket:
                    metadata = self.s3.head_object(
                        Bucket=self.bucket, Key=source["key"], VersionId=source["version_id"],
                        ExpectedBucketOwner=self.account).get("Metadata", {})
                    # A rejected queue reference cannot forge a status owner.
                    if all(metadata.get(name) == message[field] for name, field in [
                            ("ingestion-id", "ingestion_id"), ("space-id", "space_id"),
                            ("space-generation", "generation"), ("principal-id", "principal_id")]):
                        store.reject(message, delivery["MessageId"], code)
            self.last_delivery = {"message_id": delivery["MessageId"], "error_code": code, "deleted": False}
            self.runtime.log("INGESTION_REJECTED", self.last_delivery)
            # Deterministic poison gets bounded retries, then SQS redrive.
            # Native/IAM/network uncertainty does not shorten visibility.
            self.runtime.guard()
            self.sqs.change_message_visibility(
                QueueUrl=self.queue, ReceiptHandle=delivery["ReceiptHandle"], VisibilityTimeout=5)
            return None

    def run(self):
        while True:
            try:
                with self.runtime.lock:
                    approved = not self.paused
                    if approved:
                        self.runtime.guard()
                if approved:
                    result = self.sqs.receive_message(
                        QueueUrl=self.queue, MaxNumberOfMessages=1, WaitTimeSeconds=5, VisibilityTimeout=60,
                        MessageSystemAttributeNames=["All"])
                    messages = result.get("Messages", [])
                    for delivery in messages:
                        self.deliver(delivery)
                    if messages:
                        continue  # Drain a backlog without an idle pause between batches.
            except ControlError:
                pass  # Waiting/revoked writers neither receive nor commit/ACK.
            except Exception as error:
                self.runtime.log("CONSUMER_RESULT_UNKNOWN", {"exception_type": type(error).__name__})
            time.sleep(1)

    def deliver(self, delivery):
        """One FIFO delivery waits for its own unit; direct writes and import chunks get turns on the lock."""
        queued_at = time.monotonic()
        attributes = delivery.get("Attributes", {})
        timing = Timing("ingestion_commit", message_id=delivery.get("MessageId"),
                        receive_count=attributes.get("ApproximateReceiveCount"))
        sent, received = attributes.get("SentTimestamp", ""), attributes.get("ApproximateFirstReceiveTimestamp", "")
        if sent.isdigit() and received.isdigit():
            # Both instants come from SQS itself, not from this host's clock.
            timing.record("queue_wait", int(received) - int(sent), source="SQS_SERVICE_TIMESTAMPS")

        def process():
            timing.record("scheduler_wait", (time.monotonic() - queued_at) * 1000)
            if self.paused:
                return None
            with timing.segment("unit"):
                return self.process(delivery, queued_at, timing)
        try:
            receipt = self.runtime.scheduler.submit(
                "ingestion", delivery["MessageId"], process, attributes.get("MessageGroupId")).future.result()
        except BaseException as error:
            if "ingestion_id" in timing.identity:
                timing.emit(getattr(error, "code", type(error).__name__), "writer")
            raise
        if "ingestion_id" in timing.identity:
            timing.emit("COMMITTED" if receipt else "REJECTED", "writer")
