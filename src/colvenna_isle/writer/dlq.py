"""Delete a dead letter only after durable native reconciliation of its original."""

import hashlib
import time

from colvenna_isle.contracts.validation import validate
from colvenna_isle.control.http import decode_json
from colvenna_isle.control.registry import ControlError
from colvenna_isle.writer.ingestion import CommitUnknown, IngestionError


class DeadLetters:
    def __init__(self, runtime, s3, sqs, bucket, queue, account):
        self.runtime, self.s3, self.sqs = runtime, s3, sqs
        self.bucket, self.queue, self.account = bucket, queue, account

    def reconcile(self, delivery):
        message = decode_json(delivery["Body"].encode())
        validate("ingestion_message", message)
        space, generation = message["space_id"], message["generation"]
        configuration = self.runtime.registry._read(space)
        if configuration["generation"] != generation or message["source"]["bucket"] != self.bucket:
            raise IngestionError("DLQ_CONFIGURATION_MISMATCH")
        source = message["source"]
        # HEAD the exact immutable version, not the latest object or arbitrary URL.
        head = self.s3.head_object(Bucket=self.bucket, Key=source["key"], VersionId=source["version_id"],
                                   ExpectedBucketOwner=self.account)
        metadata = head.get("Metadata", {})
        for name, field in [
            ("ingestion-id", "ingestion_id"), ("space-id", "space_id"),
            ("space-generation", "generation"), ("dataset", "dataset"), ("principal-id", "principal_id"),
            ("payload-sha256", "payload_sha256"),
        ]:
            if metadata.get(name) != message[field]:
                raise IngestionError("DLQ_ARCHIVE_IDENTITY_MISMATCH")
        expected_key = (f"spaces/{space}/ingest/{generation}/"
                        f"{message['dataset'].split('.', 1)[1]}/{message['ingestion_id']}.json")
        if source["key"] != expected_key or head.get("VersionId") != source["version_id"]:
            raise IngestionError("DLQ_ARCHIVE_IDENTITY_MISMATCH")
        with self.runtime.lock:
            self.runtime.guard()
            database = self.runtime.databases.get(space + "/" + generation)
            if database is None:
                return None  # No automatic native open/permit for a dead letter.
            store = database.ingestions
            receipt = store.status(message["ingestion_id"])
            fixed_source = {
                "bucket": self.bucket, "key": expected_key, "version_id": head["VersionId"],
                "sha256": metadata.get("raw-sha256"), "bytes": head["ContentLength"],
            }
            if (receipt.get("status") != "COMMITTED" or receipt.get("source") != fixed_source
                    or any(receipt.get(field) != message[field] for field in [
                        "ingestion_id", "space_id", "generation", "dataset", "principal_id", "payload_sha256"])):
                return None
            # Invalid transport fields may have caused the original poison.
            # Preserve that exact envelope hash before deleting its queue copy.
            evidence = {
                "message_id": delivery["MessageId"], "ingestion_id": message["ingestion_id"],
                "envelope_sha256": hashlib.sha256(delivery["Body"].encode()).hexdigest(),
                "commit_sequence": receipt["commit_sequence"], "source": fixed_source,
            }
            store.reconcile_dead_letter(evidence)
            self.runtime.guard()
            result = self.sqs.delete_message(QueueUrl=self.queue, ReceiptHandle=delivery["ReceiptHandle"])
            if result.get("ResponseMetadata", {}).get("HTTPStatusCode") != 200:
                raise RuntimeError("DLQ_DELETE_UNKNOWN")
            self.runtime.log("DLQ_RECONCILED", evidence)
            return evidence

    def run(self):
        while True:
            try:
                with self.runtime.lock:
                    self.runtime.guard()
                result = self.sqs.receive_message(
                    QueueUrl=self.queue, MaxNumberOfMessages=1, WaitTimeSeconds=5, VisibilityTimeout=60,
                    MessageSystemAttributeNames=["All"])
                for delivery in result.get("Messages", []):
                    self.reconcile(delivery)
            except CommitUnknown:
                self.runtime.consumer.paused = True
                self.runtime.log("DLQ_NATIVE_RESULT_UNKNOWN")
            except ControlError:
                pass
            except Exception as error:
                self.runtime.log("DLQ_RETAINED", {"exception_type": type(error).__name__})
            time.sleep(5)
