"""Source-object admission with stable identity across uncertain S3/SQS replies."""

import base64
import hashlib
import json
import re

from botocore.exceptions import BotoCoreError, ClientError

from colvenna_isle.contracts.model import LIMITS
from colvenna_isle.contracts.validation import ContractError, validate, validate_record
from colvenna_isle.control.http import decode_json
from colvenna_isle.control.registry import ControlError, canonical
from colvenna_isle.timing import NOT_TIMED


class IntakeError(ControlError):
    def __init__(self, ingestion_id, code):
        super().__init__(503, code, "Reconcile or retry the same Idempotency-Key")
        self.ingestion_id = ingestion_id


def ingestion_identity(space, generation, dataset, idempotency_key):
    """The payload hash is deliberately absent from this immutable object identity."""
    return "ing_" + hashlib.sha256(canonical(
        [space, generation, dataset, idempotency_key]).encode()).hexdigest()


class Intake:
    def __init__(self, registry, s3, sqs, bucket, queue_url, account, source_commit, admission=None):
        self.registry, self.s3, self.sqs = registry, s3, sqs
        self.bucket, self.queue_url, self.account, self.source_commit = bucket, queue_url, account, source_commit
        self.admission = admission

    def _existing(self, key, metadata, ingestion_id):
        try:
            result = self.s3.get_object(Bucket=self.bucket, Key=key, ExpectedBucketOwner=self.account)
            with result["Body"] as stream:
                content = stream.read(LIMITS["api_body_bytes"] + 1)
        except (ClientError, BotoCoreError):
            raise IntakeError(ingestion_id, "ARCHIVE_UNCONFIRMED") from None
        stored = result.get("Metadata", {})
        if stored.get("principal-id") != metadata["principal-id"]:
            raise ControlError(403, "INGESTION_OWNER_MISMATCH")
        if stored.get("payload-sha256") != metadata["payload-sha256"]:
            raise ControlError(409, "IDEMPOTENCY_CONFLICT")
        if any(stored.get(name) != value for name, value in metadata.items() if name != "raw-sha256"):
            raise IntakeError(ingestion_id, "ARCHIVE_IDENTITY_UNAVAILABLE")
        raw_hash = hashlib.sha256(content).hexdigest()
        try:
            payload_hash = hashlib.sha256(canonical(decode_json(content)).encode()).hexdigest()
        except (ContractError, ValueError, TypeError, RecursionError):
            raise IntakeError(ingestion_id, "ARCHIVE_IDENTITY_UNAVAILABLE") from None
        if (len(content) > LIMITS["api_body_bytes"] or stored.get("raw-sha256") != raw_hash
                or payload_hash != metadata["payload-sha256"]
                or not result.get("VersionId") or result["VersionId"] == "null"):
            raise IntakeError(ingestion_id, "ARCHIVE_IDENTITY_UNAVAILABLE")
        return {"bucket": self.bucket, "key": key, "version_id": result["VersionId"],
                "sha256": raw_hash, "bytes": len(content)}

    def _archive(self, key, raw, metadata, ingestion_id):
        try:
            result = self.s3.put_object(
                Bucket=self.bucket, Key=key, Body=raw, ContentType="application/json",
                Metadata=metadata, IfNoneMatch="*", ExpectedBucketOwner=self.account,
                ChecksumSHA256=base64.b64encode(hashlib.sha256(raw).digest()).decode())
        except (ClientError, BotoCoreError):
            # A conditional conflict or lost write reply is reconciled at the original key.
            return self._existing(key, metadata, ingestion_id)
        if not result.get("VersionId") or result["VersionId"] == "null":
            raise IntakeError(ingestion_id, "ARCHIVE_UNCONFIRMED")
        return {"bucket": self.bucket, "key": key, "version_id": result["VersionId"],
                "sha256": metadata["raw-sha256"], "bytes": len(raw)}

    def submit(self, principal, space, payload, raw, idempotency_key, timing=NOT_TIMED):
        principal.authorize(space, "ingest:write")
        validate("ingestion_request", payload)
        if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", idempotency_key or ""):
            raise ControlError(428, "IDEMPOTENCY_KEY_REQUIRED")
        with timing.segment("dataset"):
            configuration, registered = self.registry.dataset(
                principal, space, "ods." + payload["dataset"], "ingest:write")
        definition = registered["definition"]
        ingestion = definition.get("ingestion", {})
        if payload["schema_version"] != definition["schema_version"]:
            raise ControlError(409, "SCHEMA_VERSION_MISMATCH")
        if payload["operation"] not in ingestion.get("operations", []):
            raise ControlError(403, "INGESTION_OPERATION_FORBIDDEN")
        required = definition["key_columns"] + [ingestion["event_id_column"]]
        if "source_version_column" in ingestion:
            required.append(ingestion["source_version_column"])
        with timing.segment("validate"):
            for record in payload["records"]:
                validate_record(definition["schema"], record, payload["operation"], required)
            if len(raw) > LIMITS["api_body_bytes"] or decode_json(raw) != payload:
                raise ControlError(400, "INVALID_REQUEST")
        ingestion_id = ingestion_identity(space, configuration["generation"], definition["dataset"], idempotency_key)
        timing.bind(space_id=space, generation=configuration["generation"], ingestion_id=ingestion_id,
                    dataset=definition["dataset"], operation=payload["operation"])
        timing.count("records", len(payload["records"]))
        timing.count("bytes", len(raw))
        if self.admission:
            with timing.segment("backpressure"):
                self.admission.check(configuration, len(payload["records"]), len(raw))
        payload_hash = hashlib.sha256(canonical(payload).encode()).hexdigest()
        metadata = {
            "ingestion-id": ingestion_id, "space-id": space,
            "space-generation": configuration["generation"], "dataset": definition["dataset"],
            "schema-version": str(payload["schema_version"]),
            "dataset-definition-sha256": registered["definition_sha256"],
            "principal-id": principal.principal_id,
            "payload-sha256": payload_hash, "raw-sha256": hashlib.sha256(raw).hexdigest(),
        }
        key = f"spaces/{space}/ingest/{configuration['generation']}/{payload['dataset']}/{ingestion_id}.json"
        with timing.segment("archive"):
            archive = self._archive(key, raw, metadata, ingestion_id)
        # Reject old-generation dispatch after an environment rebuild, including in-flight calls.
        try:
            with timing.segment("route_recheck"):
                latest, current_dataset = self.registry.dataset(
                    principal, space, definition["dataset"], "ingest:write")
        except (ClientError, BotoCoreError):
            raise IntakeError(ingestion_id, "CONTROL_READ_UNCONFIRMED") from None
        if (latest["generation"] != configuration["generation"] or latest["route_version"] != configuration["route_version"]
                or current_dataset["definition_sha256"] != registered["definition_sha256"]):
            raise ControlError(409, "SPACE_CONFIGURATION_CHANGED")
        message = self.message(configuration, registered, metadata, archive)
        with timing.segment("queue_send"):
            return self.send(message)

    def message(self, configuration, registered, metadata, archive):
        return {
            "contract_version": 2, "ingestion_id": metadata["ingestion-id"], "space_id": metadata["space-id"],
            "generation": configuration["generation"], "route_version": configuration["route_version"],
            "dataset": registered["definition"]["dataset"],
            "schema_version": registered["definition"]["schema_version"],
            "dataset_definition_sha256": registered["definition_sha256"], "principal_id": metadata["principal-id"],
            "payload_sha256": metadata["payload-sha256"], "source": archive,
            "producer_source_commit": self.source_commit,
        }

    def send(self, message):
        ingestion_id, space = message["ingestion_id"], message["space_id"]
        validate("ingestion_message", message)
        body = canonical(message)
        try:
            result = self.sqs.send_message(
                QueueUrl=self.queue_url, MessageBody=body, MessageGroupId=space, MessageDeduplicationId=ingestion_id)
        except (ClientError, BotoCoreError):
            raise IntakeError(ingestion_id, "QUEUE_SEND_UNCONFIRMED") from None
        expected_md5 = hashlib.md5(body.encode(), usedforsecurity=False).hexdigest()
        if (result.get("ResponseMetadata", {}).get("HTTPStatusCode") != 200
                or not result.get("MessageId") or not result.get("SequenceNumber")
                or result.get("MD5OfMessageBody") != expected_md5):
            raise IntakeError(ingestion_id, "QUEUE_SEND_UNCONFIRMED")
        return {"ingestion_id": ingestion_id, "status": "ACCEPTED",
                "status_url": f"/v1/spaces/{space}/ingestions/{ingestion_id}"}
