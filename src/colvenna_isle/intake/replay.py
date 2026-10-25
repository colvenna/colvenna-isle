"""Explicit, audited replay of an immutable original through the same FIFO path."""

import hashlib
import re
from datetime import datetime, timezone

from botocore.exceptions import BotoCoreError, ClientError

from colvenna_isle.contracts.validation import validate, validate_record
from colvenna_isle.control.http import decode_json
from colvenna_isle.control.registry import ControlError, canonical
from .backpressure import REPLAY_WINDOW_SECONDS, retention_decision


class Replay:
    def __init__(self, intake, ledger, audit):
        self.intake, self.ledger, self.audit_trail = intake, ledger, audit

    def original(self, principal, space, ingestion_id, *, diagnostic=False):
        if not re.fullmatch(r"ing_[0-9a-f]{64}", ingestion_id):
            raise ControlError(400, "INVALID_INGESTION_ID")
        principal.authorize(space, "monitor:read" if diagnostic else "ingest:write")
        configuration = self.intake.registry._read(space)
        self.intake.registry._authorize_generation(principal, configuration)
        for dataset, registered in configuration["datasets"].items():
            if not dataset.startswith("ods."):
                continue
            key = (f"spaces/{space}/ingest/{configuration['generation']}/"
                   f"{dataset.split('.', 1)[1]}/{ingestion_id}.json")
            try:
                head = self.intake.s3.head_object(
                    Bucket=self.intake.bucket, Key=key, ExpectedBucketOwner=self.intake.account)
            except ClientError as error:
                if error.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
                    continue
                raise
            metadata = head.get("Metadata", {})
            expected = {
                "ingestion-id": ingestion_id, "space-id": space,
                "space-generation": configuration["generation"], "dataset": dataset,
                "schema-version": str(registered["definition"]["schema_version"]),
                "dataset-definition-sha256": registered["definition_sha256"],
            }
            if (any(metadata.get(name) != value for name, value in expected.items())
                    or not re.fullmatch(r"[0-9a-f]{64}", metadata.get("raw-sha256", ""))
                    or not re.fullmatch(r"[0-9a-f]{64}", metadata.get("payload-sha256", ""))
                    or not metadata.get("principal-id") or head.get("VersionId") in {None, "null"}):
                raise ControlError(503, "ARCHIVE_IDENTITY_UNAVAILABLE")
            if not diagnostic and metadata["principal-id"] != principal.principal_id:
                raise ControlError(403, "INGESTION_OWNER_MISMATCH")
            return configuration, registered, metadata, head, key
        raise ControlError(404, "INGESTION_ARCHIVE_NOT_FOUND")

    def diagnostics(self, principal, space, ingestion_id):
        configuration, _, _, head, _ = self.original(principal, space, ingestion_id, diagnostic=True)
        value = self.ledger.diagnostics(configuration, ingestion_id)
        queue = self.intake.admission.observe(configuration)
        # Replay intents are retained separately. Without reviewing those
        # immutable records, the product never authorizes automatic deletion.
        retention = retention_decision(head["LastModified"].isoformat(), None,
                                       value["status"]["status"], False, False)
        return {"ingestion_id": ingestion_id, "space_id": space,
                "native_status": value["status"]["status"],
                "attempts": value["attempts"], "queue": queue, "retention": retention,
                "dlq_reconciliations": [
                    {field: item[field] for field in [
                        "message_id", "ingestion_id", "envelope_sha256", "commit_sequence"]}
                    for item in value["dlq_reconciliations"]],
                "replay_window_seconds": REPLAY_WINDOW_SECONDS}

    def audit(self, value, request_id):
        token = hashlib.sha256(request_id.encode()).hexdigest()
        key = (f"spaces/{value['space_id']}/ingestion-replay/{value['generation']}/"
               f"{value['ingestion_id']}/{token}.json")
        content = canonical(value).encode()
        try:
            result = self.intake.s3.put_object(
                Bucket=self.intake.bucket, Key=key, Body=content, IfNoneMatch="*",
                ExpectedBucketOwner=self.intake.account, ContentType="application/json")
        except (ClientError, BotoCoreError):
            try:
                result = self.intake.s3.get_object(
                    Bucket=self.intake.bucket, Key=key, ExpectedBucketOwner=self.intake.account)
                with result["Body"] as body:
                    if body.read(16385) != content:
                        raise ControlError(409, "REPLAY_AUDIT_CONFLICT")
            except (ClientError, BotoCoreError):
                raise ControlError(503, "REPLAY_AUDIT_UNCONFIRMED") from None
        if result.get("VersionId") in {None, "null"}:
            raise ControlError(503, "REPLAY_AUDIT_UNCONFIRMED")
        self.record(value, token, result["VersionId"])
        return token

    def record(self, value, token, version):
        """The space audit trail names the replay intent before any dispatch."""
        audit = self.audit_trail
        item = audit.put(value["space_id"], value["generation"], "INGESTION_REPLAY_INTENT", {
            "kind": "API_KEY", "principal_id": value["principal_id"], "key_id": value.get("key_id"),
            "fingerprint": (value.get("key_id") or "")[-12:] or None},
            {"kind": "ingestion", "id": value["ingestion_id"]}, identity=token, request_id=value["request_id"],
            outcome="INTENT", details={"intent_sha256": hashlib.sha256(canonical(value).encode()).hexdigest(),
                                       "intent_object_version": version})["Put"]
        # The same original request reuses its token; its marker makes the record exactly-once.
        marker = {"Put": {"TableName": audit.table, "ConditionExpression": "attribute_not_exists(PK)", "Item": {
            "PK": {"S": f"AUDITMARK#{value['space_id']}#{value['generation']}"}, "SK": {"S": "REPLAY#" + token}}}}
        try:
            audit.client.transact_write_items(TransactItems=[marker, {"Put": item}])
        except ClientError as error:
            if error.response["Error"]["Code"] == "TransactionCanceledException" and any(
                    reason.get("Code") == "ConditionalCheckFailed"
                    for reason in error.response.get("CancellationReasons", [])[:1]):
                return
            raise ControlError(503, "REPLAY_AUDIT_UNCONFIRMED") from None
        except BotoCoreError:
            raise ControlError(503, "REPLAY_AUDIT_UNCONFIRMED") from None

    def submit(self, principal, space, ingestion_id, request_id):
        principal.authorize(space, "ingest:replay")
        configuration, registered, metadata, head, key = self.original(principal, space, ingestion_id)
        # This requires a successful current-permit native read. A timeout or
        # UNKNOWN transaction result is never permission to dispatch.
        native = self.ledger.read(configuration, ingestion_id)
        if native["status"] not in {"COMMITTED", "FAILED", "UNKNOWN"}:
            raise ControlError(503, "REPLAY_AUTHORITY_UNAVAILABLE")
        if (native["status"] != "UNKNOWN"
                and (native.get("principal_id") != principal.principal_id
                     or native.get("generation") != configuration["generation"])):
            raise ControlError(403, "INGESTION_OWNER_MISMATCH")
        created = head["LastModified"]
        if (datetime.now(timezone.utc) - created).total_seconds() >= REPLAY_WINDOW_SECONDS:
            raise ControlError(409, "REPLAY_WINDOW_EXPIRED")
        result = self.intake.s3.get_object(
            Bucket=self.intake.bucket, Key=key, VersionId=head["VersionId"],
            ExpectedBucketOwner=self.intake.account)
        with result["Body"] as stream:
            raw = stream.read(1048577)
        if (result.get("VersionId") != head["VersionId"]
                or result.get("Metadata") != head.get("Metadata")
                or len(raw) != head["ContentLength"] or len(raw) > 1048576
                or hashlib.sha256(raw).hexdigest() != metadata["raw-sha256"]):
            raise ControlError(503, "ARCHIVE_IDENTITY_UNAVAILABLE")
        payload = decode_json(raw)
        validate("ingestion_request", payload)
        definition = registered["definition"]
        ingestion = definition.get("ingestion", {})
        if (hashlib.sha256(canonical(payload).encode()).hexdigest() != metadata["payload-sha256"]
                or "ods." + payload["dataset"] != definition["dataset"]
                or payload["schema_version"] != definition["schema_version"]
                or payload["operation"] not in ingestion.get("operations", [])):
            raise ControlError(503, "ARCHIVE_IDENTITY_UNAVAILABLE")
        required = definition["key_columns"] + [ingestion["event_id_column"]]
        if "source_version_column" in ingestion:
            required.append(ingestion["source_version_column"])
        for record in payload["records"]:
            validate_record(definition["schema"], record, payload["operation"], required)
        archive = {"bucket": self.intake.bucket, "key": key, "version_id": head["VersionId"],
                   "sha256": metadata["raw-sha256"], "bytes": len(raw)}
        if native["status"] == "COMMITTED" and any([
                native.get("ingestion_id") != ingestion_id, native.get("space_id") != space,
                native.get("dataset") != registered["definition"]["dataset"],
                native.get("payload_sha256") != metadata["payload-sha256"],
                native.get("source") != archive]):
            raise ControlError(409, "NATIVE_ARCHIVE_BINDING_CONFLICT")
        token = self.audit({
            "ingestion_id": ingestion_id, "space_id": space, "key_id": principal.key_id,
            "generation": configuration["generation"], "principal_id": principal.principal_id,
            "source": archive, "payload_sha256": metadata["payload-sha256"],
            "action": "REPLAY_INTENT_NOT_COMMIT_RECEIPT", "request_id": request_id,
        }, request_id)
        if native["status"] == "COMMITTED":
            return 200, {"ingestion_id": ingestion_id, "status": "COMMITTED",
                         "commit_sequence": native["commit_sequence"], "audit_id": token}
        self.intake.admission.check(configuration, len(payload["records"]), len(raw))
        # Reauthorize and recheck the immutable configuration just before send.
        latest, current = self.intake.registry.dataset(
            principal, space, registered["definition"]["dataset"], "ingest:write")
        if (latest["generation"] != configuration["generation"]
                or latest["route_version"] != configuration["route_version"]
                or current["definition_sha256"] != registered["definition_sha256"]):
            raise ControlError(409, "SPACE_CONFIGURATION_CHANGED")
        accepted = self.intake.send(self.intake.message(configuration, registered, metadata, archive))
        return 202, {**accepted, "audit_id": token}
