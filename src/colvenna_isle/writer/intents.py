"""Writer-only immutable originals; native results remain the commit authority."""

import base64
import hashlib
import json
from datetime import datetime, timedelta, timezone

from botocore.exceptions import BotoCoreError, ClientError

from colvenna_isle.contracts.model import LIMITS
from colvenna_isle.control.http import decode_json
from colvenna_isle.control.registry import ControlError, canonical
from .writes import WriteStore, business_binding, write_id


class IntentUnknown(ControlError):
    def __init__(self, identity):
        super().__init__(503, "WRITE_INTENT_UNCONFIRMED")
        self.write_id = identity


class Intents:
    def __init__(self, s3, bucket, account, source_commit, clock=lambda: datetime.now(timezone.utc)):
        self.s3, self.bucket, self.account = s3, bucket, account
        self.source_commit, self.clock = source_commit, clock

    def key(self, space, generation, identity):
        return f"spaces/{space}/write-intents/{generation}/{identity}.json"

    def read(self, space, generation, identity, version=None):
        options = {"VersionId": version} if version else {}
        try:
            value = self.s3.get_object(
                Bucket=self.bucket, Key=self.key(space, generation, identity),
                ExpectedBucketOwner=self.account, **options)
            with value["Body"] as stream:
                raw = stream.read(LIMITS["api_body_bytes"] + 8193)
        except ClientError as error:
            # The writer has no s3:ListBucket, so an absent intent answers 403 as well as 404. Absence is never
            # an authority here: the native ledger decides status and reserve re-reads after its conditional put.
            if version is None and error.response["Error"]["Code"] in {"NoSuchKey", "404", "AccessDenied", "403"}:
                return None
            raise IntentUnknown(identity) from None
        except BotoCoreError:
            raise IntentUnknown(identity) from None
        metadata = value.get("Metadata", {})
        version_id = value.get("VersionId")
        try:
            original = decode_json(raw)
            if (len(raw) > LIMITS["api_body_bytes"] + 8192
                    or hashlib.sha256(raw).hexdigest() != metadata["sha256"]
                    or original["write_id"] != identity or original["space_id"] != space
                    or original["generation"] != generation
                    or not version_id or version_id == "null"
                    or version is not None and version_id != version):
                raise ValueError("Unconfirmed immutable intent")
        except (KeyError, ValueError, TypeError):
            raise IntentUnknown(identity) from None
        return original, {"bucket": self.bucket, "key": self.key(space, generation, identity),
                          "version_id": version_id, "sha256": metadata["sha256"], "bytes": len(raw)}

    def reserve(self, configuration, principal_id, request, registered, idempotency_key):
        space, generation = configuration["space_id"], configuration["generation"]
        identity = write_id(space, generation, request["dataset"], idempotency_key)
        now = self.clock()
        original = {
            "write_id": identity, "space_id": space, "generation": generation,
            "principal_id": principal_id, "dataset": request["dataset"],
            "definition_sha256": registered["definition_sha256"],
            "binding_sha256": business_binding(request, registered), "source_commit": self.source_commit,
            "registered_at": now.isoformat(), "retry_until": (now + timedelta(days=WriteStore.RETRY_DAYS)).isoformat(),
            "request": {name: value for name, value in request.items() if name != "timeout_ms"},
        }
        raw = canonical(original).encode()
        sha = hashlib.sha256(raw).hexdigest()
        version = None
        try:
            value = self.s3.put_object(
                Bucket=self.bucket, Key=self.key(space, generation, identity), Body=raw,
                IfNoneMatch="*", ContentType="application/json", ExpectedBucketOwner=self.account,
                Metadata={"sha256": sha}, ChecksumSHA256=base64.b64encode(hashlib.sha256(raw).digest()).decode())
            version = value.get("VersionId")
            if not version or version == "null":
                raise IntentUnknown(identity)
        except (ClientError, BotoCoreError):
            # Conditional conflict or lost response: read this same original,
            # without inventing an ID or issuing another business operation.
            pass
        existing = self.read(space, generation, identity, version)
        if existing is None:
            raise IntentUnknown(identity)
        stored, reference = existing
        try:
            if business_binding(stored["request"], registered) != stored["binding_sha256"]:
                raise ValueError("Original business binding differs")
        except (KeyError, ValueError, TypeError):
            raise IntentUnknown(identity) from None
        if stored["principal_id"] != principal_id:
            raise ControlError(403, "WRITE_OWNER_REQUIRED")
        if stored["binding_sha256"] != original["binding_sha256"]:
            raise ControlError(409, "WRITE_BINDING_CONFLICT")
        if any(stored.get(name) != original[name] for name in [
                "dataset", "definition_sha256", "source_commit"]):
            raise ControlError(409, "WRITE_SOURCE_IDENTITY_CHANGED")
        if datetime.fromisoformat(stored["retry_until"]) <= self.clock():
            raise ControlError(409, "WRITE_IDENTITY_EXPIRED")
        return identity, stored, reference, version is not None

    def authorize_status(self, space, generation, identity, principal_id, admin):
        existing = self.read(space, generation, identity)
        if existing and existing[0]["principal_id"] != principal_id and not admin:
            raise ControlError(403, "WRITE_OWNER_REQUIRED")
