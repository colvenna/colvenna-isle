"""Immutable typed result manifests, owner-bound paging/export and retention."""

import base64
import hashlib
import hmac
import json
import secrets
import time

from botocore.exceptions import BotoCoreError, ClientError

from colvenna_isle.contracts.validation import validate
from colvenna_isle.control.http import decode_json
from colvenna_isle.control.registry import ControlError, canonical
from colvenna_isle.query.objects import object_versions, version_absent

MAX_BYTES = 16777216
RETENTION_SECONDS = 600
EXPORT_SECONDS = 120


class Results:
    def __init__(self, s3, bucket, account, clock=time.time):
        self.s3, self.bucket, self.account, self.clock = s3, bucket, account, clock

    @staticmethod
    def key(job):
        return (f"spaces/{job['space_id']}/query-results/{job['generation']}/"
                f"{job['query_id']}/{job['execution_id']}/manifest.json")

    def _read(self, job, reference, *, expired=False):
        if not expired and self.clock() >= job.get("result_expires_unix", float("inf")):
            raise ControlError(410, "QUERY_RESULT_EXPIRED")
        if reference["bucket"] != self.bucket or reference["key"] != self.key(job):
            raise ControlError(503, "QUERY_RESULT_UNCONFIRMED")
        try:
            value = self.s3.get_object(Bucket=self.bucket, Key=reference["key"],
                VersionId=reference["version_id"], ExpectedBucketOwner=self.account)
            with value["Body"] as stream:
                raw = stream.read(MAX_BYTES + 1)
            manifest = decode_json(raw)
            if (len(raw) != reference["bytes"] or len(raw) > MAX_BYTES
                    or hashlib.sha256(raw).hexdigest() != reference["sha256"]
                    or value["Metadata"]["sha256"] != reference["sha256"]
                    or value["VersionId"] != reference["version_id"]
                    or manifest["format_version"] != 1
                    or any(manifest[name] != job[name] for name in [
                        "query_id", "space_id", "generation", "principal_id", "execution_id",
                        "source_commit", "image_digest"])
                    or manifest["input"] != job["input"]):
                raise ValueError("Original manifest identity differs")
            validate("query_result", manifest["result"])
        except (ClientError, BotoCoreError, KeyError, ValueError, TypeError):
            raise ControlError(503, "QUERY_RESULT_UNCONFIRMED") from None
        if not expired and self.clock() >= manifest["expires_unix"]:
            raise ControlError(410, "QUERY_RESULT_EXPIRED")
        return manifest

    def prepare(self, job, result, terminal):
        validate("query_result", result)
        if (terminal["state"] != "REAPED" or terminal["outcome"] != "SUCCEEDED"
                or type(terminal["exit_code"]) is not int or terminal["exit_code"] != 0
                or not job.get("process")
                or any(terminal.get(name) != value for name, value in job["process"].items())):
            raise ControlError(503, "QUERY_PROCESS_CUSTODY_UNKNOWN")
        manifest = {name: job[name] for name in ["query_id", "space_id", "generation",
            "principal_id", "execution_id", "source_commit", "image_digest", "input"]}
        manifest.update(format_version=1, result=result, terminal=terminal,
            created_unix=self.clock(), expires_unix=self.clock() + RETENTION_SECONDS,
            token_secret=secrets.token_urlsafe(32))
        raw = canonical(manifest).encode()
        if len(raw) > MAX_BYTES:
            raise ControlError(413, "QUERY_RESULT_TOO_LARGE")
        digest = hashlib.sha256(raw).hexdigest()
        return {"raw": raw, "intent": {"bucket": self.bucket, "key": self.key(job),
            "sha256": digest, "bytes": len(raw), "expires_unix": manifest["expires_unix"]}}

    def publish(self, job, result, terminal, *, prepared=None):
        prepared = prepared or self.prepare(job, result, terminal)
        raw, intent = prepared["raw"], prepared["intent"]
        digest = hashlib.sha256(raw).hexdigest()
        if (intent["key"] != self.key(job) or intent["bucket"] != self.bucket
                or intent["sha256"] != digest or intent["bytes"] != len(raw)
                or job.get("result_intent", intent) != intent):
            raise ControlError(503, "QUERY_RESULT_BINDING_CONFLICT")
        version = None
        try:
            response = self.s3.put_object(Bucket=self.bucket, Key=self.key(job), Body=raw,
                ContentType="application/json", IfNoneMatch="*", ExpectedBucketOwner=self.account,
                Metadata={"sha256": digest},
                ChecksumSHA256=base64.b64encode(hashlib.sha256(raw).digest()).decode())
            version = response.get("VersionId")
        except (ClientError, BotoCoreError):
            pass  # Original key only. Lost replies never publish a replacement.
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=self.key(job),
                ExpectedBucketOwner=self.account, **({"VersionId": version} if version else {}))
            with response["Body"] as stream:
                stored = stream.read(MAX_BYTES + 1)
            reference = {"bucket": self.bucket, "key": self.key(job),
                "version_id": response["VersionId"], "sha256": response["Metadata"]["sha256"],
                "bytes": len(stored)}
            if reference["version_id"] in {"", "null", None}:
                raise ValueError("Versioned result required")
        except (ClientError, BotoCoreError, KeyError, ValueError):
            raise ControlError(503, "QUERY_RESULT_UNCONFIRMED") from None
        original = self._read(job, reference)
        if original["result"] != result or original["terminal"] != terminal:
            raise ControlError(409, "QUERY_RESULT_BINDING_CONFLICT")
        return reference

    @staticmethod
    def _owner(actor, job):
        actor.authorize(job["space_id"], "query:read")
        if actor.generation != job["generation"] or actor.principal_id != job["principal_id"]:
            raise ControlError(403, "QUERY_OWNER_REQUIRED")
        if job["status"] != "SUCCEEDED":
            raise ControlError(409, "QUERY_RESULT_NOT_READY")

    def _token(self, manifest, purpose, index=0, expires=None):
        payload = {"purpose": purpose, "query_id": manifest["query_id"],
            "execution_id": manifest["execution_id"], "owner": manifest["principal_id"],
            "generation": manifest["generation"], "index": index,
            "expires": expires or manifest["expires_unix"]}
        raw = canonical(payload).encode()
        signature = hmac.new(manifest["token_secret"].encode(), raw, hashlib.sha256).digest()
        return base64.urlsafe_b64encode(raw + signature).decode().rstrip("=")

    def _verify(self, manifest, token, purpose):
        try:
            if not isinstance(token, str) or not 1 <= len(token) <= 4096:
                raise ValueError("Token quota")
            raw = base64.b64decode(token + "=" * (-len(token) % 4), altchars=b"-_", validate=True)
            payload, signature = raw[:-32], raw[-32:]
            expected = hmac.new(manifest["token_secret"].encode(), payload, hashlib.sha256).digest()
            if not hmac.compare_digest(signature, expected):
                raise ValueError("Signature differs")
            value = decode_json(payload)
            if (set(value) != {"purpose", "query_id", "execution_id", "owner", "generation", "index", "expires"}
                    or value["purpose"] != purpose or any(value[name] != manifest[original]
                    for name, original in [("query_id", "query_id"), ("execution_id", "execution_id"),
                                          ("owner", "principal_id"), ("generation", "generation")])
                    or type(value["index"]) is not int or value["index"] < 0
                    or value["expires"] > manifest["expires_unix"]):
                raise ValueError("Token binding differs")
        except (ValueError, TypeError, KeyError):
            raise ControlError(403, "INVALID_QUERY_TOKEN") from None
        if self.clock() >= value["expires"]:
            raise ControlError(410, "QUERY_LINK_EXPIRED")
        return value

    def page(self, actor, job, *, token=None, size=500, guard=lambda: None):
        guard()
        self._owner(actor, job)
        if type(size) is not int or not 1 <= size <= 10000:
            raise ControlError(400, "INVALID_PAGE_SIZE")
        manifest = self._read(job, job["result"])
        index = self._verify(manifest, token, "page")["index"] if token else 0
        result = manifest["result"]
        if index > len(result["rows"]):
            raise ControlError(403, "INVALID_QUERY_TOKEN")
        stop = min(index + size, len(result["rows"]))
        value = {name: result[name] for name in ["columns", "data_versions", "truncated"]}
        value.update(rows=result["rows"][index:stop], row_count=len(result["rows"]),
            next_page_token=self._token(manifest, "page", stop) if stop < len(result["rows"]) else None)
        guard()
        if self.clock() >= manifest["expires_unix"]:
            raise ControlError(410, "QUERY_RESULT_EXPIRED")
        return value

    def export_token(self, actor, job, guard=lambda: None):
        guard()
        self._owner(actor, job)
        manifest = self._read(job, job["result"])
        token = self._token(manifest, "export",
            expires=min(manifest["expires_unix"], self.clock() + EXPORT_SECONDS))
        guard()
        self._verify(manifest, token, "export")
        return token

    def export(self, actor, job, token, guard=lambda: None):
        guard()
        self._owner(actor, job)
        manifest = self._read(job, job["result"])
        self._verify(manifest, token, "export")
        guard()
        self._verify(manifest, token, "export")
        # An authenticated API streams the immutable S3 result. A copied URL
        # cannot grant another principal a bearer capability to S3.
        return manifest["result"]

    def discard_unpublished(self, job, reference):
        if job["status"] not in {"CANCELLED", "FAILED"} or job.get("result") is not None:
            raise ControlError(503, "QUERY_RESULT_CUSTODY_UNKNOWN")
        self._read(job, reference, expired=True)
        self._delete(reference)

    def _absent(self, reference):
        return version_absent(self.s3, self.bucket, self.account, reference,
                              "QUERY_RESULT_CLEANUP_UNKNOWN")

    def _delete(self, reference):
        try:
            self.s3.delete_object(Bucket=self.bucket, Key=reference["key"],
                VersionId=reference["version_id"], ExpectedBucketOwner=self.account)
        except (ClientError, BotoCoreError):
            pass  # Reconcile this exact version; an absent reply is not proof.
        if not self._absent(reference):
            raise ControlError(503, "QUERY_RESULT_CLEANUP_UNKNOWN")

    def collect(self, job):
        """Only exact expired result versions; valid export tokens expire no later."""
        terminal = job["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"} \
            or job["status"] == "UNKNOWN" and job.get("task_stop")
        intent = job.get("result_intent")
        if not terminal or "result" not in job and not intent:
            return False
        expiry = intent["expires_unix"] if intent else job.get("result_expires_unix", float("inf"))
        if self.clock() < expiry:
            return False
        reference = job.get("result")
        if reference is None:
            versions = object_versions(self.s3, self.bucket, self.account, self.key(job),
                                       "QUERY_RESULT_CLEANUP_UNKNOWN")
            if not versions:
                return True
            if len(versions) != 1 or versions[0][1]:
                raise ControlError(503, "QUERY_RESULT_CLEANUP_UNKNOWN")
            reference = {name: intent[name] for name in ["bucket", "key", "sha256", "bytes"]}
            reference["version_id"] = versions[0][0]
        if self._absent(reference):
            return True  # Original expired version was already positively removed.
        manifest = self._read(job, reference, expired=True)
        if (intent and (manifest["expires_unix"] != intent["expires_unix"]
                        or manifest["terminal"] != job["result_terminal"])):
            raise ControlError(503, "QUERY_RESULT_CUSTODY_UNKNOWN")
        if self.clock() < manifest["expires_unix"]:
            return False
        self._delete(reference)
        return True
