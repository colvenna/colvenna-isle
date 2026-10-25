"""Exact-version task IO; runtime roles never need latest reads or listings."""

import base64
import hashlib
from pathlib import Path

from botocore.exceptions import BotoCoreError, ClientError

from colvenna_isle.account import account
from colvenna_isle.contracts.validation import safe_relative, validate
from colvenna_isle.control.registry import ControlError

CHUNK_BYTES = 65536


class UploadUnknown(ControlError):
    def __init__(self, reference):
        super().__init__(503, "TASK_UPLOAD_ORIGINAL_UNKNOWN")
        self.original_upload = reference


class TaskObjects:
    def __init__(self, s3, bucket, space, generation):
        for identity in [space, generation]:
            validate("identifier", identity)
        self.s3, self.bucket, self.space, self.generation = s3, bucket, space, generation

    def key(self, reference, namespace):
        """A signed reference still cannot point outside the installed generation."""
        validate("task_object", reference)
        prefix = f"s3://{self.bucket}/spaces/{self.space}/{namespace}/{self.generation}/"
        uri = reference["s3_uri"]
        if (not uri.startswith(prefix) or reference["object_version"] == "null"
                or not uri[len(prefix):]):
            raise ControlError(403, "TASK_OBJECT_SCOPE")
        safe_relative(uri[len(prefix):])
        return uri.removeprefix(f"s3://{self.bucket}/")

    def read(self, reference, namespace, *, maximum_bytes, guard=lambda: None, path=None):
        key = self.key(reference, namespace)
        if reference["bytes"] > maximum_bytes:
            raise ControlError(413, "TASK_OBJECT_BUDGET_EXCEEDED")
        guard()
        if path is not None:
            path = Path(path)
            path.parent.mkdir(parents=True, exist_ok=True)
        output = path.open("xb") if path is not None else None
        content, digest, size = bytearray(), hashlib.sha256(), 0
        try:
            response = self.s3.get_object(
                Bucket=self.bucket, Key=key, VersionId=reference["object_version"],
                ExpectedBucketOwner=account())
            if response["VersionId"] != reference["object_version"]:
                raise ControlError(503, "TASK_OBJECT_UNCONFIRMED")
            with response["Body"] as stream:
                while True:
                    guard()
                    chunk = stream.read(min(CHUNK_BYTES, reference["bytes"] + 1 - size))
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > reference["bytes"]:
                        raise ControlError(503, "TASK_OBJECT_UNCONFIRMED")
                    digest.update(chunk)
                    if output is None:
                        content.extend(chunk)
                    else:
                        output.write(chunk)
                        output.flush()
                    guard()
            if size != reference["bytes"] or digest.hexdigest() != reference["sha256"]:
                raise ControlError(503, "TASK_OBJECT_UNCONFIRMED")
        except (ClientError, BotoCoreError, KeyError):
            raise ControlError(503, "TASK_OBJECT_UNCONFIRMED") from None
        finally:
            if output is not None:
                output.close()
        guard()
        return bytes(content) if path is None else path

    def put(self, key, raw, *, guard=lambda: None, content_type="application/json"):
        """A lost Put reply is UNKNOWN for this key; the runtime cannot list/latest."""
        prefix = f"spaces/{self.space}/"
        if not key.startswith(prefix):
            raise ControlError(403, "TASK_OBJECT_SCOPE")
        tail = key[len(prefix):]
        if not any(tail.startswith(f"{namespace}/{self.generation}/")
                   for namespace in ["staging", "task-contexts"]):
            raise ControlError(403, "TASK_OBJECT_SCOPE")
        safe_relative(tail)
        guard()
        digest = hashlib.sha256(raw).hexdigest()
        reference = {"s3_uri": f"s3://{self.bucket}/{key}", "sha256": digest, "bytes": len(raw)}
        try:
            result = self.s3.put_object(
                Bucket=self.bucket, Key=key, Body=raw, IfNoneMatch="*",
                ContentType=content_type, ExpectedBucketOwner=account(),
                Metadata={"sha256": digest},
                ChecksumSHA256=base64.b64encode(bytes.fromhex(digest)).decode())
            version = result["VersionId"]
            if not isinstance(version, str) or not 1 <= len(version) <= 1024 or version == "null":
                raise KeyError("Missing version")
        except (ClientError, BotoCoreError, KeyError):
            raise UploadUnknown(reference) from None
        reference["object_version"] = version
        # Completion requires exact-version content confirmation, not Put success.
        namespace = tail.split("/", 1)[0]
        try:
            self.read(reference, namespace, maximum_bytes=len(raw), guard=guard)
            guard()
        except (ControlError, OSError):
            raise UploadUnknown(reference) from None
        return reference


def reconcile(objects, key, expected_sha256, maximum_bytes):
    """Confirm an uncertain conditional Put: exactly one original version with the planned hash.

    Controller-only; it needs version listing that runtime roles do not have.
    """
    if not isinstance(expected_sha256, str):
        raise ControlError(503, "TASK_OBJECT_CUSTODY_UNKNOWN")
    try:
        versions = [value for page in objects.s3.get_paginator("list_object_versions").paginate(
            Bucket=objects.bucket, Prefix=key, ExpectedBucketOwner=account())
            for value in page.get("Versions", []) + page.get("DeleteMarkers", []) if value["Key"] == key]
    except (ClientError, BotoCoreError):
        raise ControlError(503, "TASK_OBJECT_CUSTODY_UNKNOWN") from None
    if len(versions) != 1 or "Size" not in versions[0]:
        raise ControlError(503, "TASK_OBJECT_CUSTODY_UNKNOWN")
    reference = {"s3_uri": f"s3://{objects.bucket}/{key}", "object_version": versions[0]["VersionId"],
                 "sha256": expected_sha256, "bytes": versions[0]["Size"]}
    objects.read(reference, key.split("/", 3)[2], maximum_bytes=maximum_bytes)
    return reference


def input_reference(file):
    return {"s3_uri": file["s3_uri"], "object_version": file["object_version"],
            "sha256": file["sha256"], "bytes": file["bytes"]}
