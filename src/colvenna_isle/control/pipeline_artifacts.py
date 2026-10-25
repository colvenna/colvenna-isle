"""Purpose-bound uploads and verified, immutable pipeline packages."""

import base64
import hashlib
import io
import tempfile
import zipfile
from pathlib import Path

from botocore.exceptions import ClientError

from colvenna_isle.account import account
from colvenna_isle.contracts.model import LIMITS
from colvenna_isle.contracts.validation import load_document, safe_relative
from .registry import ControlError, canonical



def sha256(content):
    return hashlib.sha256(content).hexdigest()


def checksum(value):
    return base64.b64encode(bytes.fromhex(value)).decode()


class PipelineArtifacts:
    def __init__(self, s3, bucket):
        self.s3, self.bucket = s3, bucket

    def prefix(self, space, generation, pipeline):
        return f"spaces/{space}/pipeline-code/{generation}/{pipeline}/"

    def upload_key(self, space, generation, pipeline, upload):
        return self.prefix(space, generation, pipeline) + f"uploads/{upload}"

    def upload(self, record):
        declaration = record["declaration"]
        headers = {"content-type": "application/octet-stream", "if-none-match": "*",
                   "content-length": str(declaration["bytes"]),
                   "x-amz-checksum-sha256": checksum(declaration["sha256"]),
                   "x-amz-expected-bucket-owner": account()}
        url = self.s3.generate_presigned_url("put_object", Params={
            "Bucket": self.bucket, "Key": record["key"], "ExpectedBucketOwner": account(),
            "ContentLength": declaration["bytes"], "ContentType": headers["content-type"],
            "ChecksumSHA256": headers["x-amz-checksum-sha256"], "IfNoneMatch": "*",
        }, ExpiresIn=300, HttpMethod="PUT")
        return url, headers

    def _head(self, key):
        try:
            return self.s3.head_object(
                Bucket=self.bucket, Key=key, ExpectedBucketOwner=account(), ChecksumMode="ENABLED")
        except ClientError as error:
            if error.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
                raise ControlError(422, "ARTIFACT_UPLOAD_INCOMPLETE") from None
            raise

    def _read(self, key, version, size, expected):
        if not isinstance(version, str) or version in {"", "null"}:
            raise ControlError(503, "ARTIFACT_VERSION_UNAVAILABLE")
        result = self.s3.get_object(
            Bucket=self.bucket, Key=key, VersionId=version,
            ExpectedBucketOwner=account(), ChecksumMode="ENABLED")
        body = result["Body"]
        try:
            content = body.read(size + 1)
        finally:
            body.close()
        if (len(content) != size or result["ContentLength"] != size
                or result.get("VersionId") != version or sha256(content) != expected
                or result.get("ChecksumSHA256") != checksum(expected)):
            raise ControlError(422, "ARTIFACT_CONTENT_MISMATCH")
        return content

    def verified_upload(self, record):
        declaration = record["declaration"]
        actual = self._head(record["key"])
        if (actual["ContentLength"] != declaration["bytes"]
                or actual.get("ChecksumSHA256") != checksum(declaration["sha256"])):
            raise ControlError(422, "ARTIFACT_CONTENT_MISMATCH")
        content = self._read(record["key"], actual.get("VersionId"),
                             declaration["bytes"], declaration["sha256"])
        return {"key": record["key"], "object_version": actual["VersionId"],
                **declaration}, content

    def put_inline(self, record, content):
        """Control-written browser upload: the same conditional, checksummed original object."""
        declaration = record["declaration"]
        if len(content) != declaration["bytes"] or sha256(content) != declaration["sha256"]:
            raise ControlError(422, "ARTIFACT_CONTENT_MISMATCH")
        try:
            self.s3.put_object(Bucket=self.bucket, Key=record["key"], Body=content, ExpectedBucketOwner=account(),
                               IfNoneMatch="*", ContentType="application/octet-stream",
                               ChecksumSHA256=checksum(declaration["sha256"]))
        except ClientError as error:
            if error.response["Error"]["Code"] not in {"PreconditionFailed", "412", "ConditionalRequestConflict"}:
                raise
        reference, _ = self.verified_upload(record)
        return reference

    def verify_code(self, definition, files):
        """Parse source files without importing or executing submitted code."""
        for path, content in files.items():
            safe_relative(path)
            if path.split("/")[0] == ".colvenna":
                raise ControlError(422, "RESERVED_ARTIFACT_PATH")
            try:
                if path.endswith(".py"):
                    compile(content, path, "exec", dont_inherit=True)
                elif path.endswith((".yaml", ".yml")):
                    with tempfile.TemporaryDirectory(prefix="colvenna-definition-") as directory:
                        candidate = Path(directory) / "pipeline.yaml"
                        candidate.write_bytes(content)
                        if load_document(candidate) != definition:
                            raise ControlError(422, "YAML_DEFINITION_MISMATCH")
                elif path.endswith(".sql"):
                    if not content.decode("utf-8").strip():
                        raise ControlError(422, "EMPTY_SQL_ARTIFACT")
            except (SyntaxError, UnicodeError, ValueError, RecursionError):
                raise ControlError(422, "ARTIFACT_SOURCE_INVALID") from None

    def package(self, snapshot):
        files = {entry["path"]: self._read(
            entry["key"], entry["object_version"], entry["bytes"], entry["sha256"])
            for entry in snapshot["artifacts"]}
        self.verify_code(snapshot["definition"], files)
        files[".colvenna/definition.json"] = canonical(snapshot["definition"]).encode()
        files[".colvenna/manifest.json"] = canonical(snapshot).encode()
        if sum(map(len, files.values())) > LIMITS["artifact_package_bytes"]:
            raise ControlError(413, "ARTIFACT_PACKAGE_LIMIT")
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_STORED) as archive:
            for path, content in sorted(files.items()):
                info = zipfile.ZipInfo(path, (2026, 10, 15, 0, 0, 0))
                info.external_attr = 0o100644 << 16
                archive.writestr(info, content)
        content = buffer.getvalue()
        if len(content) > LIMITS["artifact_package_bytes"]:
            raise ControlError(413, "ARTIFACT_PACKAGE_LIMIT")
        expected = sha256(content)
        key = self.prefix(snapshot["space_id"], snapshot["generation"],
                          snapshot["pipeline_id"]) + f"versions/{snapshot['pipeline_version']}/package.zip"
        try:
            self.s3.put_object(
                Bucket=self.bucket, Key=key, Body=content, ExpectedBucketOwner=account(),
                IfNoneMatch="*", ContentType="application/zip", ChecksumSHA256=checksum(expected))
        except ClientError as error:
            if error.response["Error"]["Code"] not in {"PreconditionFailed", "412", "ConditionalRequestConflict"}:
                raise
        # A successful Put is not publication proof. Reread the exact version.
        actual = self._head(key)
        if (actual["ContentLength"] != len(content)
                or actual.get("ChecksumSHA256") != checksum(expected)):
            raise ControlError(503, "PIPELINE_PACKAGE_CONFLICT")
        self._read(key, actual.get("VersionId"), len(content), expected)
        return {"package_uri": f"s3://{self.bucket}/{key}",
                "package_object_version": actual["VersionId"], "package_sha256": expected,
                "package_bytes": len(content)}

    def verify_package(self, snapshot, binding):
        key = self.prefix(snapshot["space_id"], snapshot["generation"],
                          snapshot["pipeline_id"]) + f"versions/{snapshot['pipeline_version']}/package.zip"
        if binding["package_uri"] != f"s3://{self.bucket}/{key}":
            raise ControlError(503, "PIPELINE_PACKAGE_CONFLICT")
        self._read(key, binding["package_object_version"],
                   binding["package_bytes"], binding["package_sha256"])
