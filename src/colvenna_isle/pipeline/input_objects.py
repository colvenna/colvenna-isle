"""Immutable input files and a complete, fixed version-vector manifest."""

import base64
import copy
import hashlib
from pathlib import Path

from botocore.exceptions import BotoCoreError, ClientError

from colvenna_isle.contracts.validation import validate
from colvenna_isle.control.http import decode_json
from colvenna_isle.control.registry import ControlError, canonical
from colvenna_isle.query.objects import object_versions
from .parquet import describe

MAX_FILE_BYTES = 33554432
MAX_MANIFEST_BYTES = 1048576


def freeze(exported):
    """The writer saves this exact portable descriptor before external uploads."""
    if exported["status"] != "EXPORTED_LOCAL" or exported["publication_status"] != "NOT_PUBLISHED":
        raise ControlError(409, "INPUT_EXPORT_INCOMPLETE")
    result = copy.deepcopy(exported)
    for value in result["inputs"].values():
        value["file"].pop("local_path")
    return result


class InputObjects:
    def __init__(self, s3, bucket, account):
        self.s3, self.bucket, self.account = s3, bucket, account

    @staticmethod
    def prefix(record):
        for name in ["space_id", "generation", "export_id", "execution_id"]:
            validate("identifier", record[name])
        return (f"spaces/{record['space_id']}/inputs/{record['generation']}/"
                f"{record['export_id']}/{record['execution_id']}/")

    def _reference(self, key, raw):
        return {"bucket": self.bucket, "key": key, "sha256": hashlib.sha256(raw).hexdigest(),
                "bytes": len(raw)}

    def _get(self, reference, *, version=True):
        if (reference["bucket"] != self.bucket or type(reference["bytes"]) is not int
                or not 0 < reference["bytes"] <= MAX_FILE_BYTES):
            raise ControlError(503, "INPUT_OBJECT_UNCONFIRMED")
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=reference["key"],
                ExpectedBucketOwner=self.account,
                **({"VersionId": reference["version_id"]} if version else {}))
            with response["Body"] as stream:
                raw = stream.read(reference["bytes"] + 1)
            actual_version = response["VersionId"]
            if (not isinstance(actual_version, str) or not 1 <= len(actual_version) <= 1024
                    or actual_version == "null" or version and actual_version != reference["version_id"]
                    or len(raw) != reference["bytes"]
                    or hashlib.sha256(raw).hexdigest() != reference["sha256"]
                    or response["Metadata"]["sha256"] != reference["sha256"]):
                raise ValueError("Original input object differs")
        except (ClientError, BotoCoreError, KeyError, ValueError, TypeError):
            raise ControlError(503, "INPUT_OBJECT_UNCONFIRMED") from None
        if not version and object_versions(self.s3, self.bucket, self.account,
                reference["key"], "INPUT_OBJECT_UNCONFIRMED") != [(actual_version, False)]:
            raise ControlError(503, "INPUT_OBJECT_UNCONFIRMED")
        return {**reference, "version_id": actual_version}, raw

    def _put(self, key, raw, content_type):
        reference = self._reference(key, raw)
        try:
            response = self.s3.put_object(Bucket=self.bucket, Key=key, Body=raw,
                IfNoneMatch="*", ContentType=content_type, ExpectedBucketOwner=self.account,
                Metadata={"sha256": reference["sha256"]},
                ChecksumSHA256=base64.b64encode(hashlib.sha256(raw).digest()).decode())
            if response.get("VersionId") not in {"", "null", None}:
                reference["version_id"] = response["VersionId"]
        except (ClientError, BotoCoreError):
            pass  # Observe the same original key; never overwrite or choose a new execution.
        return self._get(reference, version="version_id" in reference)[0]

    def _file_key(self, record, alias):
        validate("input_alias", alias)
        return self.prefix(record) + alias + "/part-00000.parquet"

    def _manifest(self, record, files):
        staged = record["staged"]
        inputs = {}
        for alias, value in sorted(staged["inputs"].items()):
            version, reference = value["native_version"], files[alias]
            if (reference["bucket"] != self.bucket or reference["key"] != self._file_key(record, alias)
                    or any(reference[name] != value["file"][name] for name in ["bytes", "sha256"])):
                raise ControlError(503, "INPUT_FILE_BINDING_CONFLICT")
            descriptor = {"contract_version": 2, "dataset": value["dataset"],
                # This identifies a fixed input artifact, not an analytical release.
                "release_id": "input_" + hashlib.sha256(canonical([record["export_id"], alias]).encode()).hexdigest(),
                "watermark": {"ods_sequence": version["commit_sequence"]},
                "schema": value["schema"], "files": [{
                    "s3_uri": "s3://" + self.bucket + "/" + reference["key"],
                    "object_version": reference["version_id"], "sha256": reference["sha256"],
                    "rows": value["file"]["rows"], "bytes": reference["bytes"]}]}
            validate("input_manifest", descriptor)
            inputs[alias] = {"manifest": descriptor, "native_version": version,
                             "selection": value["selection"]}
        return {"format_version": 1, **{name: record[name] for name in [
            "export_id", "execution_id", "space_id", "generation", "principal_id",
            "binding_sha256", "source_commit", "image_digest"]},
            "inputs": inputs, "metrics": staged["metrics"]}

    def prepare(self, record, exported, guard):
        """All exact data files are confirmed before producing the final manifest."""
        if record.get("staged") != freeze(exported):
            raise ControlError(409, "INPUT_FILE_BINDING_CONFLICT")
        files = {}
        for alias, value in sorted(exported["inputs"].items()):
            guard()
            path = Path(value["file"]["local_path"])
            metadata = describe(path, value["schema"], maximum_bytes=MAX_FILE_BYTES,
                expected=value["file"], guard=guard)
            raw = path.read_bytes()
            if (len(raw) != metadata["bytes"]
                    or hashlib.sha256(raw).hexdigest() != metadata["sha256"] or path.is_symlink()):
                raise ControlError(409, "INPUT_FILE_CHANGED")
            files[alias] = self._put(self._file_key(record, alias), raw, "application/vnd.apache.parquet")
            guard()
        return self._prepared(record, files)

    def recover(self, record, guard):
        """Recover only uploaded originals; never refresh or export source tables."""
        files = {}
        for alias, value in sorted(record["staged"]["inputs"].items()):
            guard()
            descriptor = {"bucket": self.bucket, "key": self._file_key(record, alias),
                          **{name: value["file"][name] for name in ["sha256", "bytes"]}}
            files[alias] = self._get(descriptor, version=False)[0]
        guard()
        return self._prepared(record, files)

    def _prepared(self, record, files):
        manifest = self._manifest(record, files)
        raw = canonical(manifest).encode()
        if len(raw) > MAX_MANIFEST_BYTES:
            raise ControlError(413, "INPUT_MANIFEST_TOO_LARGE")
        return {"raw": raw, "manifest": manifest,
                "intent": self._reference(self.prefix(record) + "manifest.json", raw)}

    def publish(self, record, prepared, guard):
        """Only the manifest intent already saved by the native writer may publish."""
        guard()
        expected = self._reference(self.prefix(record) + "manifest.json", prepared["raw"])
        if record.get("manifest_intent") != expected or prepared["intent"] != expected:
            raise ControlError(409, "INPUT_MANIFEST_BINDING_CONFLICT")
        original = self._manifest(record, {alias: {
            "bucket": self.bucket, "key": value["manifest"]["files"][0]["s3_uri"].removeprefix(
                "s3://" + self.bucket + "/"), "version_id": value["manifest"]["files"][0]["object_version"],
            "sha256": value["manifest"]["files"][0]["sha256"], "bytes": value["manifest"]["files"][0]["bytes"]}
            for alias, value in prepared["manifest"]["inputs"].items()})
        if canonical(original).encode() != prepared["raw"]:
            raise ControlError(409, "INPUT_MANIFEST_BINDING_CONFLICT")
        for value in original["inputs"].values():
            file = value["manifest"]["files"][0]
            guard()
            self._get({"bucket": self.bucket,
                "key": file["s3_uri"].removeprefix("s3://" + self.bucket + "/"),
                "version_id": file["object_version"], "sha256": file["sha256"], "bytes": file["bytes"]})
        reference = self._put(expected["key"], prepared["raw"], "application/json")
        guard()
        return reference

    def read(self, record, guard):
        """Every consumer uses exact versions, never a directory listing or latest."""
        guard()
        reference = record["manifest"]
        if (reference["key"] != self.prefix(record) + "manifest.json"
                or any(reference[name] != record["manifest_intent"][name]
                       for name in ["bucket", "key", "sha256", "bytes"])):
            raise ControlError(503, "INPUT_MANIFEST_UNCONFIRMED")
        _, raw = self._get(reference)
        try:
            manifest = decode_json(raw)
            files = {}
            for alias, value in manifest["inputs"].items():
                file = value["manifest"]["files"][0]
                files[alias] = {"bucket": self.bucket, "key": self._file_key(record, alias),
                    "version_id": file["object_version"], "sha256": file["sha256"], "bytes": file["bytes"]}
                if file["s3_uri"] != "s3://" + self.bucket + "/" + files[alias]["key"]:
                    raise ValueError("Input scope differs")
            if manifest != self._manifest(record, files):
                raise ValueError("Original manifest differs")
        except (KeyError, TypeError, ValueError):
            raise ControlError(503, "INPUT_MANIFEST_UNCONFIRMED") from None
        for file in files.values():
            guard()
            self._get(file)
        guard()
        return manifest
