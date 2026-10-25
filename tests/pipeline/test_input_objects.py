"""Real Parquet with a versioned S3 double; live IAM acceptance is separate."""

import copy
import io
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from botocore.exceptions import ClientError, ReadTimeoutError

from colvenna_isle.control.registry import ControlError
from colvenna_isle.pipeline.input_objects import InputObjects, freeze
from colvenna_isle.pipeline.parquet import write


class ObjectStore:
    def __init__(self):
        self.objects, self.counter = {}, 0
        self.lose_reply, self.fail_key, self.deny_listing = False, None, False

    def put_object(self, **request):
        assert request["ExpectedBucketOwner"] == "123456789012" and request["IfNoneMatch"] == "*"
        key = request["Key"]
        if key == self.fail_key:
            raise ReadTimeoutError(endpoint_url="controlled-original-put")
        if self.objects.get(key):
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.counter += 1
        version = "version_" + str(self.counter)
        self.objects[key] = [(version, request["Body"], request["Metadata"])]
        if self.lose_reply:
            self.lose_reply = False
            raise ReadTimeoutError(endpoint_url="controlled-lost-original-reply")
        return {"VersionId": version}

    def get_object(self, **request):
        assert request["ExpectedBucketOwner"] == "123456789012"
        values = self.objects.get(request["Key"], [])
        version = request.get("VersionId")
        value = next((value for value in reversed(values) if version is None or value[0] == version), None)
        if value is None:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"VersionId": value[0], "Body": io.BytesIO(value[1]), "Metadata": value[2]}

    def list_object_versions(self, **request):
        assert request["ExpectedBucketOwner"] == "123456789012"
        if self.deny_listing:
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "ListObjectVersions")
        return {"IsTruncated": False, "Versions": [{"Key": key, "VersionId": value[0]}
            for key, values in self.objects.items() if key.startswith(request["Prefix"]) for value in values]}


class OriginalInputs(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.storage = ObjectStore()
        self.objects = InputObjects(self.storage, "bucket", "123456789012")
        columns = [{"name": "id", "type": "BIGINT", "nullable": False},
            {"name": "amount", "type": "DECIMAL(30,2)", "nullable": False}]
        inputs = {}
        for alias, sequence in [("orders", 1), ("other", 2)]:
            file = Path(self.temporary.name) / (alias + ".parquet")
            metadata = write(file, columns, [[[9007199254740993, Decimal("900719925474099.01")]]],
                maximum_bytes=1048576, maximum_rows=10)
            inputs[alias] = {"dataset": "dwd." + alias, "schema": columns,
                "selection": {"mode": "snapshot"}, "native_version": {"dataset": "dwd." + alias,
                    "release_id": None, "generation": "gen_fixture", "commit_sequence": sequence,
                    "snapshot_sequence": 2}, "file": {**metadata, "local_path": str(file)}}
        self.exported = {"status": "EXPORTED_LOCAL", "publication_status": "NOT_PUBLISHED",
            "space_id": "fixture", "generation": "gen_fixture", "inputs": inputs,
            "metrics": {"copied_bytes": sum(value["file"]["bytes"] for value in inputs.values()),
                "copied_rows": 2, "elapsed_ms": 1}}
        self.record = {"export_id": "input_original", "execution_id": "exec_original",
            "space_id": "fixture", "generation": "gen_fixture", "principal_id": "owner",
            "binding_sha256": "c" * 64, "source_commit": "a" * 40, "image_digest": "sha256:" + "b" * 64,
            "staged": freeze(self.exported)}

    def tearDown(self):
        self.temporary.cleanup()

    def publish(self):
        prepared = self.objects.prepare(self.record, self.exported, lambda: None)
        self.record["manifest_intent"] = prepared["intent"]
        self.record["manifest"] = self.objects.publish(self.record, prepared, lambda: None)
        return prepared

    def test_complete_manifest_pins_all_original_files_types_and_multiple_versions(self):
        prepared = self.publish()
        manifest = self.objects.read(self.record, lambda: None)
        self.assertEqual(manifest, prepared["manifest"])
        self.assertEqual([manifest["inputs"][alias]["native_version"]["commit_sequence"]
            for alias in ["orders", "other"]], [1, 2])
        self.assertEqual(manifest["inputs"]["orders"]["manifest"]["schema"][1]["type"], "DECIMAL(30,2)")
        self.assertEqual(len(self.storage.objects), 3)
        for value in manifest["inputs"].values():
            self.assertNotIn("local_path", value["manifest"]["files"][0])

    def test_lost_file_and_manifest_put_replies_recover_same_original_versions(self):
        self.storage.lose_reply = True
        prepared = self.objects.prepare(self.record, self.exported, lambda: None)
        self.record["manifest_intent"] = prepared["intent"]
        self.storage.lose_reply = True
        reference = self.objects.publish(self.record, prepared, lambda: None)
        self.record["manifest"] = reference
        again = self.objects.prepare(self.record, self.exported, lambda: None)
        self.assertEqual(again, prepared)
        self.assertEqual(self.objects.publish(self.record, again, lambda: None), reference)
        self.assertEqual(self.storage.counter, 3)
        self.assertEqual(self.objects.read(self.record, lambda: None), prepared["manifest"])

    def test_partial_upload_never_creates_completion_manifest(self):
        self.storage.fail_key = self.objects.prefix(self.record) + "orders/part-00000.parquet"
        with self.assertRaisesRegex(ControlError, "INPUT_OBJECT_UNCONFIRMED"):
            self.publish()
        self.assertFalse(any(key.endswith("manifest.json") for key in self.storage.objects))

    def test_recovery_without_local_files_uses_only_uploaded_originals(self):
        prepared = self.publish()
        for value in self.exported["inputs"].values():
            Path(value["file"]["local_path"]).unlink()
        self.assertEqual(self.objects.recover(self.record, lambda: None), prepared)
        self.assertEqual(self.storage.counter, 3)

    def test_wrong_bucket_key_hash_version_and_missing_file_deny_consumer(self):
        self.publish()
        for field, value in [("bucket", "other"), ("key", "spaces/other/manifest.json"),
                ("sha256", "0" * 64), ("version_id", "nonexistent")]:
            with self.subTest(field=field):
                altered = copy.deepcopy(self.record)
                altered["manifest"][field] = value
                with self.assertRaises(ControlError):
                    self.objects.read(altered, lambda: None)
        key = self.objects.prefix(self.record) + "orders/part-00000.parquet"
        self.storage.objects.pop(key)
        with self.assertRaisesRegex(ControlError, "INPUT_OBJECT_UNCONFIRMED"):
            self.objects.read(self.record, lambda: None)

    def test_recovery_denied_or_ambiguous_version_listing_is_not_ready(self):
        self.publish()
        self.storage.deny_listing = True
        with self.assertRaisesRegex(ControlError, "INPUT_OBJECT_UNCONFIRMED"):
            self.objects.recover(self.record, lambda: None)
        self.storage.deny_listing = False
        key = self.objects.prefix(self.record) + "orders/part-00000.parquet"
        version, raw, metadata = self.storage.objects[key][0]
        self.storage.objects[key].append(("other_version", raw, metadata))
        with self.assertRaisesRegex(ControlError, "INPUT_OBJECT_UNCONFIRMED"):
            self.objects.recover(self.record, lambda: None)

    def test_missing_file_after_prepare_and_forged_manifest_intent_never_publish(self):
        prepared = self.objects.prepare(self.record, self.exported, lambda: None)
        self.record["manifest_intent"] = {**prepared["intent"], "sha256": "0" * 64}
        with self.assertRaisesRegex(ControlError, "INPUT_MANIFEST_BINDING_CONFLICT"):
            self.objects.publish(self.record, prepared, lambda: None)
        self.record["manifest_intent"] = prepared["intent"]
        self.storage.objects.pop(self.objects.prefix(self.record) + "orders/part-00000.parquet")
        with self.assertRaisesRegex(ControlError, "INPUT_OBJECT_UNCONFIRMED"):
            self.objects.publish(self.record, prepared, lambda: None)
        self.assertFalse(any(key.endswith("manifest.json") for key in self.storage.objects))


if __name__ == "__main__":
    unittest.main()
