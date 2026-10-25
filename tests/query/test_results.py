"""Immutable original results and owner-bound capabilities preserve typed pages."""

import copy
import unittest

from colvenna_isle.contracts.validation import ContractError, validate
from colvenna_isle.control.registry import ControlError
from colvenna_isle.query.results import Results
from botocore.exceptions import ClientError, ReadTimeoutError
import test_jobs
from test_jobs import REAPED, Storage


class ResultRetention(unittest.TestCase):
    def setUp(self):
        self.storage = Storage()
        self.now = 100.0
        self.results = Results(self.storage, "bucket", "123456789012", lambda: self.now)
        self.fixture = test_jobs.SharedJobs()
        self.fixture.setUp()
        self.actor = self.fixture.actor
        self.job = self.fixture.claimed()
        self.value = {"columns": [
            {"name": "same alias", "type": "DECIMAL(30,2)", "encoding": "native_json"},
            {"name": "same alias", "type": "HUGEINT", "encoding": "native_json"},
            {"name": "", "type": "INTERVAL", "encoding": "native_text"}],
            "rows": [["900719925474099.01", "9007199254740993", "1 month"] for _ in range(7)],
            "data_versions": [{"dataset": "dwd.orders", "release_id": None, "generation": "gen_fixture",
                              "commit_sequence": 1, "snapshot_sequence": 2}], "truncated": False}
        reference = self.results.publish(self.job, self.value, REAPED)
        self.job.update(status="SUCCEEDED", result=reference, result_expires_unix=self.now + 600)

    def test_pages_bind_same_original_values_aliases_types_versions_and_do_not_reexecute(self):
        values, token = [], None
        while True:
            page = self.results.page(self.actor, self.job, token=token, size=3)
            self.assertEqual(page["columns"], self.value["columns"])
            self.assertEqual(page["data_versions"], self.value["data_versions"])
            values.extend(page["rows"])
            token = page["next_page_token"]
            if token is None:
                break
        self.assertEqual(values, self.value["rows"])
        self.assertEqual(len(self.storage.objects), 1)
        validate("query_status", {**page, "query_id": self.job["query_id"],
                                 "status": "SUCCEEDED", "elapsed_ms": 1})

    def test_other_owner_admin_wrong_space_generation_and_page_quota_are_denied(self):
        for actor in [self.fixture.principal("other"), self.fixture.principal("admin", admin=True),
                      self.fixture.principal("owner", space="other")]:
            with self.assertRaises(ControlError):
                self.results.page(actor, self.job)
            with self.assertRaises(ControlError):
                self.results.export_token(actor, self.job)
        for size in [0, 10001, True]:
            with self.assertRaises(ControlError):
                self.results.page(self.actor, self.job, size=size)

    def test_tampered_cross_query_and_export_tokens_cannot_be_used_as_page_tokens(self):
        page = self.results.page(self.actor, self.job, size=1)
        export = self.results.export_token(self.actor, self.job)
        for token in [page["next_page_token"] + "invalid", export]:
            with self.assertRaises(ControlError):
                self.results.page(self.actor, self.job, token=token)
        other = copy.deepcopy(self.job)
        other["query_id"] = "another"
        with self.assertRaises(ControlError):
            self.results.page(self.actor, other, token=page["next_page_token"])

    def test_export_requires_same_authenticated_owner_and_expires_before_retention(self):
        token = self.results.export_token(self.actor, self.job)
        self.assertEqual(self.results.export(self.actor, self.job, token), self.value)
        with self.assertRaises(ControlError):
            self.results.export(self.fixture.principal("other"), self.job, token)
        self.now += 121
        with self.assertRaises(ControlError) as expired:
            self.results.export(self.actor, self.job, token)
        self.assertEqual(expired.exception.code, "QUERY_LINK_EXPIRED")
        self.assertEqual(self.results.page(self.actor, self.job)["rows"], self.value["rows"])

    def test_lost_publish_response_keeps_original_version_secret_and_expiry(self):
        self.storage.objects.clear()
        self.storage.lose_put_reply = True
        original = self.results.publish(self.job, self.value, REAPED)
        first = self.results._read(self.job, original)
        self.now += 100
        self.assertEqual(self.results.publish(self.job, self.value, REAPED), original)
        self.assertEqual(self.results._read(self.job, original)["token_secret"], first["token_secret"])
        self.assertEqual(self.results._read(self.job, original)["expires_unix"], first["expires_unix"])
        self.assertEqual(len(self.storage.objects), 1)
        changed = copy.deepcopy(self.value)
        changed["rows"][0][0] = "1.00"
        with self.assertRaises(ControlError):
            self.results.publish(self.job, changed, REAPED)

    def test_gc_preserves_valid_query_and_export_refs_then_removes_exact_expired_version(self):
        self.results.export_token(self.actor, self.job)
        removed = []
        original_delete = self.storage.delete_object
        def delete(**request):
            removed.append(request)
            return original_delete(**request)
        self.storage.delete_object = delete
        self.assertFalse(self.results.collect(self.job))
        self.now += 601
        with self.assertRaises(ControlError) as expired:
            self.results.page(self.actor, self.job)
        self.assertEqual(expired.exception.code, "QUERY_RESULT_EXPIRED")
        self.assertFalse(self.results.collect({**self.job, "status": "RUNNING"}))
        self.assertTrue(self.results.collect(self.job))
        self.assertEqual(len(removed), 1)
        self.assertEqual(removed[0]["VersionId"], self.job["result"]["version_id"])
        self.assertTrue(self.results.collect(self.job))
        with self.assertRaises(ControlError) as expired:
            self.results.page(self.actor, self.job)
        self.assertEqual(expired.exception.code, "QUERY_RESULT_EXPIRED")

    def test_manifest_corruption_and_decimal_float_or_unsafe_integer_cannot_publish(self):
        for value in [1.25, 9007199254740993]:
            changed = copy.deepcopy(self.value)
            changed["rows"][0][0 if isinstance(value, float) else 1] = value
            with self.assertRaises(ContractError):
                validate("query_result", changed)
        original = next(iter(self.storage.objects.values()))
        original["Body"] += b" "
        with self.assertRaises(ControlError) as corrupt:
            self.results.page(self.actor, self.job)
        self.assertEqual(corrupt.exception.code, "QUERY_RESULT_UNCONFIRMED")

    def test_lost_delete_reply_reconciles_exact_original_and_preserves_expiry_response(self):
        original_delete = self.storage.delete_object
        def delete(**request):
            original_delete(**request)
            raise ReadTimeoutError(endpoint_url="original-delete-reply-lost")
        self.storage.delete_object = delete
        self.now += 601
        self.assertTrue(self.results.collect(self.job))
        self.assertTrue(self.results.collect(self.job))
        with self.assertRaises(ControlError) as expired:
            self.results.page(self.actor, self.job)
        self.assertEqual(expired.exception.code, "QUERY_RESULT_EXPIRED")

    def test_gc_confirms_deleted_version_when_missing_get_is_denied_and_preserves_other_versions(self):
        original_get = self.storage.get_object
        original_key = self.job["result"]["key"]
        def get(**request):
            if request["Key"] not in self.storage.objects:
                raise ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")
            return original_get(**request)
        self.storage.get_object = get
        # A different version and a key sharing the prefix do not establish
        # presence of this exact immutable original.
        original_list = self.storage.list_object_versions
        def versions(**request):
            value = original_list(**request)
            value["Versions"].extend([
                {"Key": original_key, "VersionId": "another-version"},
                {"Key": original_key + ".other", "VersionId": "original-version"}])
            return value
        self.storage.list_object_versions = versions
        self.now += 601
        self.assertTrue(self.results.collect(self.job))
        self.assertTrue(self.results.collect(self.job))

    def test_gc_requires_complete_authorized_listing_before_releasing_original_custody(self):
        self.now += 601
        key = self.job["result"]["key"]
        def denied(**request):
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "ListObjectVersions")
        for listing in [denied, lambda **request: {"IsTruncated": True},
                        lambda **request: {"IsTruncated": "false"}]:
            self.storage.list_object_versions = listing
            with self.assertRaises(ControlError) as unknown:
                self.results.collect(self.job)
            self.assertEqual(unknown.exception.code, "QUERY_RESULT_CLEANUP_UNKNOWN")
            self.assertIn(key, self.storage.objects)

    def test_gc_observes_original_on_later_version_page_and_preserves_unrelated_objects(self):
        self.now += 601
        key = self.job["result"]["key"]
        original_list = self.storage.list_object_versions
        requests = []
        def versions(**request):
            requests.append(request)
            if "KeyMarker" not in request:
                return {"IsTruncated": True, "Versions": [{"Key": key, "VersionId": "another"}],
                        "NextKeyMarker": key, "NextVersionIdMarker": "another"}
            self.assertEqual(request["KeyMarker"], key)
            self.assertEqual(request["VersionIdMarker"], "another")
            return original_list(**request)
        self.storage.list_object_versions = versions
        self.storage.objects[key + ".other"] = {"Body": b"retained other artifact"}
        self.assertTrue(self.results.collect(self.job))
        self.assertEqual(self.storage.objects, {key + ".other": {"Body": b"retained other artifact"}})
        self.assertGreaterEqual(len(requests), 4)
        self.assertTrue(all(request["Prefix"] == key and request["ExpectedBucketOwner"] == "123456789012"
                            for request in requests))

    def test_reserved_unknown_result_requires_original_task_stop_and_expiry_without_promoting_success(self):
        unknown = {name: value for name, value in self.job.items() if name != "result"}
        unknown.update(status="UNKNOWN", result_terminal=REAPED,
            result_intent={**{name: self.job["result"][name] for name in
                             ["bucket", "key", "sha256", "bytes"]}, "expires_unix": self.now + 600})
        self.now += 601
        self.assertFalse(self.results.collect(unknown))
        self.assertIn(self.results.key(unknown), self.storage.objects)
        stopped = {**unknown, "task_stop": {"last_status": "STOPPED"}}
        self.assertTrue(self.results.collect(stopped))
        self.assertEqual(stopped["status"], "UNKNOWN")
        self.assertNotIn("result", stopped)
        self.assertEqual(self.storage.objects, {})


if __name__ == "__main__":
    unittest.main()
