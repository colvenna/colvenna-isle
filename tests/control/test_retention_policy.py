"""Retention: reference tracking, exact-version collection, receipts, holds and leases."""

import copy
import hashlib
import json
import unittest
from datetime import datetime, timezone

from botocore.exceptions import ClientError

from colvenna_isle.control.audit import Audit
from colvenna_isle.control.quotas import Quotas
from colvenna_isle.control.registry import ControlError, Principal, canonical
from colvenna_isle.control.retention import Collector, Retention, Sweep, effective
from test_governance_runs import Fixture, query

DAY = 86400


class Bucket:
    """A versioned bucket: exact-version delete, listing, conditional create."""

    def __init__(self, clock):
        self.clock, self.versions, self.counter, self.deleted = clock, {}, 0, []
        self.deny_after = None

    def add(self, key, size=100, age=0):
        self.counter += 1
        version = "v" + str(self.counter)
        self.versions.setdefault(key, []).append({"Key": key, "VersionId": version, "Size": size,
                                                  "LastModified": datetime.fromtimestamp(self.clock() - age,
                                                                                         timezone.utc)})
        return version

    def list_object_versions(self, Bucket, Prefix, ExpectedBucketOwner, **kwargs):
        values = [copy.deepcopy(value) for key in sorted(self.versions) if key.startswith(Prefix)
                  for value in self.versions[key]]
        return {"Versions": values, "IsTruncated": False}

    def delete_object(self, Bucket, Key, VersionId, ExpectedBucketOwner):
        assert "/warehouse/" not in Key
        if self.deny_after is not None and len(self.deleted) >= self.deny_after:
            raise ClientError({"Error": {"Code": "AccessDenied"}}, "DeleteObject")
        self.versions[Key] = [value for value in self.versions.get(Key, []) if value["VersionId"] != VersionId]
        self.deleted.append((Key, VersionId))

    def head_object(self, Bucket, Key, VersionId, ExpectedBucketOwner):
        # Like S3 for a principal without s3:ListBucket (the collector): a missing version is 403.
        if not any(value["VersionId"] == VersionId for value in self.versions.get(Key, [])):
            raise ClientError({"Error": {"Code": "403"}}, "HeadObject")
        return {}

    def put_object(self, Bucket, Key, Body, IfNoneMatch, **kwargs):
        if self.versions.get(Key):
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.receipt = json.loads(Body)
        return {"VersionId": self.add(Key, len(Body))}


class NativeRetention:
    """Writer retention commands: COLLECTING before deletion, release drops with evidence."""

    def __init__(self, now):
        self.now, self.calls = now, []
        self.exports = {
            "input_expired": {"state": "READY", "eligible": True, "reason": "RETENTION_EXPIRED"},
            "input_pinned": {"state": "READY", "eligible": False, "reason": None},
        }
        self.releases = [
            {"release_id": "release_%d" % number, "dataset": "dwd.orders", "status": "PUBLISHED",
             "commit_sequence": number, "published_unix": now - (10 - number) * DAY, "current": number == 9}
            for number in range(5, 10)]
        self.protected = ["release_6"]
        self.generations = [{"generation_id": "generation_revoked", "leftover": True, "reason": "REVOKED_CANDIDATE"},
                            {"generation_id": "generation_live", "leftover": False, "reason": None}]

    def call(self, principal, configuration, operation, **fields):
        self.calls.append((operation, copy.deepcopy(fields)))
        principal.authorize(configuration["space_id"], "retention:manage")
        if operation == "retention_candidates":
            return {"exports": [{"export_id": name, "retained_bytes": 64, "release_ids": [], **value}
                                for name, value in self.exports.items()
                                if name not in fields["protected_exports"] or not value["eligible"]]
                    + [{"export_id": name, "retained_bytes": 64, "release_ids": [], **value, "eligible": False}
                       for name, value in self.exports.items()
                       if name in fields["protected_exports"] and value["eligible"]],
                    "releases": copy.deepcopy(self.releases), "generations": copy.deepcopy(self.generations),
                    "protected_releases": self.protected}
        if operation == "retention_mark_exports":
            for name in fields["export_ids"]:
                assert self.exports[name]["eligible"] and name not in fields["protected_exports"]
                if self.exports[name]["state"] != "COLLECTING":
                    # Like the native ledger: an original already COLLECTING keeps its marking collection.
                    self.exports[name].update(state="COLLECTING", owner=fields["collection_id"])
            return {"marked": [{"export_id": name, "state": "COLLECTING", "reason": "RETENTION_EXPIRED",
                                "previous_state": "READY",
                                "prefix": f"spaces/space_one/inputs/{configuration['generation']}/{name}/"}
                               for name in fields["export_ids"]]}
        if operation == "retention_finish_exports":
            for value in fields["results"]:
                export = self.exports[value["export_id"]]
                assert export["state"] == "COLLECTING"
                if export["owner"] != fields["collection_id"]:
                    raise ControlError(409, "INPUT_EXPORT_COLLECTION_CONFLICT")
                export.update(state="COLLECTED", eligible=False, evidence=value["evidence"])
            return {"finished": []}
        if operation == "governance_read" and fields["kind"] == "input_export":
            export = self.exports[fields["id"]]
            return {"export_id": fields["id"], "state": export["state"],
                    "collection": {"collection_id": export.get("owner")}}
        if operation == "retention_collect_native":
            for release in fields["release_ids"]:
                assert release not in self.protected and release in fields["protected_releases"] is False or \
                    release not in fields["protected_releases"]
            return {"collected": [{"kind": "release", "identity": name, "tables": ["_gen_" + name]}
                                  for name in fields["release_ids"]]
                    + [{"kind": "generation", "identity": name, "tables": ["_gen_" + name]}
                       for name in fields["generation_ids"]]}
        raise AssertionError(operation)


class Ledger:
    def __init__(self):
        self.status = {}

    def read(self, configuration, identity):
        return {"status": self.status.get(identity, "COMMITTED")}


class Quiet:
    def __init__(self):
        self.pending = 0

    def observe(self, configuration):
        return {"pending_messages": self.pending, "dlq_messages": 0}


class RetentionTests(unittest.TestCase):
    def setUp(self):
        self.f = Fixture()
        self.f.setUp()
        self.f.table.query = query(self.f.table)
        self.f.table.put_item, self.f.table.delete_item = self.conditional(self.f.table)
        self.manager = Principal("manager", frozenset(["space_one"]),
                                 frozenset(["retention:manage", "monitor:read"]), "key_manager", self.f.generation)
        self.auditor = Principal("auditor", frozenset(["space_one"]), frozenset(["audit:read"]), "key_auditor",
                                 self.f.generation)
        self.f.keys.principals.update(key_manager=self.manager, key_auditor=self.auditor)
        self.clock = lambda: self.f.now
        self.retention = Retention(self.f.registry, self.f.keys, self.f.runs, clock=self.clock)
        self.bucket = Bucket(self.clock)
        self.native, self.ledger, self.queue = NativeRetention(self.f.now), Ledger(), Quiet()
        self.collector = Collector(self.retention, self.native, self.bucket, "test-bucket", self.queue, self.ledger,
                                   Quotas(self.f.table, "control"), clock=self.clock)
        self.generation = self.f.generation

    @staticmethod
    def conditional(table):
        """Lease conditions over the fixture's items (put-if-absent-or-expired, delete-if-holder)."""
        def failed(operation):
            return ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, operation)

        def put_item(**request):
            key, values = table.key(request["Item"]), request.get("ExpressionAttributeValues", {})
            with table.lock:
                previous = table.items.get(key)
                condition = request.get("ConditionExpression")
                if condition == "attribute_not_exists(PK) OR expires_unix < :now":
                    if previous and float(previous["expires_unix"]["N"]) >= float(values[":now"]["N"]):
                        raise failed("PutItem")
                elif condition == "attribute_not_exists(PK)" and previous:
                    raise failed("PutItem")
                table.items[key] = copy.deepcopy(request["Item"])

        def delete_item(**request):
            key, values = table.key(request["Key"]), request.get("ExpressionAttributeValues", {})
            with table.lock:
                previous = table.items.get(key)
                if request.get("ConditionExpression") == "holder = :holder" and (
                        not previous or previous["holder"] != values[":holder"]):
                    raise failed("DeleteItem")
                table.items.pop(key, None)
        return put_item, delete_item

    def collect(self, dry_run=False, nonce="collect-1"):
        receipt = self.retention.request(self.manager, "space_one", {"dry_run": dry_run}, nonce)
        result = self.collector.tick(float("inf"))
        report = self.retention.collection(self.manager, "space_one", receipt["collection_id"])
        return result, report

    def seed_runs(self):
        self.f.writer.ingest(1)
        finished = self.f.drive(self.f.start_run("finished")["run_id"])
        self.f.compute.silent.add(("summary", "attempt_1"))
        self.f.writer.ingest(1)
        active = self.f.drive(self.f.start_run("active")["run_id"], rounds=8)
        self.assertEqual((finished["status"], active["status"]), ("PUBLISHED", "RUNNING"))
        for record, age in [(finished, 0), (active, 0)]:
            job = record["tasks"]["daily"]
            prefix = (f"spaces/space_one/staging/{self.generation}/orders/{record['run_id']}/{job['job_id']}/"
                      "attempt_1/exec_1/")
            for name in ["result/part-0.parquet", "state/part-0.parquet", "completion.json", "output-manifest.json"]:
                self.bucket.add(prefix + name, 500, age)
        return finished, active

    def test_policy_minimums_apply_to_product_spaces_only(self):
        with self.assertRaisesRegex(ControlError, "RETENTION_POLICY_BELOW_MINIMUM"):
            effective("space_one", {"raw_seconds": 3600})
        self.assertEqual(effective("probe_x", {"raw_seconds": 60})["raw_seconds"], 60)
        status = self.retention.status(self.manager, "space_one")
        with self.assertRaisesRegex(ControlError, "RETENTION_REVISION_CHANGED"):
            self.retention.update(self.manager, "space_one", {"state": "ENABLED", "policy": {}}, '"ret_7"')
        updated = self.retention.update(self.manager, "space_one", {"state": "ENABLED", "policy": {
            "staging_seconds": 7200}}, '"' + status["revision"] + '"')
        self.assertEqual((updated["state"], updated["policy"]["staging_seconds"]), ("ENABLED", 7200))
        with self.assertRaisesRegex(ControlError, "FORBIDDEN"):
            self.retention.update(self.f.monitor, "space_one", {"state": "DISABLED", "policy": {}},
                                  '"' + updated["revision"] + '"')

    def test_collection_keeps_bound_artifacts_and_collects_only_unreferenced_versions(self):
        finished, active = self.seed_runs()
        # An orphan upload that no draft or version references, and an unused older version.
        orphan = self.f.pipelines.upload(self.f.owner, "space_one", "orders",
                                         {"path": "sql/orphan.sql", "sha256": "e" * 64, "bytes": 3})
        orphan_key = self.f.objects.signed["Params"]["Key"]
        self.bucket.add(orphan_key, 3)
        unused = self.f.publish(self.f.dag(), sql="SELECT 77")
        unused_record = self.f.pipelines._read(self.f.pipelines._key(
            self.f.registry._read("space_one"), "PIPELINE_VERSION", "orders", unused))
        package = unused_record["package"]
        package_key = package["package_uri"].split("/", 3)[3]
        self.bucket.versions[package_key] = [{"Key": package_key, "VersionId": package["package_object_version"],
                                              "Size": package["package_bytes"],
                                              "LastModified": datetime.now(timezone.utc)}]
        # Raw originals: an expired committed one, one with a recent replay intent, one held, one uncommitted.
        raw = f"spaces/space_one/ingest/{self.generation}/ods.orders/"
        for name in ["ing_old", "ing_replayed", "ing_held", "ing_failed"]:
            self.bucket.add(raw + name + ".json", 200, age=60 * DAY)
        self.ledger.status["ing_failed"] = "FAILED"
        self.retention.place_hold(self.manager, "space_one", {
            "target": {"kind": "ingestion", "id": "ing_held"}, "reason": "audit",
            "expires_at": datetime.fromtimestamp(self.f.now + 200 * DAY, timezone.utc).isoformat().replace(
                "+00:00", "Z")})
        for name in ["input_expired", "input_pinned"]:
            self.bucket.add(f"spaces/space_one/inputs/{self.generation}/{name}/exec/a/part-00000.parquet", 64)
        # Native database objects are never listed or deleted.
        native_key = f"spaces/space_one/{self.generation}/warehouse/duckdb-object-storage/x.db/wal/1.sst"
        self.bucket.add(native_key, 999)
        self.f.now += 50 * DAY
        # A replay intent one day ago extends that original's retention by the replay grace.
        self.bucket.add(f"spaces/space_one/ingestion-replay/{self.generation}/ing_replayed/x.json", 10, age=DAY)
        _, report = self.collect()
        self.assertEqual(report["status"], "COMPLETED")
        deleted = {key for key, _ in self.bucket.deleted}
        staging = f"spaces/space_one/staging/{self.generation}/orders/"
        finished_prefix, active_prefix = staging + finished["run_id"] + "/", staging + active["run_id"] + "/"
        self.assertEqual({key.rsplit("/", 2)[-2] for key in deleted if key.startswith(finished_prefix)},
                         {"result", "state"})
        self.assertFalse(any(key.startswith(active_prefix) for key in deleted))
        self.assertTrue(self.bucket.versions[finished_prefix + next(
            key[len(finished_prefix):] for key in self.bucket.versions if key.startswith(finished_prefix)
            and key.endswith("completion.json"))])
        self.assertIn(orphan_key, deleted)
        self.assertIn(package_key, deleted)
        self.assertIn(raw + "ing_old.json", deleted)
        self.assertFalse({raw + name + ".json" for name in ["ing_replayed", "ing_held", "ing_failed"]} & deleted)
        self.assertNotIn(native_key, deleted)
        # The active run's version and its bound uploads stay usable.
        self.f.pipelines.bind_run(self.f.owner, "space_one", "orders", self.f.version)
        with self.assertRaisesRegex(ControlError, "PIPELINE_VERSION_COLLECTED"):
            self.f.pipelines.bind_run(self.f.owner, "space_one", "orders", unused)
        # Exports: native COLLECTING precedes deletion of exactly its objects.
        operations = [call[0] for call in self.native.calls]
        self.assertLess(operations.index("retention_mark_exports"), operations.index("retention_finish_exports"))
        self.assertEqual(self.native.exports["input_expired"]["state"], "COLLECTED")
        self.assertEqual(self.native.exports["input_pinned"]["state"], "READY")
        collected = next(call[1] for call in self.native.calls if call[0] == "retention_collect_native")
        # Current, the newest release_keep (3) and the protected release stay.
        self.assertEqual(collected["release_ids"], ["release_5"])
        reasons = {value["key"]: value["reason"] for value in report["objects"] if value["category"] == "releases"
                   and value["action"] == "RETAINED"}
        self.assertEqual(reasons, {"release_9": "CURRENT_RELEASE", "release_8": "WITHIN_RELEASE_KEEP",
                                   "release_7": "WITHIN_RELEASE_KEEP", "release_6": "PROTECTED_REFERENCE"})
        self.assertEqual(collected["generation_ids"], ["generation_revoked"])
        self.assertIn("release_6", collected["protected_releases"])
        # Evidence: an immutable receipt with every object action and an audit record.
        self.assertEqual(report["receipt"]["sha256"], hashlib.sha256(json.dumps(
            self.bucket.receipt, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest())
        self.assertEqual(report["summary"]["raw_archive"]["collected"], 1)
        actions = [record["action"] for record in Audit(self.f.table, "control").list(
            self.auditor, self.f.registry._read("space_one"), limit=200)["records"]]
        self.assertIn("RETENTION_COLLECTION_COMPLETED", actions)
        self.assertIn("RETENTION_HOLD_PLACED", actions)
        # A second pass finds nothing new to delete and never re-deletes collected originals.
        before = len(self.bucket.deleted)
        self.collect(nonce="collect-2")
        self.assertEqual(len(self.bucket.deleted), before)

    def test_dry_run_deletes_nothing_and_a_held_lease_waits(self):
        self.seed_runs()
        self.f.now += 50 * DAY
        _, report = self.collect(dry_run=True)
        self.assertEqual(self.bucket.deleted, [])
        self.assertIn("WOULD_COLLECT", {value["action"] for value in report["objects"]})
        self.assertNotIn("retention_mark_exports", [call[0] for call in self.native.calls])
        self.collector._lease("space_one", "other-collector")
        receipt = self.retention.request(self.manager, "space_one", {"dry_run": False}, "collect-concurrent")
        self.assertEqual(self.collector.tick(float("inf"))["collections"][0]["status"], "WAITING_FOR_LEASE")
        self.assertEqual(self.retention.collection(self.manager, "space_one", receipt["collection_id"])["status"],
                         "QUEUED")
        self.collector._lease("space_one", "other-collector", release=True)
        self.collector.tick(float("inf"))
        self.assertEqual(self.retention.collection(self.manager, "space_one", receipt["collection_id"])["status"],
                         "COMPLETED")

    def test_scope_guard_and_revoked_principal_fail_closed(self):
        sweep = Sweep(self.collector, self.manager, self.f.registry._read("space_one"), "col_x", False, float("inf"))
        with self.assertRaisesRegex(ControlError, "RETENTION_KEY_SCOPE"):
            sweep.delete("staging", f"spaces/space_one/{self.generation}/warehouse/x.sst", "v1", 1, "x")
        with self.assertRaisesRegex(ControlError, "RETENTION_KEY_SCOPE"):
            sweep.delete("staging", "spaces/other_space/staging/x", "v1", 1, "x")
        receipt = self.retention.request(self.manager, "space_one", {"dry_run": False}, "collect-revoked")
        self.f.keys.revoked.add("key_manager")
        self.collector.tick(float("inf"))
        self.f.keys.revoked.discard("key_manager")
        report = self.retention.collection(self.manager, "space_one", receipt["collection_id"])
        self.assertEqual((report["status"], report["error_code"]), ("FAILED", "FORBIDDEN"))
        self.assertEqual(self.bucket.deleted, [])


    def test_an_original_left_collecting_by_an_interrupted_pass_is_resumed_and_finished(self):
        self.seed_runs()
        self.f.now += 50 * DAY
        self.native.exports["input_interrupted"] = {"state": "COLLECTING", "eligible": True,
                                                    "reason": "RESUME_COLLECTION", "owner": "col_interrupted"}
        prefix = f"spaces/space_one/inputs/{self.generation}/input_interrupted/"
        self.bucket.add(prefix + "manifest.json", 200)
        _, report = self.collect(nonce="collect-resume")
        self.assertEqual(report["status"], "COMPLETED")
        interrupted = self.native.exports["input_interrupted"]
        self.assertEqual((interrupted["state"], interrupted["owner"]), ("COLLECTED", "col_interrupted"))
        self.assertEqual(self.native.exports["input_expired"]["state"], "COLLECTED")
        resumed = [value for value in self.bucket.receipt["objects"] if value["key"].startswith(prefix)]
        self.assertEqual([(value["action"], value["reason"]) for value in resumed],
                         [("COLLECTED", "RESUMED_FROM_col_interrupted")])
        finishes = [fields["collection_id"] for operation, fields in self.native.calls
                    if operation == "retention_finish_exports"]
        self.assertEqual(sorted(finishes), sorted(["col_interrupted", report["collection_id"]]))

    def test_failed_pass_names_the_failing_call_and_keeps_evidence_of_what_it_deleted(self):
        self.seed_runs()
        self.f.now += 50 * DAY
        self.bucket.deny_after = 1
        _, report = self.collect(nonce="collect-denied")
        self.assertEqual((report["status"], report["error_code"]), ("FAILED", "RETENTION_DEPENDENCY_UNAVAILABLE"))
        self.assertEqual(report["failure"], {"operation": "DeleteObject", "code": "AccessDenied"})
        self.assertEqual(len(self.bucket.deleted), 1)
        collected = [value for value in self.bucket.receipt["objects"] if value["action"] == "COLLECTED"]
        # Native collections are recorded too; every S3 version actually deleted is in the receipt.
        self.assertLessEqual(set(self.bucket.deleted), {(value["key"], value["version_id"]) for value in collected})
        self.assertEqual(self.bucket.receipt["failure"], report["failure"])
        self.assertEqual(report["receipt"]["sha256"], hashlib.sha256(canonical(self.bucket.receipt).encode())
                         .hexdigest())


def load_tests(loader, tests, pattern):
    return loader.loadTestsFromTestCase(RetentionTests)


if __name__ == "__main__":
    unittest.main()
