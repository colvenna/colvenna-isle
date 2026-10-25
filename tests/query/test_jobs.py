"""Concurrent CAS clients exercise admission ambiguity and execution custody.

The storage double provides atomic conditional transactions, not an AWS claim.
Real S3/DynamoDB/service acceptance is separate.
"""

import concurrent.futures
import copy
import io
import json
import sys
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path

from botocore.exceptions import ClientError, ReadTimeoutError

from colvenna_isle.control.registry import ControlError, Principal
from colvenna_isle.query.jobs import Jobs, SPACE_PENDING
from colvenna_isle.query.process import run

SOURCE, IMAGE = "a" * 40, "sha256:" + "b" * 64
EXECUTOR = {"source_commit": SOURCE, "image_digest": IMAGE,
            "task_arn": "arn:aws:ecs:us-west-2:123456789012:task/query/original",
            "task_definition": "arn:aws:ecs:us-west-2:123456789012:task-definition/query:1",
            "instance_nonce": "original"}
PROCESS = {"pid": 123, "start_ticks": "456", "boot_id": "original",
           "command_sha256": "c" * 64}
REAPED = {**PROCESS, "state": "REAPED", "outcome": "SUCCEEDED", "exit_code": 0}


class Storage:
    def __init__(self):
        self.items, self.objects, self.lock = {}, {}, threading.RLock()
        self.lose_put_reply, self.lose_transaction_reply = False, False

    @staticmethod
    def key(key):
        return key["PK"]["S"], key["SK"]["S"]

    def space(self, name, generation="gen_fixture"):
        self.items["SPACE#" + name, "META"] = {
            "PK": {"S": "SPACE#" + name}, "SK": {"S": "META"},
            "generation": {"S": generation}, "revision": {"N": "1"}}

    def get_item(self, **request):
        with self.lock:
            item = self.items.get(self.key(request["Key"]))
            return {"Item": copy.deepcopy(item)} if item else {}

    def query(self, **request):
        with self.lock:
            values = request["ExpressionAttributeValues"]
            partition = values[":pk"]["S"]
            predicate = lambda key: key[1].startswith(values[":prefix"]["S"]) if ":prefix" in values \
                else key[1] <= values[":until"]["S"]
            keys = sorted([key for key in self.items if key[0] == partition and predicate(key)],
                          reverse=not request.get("ScanIndexForward", True))
            return {"Items": [copy.deepcopy(self.items[key]) for key in keys[:request["Limit"]]]}

    def transact_write_items(self, **request):
        with self.lock:
            writes = []
            for operation in request["TransactItems"]:
                name, parameters = next(iter(operation.items()))
                item = parameters.get("Item", parameters.get("Key"))
                key = self.key(item)
                old = self.items.get(key)
                condition = parameters["ConditionExpression"]
                values = parameters.get("ExpressionAttributeValues", {})
                valid = True
                if condition == "attribute_not_exists(PK)":
                    valid = old is None
                elif condition == "#revision=:revision":
                    valid = old is not None and old["revision"] == values[":revision"]
                elif condition == "generation=:generation AND #revision=:revision":
                    valid = old is not None and old["revision"] == values[":revision"] \
                        and old["generation"] == values[":generation"]
                else:
                    raise AssertionError("Unsupported conditional transaction: " + condition)
                if not valid:
                    raise ClientError({"Error": {"Code": "TransactionCanceledException"}}, "TransactWriteItems")
                if name == "Put":
                    writes.append((key, copy.deepcopy(item)))
                elif name == "Delete":
                    writes.append((key, None))
                else:
                    assert name == "ConditionCheck"
            for key, item in writes:
                if item is None:
                    self.items.pop(key, None)
                else:
                    self.items[key] = item
            if self.lose_transaction_reply:
                self.lose_transaction_reply = False
                raise ReadTimeoutError(endpoint_url="original-transaction-reply-lost")

    def put_object(self, **request):
        with self.lock:
            if request["Key"] in self.objects:
                raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
            assert request["IfNoneMatch"] == "*"
            self.objects[request["Key"]] = copy.deepcopy(request)
        if self.lose_put_reply:
            raise ReadTimeoutError(endpoint_url="original-intent-reply-lost")
        return {"VersionId": "original-version"}

    def get_object(self, **request):
        with self.lock:
            if request["Key"] not in self.objects:
                raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
            assert request.get("VersionId", "original-version") == "original-version"
            value = self.objects[request["Key"]]
            return {"Body": io.BytesIO(value["Body"]), "Metadata": value["Metadata"],
                    "VersionId": "original-version"}

    def delete_object(self, **request):
        with self.lock:
            assert request["VersionId"] == "original-version"
            self.objects.pop(request["Key"], None)
        return {}

    def list_object_versions(self, **request):
        with self.lock:
            return {"IsTruncated": False, "Versions": [
                {"Key": key, "VersionId": "original-version"} for key in self.objects
                if key.startswith(request["Prefix"])]}


class SharedJobs(unittest.TestCase):
    def setUp(self):
        self.storage = Storage()
        self.storage.space("fixture")
        self.jobs = Jobs(self.storage, "table", self.storage, "bucket", "123456789012", SOURCE, IMAGE)
        self.actor = self.principal("owner")
        self.configuration = {"space_id": "fixture", "generation": "gen_fixture", "revision": 1}
        self.request = {"sql": "SELECT 'private original SQL'", "client_request_id": "one"}

    def principal(self, owner, space="fixture", admin=False):
        return Principal(owner, frozenset([space]),
            frozenset(["query:read"] + (["query:manage"] if admin else [])),
            "key_" + owner, "gen_fixture")

    def admit(self, nonce="one", space="fixture", actor=None):
        config = {**self.configuration, "space_id": space}
        return self.jobs.admit(actor or self.principal("owner", space), config,
                               {**self.request, "client_request_id": nonce}, nonce)

    def claimed(self):
        queued = self.admit()
        running = self.jobs.claim(EXECUTOR)
        self.assertEqual(running["query_id"], queued["query_id"])
        return self.jobs.attach_process(running, PROCESS)

    def result(self, job):
        return {"bucket": "bucket", "key": f"spaces/{job['space_id']}/query-results/{job['generation']}/"
                f"{job['query_id']}/{job['execution_id']}/manifest.json",
                "version_id": "original", "sha256": "e" * 64, "bytes": 100}

    def reserve(self, job, result=None):
        result = result or self.result(job)
        intent = {name: result[name] for name in ["bucket", "key", "sha256", "bytes"]}
        intent["expires_unix"] = self.jobs.clock() + 600
        return self.jobs.reserve_result(job, REAPED, intent)

    def test_lost_intent_and_transaction_replies_keep_one_original_job(self):
        self.storage.lose_put_reply = self.storage.lose_transaction_reply = True
        first = self.admit()
        second = self.admit()
        self.assertEqual(first, second)
        self.assertEqual(len(self.storage.objects), 1)
        self.assertEqual(len(self.jobs._queue()["pending"]), 1)
        self.assertEqual(self.jobs.input(first)["request"], self.request)
        self.assertNotIn("private original SQL", json.dumps(list(self.storage.items.values())))

    def test_changed_same_client_request_cannot_replace_original(self):
        original = self.admit()
        with self.assertRaises(ControlError) as failure:
            self.jobs.admit(self.actor, self.configuration,
                           {"sql": "SELECT 2", "client_request_id": "one"}, "one")
        self.assertEqual(failure.exception.code, "QUERY_BINDING_CONFLICT")
        self.assertEqual(self.jobs.input(original)["request"], self.request)

    def test_concurrent_duplicate_admission_and_claim_start_only_once(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            jobs = list(pool.map(lambda _: self.admit(), range(4)))
            claims = list(pool.map(lambda _: self.jobs.claim(EXECUTOR), range(4)))
        self.assertEqual(len({job["query_id"] for job in jobs}), 1)
        self.assertEqual(sum(claim is not None for claim in claims), 1)
        self.assertEqual(len(self.jobs._queue()["running"]), 1)

    def test_space_quota_and_running_space_do_not_block_another_space(self):
        for index in range(SPACE_PENDING):
            self.admit("one_" + str(index))
        with self.assertRaises(ControlError) as quota:
            self.admit("over")
        self.assertEqual(quota.exception.code, "QUERY_QUEUE_QUOTA")
        self.storage.space("other")
        other = self.admit(space="other")
        first, second = self.jobs.claim(EXECUTOR), self.jobs.claim(EXECUTOR)
        self.assertEqual(first["space_id"], "fixture")
        self.assertEqual(second["query_id"], other["query_id"])
        self.assertIsNone(self.jobs.claim(EXECUTOR))

    def test_queued_cancel_is_repeatable_and_never_creates_an_execution(self):
        original = self.admit()
        cancelled = self.jobs.cancel(self.actor, "fixture", "gen_fixture", original["query_id"])
        self.assertEqual(cancelled["status"], "CANCELLED")
        self.assertNotIn("execution_id", cancelled)
        self.assertEqual(self.jobs.cancel(self.actor, "fixture", "gen_fixture",
                                         original["query_id"]), cancelled)
        self.assertIsNone(self.jobs.claim(EXECUTOR))

    def test_running_cancel_keeps_capacity_until_the_same_process_is_reaped(self):
        running = self.claimed()
        next_job = self.admit("next")
        cancelling = self.jobs.cancel(self.actor, "fixture", "gen_fixture", running["query_id"])
        self.assertEqual(cancelling["status"], "CANCELLING")
        self.assertIsNone(self.jobs.claim(EXECUTOR))
        for wrong in [{**REAPED, "state": "RUNNING"}, {**REAPED, "pid": 999},
                      {**REAPED, "start_ticks": "wrong"}]:
            with self.assertRaises(ControlError):
                self.jobs.finish(running, wrong, result={"key": "private"})
        completed = self.jobs.finish(running, REAPED, result={"key": "private"})
        self.assertEqual(completed["status"], "CANCELLED")
        self.assertNotIn("result", completed)
        self.assertEqual(self.jobs.claim(EXECUTOR)["query_id"], next_job["query_id"])

    def test_success_wins_before_cancel_and_repeated_finish_keeps_original(self):
        running = self.reserve(self.claimed())
        completed = self.jobs.finish(running, REAPED, result=self.result(running),
                                     expires_unix=running["result_intent"]["expires_unix"])
        self.assertEqual(completed["status"], "SUCCEEDED")
        self.assertEqual(self.jobs.cancel(self.actor, "fixture", "gen_fixture",
                                         running["query_id"]), completed)
        self.assertEqual(self.jobs.finish(running, REAPED, result={"key": "replacement"}), completed)

    def test_unowned_job_is_private_and_admin_metadata_does_not_grant_result_access(self):
        running = self.claimed()
        other, admin = self.principal("other"), self.principal("administrator", admin=True)
        for actor in [other, self.principal("owner", space="another")]:
            with self.assertRaises(ControlError):
                self.jobs.get(actor, "fixture", "gen_fixture", running["query_id"])
            with self.assertRaises(ControlError):
                self.jobs.cancel(actor, "fixture", "gen_fixture", running["query_id"])
        self.assertEqual(self.jobs.get(admin, "fixture", "gen_fixture", running["query_id"]), running)
        with self.assertRaises(ControlError):
            self.jobs.get(admin, "fixture", "gen_fixture", running["query_id"], data=True)
        self.assertEqual(self.jobs.cancel(admin, "fixture", "gen_fixture",
                                          running["query_id"])["status"], "CANCELLING")

    def test_missing_queue_after_initialization_cannot_reset_execution_custody(self):
        running = self.claimed()
        del self.storage.items["QUERY#QUEUE", "META"]
        with self.assertRaises(ControlError) as lost:
            self.admit("another")
        self.assertEqual(lost.exception.code, "QUERY_QUEUE_UNKNOWN")
        self.assertEqual(self.jobs.get(self.actor, "fixture", "gen_fixture",
                                      running["query_id"])["status"], "RUNNING")

    def test_old_source_cannot_be_claimed_by_new_executor_or_replaced_as_new(self):
        original = self.admit()
        upgraded = Jobs(self.storage, "table", self.storage, "bucket", "123456789012",
                        "d" * 40, "sha256:" + "e" * 64)
        executor = {**EXECUTOR, "source_commit": upgraded.source, "image_digest": upgraded.image}
        self.assertIsNone(upgraded.claim(executor))
        self.assertEqual(upgraded.admit(self.actor, self.configuration, self.request, "one"), original)
        self.assertEqual(len(self.storage.objects), 1)

    def test_real_running_child_cancel_is_bound_to_job_and_reaped_before_capacity_release(self):
        self.admit()
        job = self.jobs.claim(EXECUTOR)
        with tempfile.TemporaryDirectory() as directory:
            ready = threading.Event()

            def started(identity):
                self.jobs.attach_process(job, identity)
                ready.set()

            def cancelling():
                value = self.jobs.get(self.actor, "fixture", "gen_fixture", job["query_id"])
                return value["status"] == "CANCELLING"

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                execution = pool.submit(run, [sys.executable, "-c", "import time; time.sleep(60)"],
                    Path(directory) / "original", timeout=5, on_started=started, cancel=cancelling)
                self.assertTrue(ready.wait(timeout=3))
                self.jobs.cancel(self.actor, "fixture", "gen_fixture", job["query_id"])
                self.assertEqual(len(self.jobs._queue()["running"]), 1)
                terminal = execution.result(timeout=3)
            self.assertEqual(terminal["state"], "REAPED")
            self.assertEqual(terminal["outcome"], "CANCELLED")
            self.assertFalse(Path("/proc/" + str(terminal["pid"])).exists())
            self.assertEqual(self.jobs.finish(job, terminal)["status"], "CANCELLED")
            self.assertEqual(self.jobs._queue()["running"], [])

    def test_cancel_before_child_start_releases_reservation_with_positive_not_started_proof(self):
        self.admit()
        running = self.jobs.claim(EXECUTOR)
        self.jobs.cancel(self.actor, "fixture", "gen_fixture", running["query_id"])
        with tempfile.TemporaryDirectory() as directory:
            terminal = run([sys.executable, "-c", "raise SystemExit(99)"], Path(directory) / "original",
                           timeout=5, cancel=lambda: True)
        self.assertEqual(self.jobs.finish(running, terminal)["status"], "CANCELLED")
        self.assertEqual(self.jobs._queue()["running"], [])

    def test_concurrent_completion_and_cancel_choose_one_terminal_and_keep_it(self):
        running = self.reserve(self.claimed())
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            complete = pool.submit(self.jobs.finish, running, REAPED, self.result(running),
                                   expires_unix=running["result_intent"]["expires_unix"])
            cancel = pool.submit(self.jobs.cancel, self.actor, "fixture", "gen_fixture", running["query_id"])
            first, second = complete.result(), cancel.result()
        final = self.jobs.get(self.actor, "fixture", "gen_fixture", running["query_id"])
        self.assertIn(final["status"], {"SUCCEEDED", "CANCELLED"})
        self.assertEqual(first["status"], final["status"])
        self.assertIn(second["status"], {final["status"], "CANCELLING"})
        self.assertEqual("result" in final, final["status"] == "SUCCEEDED")
        self.assertEqual(self.jobs._queue()["running"], [])

    def test_result_publication_cannot_store_rows_or_reference_another_execution(self):
        running = self.claimed()
        for result in [{**self.result(running), "rows": [["private"]]},
                       {**self.result(running), "key": "spaces/other/private"},
                       {**self.result(running), "version_id": "null"}, None]:
            with self.assertRaises(ControlError) as denied:
                self.jobs.finish(running, REAPED, result)
            self.assertEqual(denied.exception.code, "QUERY_RESULT_UNCONFIRMED")
        self.assertEqual(len(self.jobs._queue()["running"]), 1)

    def test_lost_claim_and_birth_attachment_replies_reconcile_same_execution(self):
        self.admit()
        self.storage.lose_transaction_reply = True
        original = self.jobs.claim(EXECUTOR)
        self.storage.lose_transaction_reply = True
        attached = self.jobs.attach_process(original, PROCESS)
        self.assertEqual(attached["execution_id"], original["execution_id"])
        self.assertEqual(attached["process"], PROCESS)
        self.assertIsNone(self.jobs.claim(EXECUTOR))

    def test_stopped_original_task_releases_capacity_but_preserves_unknown_without_retry(self):
        running = self.reserve(self.claimed())
        next_job = self.admit("next")
        task = {"taskArn": EXECUTOR["task_arn"], "taskDefinitionArn": EXECUTOR["task_definition"],
                "lastStatus": "STOPPED", "stoppedAt": datetime.now(timezone.utc),
                "containers": [{"name": "baseline", "lastStatus": "STOPPED", "imageDigest": IMAGE}]}
        for wrong in [{**task, "lastStatus": "RUNNING"}, {**task, "taskArn": "replacement"},
                      {**task, "taskDefinitionArn": "replacement"},
                      {**task, "containers": [{"name": "baseline", "lastStatus": "STOPPED",
                                               "imageDigest": "sha256:" + "e" * 64}]}]:
            with self.assertRaises(ControlError):
                self.jobs.reconcile_stopped(running, wrong)
            self.assertIsNone(self.jobs.claim(EXECUTOR))
        reconciled = self.jobs.reconcile_stopped(running, task)
        self.assertEqual(reconciled["status"], "UNKNOWN")
        self.assertNotIn("terminal", reconciled)
        self.assertNotIn("result", reconciled)
        self.assertEqual(reconciled["result_intent"], running["result_intent"])
        self.assertEqual(len(self.jobs._queue()["retained"]), 1)
        self.assertEqual(self.jobs.reconcile_stopped(running, task), reconciled)
        self.assertEqual(self.admit(), reconciled)
        self.assertEqual(self.jobs.claim(EXECUTOR)["query_id"], next_job["query_id"])

    def test_history_gc_retains_active_originals_then_removes_exact_expired_sql_without_replay(self):
        now = [100.0]
        self.jobs.clock = lambda: now[0]
        original = self.admit()
        self.assertEqual(self.jobs.history(self.actor, "fixture", "gen_fixture"), [original])
        now[0] += 86401
        self.assertEqual(self.jobs.expired_history(), [])
        self.assertEqual(self.jobs.input(original)["request"], self.request)
        cancelled = self.jobs.cancel(self.actor, "fixture", "gen_fixture", original["query_id"])
        with self.assertRaises(ControlError):
            self.jobs.collect_history(cancelled)
        now[0] += 86401
        self.assertEqual(self.jobs.history(self.actor, "fixture", "gen_fixture"), [])
        with self.assertRaises(ControlError) as expired:
            self.jobs.input(cancelled)
        self.assertEqual(expired.exception.code, "QUERY_HISTORY_EXPIRED")
        self.assertEqual(self.jobs.expired_history()[0]["query_id"], original["query_id"])
        self.storage.lose_transaction_reply = True
        with self.assertRaises(ControlError):
            self.jobs.collect_history(cancelled)
        self.jobs.collect_history(cancelled)
        self.assertEqual(self.jobs.expired_history(), [])
        self.assertEqual(self.storage.objects, {})
        self.assertEqual(self.admit()["query_id"], original["query_id"])
        self.assertIsNone(self.jobs.claim(EXECUTOR))

    def test_history_gc_confirms_deleted_version_without_missing_object_get_permission(self):
        now = [100.0]
        self.jobs.clock = lambda: now[0]
        original = self.admit()
        cancelled = self.jobs.cancel(self.actor, "fixture", "gen_fixture", original["query_id"])
        now[0] += 86401
        original_get = self.storage.get_object
        def get(**request):
            if request["Key"] not in self.storage.objects:
                raise ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject")
            return original_get(**request)
        self.storage.get_object = get
        self.jobs.collect_history(cancelled)
        self.assertEqual(self.jobs.expired_history(), [])
        self.assertEqual(self.storage.objects, {})
        self.assertEqual(self.admit()["query_id"], original["query_id"])
        self.assertIsNone(self.jobs.claim(EXECUTOR))

    def test_result_retention_quota_has_no_success_and_reaped_failure_releases_execution(self):
        for nonce in ["one", "two"]:
            self.admit(nonce)
            job = self.jobs.attach_process(self.jobs.claim(EXECUTOR), PROCESS)
            reference = {**self.result(job), "bytes": 14680064}
            reserved = self.reserve(job, reference)
            self.jobs.finish(reserved, REAPED, reference,
                             expires_unix=reserved["result_intent"]["expires_unix"])
        self.admit("three")
        job = self.jobs.attach_process(self.jobs.claim(EXECUTOR), PROCESS)
        with self.assertRaises(ControlError) as quota:
            self.reserve(job, {**self.result(job), "bytes": 14680064})
        self.assertEqual(quota.exception.code, "QUERY_RESULT_RETENTION_QUOTA")
        self.assertEqual(len(self.jobs._queue()["running"]), 1)
        failed = self.jobs.finish(job, REAPED, failure_code=quota.exception.code)
        self.assertEqual(failed["status"], "FAILED")
        self.assertNotIn("result", failed)
        self.assertEqual(self.jobs._queue()["running"], [])
        self.assertEqual(len(self.jobs._queue()["retained"]), 2)

    def test_lost_artifact_reservation_reply_reconciles_one_original_bounded_charge(self):
        job = self.claimed()
        self.storage.lose_transaction_reply = True
        reserved = self.reserve(job)
        self.assertEqual(self.jobs.reserve_result(job, REAPED, reserved["result_intent"]), reserved)
        self.assertEqual(len(self.jobs._queue()["retained"]), 1)
        self.assertFalse(any("/query-results/" in key for key in self.storage.objects))
        with self.assertRaises(ControlError) as changed:
            self.jobs.reserve_result(job, REAPED, {**reserved["result_intent"], "bytes": 101})
        self.assertEqual(changed.exception.code, "QUERY_RESULT_BINDING_CONFLICT")
        with self.assertRaises(ControlError):
            self.jobs.finish(reserved, REAPED, {**self.result(job), "sha256": "f" * 64},
                             expires_unix=reserved["result_intent"]["expires_unix"])

    def test_expired_unconfirmed_artifacts_hold_quota_while_other_space_can_complete(self):
        now = [100.0]
        self.jobs.clock = lambda: now[0]
        for nonce in ["one", "two"]:
            self.admit(nonce)
            job = self.jobs.attach_process(self.jobs.claim(EXECUTOR), PROCESS)
            reserved = self.reserve(job, {**self.result(job), "bytes": 14680064})
            self.jobs.finish(reserved, REAPED, failure_code="QUERY_RESULT_UNCONFIRMED")
        now[0] += 601
        self.admit("three")
        job = self.jobs.attach_process(self.jobs.claim(EXECUTOR), PROCESS)
        with self.assertRaises(ControlError) as quota:
            self.reserve(job, {**self.result(job), "bytes": 14680064})
        self.assertEqual(quota.exception.code, "QUERY_RESULT_RETENTION_QUOTA")
        self.jobs.finish(job, REAPED, failure_code=quota.exception.code)
        self.storage.space("other")
        self.admit("elsewhere", space="other")
        other = self.jobs.attach_process(self.jobs.claim(EXECUTOR), PROCESS)
        reference = {**self.result(other), "bytes": 14680064}
        reserved = self.reserve(other, reference)
        finished = self.jobs.finish(reserved, REAPED, reference,
                                    expires_unix=reserved["result_intent"]["expires_unix"])
        self.assertEqual(finished["status"], "SUCCEEDED")
        self.assertEqual(len(self.jobs._queue()["retained"]), 3)

    def test_global_artifact_quota_is_shared_across_spaces_before_upload(self):
        self.storage.space("other")
        self.storage.space("third")
        for space in ["fixture", "other"]:
            for nonce in ["one", "two"]:
                self.admit(nonce, space=space)
                job = self.jobs.attach_process(self.jobs.claim(EXECUTOR), PROCESS)
                reference = {**self.result(job), "bytes": 16777216}
                reserved = self.reserve(job, reference)
                self.jobs.finish(reserved, REAPED, reference,
                                 expires_unix=reserved["result_intent"]["expires_unix"])
        self.admit("over-global", space="third")
        job = self.jobs.attach_process(self.jobs.claim(EXECUTOR), PROCESS)
        with self.assertRaises(ControlError) as quota:
            self.reserve(job)
        self.assertEqual(quota.exception.code, "QUERY_RESULT_RETENTION_QUOTA")
        self.assertNotIn("result_intent", self.jobs._get(self.jobs.key("third", "gen_fixture", job["query_id"])))
        self.assertFalse(any("/query-results/" in key for key in self.storage.objects))


if __name__ == "__main__":
    unittest.main()
