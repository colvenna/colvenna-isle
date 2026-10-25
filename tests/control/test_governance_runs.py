"""Run-controller governance: sample trials, space quotas, run history and audit records.

Uses the run-controller fixture's simulated writer ledger and compute; actual
Native, Lambda/ECS and DynamoDB behaviour is qualified separately on AWS.
"""

import copy
import json
import re
import unittest

from colvenna_isle.control.audit import Audit
from colvenna_isle.control.quotas import Quotas
from colvenna_isle.control.registry import ControlError, Principal
from colvenna_isle.control.runs import Runs
from colvenna_isle.pipeline.task_objects import TaskObjects
from test_runs import RunControllerTests


def query(table):
    """The key-condition/filter subset the control services use, over the fixture's items."""
    def run(**request):
        values = request.get("ExpressionAttributeValues", {})
        condition = request["KeyConditionExpression"].replace(" ", "")
        partition = values[":pk"]["S"]
        with table.lock:
            items = [copy.deepcopy(value) for key, value in sorted(table.items.items()) if key[0] == partition]
        if "begins_with(SK,:prefix)" in condition:
            items = [item for item in items if item["SK"]["S"].startswith(values[":prefix"]["S"])]
        if "SKBETWEEN:lowAND:high" in condition:
            items = [item for item in items if values[":low"]["S"] <= item["SK"]["S"] <= values[":high"]["S"]]
        if "SK<=:until" in condition:
            items = [item for item in items if item["SK"]["S"] <= values[":until"]["S"]]
        if request.get("ScanIndexForward") is False:
            items.reverse()
        if "ExclusiveStartKey" in request:
            start = request["ExclusiveStartKey"]["SK"]["S"]
            items = [item for item in items if (item["SK"]["S"] < start if request.get("ScanIndexForward") is False
                                                else item["SK"]["S"] > start)]
        limit = request.get("Limit")
        page, more = (items[:limit], len(items) > limit) if limit else (items, False)
        if request.get("FilterExpression") == "#action = :action":
            page = [item for item in page if item["action"] == values[":action"]]
        result = {"Items": page}
        if more:
            result["LastEvaluatedKey"] = {"PK": page[-1]["PK"], "SK": page[-1]["SK"]}
        return result
    return run


class Fixture(RunControllerTests):
    def runTest(self):
        pass


class GovernanceRunTests(unittest.TestCase):
    def setUp(self):
        self.f = Fixture()
        self.f.setUp()
        self.f.table.query = query(self.f.table)
        self.quotas = Quotas(self.f.table, "control")
        self.runs = Runs(self.f.pipelines, self.f.keys, self.f.writer, self.f.dispatcher,
                         lambda space, generation: TaskObjects(self.f.objects, "test-bucket", space, generation),
                         clock=lambda: self.f.now, quotas=self.quotas)
        self.f.runs = self.runs
        self.reader = Principal("reader", frozenset(["space_one"]),
                                frozenset(["pipeline:run", "query:read"]), "key_reader", self.f.generation)
        self.auditor = Principal("auditor", frozenset(["space_one"]), frozenset(["audit:read"]), "key_auditor",
                                 self.f.generation)
        self.f.keys.principals.update(key_reader=self.reader, key_auditor=self.auditor)

    def trial(self, principal=None, nonce="trial-1", task_id="clean", sample_rows=50):
        return self.runs.trial(principal or self.f.owner, "space_one", "orders",
                               {"pipeline_version": self.f.version, "task_id": task_id, "sample_rows": sample_rows},
                               nonce)

    def drive(self, run_id, rounds=20, principal=None):
        principal = principal or self.f.owner
        for _ in range(rounds):
            record = self.runs.advance(self.runs.read(principal, "space_one", "orders", run_id))
            if record["status"] in {"PUBLISHED", "FAILED", "CANCELLED", "VALIDATED"}:
                return record
        return self.runs.read(principal, "space_one", "orders", run_id)

    def audit(self, action=None):
        configuration = self.f.registry._read("space_one")
        return Audit(self.f.table, "control").list(self.auditor, configuration, limit=200, action=action)["records"]

    def test_trial_validates_without_grant_import_release_or_watermark_change(self):
        self.f.writer.ingest(4)
        receipt = self.trial()
        self.assertEqual(self.trial(), receipt)
        with self.assertRaisesRegex(ControlError, "IDEMPOTENCY_KEY_CONFLICT"):
            self.trial(sample_rows=51)
        record = self.drive(receipt["run_id"])
        self.assertEqual(record["status"], "VALIDATED")
        self.assertEqual(list(record["tasks"]), ["clean"])
        job = record["tasks"]["clean"]
        self.assertEqual((job["status"], job["epoch"], job["attempts"][-1]["status"]), ("VALIDATED", 0, "VALIDATED"))
        operations = [call[0] for call in self.f.writer.calls]
        self.assertNotIn("publication_grant", operations)
        self.assertNotIn("publication_import", operations)
        self.assertEqual((self.f.writer.current, self.f.writer.tasks, self.f.writer.jobs), ({}, {}, {}))
        self.assertEqual(self.f.writer.samples[job["job_id"]], 50)
        status = self.runs.get(self.f.owner, "space_one", "orders", receipt["run_id"])
        self.assertEqual((status["mode"], status["trial"]["publication"], status["trial"]["validation"]["status"]),
                         ("trial", "NOT_PUBLISHED", "PASSED"))
        # Its fixed input pin is released like any terminal job.
        self.assertEqual(self.f.writer.exports[job["export_id"]]["references"][job["job_id"]], "RELEASED")
        with self.assertRaisesRegex(ControlError, "RUN_NOT_RETRYABLE"):
            self.runs.retry(self.f.owner, "space_one", "orders", receipt["run_id"], "retry-trial")

    def test_trial_preview_requires_query_read_and_rejection_fails_the_trial(self):
        self.f.writer.ingest(2)
        run_id = self.trial(self.reader)["run_id"]
        self.assertEqual(self.drive(run_id, principal=self.reader)["status"], "VALIDATED")
        preview = self.runs.preview(self.reader, "space_one", "orders", run_id, 2)
        self.assertEqual((preview["rows"], preview["truncated"], preview["publication"]), ([[1], [2]], True,
                                                                                          "NOT_PUBLISHED"))
        with self.assertRaisesRegex(ControlError, "FORBIDDEN"):
            self.runs.preview(self.f.owner, "space_one", "orders", run_id, 2)
        self.f.writer.preview_rejects.add("clean")
        failed = self.drive(self.trial(nonce="trial-rejected")["run_id"])
        self.assertEqual((failed["status"], failed["error_code"]), ("FAILED", "OUTPUT_SCHEMA_MISMATCH"))
        self.assertEqual(failed["trial"]["validation"]["status"], "REJECTED")

    def test_downstream_trial_binds_the_upstream_current_release_or_fails(self):
        self.f.writer.ingest(1)
        failed = self.drive(self.trial(nonce="trial-daily", task_id="daily")["run_id"])
        self.assertEqual((failed["status"], failed["error_code"]), ("FAILED", "TRIAL_UPSTREAM_NOT_PUBLISHED"))
        published = self.drive(self.f.start_run()["run_id"])
        self.assertEqual(published["status"], "PUBLISHED")
        releases = dict(self.f.writer.current)
        trial = self.drive(self.trial(nonce="trial-daily-2", task_id="daily")["run_id"])
        self.assertEqual(trial["status"], "VALIDATED")
        self.assertEqual(trial["trial"]["upstream"], {"clean": published["tasks"]["clean"]["release_id"]})
        self.assertEqual(self.f.writer.current, releases)

    def test_space_run_quota_waits_and_dispatch_quota_holds_admitted_work(self):
        self.quotas.set(self.f.registry._read("space_one"), {"max_active_runs": 1, "max_dispatched_jobs": 1},
                        "arn:aws:sts::123456789012:assumed-role/operator/unit")
        self.f.writer.ingest(2)
        # The formal run's Lambda task stays RUNNING (its completion never arrives yet).
        self.f.compute.silent.add(("summary", "attempt_1"))
        formal = self.f.start_run()["run_id"]
        record = self.drive(formal, rounds=8)
        self.assertEqual(record["tasks"]["summary"]["status"], "RUNNING")
        waiting = self.trial()["run_id"]
        record = self.drive(waiting, rounds=3)
        self.assertEqual((record["status"], record["waiting"]), ("QUEUED", "SPACE_RUN_QUOTA"))
        self.assertEqual(self.runs.status(record)["waiting"]["code"], "SPACE_RUN_QUOTA")
        self.quotas.set(self.f.registry._read("space_one"), {"max_active_runs": 2, "max_dispatched_jobs": 1},
                        "arn:aws:sts::123456789012:assumed-role/operator/unit")
        record = self.drive(waiting, rounds=3)
        job = record["tasks"]["clean"]
        self.assertEqual((record["status"], job["status"], job["transient"]["code"]),
                         ("RUNNING", "DISPATCHING", "SPACE_TASK_QUOTA"))
        self.assertEqual(self.runs.status(record)["tasks"][0]["transient_code"], "SPACE_TASK_QUOTA")
        self.f.now += 300 + 121
        self.assertEqual(self.drive(formal)["status"], "PUBLISHED")
        self.assertEqual(self.drive(waiting)["status"], "VALIDATED")

    def test_queued_run_shows_why_it_waits_and_clears_it_on_start(self):
        self.f.writer.ingest(1)
        self.f.writer.unavailable = 1
        run_id = self.f.start_run()["run_id"]
        record = self.runs.advance(self.runs.read(self.f.owner, "space_one", "orders", run_id))
        status = self.runs.status(record)
        self.assertEqual((status["status"], status["waiting"]["code"]), ("QUEUED", "WRITER_AUTHORITY_UNAVAILABLE"))
        record = self.drive(run_id)
        self.assertEqual(record["status"], "PUBLISHED")
        self.assertIsNone(record["waiting"])
        self.assertNotIn("waiting", self.runs.status(record))

    def test_history_is_newest_first_paginated_and_owner_scoped(self):
        self.f.writer.ingest(1)
        first = self.f.start_run("run-a")["run_id"]
        self.f.now += 10
        second = self.trial()["run_id"]
        self.f.now += 10
        third = self.runs.create(self.f.other, "space_one", "orders",
                                 {"pipeline_version": self.f.version, "mode": "snapshot"}, "run-other")["run_id"]
        page = self.runs.history(self.f.owner, "space_one", "orders", limit=2)
        self.assertEqual([value["run_id"] for value in page["runs"]], [third, second])
        self.assertEqual(page["runs"][1]["mode"], "trial")
        rest = self.runs.history(self.f.owner, "space_one", "orders", limit=2, page_token=page["next_page_token"])
        self.assertEqual([value["run_id"] for value in rest["runs"]], [first])
        own = self.runs.history(self.f.other, "space_one", "orders")
        self.assertEqual([value["run_id"] for value in own["runs"]], [third])
        with self.assertRaisesRegex(ControlError, "INVALID_PAGE_TOKEN"):
            self.runs.history(self.f.owner, "space_one", "orders", page_token="bm90LWEta2V5")

    def test_run_admission_cancel_and_retry_are_audited_atomically(self):
        self.f.writer.ingest(1)
        run_id = self.f.start_run()["run_id"]
        self.runs.cancel(self.f.owner, "space_one", "orders", run_id)
        records = self.audit()
        actions = [record["action"] for record in records]
        self.assertIn("RUN_CREATED", actions)
        self.assertIn("RUN_CANCEL_REQUESTED", actions)
        created = next(record for record in records if record["action"] == "RUN_CREATED")
        self.assertEqual((created["actor"]["principal_id"], created["actor"]["fingerprint"], created["target"]),
                         ("owner", "key_owner"[-12:], {"kind": "run", "id": run_id, "pipeline_id": "orders"}))
        self.assertEqual(created["details"]["mode"], "snapshot")
        only = self.audit("RUN_CANCEL_REQUESTED")
        self.assertEqual([record["target"]["id"] for record in only], [run_id])
        # The idempotent repeat of the same admission writes no second record.
        self.f.start_run()
        self.assertEqual(len(self.audit("RUN_CREATED")), 1)
        with self.assertRaisesRegex(ControlError, "FORBIDDEN"):
            Audit(self.f.table, "control").list(self.f.owner, self.f.registry._read("space_one"))
        text = json.dumps(records)
        self.assertNotIn("token", text.lower().replace("key_", ""))
        self.assertTrue(all(re.fullmatch(r"aud_[0-9a-f]{40}", record["audit_id"]) for record in records))


def load_tests(loader, tests, pattern):
    # The imported fixture's own tests run in test_runs.py, not again here.
    return loader.loadTestsFromTestCase(GovernanceRunTests)


if __name__ == "__main__":
    unittest.main()
