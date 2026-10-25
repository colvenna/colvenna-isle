"""Writer-executor SQL lifecycle with the actual runner; the writer runtime is a double."""

import time
import unittest

from colvenna_isle.control.registry import ControlError
from colvenna_isle.writer.tasks import WriterTasks
from test_sql_tasks import CLEAN, SqlTasks


class Runtime:
    def __init__(self, store, root):
        self.identity = {"source_commit": "f" * 40, "task_arn": "arn:aws:ecs:us-west-2:123456789012:task/c/1",
                         "process_nonce": "nonce"}
        self.consumer = type("Consumer", (), {"s3": store, "bucket": "test-bucket"})()
        self.bindings, self.logs = [], []
        runtime = self

        class Inputs:
            def authorize(self, request):
                if request.get("key_id") != "runner":
                    raise ControlError(403, "FORBIDDEN")
                return "runner", {"space_id": "owned", "generation": "gen_owned", "route_version": 1}

        class Publisher:
            def binding(self, actor, configuration, descriptor):
                runtime.bindings.append(descriptor["job_id"])
                if descriptor["job_id"] == "job_changed":
                    raise ControlError(409, "PUBLICATION_DEFINITION_BINDING_CHANGED")

        self.inputs, self.publisher = Inputs(), Publisher()

    def log(self, event, fields=None):
        self.logs.append(event)


class WriterTaskLifecycle(SqlTasks):
    def tasks(self):
        return WriterTasks(Runtime(self.store, self.root), workspace_root=self.root / "writer-tasks",
                           python_path=__import__("test_task_adapter").PYTHON_PATH, definition_epoch="e" * 64)

    def wait(self, tasks, request):
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            state = tasks.execute({**request, "operation": "writer_task_status"})
            if state["state"] != "RUNNING":
                return state
            time.sleep(0.2)
        self.fail("writer task did not finish")

    def test_idempotent_execution_stages_once_and_status_is_diagnostic_only(self):
        event = self.event("writer", CLEAN)
        tasks = self.tasks()
        request = {"operation": "writer_task_execute", "key_id": "runner", "descriptor": event["descriptor"]}
        first = tasks.execute(request)
        self.assertEqual(first["state"], "RUNNING")
        again = tasks.execute(request)
        self.assertEqual(again["started_unix"], first["started_unix"])
        state = self.wait(tasks, request)
        self.assertEqual((state["state"], state["row_count"]), ("STAGED", 1))
        completion, values = self.rows({"completion": state["completion"]})
        self.assertEqual(completion["invocation"]["executor"], "writer")
        self.assertEqual(len(values), 1)
        # Same original after completion returns the same diagnostic, not a new execution.
        self.assertEqual(tasks.execute(request)["execution_id"], state["execution_id"])
        restarted = self.tasks()
        self.assertEqual(restarted.execute({**request, "operation": "writer_task_status"})["state"], "UNKNOWN")

    def test_executor_mismatch_binding_change_and_authority_are_rejected(self):
        tasks = self.tasks()
        lambda_event = self.event("lambda", CLEAN)
        with self.assertRaisesRegex(ControlError, "WRITER_TASK_EXECUTOR_MISMATCH"):
            tasks.execute({"operation": "writer_task_execute", "key_id": "runner",
                           "descriptor": lambda_event["descriptor"]})
        with self.assertRaisesRegex(ControlError, "FORBIDDEN"):
            tasks.execute({"operation": "writer_task_execute", "key_id": "other",
                           "descriptor": self.event("writer", CLEAN)["descriptor"]})

    def test_rejected_sql_reports_failed_code_without_staging(self):
        event = self.event("writer", "DELETE FROM events")
        tasks = self.tasks()
        request = {"operation": "writer_task_execute", "key_id": "runner", "descriptor": event["descriptor"]}
        tasks.execute(request)
        state = self.wait(tasks, request)
        self.assertEqual((state["state"], state["code"]), ("FAILED", "SQL_READ_ONLY_REQUIRED"))
        self.assertFalse([key for key in self.store.objects if "/staging/" in key])


if __name__ == "__main__":
    unittest.main()
