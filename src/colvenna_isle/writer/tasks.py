"""Short controlled SQL on the sole writer task, outside its native lock.

The writer executor reads the immutable descriptor, re-verifies the published
version binding, and runs the same SQL runner and staging contract as Lambda and
ECS in a separate bounded process without credentials. Only the produced
completion can later enter the fair-scheduled native publication ledger.
"""

import copy
import threading
import time
from pathlib import Path

from colvenna_isle.contracts.validation import validate
from colvenna_isle.control.http import decode_json
from colvenna_isle.control.pipelines import DEFINITION_EPOCH
from colvenna_isle.control.registry import ControlError
from colvenna_isle.pipeline.task_adapter import ExecutionFailure, TaskAdapter
from colvenna_isle.pipeline.task_descriptor import MAX_DESCRIPTOR_BYTES, check_descriptor, descriptor_key
from colvenna_isle.pipeline.task_objects import TaskObjects
from .ingestion import digest

OPERATIONS = {"writer_task_execute", "writer_task_status"}
MAX_RETAINED_WORKERS = 256


class WriterTasks:
    def __init__(self, runtime, *, workspace_root=Path("/work/tasks"), python_path="/opt/colvenna/application",
                 definition_epoch=DEFINITION_EPOCH):
        self.runtime, self.workspace_root, self.python_path = runtime, workspace_root, python_path
        self.definition_epoch = definition_epoch
        self.lock = threading.Lock()
        self.workers = {}
        # One short SQL process at a time; ingestion keeps the writer's CPU.
        self.slots = threading.BoundedSemaphore(1)

    def execute(self, request):
        operation = request.get("operation")
        if operation not in OPERATIONS:
            raise ControlError(400, "UNKNOWN_WRITER_TASK_OPERATION")
        actor, configuration = self.runtime.inputs.authorize(request)
        reference = request.get("descriptor")
        validate("task_object", reference)
        objects = TaskObjects(self.runtime.consumer.s3, self.runtime.consumer.bucket,
                              configuration["space_id"], configuration["generation"])
        key = digest([configuration["space_id"], configuration["generation"], reference])
        if operation == "writer_task_status":
            with self.lock:
                worker = copy.deepcopy(self.workers.get(key))
            # In-memory progress is a diagnostic. Completion custody is the
            # original staging object; a restarted writer reports UNKNOWN.
            return worker or {"state": "UNKNOWN", "code": "WRITER_TASK_NOT_OBSERVED"}
        descriptor = decode_json(objects.read(reference, "task-contexts", maximum_bytes=MAX_DESCRIPTOR_BYTES))
        check_descriptor(descriptor)
        if objects.key(reference, "task-contexts") != descriptor_key(descriptor):
            raise ControlError(403, "TASK_DESCRIPTOR_SCOPE")
        if descriptor["task"]["executor"] != "writer":
            raise ControlError(409, "WRITER_TASK_EXECUTOR_MISMATCH")
        self.runtime.publisher.binding(actor, configuration, descriptor)
        with self.lock:
            prior = self.workers.get(key)
            if prior:
                return copy.deepcopy(prior)
            if not self.slots.acquire(blocking=False):
                raise ControlError(429, "WRITER_TASK_EXECUTOR_QUOTA")
            if len(self.workers) >= MAX_RETAINED_WORKERS:
                finished = [name for name, value in self.workers.items() if "finished_unix" in value]
                for name in sorted(finished, key=lambda name: self.workers[name]["finished_unix"])[:64]:
                    self.workers.pop(name)
            worker = {"state": "RUNNING", "job_id": descriptor["job_id"], "attempt_id": descriptor["attempt_id"],
                      "started_unix": time.time()}
            self.workers[key] = worker
        threading.Thread(target=self.run, args=(descriptor, reference, worker), daemon=True,
                         name="writer-task-" + descriptor["job_id"][-12:]).start()
        return copy.deepcopy(worker)

    def run(self, descriptor, reference, worker):
        identity = self.runtime.identity
        environment = {"bucket": self.runtime.consumer.bucket, "space_id": descriptor["space_id"],
                       "generation": descriptor["generation"], "definition_epoch": self.definition_epoch,
                       "executor": "writer", "source_commit": identity["source_commit"]}
        try:
            adapter = TaskAdapter(self.runtime.consumer.s3, environment, workspace_root=self.workspace_root,
                                  python_path=self.python_path)
            result = adapter.execute({"operation": "task_execute", "descriptor": reference},
                invocation={"executor": "writer", "task_arn": identity["task_arn"],
                            "process_nonce": identity["process_nonce"]})
            outcome = {"state": result["status"], "execution_id": result["execution_id"],
                       "completion": result["completion"], "row_count": result["row_count"]}
        except ExecutionFailure as error:
            outcome = {"state": error.receipt["status"], "code": error.code,
                       "execution_id": error.receipt["execution_id"]}
        except ControlError as error:
            outcome = {"state": "FAILED", "code": error.code}
        except Exception as error:
            outcome = {"state": "UNKNOWN", "code": "WRITER_TASK_UNKNOWN"}
            self.runtime.log("WRITER_TASK_UNKNOWN", {"exception_type": type(error).__name__})
        finally:
            with self.lock:
                worker.update(outcome if "outcome" in locals() else {"state": "UNKNOWN"},
                              finished_unix=time.time())
            self.slots.release()
