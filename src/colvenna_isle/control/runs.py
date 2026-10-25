"""Durable pipeline runs: admission, DAG progression, reconciliation, cancel and retry.

One RUN record (colvenna-control, PK=SPACE#<space>) fixes the published version,
parameters, root watermarks and every job/attempt identity. The scheduler
advances it in short, conditional steps. Authority never moves to DynamoDB: the
writer's native ledger decides eligibility, publication and watermarks, and
every uncertain external effect is reconciled against its original identity
before any new attempt is made.
"""

import copy
import hashlib
import json
import re
import time
from datetime import datetime, timezone

from colvenna_isle.contracts.validation import ContractError, validate, validate_parameters, validate_run
from colvenna_isle.pipeline.task_descriptor import MAX_DESCRIPTOR_BYTES, TaskAdmission, descriptor_key, digest
from colvenna_isle.pipeline.task_objects import UploadUnknown, reconcile
from .audit import Audit, key_actor
from .dispatch import DispatchUnknown
from .registry import ControlError, canonical
from .writer_client import WriterUnknown

ACTIVE_INDEX = "PIPELINERUNS#1"
# A trial ends VALIDATED: its candidate was checked like an import, never granted or published.
TERMINAL_RUN = {"PUBLISHED", "FAILED", "CANCELLED", "VALIDATED"}
TERMINAL_TASK = {"PUBLISHED", "FAILED", "CANCELLED", "VALIDATED"}
DISPATCHED_TASK = {"RUNNING", "STAGED", "IMPORTING"}
# A dispatched attempt without a completion becomes eligible for replacement
# only after its whole execution deadline plus this observation grace.
GRACE_SECONDS = 120
WRITER_GRACE_SECONDS = 60
# An approved dispatch that cannot be confirmed for this long ends its attempt;
# a later attempt's higher native epoch still fences any late execution.
DISPATCH_SECONDS = 600
# Bounds one RUN record well below the 350 KB control-record limit.
MAX_RECORD_PARTITIONS = 4000
MAX_ATTEMPTS = 10


def stamp(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")


def order(definition):
    tasks = {task["task_id"]: task for task in definition["tasks"]}
    result, seen = [], set()

    def visit(name):
        if name in seen:
            return
        for dependency in tasks[name].get("depends_on", []):
            visit(dependency)
        seen.add(name)
        result.append(name)

    for name in sorted(tasks):
        visit(name)
    return [tasks[name] for name in result]


class Runs:
    def __init__(self, pipelines, keys, writer, dispatcher, objects, *, clock=time.time, wake=None, quotas=None):
        self.pipelines, self.registry = pipelines, pipelines.registry
        self.client, self.table = pipelines.client, pipelines.table
        self.keys, self.writer, self.dispatcher = keys, writer, dispatcher
        self.objects, self.clock, self.wake = objects, clock, wake or (lambda: None)
        self.quotas, self.audit = quotas, Audit(self.client, self.table)
        self._records = {}

    # -- records ---------------------------------------------------------------------------------

    def _key(self, configuration, pipeline, run_id):
        return self.pipelines._key(configuration, "RUN", pipeline, run_id)

    @staticmethod
    def _index(space, generation, pipeline, run_id):
        return {"PK": {"S": ACTIVE_INDEX}, "SK": {"S": "#".join([space, generation, pipeline, run_id])}}

    @staticmethod
    def _history(space, generation, pipeline, created, run_id):
        """Newest-first run history; the RUN record stays the only state."""
        return {"PK": {"S": "SPACE#" + space},
                "SK": {"S": f"RUNHISTORY#{generation}#{pipeline}#{int(created * 1000000):020d}#{run_id}"}}

    def _audited(self, principal, record, action, identity, **details):
        return self.audit.put(record["space_id"], record["generation"], action, key_actor(principal),
                              {"kind": "run", "id": record["run_id"], "pipeline_id": record["pipeline_id"]},
                              identity=identity, details=details)

    def _save(self, record, previous, extra=()):
        configuration = {"space_id": record["space_id"], "generation": record["generation"]}
        key = self._key(configuration, record["pipeline_id"], record["run_id"])
        record["updated_unix"] = self.clock()
        operations = [self.pipelines._put(key, record, previous), *extra]
        if record["status"] in TERMINAL_RUN:
            operations.append({"Delete": {"TableName": self.table, "Key": self._index(
                record["space_id"], record["generation"], record["pipeline_id"], record["run_id"])}})
        if not self.pipelines._transaction(operations):
            return None
        return self.pipelines._read(key)

    def _receipt(self, record):
        return {"run_id": record["run_id"], "pipeline_version": record["pipeline_version"], "status": "QUEUED",
                "status_url": f"/v1/spaces/{record['space_id']}/pipelines/{record['pipeline_id']}/runs/{record['run_id']}"}

    def _owner(self, principal, record, scope="pipeline:run"):
        if principal.principal_id != record["principal"]["principal_id"] and not (
                {"monitor:read", "pipeline:manage"} & set(principal.scopes)):
            raise ControlError(403, "RUN_OWNER_REQUIRED")

    # -- API ---------------------------------------------------------------------------------------

    def create(self, principal, space, pipeline, request, nonce, *, trigger="api", window=None):
        configuration = self.pipelines._authorize(principal, space, "pipeline:run")
        validate("run_request", request)
        if not isinstance(nonce, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", nonce):
            raise ControlError(428, "IDEMPOTENCY_KEY_REQUIRED")
        run_id = "run_" + digest({"principal": principal.principal_id, "nonce": nonce, "space": space,
                                  "generation": configuration["generation"], "pipeline": pipeline})[:40]
        key = self._key(configuration, pipeline, run_id)
        existing = self.pipelines._read(key, missing=None)
        if existing is not None:
            if existing["request_sha256"] != digest(request):
                raise ControlError(409, "IDEMPOTENCY_KEY_CONFLICT")
            return self._receipt(existing)
        bound = self.pipelines.bind_run(principal, space, pipeline, request["pipeline_version"])
        definition = bound["snapshot"]["definition"]
        if trigger == "api" and self.pipelines._current(configuration, pipeline)["schedule"]["owner"] == "external":
            # The external orchestrator owns automatic triggering; Colvenna Isle still
            # owns this run's internal DAG, imports and publication.
            trigger = "external"
        try:
            validate_run(request, definition)
        except ContractError:
            raise ControlError(422, "RUN_REQUEST_INVALID") from None
        for name in request.get("inputs", {}):
            task_id, alias = name.split(".", 1)
            task = next(value for value in definition["tasks"] if value["task_id"] == task_id)
            dataset = task["inputs"][alias]["dataset"]
            owner = configuration["datasets"][dataset]["definition"].get("write_owner")
            if owner != "pipeline":
                # Only immutable releases can be named; ODS/direct roots bind at admission.
                raise ControlError(422, "RUN_INPUT_RELEASE_REQUIRES_PIPELINE_DATASET")
        parameters = {name: value["default"] for name, value in definition.get("parameters", {}).items()
                      if "default" in value}
        parameters.update(request.get("parameters", {}))
        validate_parameters(definition.get("parameters", {}), parameters)
        now = self.clock()
        record = {"run_id": run_id, "space_id": space, "generation": configuration["generation"],
                  "pipeline_id": pipeline, "pipeline_version": request["pipeline_version"],
                  "definition_epoch": bound["snapshot"]["definition_epoch"],
                  "snapshot_sha256": digest(bound["snapshot"]), "request": copy.deepcopy(request),
                  "request_sha256": digest(request), "trigger": {"kind": trigger, "window": window,
                  "nonce_sha256": hashlib.sha256(nonce.encode()).hexdigest()},
                  "principal": {"principal_id": principal.principal_id, "key_id": principal.key_id},
                  "status": "QUEUED", "parameters": parameters, "root_inputs": None,
                  "cancel_requested": False, "retries": {}, "created_unix": now, "updated_unix": now, "events": [],
                  "tasks": {task["task_id"]: {
                      "job_id": "job_" + digest([run_id, task["task_id"]])[:40], "status": "BLOCKED",
                      "executor": task["executor"], "attempt_number": 0, "attempt_limit": task["max_attempts"],
                      "attempts": [], "epoch": 0} for task in definition["tasks"]}}
        return self._admit(principal, configuration, key, record, "RUN_CREATED")

    def _admit(self, principal, configuration, key, record, action):
        space, pipeline, run_id, now = record["space_id"], record["pipeline_id"], record["run_id"], record["created_unix"]
        operations = [self.pipelines._put(key, record),
                      {"Put": {"TableName": self.table, "Item": {
                          **self._index(space, configuration["generation"], pipeline, run_id),
                          "created_unix": {"N": str(now)}}}},
                      {"Put": {"TableName": self.table, "Item": {
                          **self._history(space, configuration["generation"], pipeline, now, run_id),
                          "run_id": {"S": run_id}, "mode": {"S": record["request"]["mode"]}},
                          "ConditionExpression": "attribute_not_exists(PK)"}},
                      self._audited(principal, record, action, run_id, pipeline_version=record["pipeline_version"],
                                    mode=record["request"]["mode"], trigger=record["trigger"]["kind"],
                                    task_id=record["request"].get("task_id")),
                      *self.keys.operation_conditions(principal, space, configuration["generation"], "pipeline:run")]
        if not self.pipelines._transaction(operations):
            existing = self.pipelines._read(key)
            if existing["request_sha256"] != record["request_sha256"]:
                raise ControlError(409, "IDEMPOTENCY_KEY_CONFLICT")
            return self._receipt(existing)
        self.wake()
        return self._receipt(record)

    def trial(self, principal, space, pipeline, request, nonce):
        """A sample run of one task: inputs fixed and output validated, never granted or published."""
        configuration = self.pipelines._authorize(principal, space, "pipeline:run")
        validate("trial_request", request)
        if not isinstance(nonce, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", nonce):
            raise ControlError(428, "IDEMPOTENCY_KEY_REQUIRED")
        if self.quotas is not None and request["sample_rows"] > self.quotas.policy(
                space, configuration["generation"])["max_trial_sample_rows"]:
            raise ControlError(422, "SPACE_TRIAL_SAMPLE_QUOTA")
        run_id = "run_" + digest({"principal": principal.principal_id, "nonce": nonce, "space": space,
                                  "generation": configuration["generation"], "pipeline": pipeline,
                                  "trial": True})[:40]
        key = self._key(configuration, pipeline, run_id)
        stored = {**copy.deepcopy(request), "mode": "trial"}
        existing = self.pipelines._read(key, missing=None)
        if existing is not None:
            if existing["request_sha256"] != digest(stored):
                raise ControlError(409, "IDEMPOTENCY_KEY_CONFLICT")
            return self._receipt(existing)
        bound = self.pipelines.bind_run(principal, space, pipeline, request["pipeline_version"])
        definition = bound["snapshot"]["definition"]
        task = next((value for value in definition["tasks"] if value["task_id"] == request["task_id"]), None)
        if task is None:
            raise ControlError(404, "TASK_NOT_FOUND")
        parameters = {name: value["default"] for name, value in definition.get("parameters", {}).items()
                      if "default" in value}
        parameters.update(request.get("parameters", {}))
        validate_parameters(definition.get("parameters", {}), parameters)
        now = self.clock()
        record = {"run_id": run_id, "space_id": space, "generation": configuration["generation"],
                  "pipeline_id": pipeline, "pipeline_version": request["pipeline_version"],
                  "definition_epoch": bound["snapshot"]["definition_epoch"],
                  "snapshot_sha256": digest(bound["snapshot"]), "request": stored,
                  "request_sha256": digest(stored), "trigger": {"kind": "api", "window": None,
                  "nonce_sha256": hashlib.sha256(nonce.encode()).hexdigest()},
                  "principal": {"principal_id": principal.principal_id, "key_id": principal.key_id},
                  "status": "QUEUED", "parameters": parameters, "root_inputs": None,
                  "cancel_requested": False, "retries": {}, "created_unix": now, "updated_unix": now, "events": [],
                  "trial": {"task_id": task["task_id"], "sample_rows": request["sample_rows"],
                            "upstream": None, "validation": {"status": "PENDING"}},
                  "tasks": {task["task_id"]: {
                      "job_id": "job_" + digest([run_id, task["task_id"]])[:40], "status": "BLOCKED",
                      "executor": task["executor"], "attempt_number": 0, "attempt_limit": task["max_attempts"],
                      "attempts": [], "epoch": 0}}}
        return self._admit(principal, configuration, key, record, "TRIAL_CREATED")

    def history(self, principal, space, pipeline, *, limit=20, page_token=None):
        configuration = self.pipelines._authorize(
            principal, space, "pipeline:run" if "pipeline:run" in principal.scopes else
            "pipeline:manage" if "pipeline:manage" in principal.scopes else "monitor:read")
        validate("identifier", pipeline)
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ControlError(400, "INVALID_RUN_LIST_LIMIT")
        prefix = f"RUNHISTORY#{configuration['generation']}#{pipeline}#"
        request = {"TableName": self.table, "ConsistentRead": True, "ScanIndexForward": False, "Limit": limit,
                   "KeyConditionExpression": "PK = :pk AND begins_with(SK, :prefix)",
                   "ExpressionAttributeValues": {":pk": {"S": "SPACE#" + space}, ":prefix": {"S": prefix}}}
        if page_token is not None:
            import base64
            try:
                sort = base64.urlsafe_b64decode(page_token + "=" * (-len(page_token) % 4)).decode()
            except (ValueError, UnicodeError):
                raise ControlError(400, "INVALID_PAGE_TOKEN") from None
            if not re.fullmatch(re.escape(prefix) + r"[0-9]{20}#run_[0-9a-f]{40}", sort):
                raise ControlError(400, "INVALID_PAGE_TOKEN")
            request["ExclusiveStartKey"] = {"PK": {"S": "SPACE#" + space}, "SK": {"S": sort}}
        page = self.client.query(**request)
        runs = []
        owner_only = not {"monitor:read", "pipeline:manage"} & set(principal.scopes)
        for item in page.get("Items", []):
            record = self.pipelines._read(self._key(configuration, pipeline, item["run_id"]["S"]), missing=None)
            if record is None or owner_only and record["principal"]["principal_id"] != principal.principal_id:
                continue
            runs.append({"run_id": record["run_id"], "pipeline_version": record["pipeline_version"],
                         "status": record["status"], "trigger": record["trigger"]["kind"],
                         "mode": record["request"]["mode"], "created_at": stamp(record["created_unix"]),
                         "finished_at": stamp(record["finished_unix"]) if record.get("finished_unix") else None,
                         "error_code": record.get("error_code"),
                         "task_id": (record.get("trial") or {}).get("task_id")})
        last = page.get("LastEvaluatedKey")
        if last:
            import base64
            last = base64.urlsafe_b64encode(last["SK"]["S"].encode()).decode().rstrip("=")
        result = {"runs": runs, "next_page_token": last}
        validate("run_list", result)
        return result

    def preview(self, principal, space, pipeline, run_id, rows=20):
        """Unpublished trial rows are data: the owner also needs query:read."""
        principal.authorize(space, "query:read")
        record = self.read(principal, space, pipeline, run_id)
        if record["request"]["mode"] != "trial":
            raise ControlError(409, "RUN_NOT_TRIAL")
        task_id = record["trial"]["task_id"]
        job = record["tasks"][task_id]
        if job["status"] != "VALIDATED":
            raise ControlError(409, "TRIAL_NOT_VALIDATED")
        if type(rows) is not int or not 1 <= rows <= 100:
            raise ControlError(400, "INVALID_PREVIEW_ROWS")
        attempt = job["attempts"][-1]
        configuration = self.registry._read(space)
        result = self.writer.call(principal, configuration, "publication_preview", descriptor=attempt["descriptor"],
                                  execution_id=attempt["execution_id"], completion=attempt["completion"], rows=rows)
        value = {"run_id": run_id, "task_id": task_id, "publication": "NOT_PUBLISHED",
                 "columns": result["preview"]["columns"], "rows": result["preview"]["rows"],
                 "row_count": result["validation"]["row_count"], "truncated": result["preview"]["truncated"],
                 "observed_at": stamp(self.clock())}
        validate("run_preview", value)
        return value

    def read(self, principal, space, pipeline, run_id):
        scope = "pipeline:run" if "pipeline:run" in principal.scopes else "monitor:read"
        configuration = self.pipelines._authorize(principal, space, scope)
        record = self.pipelines._read(self._key(configuration, pipeline, run_id), missing="RUN_NOT_FOUND")
        self._owner(principal, record)
        return record

    def status(self, record):
        definition_tasks = list(record["tasks"])
        tasks = []
        for task_id in definition_tasks:
            job = record["tasks"][task_id]
            value = {"task_id": task_id, "job_id": job["job_id"], "status": job["status"],
                     "executor": job["executor"], "attempt_number": job["attempt_number"],
                     "eligibility_epoch": job["epoch"]}
            if job["attempts"]:
                attempt = job["attempts"][-1]
                value["attempt_id"] = attempt["attempt_id"]
                dispatch = attempt.get("dispatch") or {}
                identity = dispatch.get("request_id") or dispatch.get("task_arn") or dispatch.get("process_nonce")
                if identity:
                    value["dispatch_id"] = identity
                if attempt.get("execution_id"):
                    value["execution_id"] = attempt["execution_id"]
            for name in ["release_id", "error_code"]:
                if job.get(name) is not None:
                    value[name] = copy.deepcopy(job[name])
            if (job.get("plan") or {}).get("processing") or job.get("processing"):
                value["processing"] = copy.deepcopy((job.get("plan") or {}).get("processing") or job["processing"])
            if job.get("export_id"):
                value["input_export_id"] = job["export_id"]
            if job.get("input_versions"):
                value["input_versions"] = {alias: {name: member for name, member in version.items()
                                                   if name in {"dataset", "release_id", "commit_sequence",
                                                               "from_sequence", "to_sequence", "state_release_id"}}
                                           for alias, version in job["input_versions"].items()}
            if job.get("observed_unix"):
                value["observed_at"] = stamp(job["observed_unix"])
            value["attempts"] = [self._attempt_status(attempt) for attempt in job["attempts"]][-10:]
            if job.get("progress"):
                value["progress"] = copy.deepcopy(job["progress"])
            if job.get("transient"):
                value["transient_code"] = job["transient"]["code"]
            if job.get("skipped"):
                value["skipped"] = job["skipped"]
            tasks.append(value)
        result = {"run_id": record["run_id"], "pipeline_version": record["pipeline_version"],
                  "status": record["status"], "observed_at": stamp(record["updated_unix"]),
                  "projection_version": record["record_revision"], "tasks": tasks,
                  "trigger": record["trigger"]["kind"], "parameters": record["parameters"],
                  "snapshot_sha256": record["snapshot_sha256"], "created_at": stamp(record["created_unix"]),
                  "cancel_requested": record["cancel_requested"], "mode": record["request"]["mode"],
                  "pipeline_id": record["pipeline_id"]}
        if record["request"].get("partitions"):
            result["partitions"] = copy.deepcopy(record["request"]["partitions"])
        if record.get("trial"):
            trial = record["trial"]
            result["trial"] = {"task_id": trial["task_id"], "sample_rows": trial["sample_rows"],
                               "publication": "NOT_PUBLISHED", "validation": copy.deepcopy(trial["validation"]),
                               "upstream": copy.deepcopy(trial.get("upstream") or {})}
        if record.get("root_inputs") is not None:
            result["root_inputs"] = copy.deepcopy(record["root_inputs"])
        if record.get("finished_unix"):
            result["finished_at"] = stamp(record["finished_unix"])
        if record.get("error_code"):
            result["error_code"] = record["error_code"]
        if record["status"] == "QUEUED" and record.get("waiting"):
            result["waiting"] = {"code": record["waiting"],
                                 "since": stamp(record.get("waiting_unix") or record["updated_unix"])}
        validate("run_status", result)
        return result

    @staticmethod
    def _attempt_status(attempt):
        dispatch = attempt.get("dispatch") or {}
        value = {"attempt_id": attempt["attempt_id"], "status": attempt["status"],
                 "created_at": stamp(attempt["created_unix"]) if attempt.get("created_unix") else None,
                 "dispatched_at": stamp(attempt["dispatched_unix"]) if attempt.get("dispatched_unix") else None,
                 "ended_at": stamp(attempt["ended_unix"]) if attempt.get("ended_unix") else None,
                 "deadline_at": stamp(attempt["deadline_unix"]) if attempt.get("deadline_unix") else None,
                 "dispatch_id": dispatch.get("request_id") or dispatch.get("task_arn") or dispatch.get("process_nonce"),
                 "execution_id": attempt.get("execution_id"), "eligibility_epoch": attempt.get("epoch"),
                 "code": attempt.get("code"), "usage": copy.deepcopy(attempt.get("usage") or {})}
        return value

    def get(self, principal, space, pipeline, run_id):
        return self.status(self.read(principal, space, pipeline, run_id))

    def cancel(self, principal, space, pipeline, run_id):
        record = self.read(principal, space, pipeline, run_id)
        for _ in range(8):
            if record["status"] in TERMINAL_RUN or record["cancel_requested"]:
                break
            updated = copy.deepcopy(record)
            updated.update(cancel_requested=True, status="CANCELLING")
            updated["events"] = (updated["events"] + [{"event": "CANCEL_REQUESTED", "unix": self.clock(),
                                                       "principal_id": principal.principal_id}])[-64:]
            saved = self._save(updated, record, [self._audited(principal, record, "RUN_CANCEL_REQUESTED",
                                                               record["run_id"] + ":cancel",
                                                               status=record["status"])])
            if saved:
                record = saved
                break
            record = self.read(principal, space, pipeline, run_id)
        # Revoke eligibility through the writer now; publication may still win.
        record = self.advance(record) or record
        return {"run_id": run_id, "status": record["status"]}

    def retry(self, principal, space, pipeline, run_id, nonce):
        if not isinstance(nonce, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", nonce):
            raise ControlError(428, "IDEMPOTENCY_KEY_REQUIRED")
        identity = hashlib.sha256(canonical([principal.principal_id, nonce]).encode()).hexdigest()
        for _ in range(8):
            record = self.read(principal, space, pipeline, run_id)
            if identity in record["retries"]:
                return self._receipt(record)
            if record["status"] != "FAILED" or record["request"]["mode"] == "trial":
                raise ControlError(409, "RUN_NOT_RETRYABLE")
            if any(job["status"] == "UNKNOWN" for job in record["tasks"].values()):
                raise ControlError(409, "RUN_RECONCILIATION_REQUIRED")
            self.pipelines._authorize(principal, space, "pipeline:run")
            definition = self._definition(principal, record)
            updated = copy.deepcopy(record)
            for task in definition["tasks"]:
                job = updated["tasks"][task["task_id"]]
                if job["status"] == "FAILED":
                    if job["attempt_number"] >= MAX_ATTEMPTS:
                        raise ControlError(409, "RUN_ATTEMPT_LIMIT")
                    # New attempts with the original inputs, code and environment.
                    job.update(status="RETRY_WAIT" if job.get("export_id") else "BLOCKED", error_code=None,
                               attempt_limit=min(MAX_ATTEMPTS, job["attempt_number"] + task["max_attempts"]))
            updated.update(status="QUEUED", error_code=None, finished_unix=None)
            updated["retries"][identity] = len(updated["retries"]) + 1
            updated["events"] = (updated["events"] + [{"event": "RETRY", "unix": self.clock(),
                                                       "principal_id": principal.principal_id}])[-64:]
            index = {"Put": {"TableName": self.table, "Item": {
                **self._index(space, record["generation"], pipeline, run_id),
                "created_unix": {"N": str(record["created_unix"])}}}}
            audit = self._audited(principal, record, "RUN_RETRIED", record["run_id"] + ":retry:" + identity,
                                  retry=updated["retries"][identity])
            if self._save(updated, record, [index, audit]):
                self.wake()
                return self._receipt(updated)
        raise ControlError(503, "RUN_UPDATE_BUSY")

    def notify(self, observation):
        """Attach a completion/stop notification to its original attempt; never authoritative."""
        for space, generation, pipeline, run_id in self.active():
            if (space, generation, run_id) != (observation["space_id"], observation["generation"],
                                               observation["run_id"]):
                continue
            configuration = {"space_id": space, "generation": generation}
            record = self.pipelines._read(self._key(configuration, pipeline, run_id))
            for task_id, job in record["tasks"].items():
                if job["job_id"] != observation["job_id"]:
                    continue
                for index, attempt in enumerate(job["attempts"]):
                    if attempt["attempt_id"] != observation["attempt_id"]:
                        continue
                    value = {name: observation.get(name) for name in [
                        "source", "request_id", "condition", "status", "code", "execution_id", "task_arn",
                        "exit_code"] if observation.get(name) is not None}

                    def change(updated, task_id=task_id, index=index):
                        attempt = updated["tasks"][task_id]["attempts"][index]
                        attempt["notifications"] = (attempt.get("notifications", []) + [
                            {**value, "unix": self.clock()}])[-8:]
                    return self._mutate(record, change)
        return None

    # -- controller --------------------------------------------------------------------------------

    def active(self, limit=None):
        """Every active run index entry (paginated); limit bounds a caller's scan only."""
        items, start = [], None
        while True:
            request = {"TableName": self.table, "KeyConditionExpression": "PK = :pk",
                       "ExpressionAttributeValues": {":pk": {"S": ACTIVE_INDEX}}, "ConsistentRead": True}
            if start:
                request["ExclusiveStartKey"] = start
            result = self.client.query(**request)
            items += result.get("Items", [])
            start = result.get("LastEvaluatedKey")
            if not start or limit and len(items) >= limit:
                break
        return [item["SK"]["S"].split("#") for item in items]

    def tick(self, deadline):
        summary = {"advanced": [], "errors": []}
        self._records = {}
        for space, generation, pipeline, run_id in self.active():
            if time.monotonic() >= deadline:
                break
            configuration = {"space_id": space, "generation": generation}
            record = self.pipelines._read(self._key(configuration, pipeline, run_id), missing=None)
            if record is None:
                self.client.delete_item(TableName=self.table, Key=self._index(space, generation, pipeline, run_id))
                continue
            try:
                record = self.advance(record, deadline=deadline) or record
                summary["advanced"].append({"run_id": run_id, "status": record["status"]})
            except Exception as error:  # One run's failure never stops the others.
                summary["errors"].append({"run_id": run_id, "error": getattr(error, "code", type(error).__name__)})
        return summary

    def _space_runs(self, record):
        """Other active runs of the same space/generation (records cached for one tick)."""
        result = []
        for space, generation, pipeline, run_id in self.active():
            if (space, generation) != (record["space_id"], record["generation"]) or run_id == record["run_id"]:
                continue
            identity = (space, generation, pipeline, run_id)
            if identity not in self._records:
                self._records[identity] = self.pipelines._read(
                    self._key({"space_id": space, "generation": generation}, pipeline, run_id), missing=None)
            if self._records[identity] is not None:
                result.append(self._records[identity])
        return result

    def _policy(self, record):
        return self.quotas.policy(record["space_id"], record["generation"]) if self.quotas is not None else None

    def _principal(self, record):
        principal = self.keys.authenticate_id(record["principal"]["key_id"])
        if principal.principal_id != record["principal"]["principal_id"] or principal.generation != record["generation"]:
            raise ControlError(403, "RUN_PRINCIPAL_CHANGED")
        principal.authorize(record["space_id"], "pipeline:run")
        return principal

    def _definition(self, principal, record):
        bound = self.pipelines.bind_run(principal, record["space_id"], record["pipeline_id"],
                                        record["pipeline_version"])
        if digest(bound["snapshot"]) != record["snapshot_sha256"]:
            raise ControlError(409, "RUN_DEFINITION_BINDING_CHANGED")
        self._bound = bound
        return bound["snapshot"]["definition"]

    def advance(self, record, *, deadline=None):
        """Advance one run by bounded idempotent steps; returns the latest saved record."""
        deadline = deadline or time.monotonic() + 30
        if record["status"] in TERMINAL_RUN:
            self.client.delete_item(TableName=self.table, Key=self._index(
                record["space_id"], record["generation"], record["pipeline_id"], record["run_id"]))
            return record
        try:
            principal = self._principal(record)
            configuration = self.registry._read(record["space_id"])
            if configuration["generation"] != record["generation"]:
                raise ControlError(409, "SPACE_GENERATION_CHANGED")
            definition = self._definition(principal, record)
        except ControlError as error:
            # A revoked principal, rebuilt space or changed contract cannot run
            # further steps. Already published releases remain published.
            return self._finish(record, "FAILED", error.code, terminal_jobs=True)
        context = {"principal": principal, "configuration": configuration, "definition": definition}
        if record["status"] == "QUEUED":
            try:
                record = self._start(record, context)
            except ControlError as error:
                if error.status in {429, 503}:
                    # Root watermarks are bound once the writer answers; the wait is shown.
                    return self._wait(record, error.code)
                return self._finish(record, "FAILED", error.code, terminal_jobs=True)
            if record["status"] == "QUEUED":
                return record
        for _ in range(64):
            if time.monotonic() >= deadline:
                break
            progressed = False
            for task in order(definition):
                if time.monotonic() >= deadline:
                    break
                if task["task_id"] not in record["tasks"]:
                    continue  # A trial runs only its selected task.
                job = record["tasks"][task["task_id"]]
                if job["status"] in TERMINAL_TASK:
                    continue
                result = self._step(record, task, context)
                if result is not None:
                    record, changed = result
                    progressed = progressed or changed
            record = self._conclude(record, definition)
            if record["status"] in TERMINAL_RUN or not progressed:
                break
        if record["status"] in TERMINAL_RUN:
            record = self._release_inputs(record, context)
        return record

    def _release_inputs(self, record, context):
        """Terminal jobs no longer pin their fixed input exports (best effort, retried)."""
        for task_id, job in record["tasks"].items():
            if job.get("export_id") and not job.get("input_released") and job["status"] in TERMINAL_TASK:
                try:
                    self.writer.call(context["principal"], context["configuration"], "input_release",
                                     export_id=job["export_id"], consumer_id=job["job_id"])
                except ControlError as error:
                    if error.code != "INPUT_REFERENCE_NOT_FOUND":
                        continue
                record = self._job_change(record, task_id, lambda value: value.update(input_released=True),
                                          cancel=True)
        return record

    def _mutate(self, record, change):
        """Apply one in-memory change and persist it conditionally."""
        for _ in range(4):
            updated = copy.deepcopy(record)
            change(updated)
            saved = self._save(updated, record)
            if saved is not None:
                return saved
            configuration = {"space_id": record["space_id"], "generation": record["generation"]}
            record = self.pipelines._read(self._key(configuration, record["pipeline_id"], record["run_id"]))
        raise ControlError(503, "RUN_UPDATE_BUSY")

    def _start(self, record, context):
        configuration, definition, principal = context["configuration"], context["definition"], context["principal"]
        if record["cancel_requested"]:
            return self._finish(record, "CANCELLED", None, terminal_jobs=True)
        trial = record["request"]["mode"] == "trial"
        others = self._space_runs(record)
        # max_active_runs=1 per pipeline: a queued run waits; pending data
        # coalesces. Trials never publish, so they neither wait for nor block runs.
        if not trial and any(other["pipeline_id"] == record["pipeline_id"] and other["request"]["mode"] != "trial"
                             and other["status"] in {"RUNNING", "CANCELLING", "UNKNOWN"} for other in others):
            return self._wait(record, "PIPELINE_RUN_ACTIVE")
        policy = self._policy(record)
        if policy is not None and sum(other["status"] in {"RUNNING", "CANCELLING", "UNKNOWN"}
                                      for other in others) >= policy["max_active_runs"]:
            # The space quota leaves an admitted run waiting; nothing is discarded.
            return self._wait(record, "SPACE_RUN_QUOTA")
        tasks = [task for task in definition["tasks"] if task["task_id"] in record["tasks"]]
        roots = sorted({binding["dataset"] for task in tasks
                        for binding in task["inputs"].values() if "dataset" in binding})
        if record.get("root_inputs") is not None:
            # A retried run keeps the original root watermarks of every task.
            return self._mutate(record, lambda updated: updated.update(status="RUNNING"))
        upstream = sorted({binding["task"] for task in tasks for binding in task["inputs"].values()
                           if "task" in binding}) if trial else []
        marks = self.writer.call(principal, configuration, "pipeline_watermarks",
                                 pipeline_id=record["pipeline_id"], datasets=roots or sorted(
                                     {next(value for value in definition["tasks"] if value["task_id"] == name)[
                                         "output"]["dataset"] for name in upstream})
                                 ) if roots or upstream else {"datasets": {}, "tasks": {}}
        releases = {}
        for name in upstream:
            # A trial reads each upstream task's current published output.
            releases[name] = (marks.get("tasks", {}).get(name) or {}).get("release_id")
            if releases[name] is None:
                return self._finish(record, "FAILED", "TRIAL_UPSTREAM_NOT_PUBLISHED", terminal_jobs=True)
        overrides = {}
        for name, value in record["request"].get("inputs", {}).items():
            task_id, alias = name.split(".", 1)
            dataset = next(task for task in definition["tasks"] if task["task_id"] == task_id)["inputs"][alias]["dataset"]
            overrides[dataset] = value["release_id"]
        bound = {}
        for dataset in roots:
            owner = configuration["datasets"][dataset]["definition"].get("write_owner")
            current = marks["datasets"][dataset]
            if owner == "ingestion":
                bound[dataset] = {"to_sequence": current["commit_sequence"]}
            elif owner == "pipeline":
                release = overrides.get(dataset, current["release_id"])
                if release is None:
                    return self._finish(record, "FAILED", "RUN_ROOT_RELEASE_NOT_PUBLISHED", terminal_jobs=True)
                bound[dataset] = {"release_id": release}
            else:
                # Direct tables are fixed by their own export transaction.
                bound[dataset] = {"commit_sequence": current["commit_sequence"]}

        def change(updated):
            updated.update(status="RUNNING", root_inputs=bound, started_unix=self.clock(), waiting=None,
                           waiting_unix=None)
            if trial:
                updated["trial"]["upstream"] = releases
            for task in tasks:
                job = updated["tasks"][task["task_id"]]
                if job["status"] == "BLOCKED" and (trial or not task.get("depends_on")):
                    job["status"] = "READY"
        return self._mutate(record, change)

    def _wait(self, record, code):
        """A queued run records why it has not started and since when; the reason is never inferred."""
        if record.get("waiting") == code:
            return record
        try:
            return self._mutate(record, lambda updated: updated.update(waiting=code, waiting_unix=self.clock()))
        except ControlError:
            return record

    def _finish(self, record, status, code, *, terminal_jobs=False):
        def change(updated):
            updated.update(status=status, error_code=code, finished_unix=self.clock())
            if terminal_jobs:
                for job in updated["tasks"].values():
                    if job["status"] not in TERMINAL_TASK and not job.get("epoch"):
                        job["status"] = "CANCELLED" if status == "CANCELLED" else "FAILED"
                        job["error_code"] = job.get("error_code") or code
        return self._mutate(record, change)

    def _conclude(self, record, definition):
        jobs = record["tasks"]
        states = {job["status"] for job in jobs.values()}
        if states == {"PUBLISHED"}:
            return self._finish(record, "PUBLISHED", None)
        if states == {"VALIDATED"}:
            return self._finish(record, "VALIDATED", None)
        if record["cancel_requested"] and states <= TERMINAL_TASK:
            # A job published before revocation stays PUBLISHED.
            return self._finish(record, "PUBLISHED" if states == {"PUBLISHED"} else "CANCELLED", None)
        failed = {name for name, job in jobs.items() if job["status"] == "FAILED"}
        if failed:
            blocked = {task["task_id"] for task in definition["tasks"]
                       if task["task_id"] in jobs and jobs[task["task_id"]]["status"] == "BLOCKED"}
            active = {name for name, job in jobs.items() if job["status"] not in TERMINAL_TASK} - blocked
            if not active:
                return self._finish(record, "FAILED", jobs[sorted(failed)[0]].get("error_code") or "RUN_TASK_FAILED")
        unknown = any(job["status"] == "UNKNOWN" for job in jobs.values())
        if unknown != (record["status"] == "UNKNOWN") and not record["cancel_requested"]:
            return self._mutate(record, lambda updated: updated.update(status="UNKNOWN" if unknown else "RUNNING"))
        return record

    # -- jobs --------------------------------------------------------------------------------------

    def _job_change(self, record, task_id, change, *, event=None, cancel=False):
        def apply(updated):
            if updated["cancel_requested"] and not cancel:
                # A concurrent cancellation wins; its own step revokes whatever
                # this step may already have created natively.
                raise ControlError(409, "RUN_CANCEL_REQUESTED")
            job = updated["tasks"][task_id]
            unknown = job["status"] == "UNKNOWN"
            change(job)
            if unknown and job["status"] not in {"UNKNOWN", "FAILED", "CANCELLED"}:
                # Reconciled: the projection leaves UNKNOWN with no stale reason.
                job.update(phase=None, transient=None, error_code=None)
            job["observed_unix"] = self.clock()
            if event:
                updated["events"] = (updated["events"] + [{"task_id": task_id, "unix": self.clock(), **event}])[-64:]
        return self._mutate(record, apply)

    def _fail_attempt(self, record, task, code, *, unknown=False):
        """The attempt ends; a later attempt bumps the native epoch, fencing it."""
        def change(job):
            attempt = job["attempts"][-1] if job["attempts"] else None
            if attempt is not None:
                attempt.update(status="UNKNOWN" if unknown else "FAILED", code=code, ended_unix=self.clock())
            if job["attempt_number"] < job["attempt_limit"] and job.get("export_id"):
                job.update(status="RETRY_WAIT", error_code=code)
            else:
                job.update(status="FAILED", error_code=code)
        return self._job_change(record, task["task_id"], change, event={"event": "ATTEMPT_ENDED", "code": code})

    def _step(self, record, task, context):
        job = record["tasks"][task["task_id"]]
        status = job["status"]
        try:
            if record["cancel_requested"]:
                return self._cancel_job(record, task, context)
            if status == "BLOCKED":
                upstream = [record["tasks"][name]["status"] for name in task.get("depends_on", [])]
                if all(value == "PUBLISHED" for value in upstream):
                    return self._job_change(record, task["task_id"], lambda value: value.update(status="READY")), True
                return None
            if status == "READY" or status == "UNKNOWN" and job.get("phase") == "READY":
                return self._prepare(record, task, context)
            if status in {"RETRY_WAIT", "DISPATCHING"} or status == "UNKNOWN" and job.get("phase") in {
                    "DISPATCHING", "RETRY_WAIT"}:
                return self._dispatch(record, task, context)
            if status in {"RUNNING"} or status == "UNKNOWN" and job.get("phase") == "RUNNING":
                return self._observe(record, task, context)
            if status == "STAGED" or status == "UNKNOWN" and job.get("phase") == "STAGED":
                if record.get("trial"):
                    return self._validate_trial(record, task, context)
                return self._import(record, task, context)
            if status in {"VALIDATING", "IMPORTING"} or status == "UNKNOWN" and job.get("phase") == "IMPORTING":
                return self._published(record, task, context)
        except WriterUnknown as error:
            # The original command may have executed; keep its phase and
            # reconcile with the same identity on the next step.
            def change(value):
                value.update(phase=value.get("phase") if value["status"] == "UNKNOWN" else value["status"],
                             status="UNKNOWN", error_code=error.code)
            try:
                return self._job_change(record, task["task_id"], change), False
            except ControlError:
                return None
        except ControlError as error:
            if error.code == "RUN_CANCEL_REQUESTED":
                return None
            if error.status in {429, 503}:
                return self._transient(record, task, error.code)
            if record["cancel_requested"]:
                # No native eligibility can be confirmed for a cancelled job.
                return self._job_change(record, task["task_id"], lambda value: value.update(
                    status="CANCELLED", error_code=error.code), cancel=True), True
            return self._job_change(record, task["task_id"], lambda value: value.update(
                status="FAILED", error_code=error.code)), True
        except ContractError:
            return self._job_change(record, task["task_id"], lambda value: value.update(
                status="FAILED", error_code="TASK_CONTRACT_INVALID")), True
        return None

    def _transient(self, record, task, code):
        """Writer/queue unavailability is retried; it is shown, never turned into FAILED.

        An unavailable writer authority makes the job's state UNKNOWN in the
        projection until the same phase can be reconciled; a full queue only
        records the transient condition.
        """
        def change(value):
            value["transient"] = {"code": code, "since": (value.get("transient") or {}).get("since", self.clock())}
            if code.startswith("WRITER_") and value["status"] != "UNKNOWN":
                value.update(phase=value["status"], status="UNKNOWN", error_code=code)
        try:
            return self._job_change(record, task["task_id"], change), False
        except ControlError:
            return None

    def _input_bindings(self, record, task, context, previous):
        configuration, principal = context["configuration"], context["principal"]
        outputs = {value["task_id"]: value["output"]["dataset"] for value in context["definition"]["tasks"]}
        mode = task["processing"]["mode"]
        bindings, processing = {}, {"mode": mode}
        compatible = self._compatible(record, task, context, previous)
        partitions = None
        if mode == "partition_recompute":
            # The writer recomputes partition impact incrementally only within
            # one pipeline version; any version change replaces every partition.
            compatible = compatible and previous.get("pipeline_version") == record["pipeline_version"]
            partitions = self._impact(record, task, context, previous, compatible)
            if isinstance(partitions, str):
                return partitions, None
        for alias, declared in task["inputs"].items():
            if "state" in declared:
                continue
            if "task" in declared:
                binding = {"dataset": outputs[declared["task"]],
                           "release_id": self._upstream_release(record, declared["task"])}
            else:
                dataset = declared["dataset"]
                root = record["root_inputs"][dataset]
                binding = {"dataset": dataset, **({"to_sequence": root["to_sequence"]} if "to_sequence" in root else {}),
                           **({"release_id": root["release_id"]} if "release_id" in root else {})}
            if declared["selection"] == "changes":
                # Rule/parameter change or no previous release: complete bootstrap
                # from the full committed history. Missing prior interval state
                # follows the task's declared on_missing_state policy.
                start, bootstrap = 0, not compatible
                if compatible:
                    prior = (previous.get("input_versions") or {}).get(alias) or {}
                    if prior.get("to_sequence") is None:
                        if task["processing"].get("on_missing_state") == "fail":
                            return "TASK_STATE_UNAVAILABLE", None
                        bootstrap = True
                    else:
                        start = prior["to_sequence"]
                binding.update(selection={"mode": "changes"}, from_sequence=start)
                processing.update(from_watermark={"ods_sequence": start},
                                  to_watermark={"ods_sequence": binding["to_sequence"]},
                                  bootstrap=bootstrap, state_release_id=None if bootstrap else previous["release_id"])
            elif declared["selection"] == "affected_partitions":
                binding["selection"] = {"mode": "affected_partitions", "partitions": partitions}
            else:
                binding["selection"] = {"mode": "snapshot"}
            bindings[alias] = binding
        for alias, declared in task["inputs"].items():
            if "state" in declared:
                changes = next(name for name, value in task["inputs"].items() if value["selection"] == "changes")
                bindings[alias] = {"dataset": task["output"]["dataset"], "selection": {"mode": "affected_keys"},
                                   "release_id": processing["state_release_id"], "keys_from": changes}
        if mode == "partition_recompute":
            processing["affected_partitions"] = partitions
        return bindings, processing

    @staticmethod
    def _upstream_release(record, task_id):
        if record.get("trial"):
            return record["trial"]["upstream"][task_id]
        return record["tasks"][task_id]["release_id"]

    def _compatible(self, record, task, context, previous):
        """Incremental state/impact bases are reusable only for identical rules.

        The previous release must come from the same task definition, entrypoint
        code and parameters. Otherwise the task is completely recomputed or
        re-bootstrapped; no old state is migrated across rule changes.
        """
        if previous is None or previous.get("task_id") != task["task_id"] \
                or previous.get("task_sha256") != digest(task) \
                or previous.get("parameters_sha256") != digest(record["parameters"]):
            return False
        if previous.get("pipeline_version") == record["pipeline_version"]:
            return True
        try:
            old = self.pipelines.bind_run(context["principal"], record["space_id"], record["pipeline_id"],
                                          previous["pipeline_version"])["snapshot"]
        except ControlError:
            return False
        code = {entry["path"]: entry["sha256"] for entry in self._bound["snapshot"]["artifacts"]}
        prior = {entry["path"]: entry["sha256"] for entry in old["artifacts"]}
        return (code.get(task["entrypoint"]) == prior.get(task["entrypoint"])
                and self._bound["snapshot"]["executors"].get(task["task_id"]) == old["executors"].get(task["task_id"])
                and self._bound["snapshot"]["environments"].get(task["task_id"])
                == old["environments"].get(task["task_id"]))

    def _impact(self, record, task, context, previous, compatible):
        """Affected partitions from upstream release differences, or a declared full fallback."""
        configuration, principal = context["configuration"], context["principal"]
        outputs = {value["task_id"]: value["output"]["dataset"] for value in context["definition"]["tasks"]}
        names = task["processing"]["partition_columns"]
        if record["request"]["mode"] == "backfill":
            return copy.deepcopy(record["request"]["partitions"])
        affected, unbounded = {}, not compatible
        prior_versions = (previous or {}).get("input_versions") or {}
        for alias, declared in task["inputs"].items():
            if "task" in declared:
                dataset, release = outputs[declared["task"]], self._upstream_release(record, declared["task"])
            else:
                dataset = declared["dataset"]
                release = record["root_inputs"][dataset].get("release_id")
            before = (prior_versions.get(alias) or {}).get("release_id") if compatible else None
            if declared["selection"] == "affected_partitions":
                result = self.writer.call(principal, configuration, "pipeline_impact", dataset=dataset,
                                          from_release=before, to_release=release, partition_columns=names)
                if not result["bounded"]:
                    unbounded = True
                else:
                    for part in result["partitions"]:
                        affected[canonical(part)] = part
            else:
                prior = prior_versions.get(alias) or {}
                root = record["root_inputs"].get(dataset, {}) if "dataset" in declared else {}
                if release is not None and release != prior.get("release_id"):
                    # A full upstream task republishes each run; only a snapshot
                    # whose rows actually changed lacks a declared impact mapping.
                    if prior.get("release_id") is None or self.writer.call(
                            principal, configuration, "pipeline_impact", dataset=dataset,
                            from_release=prior["release_id"], to_release=release, partition_columns=[]
                    )["partitions"]:
                        unbounded = True
                elif ("to_sequence" in root and root["to_sequence"] != prior.get("to_sequence")
                        or "commit_sequence" in root and root["commit_sequence"] != prior.get("commit_sequence")):
                    # A changed dimension/fact snapshot has no declared impact mapping.
                    unbounded = True
        if unbounded and previous is not None:
            if task["processing"]["on_unbounded_impact"] == "fail":
                return "TASK_IMPACT_UNBOUNDED"
            for entry in previous.get("partitions", []):
                affected[canonical(entry["partition"])] = entry["partition"]
            for alias, declared in task["inputs"].items():
                if declared["selection"] != "affected_partitions":
                    continue
                dataset = outputs[declared["task"]] if "task" in declared else declared["dataset"]
                release = (self._upstream_release(record, declared["task"]) if "task" in declared
                           else record["root_inputs"][dataset].get("release_id"))
                result = self.writer.call(principal, configuration, "pipeline_impact", dataset=dataset,
                                          from_release=None, to_release=release, partition_columns=names)
                if not result["bounded"]:
                    return "TASK_IMPACT_UNBOUNDED"
                for part in result["partitions"]:
                    affected[canonical(part)] = part
        return [affected[name] for name in sorted(affected)]

    def _prepare(self, record, task, context):
        """Fix target CAS, interval/state/impact and the job's input export once."""
        configuration, principal = context["configuration"], context["principal"]
        task_id = task["task_id"]
        job = record["tasks"][task_id]
        if not job.get("plan"):
            target = self.writer.call(principal, configuration, "publication_target",
                pipeline_id=record["pipeline_id"], pipeline_version=record["pipeline_version"], task_id=task_id)
            bindings, processing = self._input_bindings(record, task, context, target.get("previous"))
            if processing is None:
                return self._job_change(record, task_id, lambda value: value.update(
                    status="FAILED", error_code=bindings)), True
            if len(processing.get("affected_partitions", [])) > MAX_RECORD_PARTITIONS:
                return self._job_change(record, task_id, lambda value: value.update(
                    status="FAILED", error_code="TASK_IMPACT_RECORD_LIMIT")), True
            if task["processing"]["mode"] == "partition_recompute" and not processing["affected_partitions"]:
                # Nothing changed for this task: downstream reads its current release.
                def skip(value):
                    value.update(status="PUBLISHED", release_id=target["expected_previous_release"],
                                 skipped="NO_AFFECTED_PARTITIONS", plan={"processing": processing})
                return self._job_change(record, task_id, skip, event={"event": "SKIPPED"}), True
            # Partition lists are stored once, in processing, and expanded per call.
            stored = {alias: {**value, "selection": {"mode": value["selection"]["mode"]}}
                      if value["selection"]["mode"] == "affected_partitions" else value
                      for alias, value in bindings.items()}
            plan = {"bindings": stored, "processing": processing,
                    "expected_previous_release": target["expected_previous_release"]}
            record = self._job_change(record, task_id, lambda value: value.update(plan=plan))
            job = record["tasks"][task_id]
        plan = job["plan"]
        retention = max(600, min(2592000, task["timeout_seconds"] * (task["max_attempts"] + 1) + 3600))
        bindings = {alias: {**value, "selection": {"mode": "affected_partitions",
                                                   "partitions": plan["processing"]["affected_partitions"]}}
                    if value["selection"]["mode"] == "affected_partitions" else value
                    for alias, value in plan["bindings"].items()}
        try:
            sample = {"sample_rows": record["trial"]["sample_rows"]} if record.get("trial") else {}
            export = self.writer.call(principal, configuration, "input_prepare", client_request_id=job["job_id"],
                                      inputs=bindings, retention_seconds=retention, **sample)
        except WriterUnknown:
            raise
        except ControlError as error:
            if error.status in {429, 503}:
                return self._transient(record, task, error.code)
            return self._job_change(record, task_id, lambda value: value.update(
                status="FAILED", error_code=error.code)), True
        if export["state"] != "READY":
            return None
        self.writer.call(principal, configuration, "input_retain", export_id=export["export_id"],
                         consumer_id=job["job_id"], retain_until_unix=self.clock() + retention)
        versions = {alias: value["native_version"] for alias, value in export["manifest"]["inputs"].items()}

        def change(value):
            value.update(status="DISPATCHING", export_id=export["export_id"], input_versions=versions,
                         input_metrics=export.get("metrics"), phase=None, error_code=None, transient=None)
        return self._job_change(record, task_id, change, event={"event": "INPUTS_FIXED"}), True

    def _admission(self, context):
        principal = context["principal"]
        return TaskAdmission(self.pipelines,
            lambda actor, export_id: self.writer.call(actor, context["configuration"], "input_manifest",
                                                      export_id=export_id),
            self.objects(context["configuration"]["space_id"], context["configuration"]["generation"]),
            lambda actor, space, pipeline, version, task_id: self.writer.call(
                actor, context["configuration"], "publication_target", pipeline_id=pipeline,
                pipeline_version=version, task_id=task_id))

    def _descriptor(self, record, task, context, attempt_id):
        job = record["tasks"][task["task_id"]]
        objects = self.objects(record["space_id"], record["generation"])
        admission = self._admission(context)
        try:
            prepared = admission.prepare(context["principal"], record["space_id"], record["pipeline_id"],
                record["pipeline_version"], task["task_id"], run_id=record["run_id"], job_id=job["job_id"],
                attempt_id=attempt_id, export_id=job["export_id"], parameters=record["request"].get("parameters", {}),
                processing=job["plan"]["processing"])
            return prepared["descriptor"]
        except UploadUnknown as error:
            # The original key may hold the identical descriptor. Confirm it
            # by exact version/hash instead of writing a second descriptor.
            original = getattr(error, "original_upload", None) or {}
            key = descriptor_key({"space_id": record["space_id"], "generation": record["generation"],
                                  "run_id": record["run_id"], "job_id": job["job_id"], "attempt_id": attempt_id})
            return reconcile(objects, key, original.get("sha256"), MAX_DESCRIPTOR_BYTES)

    def _dispatch(self, record, task, context):
        task_id = task["task_id"]
        job = record["tasks"][task_id]
        attempt = job["attempts"][-1] if job["attempts"] else None
        configuration, principal = context["configuration"], context["principal"]
        if job["status"] == "RETRY_WAIT" or attempt is None or attempt.get("status") in {"FAILED", "UNKNOWN"}:
            if job["attempt_number"] >= job["attempt_limit"]:
                return self._job_change(record, task_id, lambda value: value.update(
                    status="FAILED", error_code=value.get("error_code") or "RUN_ATTEMPTS_EXHAUSTED")), True
            if job.get("input_released"):
                # A retried job ended once and released its pin: re-pin the same
                # original export before the writer can grant a new attempt.
                retention = max(600, min(2592000, task["timeout_seconds"] * (task["max_attempts"] + 1) + 3600))
                try:
                    self.writer.call(principal, configuration, "input_retain", export_id=job["export_id"],
                                     consumer_id=job["job_id"], retain_until_unix=self.clock() + retention)
                except WriterUnknown:
                    raise
                except ControlError as error:
                    if error.status in {429, 503}:
                        return self._transient(record, task, error.code)
                    return self._job_change(record, task_id, lambda value: value.update(
                        status="FAILED", error_code=error.code)), True
                record = self._job_change(record, task_id, lambda value: value.update(input_released=False))
                job = record["tasks"][task_id]
            number = job["attempt_number"] + 1
            attempt_id = "attempt_" + str(number)

            def add(value):
                value["attempts"].append({"attempt_id": attempt_id, "status": "PREPARING", "created_unix": self.clock()})
                value.update(attempt_number=number, status="DISPATCHING", phase=None)
            record = self._job_change(record, task_id, add, event={"event": "ATTEMPT", "attempt_id": attempt_id})
            job = record["tasks"][task_id]
            attempt = job["attempts"][-1]
        if "descriptor" not in attempt:
            descriptor = self._descriptor(record, task, context, attempt["attempt_id"])
            record = self._job_change(record, task_id, lambda value: value["attempts"][-1].update(descriptor=descriptor))
            job, attempt = record["tasks"][task_id], record["tasks"][task_id]["attempts"][-1]
        if "epoch" not in attempt and record.get("trial"):
            # A trial attempt is never eligible: no native job exists to import into.
            record = self._job_change(record, task_id, lambda value: value["attempts"][-1].update(
                epoch=0, status="UNGRANTED_TRIAL"))
            job, attempt = record["tasks"][task_id], record["tasks"][task_id]["attempts"][-1]
        if "epoch" not in attempt:
            try:
                granted = self.writer.call(principal, configuration, "publication_grant",
                    descriptor=attempt["descriptor"], input_export_id=job["export_id"], expected_epoch=job["epoch"])
            except WriterUnknown:
                raise
            except ControlError as error:
                if error.code == "PUBLICATION_ELIGIBILITY_CHANGED":
                    # Reconcile the original grant from the native ledger.
                    granted = self.writer.call(principal, configuration, "publication_status", job_id=job["job_id"])
                    if granted.get("status") == "PUBLISHED":
                        return self._published(record, task, context)
                    if granted["epoch"] <= job["epoch"]:
                        raise
                elif error.status in {429, 503}:
                    return self._transient(record, task, error.code)
                else:
                    return self._fail_attempt(record, task, error.code), True
            epoch = granted["epoch"]
            record = self._job_change(record, task_id, lambda value: (
                value.update(epoch=epoch), value["attempts"][-1].update(epoch=epoch, status="GRANTED")))
            job, attempt = record["tasks"][task_id], record["tasks"][task_id]["attempts"][-1]
        binding = self._bound["snapshot"]["executors"][task_id]
        dispatch = attempt.get("dispatch")
        timeout = task["timeout_seconds"]
        if dispatch is None:
            policy = self._policy(record)
            if policy is not None:
                dispatched = sum(job_value["status"] in DISPATCHED_TASK or job_value["status"] == "DISPATCHING"
                                 and (job_value["attempts"] or [{}])[-1].get("dispatch") is not None
                                 for other in [*self._space_runs(record), record]
                                 for job_value in other["tasks"].values())
                if dispatched >= policy["max_dispatched_jobs"]:
                    # Admitted work waits for space task capacity; it is not rejected.
                    return self._transient(record, task, "SPACE_TASK_QUOTA")
            # Persist the dispatch intent before any non-idempotent call.
            record = self._job_change(record, task_id, lambda value: value["attempts"][-1].update(
                dispatch={"state": "DISPATCHING", "executor": binding["executor"], "unix": self.clock()}))
            job, attempt = record["tasks"][task_id], record["tasks"][task_id]["attempts"][-1]
            dispatch = attempt["dispatch"]
        if dispatch["state"] == "DISPATCHING":
            if self.clock() - dispatch["unix"] > DISPATCH_SECONDS and binding["executor"] != "lambda":
                return self._fail_attempt(record, task, "DISPATCH_UNCONFIRMED"), True
            if binding["executor"] == "lambda" and dispatch.get("attempted"):
                # An earlier Invoke reply was lost: it may run. Only wait for its
                # completion or deadline; Lambda gives no idempotent re-invoke.
                return self._running(record, task, {**dispatch, "state": "UNKNOWN_INVOKE"}, timeout)
            try:
                if binding["executor"] == "lambda":
                    record = self._job_change(record, task_id, lambda value: value["attempts"][-1]["dispatch"].update(
                        attempted=True))
                    identity = self.dispatcher.lambda_invoke(binding, attempt["descriptor"])
                elif binding["executor"] == "ecs_task":
                    identity = self.dispatcher.ecs_run(binding, attempt["descriptor"], {
                        "Project": "colvenna-isle", "ColvennaRun": record["run_id"][:120], "ColvennaJob": job["job_id"][:120],
                        "ColvennaAttempt": attempt["attempt_id"]})
                else:
                    worker = self.writer.call(principal, configuration, "writer_task_execute",
                                              descriptor=attempt["descriptor"])
                    identity = {"executor": "writer", "worker": worker.get("state"),
                                "process_nonce": configuration["space_id"] + ":" + job["job_id"]}
            except DispatchUnknown as error:
                if binding["executor"] == "ecs_task":
                    from .dispatch import token
                    found = self.dispatcher.ecs_find(token(attempt["descriptor"]))
                    if len(found) == 1:
                        identity = {"executor": "ecs_task", "task_arn": found[0], "token": token(attempt["descriptor"]),
                                    "task_definition_arn": binding["task_definition_arn"]}
                    elif not found:
                        return None  # Retry the same clientToken next step.
                    else:
                        return self._fail_attempt(record, task, "ECS_DUPLICATE_DISPATCH", unknown=True), True
                else:
                    return self._running(record, task, {**dispatch, "state": "UNKNOWN_INVOKE",
                                                        "code": error.code}, timeout)
            except WriterUnknown:
                raise
            except ControlError as error:
                if error.status in {429, 503}:
                    return self._transient(record, task, error.code)
                return self._fail_attempt(record, task, error.code), True
            return self._running(record, task, {**identity, "state": "DISPATCHED"}, timeout)
        return None

    def _running(self, record, task, dispatch, timeout):
        now = self.clock()
        grace = WRITER_GRACE_SECONDS if task["executor"] == "writer" else GRACE_SECONDS

        def change(job):
            attempt = job["attempts"][-1]
            attempt.update(dispatch={**dispatch, "unix": attempt.get("dispatch", {}).get("unix", now)},
                           dispatched_unix=attempt.get("dispatched_unix", now), status="RUNNING")
            attempt["deadline_unix"] = attempt["dispatched_unix"] + timeout + grace
            job.update(status="RUNNING", phase=None)
        return self._job_change(record, task["task_id"], change, event={"event": "DISPATCHED"}), True

    def _observe(self, record, task, context):
        task_id = task["task_id"]
        job = record["tasks"][task_id]
        attempt = job["attempts"][-1]
        descriptor = {"space_id": record["space_id"], "generation": record["generation"],
                      "pipeline_id": record["pipeline_id"], "run_id": record["run_id"],
                      "job_id": job["job_id"], "attempt_id": attempt["attempt_id"]}
        try:
            found = self.dispatcher.completions(descriptor)
        except DispatchUnknown:
            return None
        dispatch = attempt["dispatch"]
        if found:
            selected = found[0]

            def staged(value):
                attempt_value = value["attempts"][-1]
                invocation = selected["invocation"] or {}
                usage = {"window_seconds": round(max(0.0, self.clock() - attempt_value.get(
                    "dispatched_unix", self.clock())), 3),
                    **{name: invocation[name] for name in ["elapsed_ms", "memory_mb"] if name in invocation}}
                attempt_value.update(status="STAGED", execution_id=selected["execution_id"],
                                     completion=selected["reference"], candidates=len(found),
                                     invocation=selected["invocation"], usage=usage, staged_unix=self.clock())
                value.update(status="STAGED", phase=None)
            return self._job_change(record, task_id, staged, event={
                "event": "STAGED", "execution_id": selected["execution_id"], "candidates": len(found)}), True
        if dispatch.get("executor") == "ecs_task" and dispatch.get("task_arn"):
            observed = self.dispatcher.ecs_observe(dispatch["task_arn"])
            if observed["last_status"] == "STOPPED" and self.dispatcher.completions(descriptor):
                return None  # Completed between the two observations; select it next step.
            if observed["last_status"] == "STOPPED":
                # Exit code and staging decide together; a late completion is
                # still found above before this attempt is ended.
                code = "ECS_TASK_EXIT_" + str(observed.get("exit_code")) if observed.get("exit_code") not in {0} \
                    else "ECS_TASK_STOPPED_WITHOUT_COMPLETION"
                return self._fail_attempt(record, task, code), True
        if dispatch.get("executor") == "lambda":
            for note in attempt.get("notifications", []):
                # The adapter's own FAILED result, or Lambda's OnFailure record,
                # for this exact invocation: nothing was staged by it.
                if (note.get("source") == "lambda" and note.get("request_id") == dispatch.get("request_id")
                        and note.get("status") in {"FAILED", "UNKNOWN"}):
                    return self._fail_attempt(record, task, note.get("code") or "LAMBDA_TASK_FAILED",
                                              unknown=note.get("status") == "UNKNOWN"), True
        if dispatch.get("executor") == "writer":
            worker = self.writer.call(context["principal"], context["configuration"], "writer_task_status",
                                      descriptor=attempt["descriptor"])
            if worker.get("state") == "FAILED":
                return self._fail_attempt(record, task, worker.get("code") or "WRITER_TASK_FAILED"), True
        if self.clock() >= attempt["deadline_unix"]:
            if dispatch.get("executor") == "ecs_task" and dispatch.get("task_arn"):
                self.dispatcher.ecs_stop(dispatch["task_arn"], "Colvenna Isle attempt deadline")
            return self._fail_attempt(record, task, "TASK_ATTEMPT_DEADLINE",
                                      unknown=dispatch.get("state") == "UNKNOWN_INVOKE"), True
        return None

    def _import(self, record, task, context):
        task_id = task["task_id"]
        job = record["tasks"][task_id]
        attempt = job["attempts"][-1]
        try:
            result = self.writer.call(context["principal"], context["configuration"], "publication_import",
                job_id=job["job_id"], eligibility_epoch=attempt["epoch"], attempt_id=attempt["attempt_id"],
                execution_id=attempt["execution_id"], completion=attempt["completion"])
        except WriterUnknown:
            raise
        except ControlError as error:
            if error.status in {429, 503}:
                return self._transient(record, task, error.code)
            return self._fail_attempt(record, task, error.code), True
        if result.get("status") == "PUBLISHED":
            return self._published(record, task, context, result)
        return self._job_change(record, task_id, lambda value: value.update(status="IMPORTING", phase=None)), True

    def _validate_trial(self, record, task, context):
        """Verify the staged candidate exactly as an import would, without any native job."""
        task_id = task["task_id"]
        attempt = record["tasks"][task_id]["attempts"][-1]
        try:
            result = self.writer.call(context["principal"], context["configuration"], "publication_preview",
                                      descriptor=attempt["descriptor"], execution_id=attempt["execution_id"],
                                      completion=attempt["completion"], rows=0)
        except WriterUnknown:
            raise
        except ControlError as error:
            if error.status in {429, 503}:
                return self._transient(record, task, error.code)
            raise
        validation = {**result["validation"], "observed_at": stamp(self.clock())}
        passed = validation["status"] == "PASSED"

        def change(value):
            value.update(status="VALIDATED" if passed else "FAILED", phase=None, transient=None,
                         error_code=None if passed else validation.get("error_code"))
            value["attempts"][-1]["status"] = "VALIDATED" if passed else "FAILED"
        record = self._job_change(record, task_id, change, event={"event": "TRIAL_VALIDATED", "passed": passed})
        return self._mutate(record, lambda updated: updated["trial"].update(validation=validation)), True

    def _published(self, record, task, context, result=None):
        task_id = task["task_id"]
        job = record["tasks"][task_id]
        configuration, principal = context["configuration"], context["principal"]
        if result is None or result.get("status") != "PUBLISHED":
            result = self.writer.call(principal, configuration, "publication_status", job_id=job["job_id"])
        if result.get("status") == "PUBLISHED":
            release = result["release"]
            try:
                self.writer.call(principal, configuration, "input_release", export_id=job["export_id"],
                                 consumer_id=job["job_id"])
                released = True
            except ControlError:
                released = False

            def change(value):
                value.update(status="PUBLISHED", release_id=release["release_id"], phase=None, error_code=None,
                             input_released=released, commit_sequence=release["commit_sequence"], transient=None)
                value["import_metrics"] = {"row_count": release.get("row_count"),
                                           "copied_rows": (release.get("base") or {}).get("copied_rows"),
                                           "copy_elapsed_ms": (release.get("base") or {}).get("elapsed_ms"),
                                           "import_elapsed_ms": (result.get("worker") or {}).get("elapsed_ms")}
                if value.get("progress"):
                    value["progress"] = {**value["progress"], "state": "COMPLETE", "observed_at": stamp(self.clock())}
                if value["attempts"]:
                    value["attempts"][-1]["status"] = "PUBLISHED"
            # A committed publication is authoritative even if cancellation raced it.
            return self._job_change(record, task_id, change, cancel=True, event={
                "event": "PUBLISHED", "release_id": release["release_id"]}), True
        worker = result.get("worker") or {}
        if worker and "finished_unix" not in worker:
            progress = {"state": worker.get("state", "UNKNOWN"), "imported_chunks": worker.get("imported_chunks", 0),
                        "total_chunks": worker.get("total_chunks"), "observed_at": stamp(self.clock()),
                        "error_code": worker.get("error_code")}
            previous = job.get("progress") or {}
            if any(previous.get(name) != progress[name] for name in ["state", "imported_chunks", "total_chunks"]):
                record = self._job_change(record, task_id, lambda value: value.update(progress=progress))
                job = record["tasks"][task_id]
        if result.get("status") == "REVOKED":
            return self._job_change(record, task_id, lambda value: value.update(status="CANCELLED"), cancel=True), True
        if worker.get("state") == "REJECTED" and worker.get("error_code") not in {
                "PUBLICATION_QUEUE_FULL", "PUBLICATION_EXECUTOR_QUOTA", "SPACE_CONFIGURATION_CHANGED"}:
            return self._fail_attempt(record, task, worker.get("error_code") or "PUBLICATION_REJECTED"), True
        if not worker or "finished_unix" in worker:
            # No live worker (writer replaced, or a transient/unknown worker end):
            # resume the same original selected execution; the native ledger
            # makes this idempotent and never imports a second candidate.
            if worker.get("state") == "UNKNOWN" and job["status"] != "UNKNOWN":
                record = self._job_change(record, task_id, lambda value: value.update(
                    status="UNKNOWN", phase="IMPORTING", error_code=worker.get("error_code")))
            return self._import(record, task, context)
        if job["status"] == "UNKNOWN":
            return self._job_change(record, task_id, lambda value: value.update(status="IMPORTING", phase=None)), True
        return None

    def _cancel_job(self, record, task, context):
        task_id = task["task_id"]
        job = record["tasks"][task_id]
        configuration, principal = context["configuration"], context["principal"]
        attempt = job["attempts"][-1] if job["attempts"] else {}
        if not job.get("epoch") and "descriptor" not in attempt:
            # No descriptor was ever written, so no native eligibility exists.
            return self._job_change(record, task_id, lambda value: value.update(status="CANCELLED"),
                                    cancel=True), True
        if not attempt.get("stop_requested") and attempt.get("descriptor") and job["executor"] == "ecs_task":
            from .dispatch import token
            arns = set(self.dispatcher.ecs_find(token(attempt["descriptor"])))
            if (attempt.get("dispatch") or {}).get("task_arn"):
                arns.add(attempt["dispatch"]["task_arn"])
            stopped = all(self.dispatcher.ecs_stop(arn, "Colvenna Isle run cancelled") for arn in sorted(arns))
            record = self._job_change(record, task_id, lambda value: value["attempts"][-1].update(
                stop_requested=stopped), cancel=True)
        try:
            current = self.writer.call(principal, configuration, "publication_status", job_id=job["job_id"])
        except ControlError as error:
            if error.code != "PUBLICATION_JOB_NOT_FOUND":
                raise
            # The grant never committed; with cancellation recorded, none will.
            return self._job_change(record, task_id, lambda value: value.update(status="CANCELLED"),
                                    cancel=True), True
        if current["status"] == "PUBLISHED":
            return self._published(record, task, context, current)
        result = self.writer.call(principal, configuration, "publication_revoke", job_id=job["job_id"],
                                  eligibility_epoch=current["epoch"])
        if result["status"] == "PUBLISHED":
            return self._published(record, task, context, {"status": "PUBLISHED", "release": result["release"]})

        def change(value):
            value.update(status="CANCELLED", epoch=result["epoch"], phase=None)
            if value["attempts"]:
                value["attempts"][-1]["status"] = "REVOKED"
        return self._job_change(record, task_id, change, cancel=True,
                                event={"event": "REVOKED", "epoch": result["epoch"]}), True
