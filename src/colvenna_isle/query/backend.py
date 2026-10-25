"""Authenticated HTTP operations and independent shared-queue execution."""

import json
import shutil
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import quote

from colvenna_isle.contracts.validation import validate
from colvenna_isle.control.registry import ControlError
from colvenna_isle.timing import Timing
from .executor import execute
from .jobs import TERMINAL


class Backend:
    def __init__(self, keys, registry, jobs, results, identity, directory, api_url, *,
                 executor=execute, emit=lambda value: None, ecs=None, cluster=None):
        self.keys, self.registry, self.jobs, self.results = keys, registry, jobs, results
        self.identity, self.directory, self.api_url = identity, Path(directory), api_url.rstrip("/")
        self.execute, self.emit = executor, emit
        self.ecs, self.cluster = ecs, cluster
        self.last_reconcile = 0.0
        self.last_collect = 0.0
        self.stopping = threading.Event()
        self.slots = threading.BoundedSemaphore(2)
        self.threads = []
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)

    def guard(self, actor, space, generation):
        if self.stopping.is_set():
            raise ControlError(503, "QUERY_EXECUTOR_STOPPING")
        if self.keys.authenticate_id(actor.key_id) != actor:
            raise ControlError(403, "FORBIDDEN")
        current = self.registry._read(space)
        self.registry._authorize_generation(actor, current)
        if current["generation"] != generation:
            raise ControlError(403, "SPACE_GENERATION_CHANGED")

    def catalog(self, actor, configuration):
        if not self.slots.acquire(timeout=0.2):
            raise ControlError(429, "QUERY_EXECUTOR_QUOTA")
        path = self.directory / ("catalog_" + uuid.uuid4().hex)
        try:
            value = self.execute(configuration, [], path, timeout=15, catalog_only=True,
                guard=lambda: self.guard(actor, configuration["space_id"], configuration["generation"]))
            if value["terminal"]["outcome"] != "SUCCEEDED":
                raise ControlError(503, (value.get("failure") or {}).get("code", "NATIVE_CATALOG_UNAVAILABLE"))
            result = {"datasets": value["result"]["catalog"]}
            validate("catalog", result)
            return result
        finally:
            terminal = path / "process/terminal.json"
            if terminal.exists() and json.loads(terminal.read_text())["state"] in {"REAPED", "NOT_STARTED"}:
                shutil.rmtree(path)
            self.slots.release()

    def submit(self, actor, configuration, request):
        validate("query_request", request)
        if "client_request_id" not in request:
            raise ControlError(400, "CLIENT_REQUEST_ID_REQUIRED")
        guard = lambda: self.guard(actor, configuration["space_id"], configuration["generation"])
        job = self.jobs.admit(actor, configuration, request, request["client_request_id"], guard)
        return {"query_id": job["query_id"], "status": job["status"],
                "status_url": f"/v1/spaces/{job['space_id']}/queries/{job['query_id']}"}

    @staticmethod
    def summary(job):
        now = job.get("finished_unix", time.time())
        result = {"query_id": job["query_id"], "status": job["status"],
            "source_commit": job["source_commit"], "view": "metadata",
            "elapsed_ms": max(0, round((now - job.get("started_unix", job["created_unix"])) * 1000))}
        if "execution_id" in job:
            result["execution_id"] = job["execution_id"]
        code = job.get("error_code") or job.get("terminal", {}).get("error_code")
        if code:
            result["error_code"] = code
        return result

    def status(self, actor, configuration, identity, options):
        job = self.jobs.get(actor, configuration["space_id"], configuration["generation"], identity)
        result = self.summary(job)
        guard = lambda: self.guard(actor, job["space_id"], job["generation"])
        if options.get("view") in {"result", "sql"} or "page_token" in options or "page_size" in options:
            self.jobs._authorize(actor, job, data=True)
        if options.get("view") == "sql":
            self.jobs._authorize(actor, job, data=True)
            result["sql"] = self.jobs.input(job)["request"]["sql"]
            result["view"] = "sql"
        elif options.get("view") != "metadata" and job["status"] == "SUCCEEDED" \
                and actor.principal_id == job["principal_id"]:
            result["view"] = "result"
            result.update(self.results.page(actor, job, token=options.get("page_token"),
                                           size=int(options.get("page_size", "500")), guard=guard))
            token = self.results.export_token(actor, job, guard=guard)
            result["export_url"] = (self.api_url + f"/v1/spaces/{job['space_id']}/queries/{identity}"
                                    + "/export?token=" + quote(token))
        guard()
        return result

    def cancel(self, actor, configuration, identity):
        job = self.jobs.cancel(actor, configuration["space_id"], configuration["generation"], identity)
        return {"query_id": job["query_id"], "status": job["status"]}

    def history(self, actor, configuration):
        values = self.jobs.history(actor, configuration["space_id"], configuration["generation"])
        self.guard(actor, configuration["space_id"], configuration["generation"])
        return {"queries": [self.summary(job) for job in values]}

    def export(self, actor, configuration, identity, token):
        job = self.jobs.get(actor, configuration["space_id"], configuration["generation"], identity, data=True)
        return self.results.export(actor, job, token,
            guard=lambda: self.guard(actor, job["space_id"], job["generation"]))

    def start(self):
        for index in range(2):
            thread = threading.Thread(target=self._loop, name="query-worker-" + str(index), daemon=True)
            self.threads.append(thread)
            thread.start()

    def close(self):
        self.stopping.set()
        for thread in self.threads:
            thread.join(timeout=8)

    def _loop(self):
        while not self.stopping.is_set():
            if not self.slots.acquire(blocking=False):
                self.stopping.wait(0.1)
                continue
            idle, delay = False, 0.0
            try:
                job = self.jobs.claim(self.identity)
                if job is not None:
                    self._run(job)
                else:
                    idle, delay = True, 0.2
            except Exception:
                self.emit({"event": "QUERY_WORKER_OBSERVATION_UNAVAILABLE"})
                delay = 0.5
            finally:
                self.slots.release()
            if idle:
                try:
                    self._collect()
                except Exception:
                    self.emit({"event": "QUERY_WORKER_OBSERVATION_UNAVAILABLE"})
                    delay = 0.5
            if delay:
                self.stopping.wait(delay)

    def _run(self, admitted):
        holder = {"job": admitted}
        path = self.directory / admitted["execution_id"]
        called, reference, terminal = False, None, None
        timing = Timing("query_execution", query_id=admitted["query_id"], execution_id=admitted["execution_id"],
                        space_id=admitted["space_id"], generation=admitted["generation"])
        outcome = "UNKNOWN"

        def job():
            current = self.jobs._get(self.jobs.key(admitted["space_id"], admitted["generation"],
                                                  admitted["query_id"]))
            self.jobs._execution(current, admitted)
            return current

        try:
            with timing.segment("authorize"):
                original = self.jobs.input(admitted)
                actor = self.keys.authenticate_id(original["key_id"])
                if actor.principal_id != admitted["principal_id"]:
                    raise ControlError(403, "QUERY_OWNER_REQUIRED")
                self.guard(actor, admitted["space_id"], admitted["generation"])
            cached = [0.0]

            def guard():
                if self.stopping.is_set():
                    raise ControlError(503, "QUERY_EXECUTOR_STOPPING")
                if time.monotonic() - cached[0] >= 0.25:
                    self.guard(actor, admitted["space_id"], admitted["generation"])
                    cached[0] = time.monotonic()

            def started(identity):
                holder["job"] = self.jobs.attach_process(admitted, identity)
                self.emit({"event": "QUERY_JOB_STARTED", "query_id": admitted["query_id"],
                           "execution_id": admitted["execution_id"]})

            request = original["request"]
            timing.bind(consistency=request.get("consistency", "published"))
            called = True
            with timing.segment("process"):
                output = self.execute(original["configuration"], [
                    {"sql": request["sql"], "max_rows": request.get("max_rows", 1000)}],
                    path, timeout=request.get("timeout_ms", 30000) / 1000,
                    consistency=request.get("consistency", "published"),
                    condition={name: request[name] for name in ["release_id", "write_id", "ingestion_id"]
                               if name in request},
                    source={name: admitted[name] for name in [
                        "space_id", "generation", "key_id", "principal_id", "query_id", "execution_id"]},
                    guard=guard, cancel=lambda: job()["status"] == "CANCELLING", on_started=started)
            timing.adopt((output.get("result") or {}).get("timing"), "process")
            terminal = output["terminal"]
            code = None
            if terminal["outcome"] == "SUCCEEDED":
                self.guard(actor, admitted["space_id"], admitted["generation"])
                if job()["status"] != "CANCELLING":
                    value = output["result"]["results"][0]
                    with timing.segment("result_publish"):
                        prepared = self.results.prepare(holder["job"], value, terminal)
                        holder["job"] = self.jobs.reserve_result(holder["job"], terminal, prepared["intent"])
                        reference = self.results.publish(holder["job"], value, terminal, prepared=prepared)
            else:
                code = (output.get("failure") or {}).get("code") or terminal.get("error_code") or {
                    "TIMED_OUT": "QUERY_TIMEOUT", "QUOTA_EXCEEDED": "QUERY_DISK_QUOTA",
                    "CANCELLED": "QUERY_CANCELLED"}.get(terminal["outcome"], "QUERY_EXECUTION_FAILED")
            expiry = self.results._read(holder["job"], reference)["expires_unix"] if reference else None
            self.guard(actor, admitted["space_id"], admitted["generation"])
            with timing.segment("finish"):
                finished = self.jobs.finish(holder["job"], terminal, reference,
                                            failure_code=code, expires_unix=expiry)
            outcome = finished["status"] + (":" + code if code else "")
            if reference and finished["status"] != "SUCCEEDED":
                self.results.discard_unpublished(finished, reference)
                self.jobs.release_result(finished)
            self.emit({"event": "QUERY_JOB_FINISHED", "query_id": admitted["query_id"],
                       "status": finished["status"]})
        except Exception as error:
            code = error.code if isinstance(error, ControlError) else "QUERY_EXECUTION_UNCONFIRMED"
            outcome = "ERROR:" + code
            proof = path / "process/terminal.json"
            if terminal is None and proof.exists():
                terminal = json.loads(proof.read_text())
            if not called:
                terminal = {"state": "NOT_STARTED", "outcome": "LAUNCH_FAILED", "pid": None, "exit_code": None}
            if terminal is not None and terminal["state"] in {"REAPED", "NOT_STARTED"}:
                try:
                    finished = self.jobs.finish(holder["job"], terminal, failure_code=code)
                    if reference and finished["status"] != "SUCCEEDED":
                        self.results.discard_unpublished(finished, reference)
                        self.jobs.release_result(finished)
                except Exception:
                    self.jobs.mark_unknown(admitted, code)
            else:
                self.jobs.mark_unknown(admitted, code)
        finally:
            timing.emit(outcome, "query")
            current = job()
            if current["status"] in TERMINAL and path.exists():
                shutil.rmtree(path)

    def _collect(self):
        if time.monotonic() - self.last_reconcile >= 5:
            self.last_reconcile = time.monotonic()
            for job in self.jobs.running_jobs():
                if job["executor"] == self.identity:
                    if job["status"] == "UNKNOWN":
                        path = self.directory / job["execution_id"]
                        proof = path / "process/terminal.json"
                        if proof.exists():
                            terminal = json.loads(proof.read_text())
                            if terminal["state"] == "REAPED" and job["process"] is None:
                                original = json.loads((path / "process/identity.json").read_text())
                                self.jobs.attach_process(job, original)
                            finished = self.jobs.finish(job, terminal,
                                failure_code=job.get("error_code", "QUERY_EXECUTION_UNCONFIRMED"))
                            if finished["status"] in TERMINAL:
                                shutil.rmtree(path)
                    continue
                if self.ecs is None:
                    continue
                response = self.ecs.describe_tasks(cluster=self.cluster, tasks=[job["executor"]["task_arn"]])
                tasks = response.get("tasks", [])
                if len(tasks) == 1 and not response.get("failures") and tasks[0]["lastStatus"] == "STOPPED":
                    self.jobs.reconcile_stopped(job, tasks[0])
            # Controlled reads can outlive a reaped query child. Their own
            # producer must close the read or be positively stopped before quota
            # is released; a query terminal or elapsed deadline is insufficient.
            if self.ecs is not None:
                for job in self.jobs.source_reads():
                    response = self.ecs.describe_tasks(cluster=self.cluster,
                        tasks=[job["source_read"]["producer"]["task_arn"]])
                    tasks = response.get("tasks", [])
                    if len(tasks) == 1 and not response.get("failures") and tasks[0]["lastStatus"] == "STOPPED":
                        self.jobs.source_stopped(job, tasks[0])
        for entry in self.jobs.expired_results()[:4]:
            job = self.jobs._get(self.jobs.key(entry["space_id"], entry["generation"], entry["query_id"]))
            if self.results.collect(job):
                self.jobs.release_result(job)
        if time.monotonic() - self.last_collect >= 30:
            self.last_collect = time.monotonic()
            for entry in self.jobs.expired_history():
                job = self.jobs._get(self.jobs.key(entry["space_id"], entry["generation"], entry["query_id"]))
                source = job.get("source_read", {})
                if source.get("state") == "CAPTURING":
                    if self.ecs is None:
                        continue
                    observation = self.ecs.describe_tasks(cluster=self.cluster,
                                                         tasks=[source["producer"]["task_arn"]])
                    tasks = observation.get("tasks", [])
                    if len(tasks) != 1 or observation.get("failures") or tasks[0]["lastStatus"] != "STOPPED":
                        continue
                    job = self.jobs.source_stopped(job, tasks[0])
                if "result" in job or "result_intent" in job:
                    if not self.results.collect(job):
                        continue
                    self.jobs.release_result(job)
                self.jobs.collect_history(job)
