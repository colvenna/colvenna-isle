"""Actual local Chromium with a synthetic, contract-checked Admin API; not AWS acceptance.

The fixture routes requests through the contract's own route table, validates every
request body against its request schema and every fixture reply against its response
schema, so a console request that does not conform is recorded as a violation.
"""
import copy
import hashlib
import importlib.util
import json
import re
import socket
import threading
import time
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

from playwright.sync_api import expect, sync_playwright

from colvenna_isle.contracts.model import (
    ALTERNATE_SCOPES, AUDIT_ACTIONS, QUERY_PARAMETERS, RETENTION_POLICY, ROUTES, SCOPES)
from colvenna_isle.contracts.validation import validate

ROOT = Path(__file__).resolve().parents[2]
WEB = ROOT / "infra/web"
DEPLOYMENT_SPEC = importlib.util.spec_from_file_location("admin_browser_template", ROOT / "infra/template.py")
DEPLOYMENT = importlib.util.module_from_spec(DEPLOYMENT_SPEC)
DEPLOYMENT_SPEC.loader.exec_module(DEPLOYMENT)
CSP = DEPLOYMENT.template()["Resources"]["Headers"]["Properties"]["ResponseHeadersPolicyConfig"][
    "SecurityHeadersConfig"]["ContentSecurityPolicy"]["ContentSecurityPolicy"]
NOW = "2026-10-18T02:00:00Z"
GENERATION = "gen_00112233445566778899aabbccddeeff"
KEYS = {"fixture_admin": set(SCOPES), "fixture_monitor": {"monitor:read"},
        "fixture_runner": {"pipeline:manage", "pipeline:run", "monitor:read"}}
HTML = "<img src=x onerror=window.PWNED=1>"
TABLE = []
for _method, _suffix, _request, _response, _status, _scope, _conditional in ROUTES:
    TABLE.append((_method, re.compile("^/v1" + re.sub(r"\{[^}]+\}", "([^/]+)", _suffix) + "$"), _suffix,
                  _request, _response, _status, [_scope] + ALTERNATE_SCOPES.get((_method, _response), []),
                  _conditional))

DEFINITION = {
    "spec_version": 1, "pipeline_id": "orders", "name": "Orders",
    "schedule": {"owner": "colvenna", "expression": "rate(1 hour)", "timezone": "UTC"},
    "max_active_runs": 1, "coalesce_pending": True,
    "parameters": {"high_value_threshold": {"type": "decimal_string", "default": "500.00", "minimum": "0"}},
    "tasks": [
        {"task_id": "clean_orders", "language": "sql", "processing": {"mode": "full"}, "executor": "writer",
         "entrypoint": "sql/clean_orders.sql", "resource_profile": "writer_short", "timeout_seconds": 30,
         "max_attempts": 3, "inputs": {"events": {"dataset": "ods.orders", "selection": "snapshot"}},
         "output": {"dataset": "dwd.orders", "write_mode": "versioned_replace", "key_columns": ["order_id"],
                    "allow_empty": True, "schema": [{"name": "order_id", "type": "BIGINT", "nullable": False}]}},
        {"task_id": "score_orders", "language": "python", "processing": {"mode": "full"}, "executor": "lambda",
         "entrypoint": "python/score_orders.py", "runtime_environment": {"environment_id": "pandas-demo", "version": "v1"},
         "resource_profile": "python_lambda_standard", "input_mode": "local_files", "timeout_seconds": 300,
         "max_attempts": 3, "depends_on": ["clean_orders"],
         "inputs": {"orders": {"task": "clean_orders", "selection": "snapshot"}},
         "output": {"dataset": "dwd.order_scores", "write_mode": "versioned_replace", "key_columns": ["order_id"],
                    "allow_empty": True, "schema": [{"name": "order_id", "type": "BIGINT", "nullable": False}]}},
    ]}
DEMO = {"spec_version": 1, "pipeline_id": "demo", "name": "Demo", "schedule": {"owner": "none"},
        "max_active_runs": 1, "coalesce_pending": True,
        "tasks": [{"task_id": "demo_task", "language": "sql", "processing": {"mode": "full"}, "executor": "writer",
                   "entrypoint": "sql/demo.sql", "resource_profile": "writer_short", "timeout_seconds": 30,
                   "max_attempts": 1, "inputs": {"source": {"dataset": "ods.orders", "selection": "snapshot"}},
                   "output": {"dataset": "dwd.demo", "write_mode": "versioned_replace", "key_columns": ["id"],
                              "allow_empty": True, "schema": [{"name": "id", "type": "BIGINT", "nullable": False}]}}]}
SOURCES = {"sql/clean_orders.sql": "SELECT order_id FROM events;\n",
           "python/score_orders.py": "def run(context):\n    return None\n", "assets/model.bin": None}


def error(code, message="Rejected by fixture"):
    return {"code": code, "message": message, "request_id": "req_fixture"}


def sha(value):
    return hashlib.sha256(value.encode()).hexdigest()


def revision():
    return "rev_" + uuid.uuid4().hex


def observation(status, observed, age, source, metrics, reason=None):
    value = {"status": status, "observed_at": observed, "age_seconds": age, "source": source, "metrics": metrics}
    if reason:
        value["reason"] = reason
    return value


def task(task_id, status, executor, attempt_number=1, **extra):
    value = {"task_id": task_id, "job_id": "job_" + task_id, "status": status, "executor": executor,
             "attempt_number": attempt_number, "eligibility_epoch": 1}
    value.update(extra)
    return value


def attempt(identity, status, code=None, **extra):
    value = {"attempt_id": identity, "status": status, "created_at": "2026-10-18T01:50:00Z",
             "dispatched_at": "2026-10-18T01:50:01Z", "ended_at": "2026-10-18T01:51:00Z", "deadline_at": None,
             "dispatch_id": "dispatch_" + identity, "execution_id": "exec_" + identity, "eligibility_epoch": 1,
             "code": code, "usage": {}}
    value.update(extra)
    return value


def progress(stage, done, total):
    return {"state": stage, "imported_chunks": done, "total_chunks": total, "observed_at": NOW, "error_code": None}


RUNNING = {"status": "RUNNING", "tasks": [
    task("clean_orders", "IMPORTING", "writer", attempt_id="att_clean", progress=progress("IMPORTING", 1, 3),
         attempts=[attempt("att_clean", "RUNNING", ended_at=None)], observed_at=NOW),
    task("score_orders", "BLOCKED", "lambda", attempt_number=0, attempts=[])]}
PUBLISHED = {"status": "PUBLISHED", "finished_at": NOW, "root_inputs": {"ods.orders": {"commit_sequence": 42}}, "tasks": [
    task("clean_orders", "PUBLISHED", "writer", release_id="rel_clean_1", progress=progress("COMPLETE", 3, 3),
         attempts=[attempt("att_clean", "SUCCEEDED")], observed_at=NOW),
    task("score_orders", "PUBLISHED", "lambda", release_id="rel_score_1", attempts=[attempt("att_score", "SUCCEEDED")]),
    task("daily_orders", "PUBLISHED", "ecs_task", attempt_number=0, release_id="rel_daily_0",
         skipped="NO_AFFECTED_PARTITIONS", attempts=[])]}
FAILED = {"status": "FAILED", "finished_at": "2026-10-18T01:52:00Z", "error_code": "TASK_FAILED", "tasks": [
    task("clean_orders", "PUBLISHED", "writer", release_id="rel_clean_0", attempts=[attempt("att_first", "SUCCEEDED")]),
    task("score_orders", "FAILED", "lambda", attempt_number=2, error_code="OUTPUT_VALIDATION_FAILED",
         transient_code="LAMBDA_THROTTLED", attempts=[attempt("att_fail_1", "FAILED", "TASK_TIMEOUT"),
                                                     attempt("att_fail_2", "FAILED", "OUTPUT_VALIDATION_FAILED")])]}
RETRIED = {"status": "QUEUED", "waiting": {"code": "PIPELINE_RUN_ACTIVE", "since": NOW}, "tasks": [
    task("clean_orders", "PUBLISHED", "writer", release_id="rel_clean_0"),
    task("score_orders", "BLOCKED", "lambda", attempt_number=2)]}
TRIAL_PENDING = {"status": "RUNNING", "tasks": [task("score_orders", "RUNNING", "lambda")],
                 "validation": {"status": "PENDING"}}
TRIAL_DONE = {"status": "VALIDATED", "finished_at": NOW, "tasks": [task("score_orders", "VALIDATED", "lambda")],
              "validation": {"status": "PASSED", "error_code": None, "row_count": 50, "files": 1, "bytes": 2048,
                             "observed_at": NOW}}
ARTIFACT = {"s3_uri": "s3://colvenna-data/spaces/analysis/environments/pandas/lock.txt", "object_version": "v1",
            "sha256": "e" * 64, "bytes": 120}
ENVIRONMENT_READY = {
    "status": "READY", "build_id": "build_ready", "codebuild_id": "colvenna-env:1234", "image_digest": "sha256:" + "f" * 64,
    "dependency_lock_sha256": "e" * 64, "dependency_lock": ARTIFACT, "provenance": dict(ARTIFACT, sha256="d" * 64),
    "runtime": {"python_version": "3.12.7", "python_abi": "cp312", "platform": "linux_arm64",
                "base_image": "public.ecr.aws/lambda/python@sha256:" + "b" * 64, "adapter_sha256": "c" * 64,
                "source_commit": "a" * 40},
    "executor_bindings": [{"executor": "lambda", "profile": "python_lambda_standard", "lambda_mode": "standard",
                           "function_arn": "arn:aws:lambda:us-west-2:123456789012:function:colvenna-env-pandas:3"}]}
FAILURE = {"phase": "BUILD", "aws_status": "FAILED", "code": "DEPENDENCY_RESOLUTION_FAILED",
           "message": "pip: <b>no matching distribution</b> for duckdb==99"}


class Fixture:
    def __init__(self):
        self.lock = threading.Lock()
        self.requests, self.violations, self.tokens = [], [], []
        self.lose, self.gateway_timeout = set(), set()
        self.writer_state, self.diagnostics, self.conflict, self.cancel_race = "ACTIVE", False, False, False
        self.keys = [
            {"key_id": "key_admin0000", "name": "fixture admin", "fingerprint": "admin0000", "scopes": sorted(SCOPES),
             "created_at": "2026-10-01T00:00:00Z", "expires_at": "2027-01-01T00:00:00Z", "status": "ACTIVE",
             "last_used_at": "2026-10-18T01:00:00Z"},
            {"key_id": "key_monitor000", "name": "monitor", "fingerprint": "monitor000", "scopes": ["monitor:read"],
             "created_at": "2026-10-01T00:00:00Z", "expires_at": "2027-01-01T00:00:00Z", "status": "ACTIVE",
             "last_used_at": None}]
        self.uploads, self.created, self.nonces = {}, {}, {}
        artifacts = []
        for path, content in SOURCES.items():
            identity = "upload_" + uuid.uuid4().hex
            size = len(content.encode()) if content else 2048
            digest = sha(content) if content else "9" * 64
            self.uploads[identity] = {"path": path, "sha256": digest, "bytes": size, "content": content}
            artifacts.append({"path": path, "upload_id": identity, "sha256": digest, "bytes": size})
        current = revision()
        self.pipelines = {"orders": {
            "revision": current, "draft_revision": current, "status": "PAUSED", "active_version": "pv_orders_1",
            "schedule": {**DEFINITION["schedule"], "state": "PAUSED"}, "definition": copy.deepcopy(DEFINITION),
            "artifacts": artifacts}}
        self.versions = {"orders": [{"pipeline_version": "pv_orders_1", "draft_revision": current,
                                     "created_by": "principal_admin", "created_at": "2026-10-17T00:00:00Z",
                                     "code_hashes": {item["path"]: item["sha256"] for item in artifacts}}]}
        self.runs = {}
        self.add_run("run_failed_fixture", "snapshot", [FAILED], {})
        self.add_run("run_trial_fixture", "trial", [TRIAL_DONE], {"task_id": "score_orders", "sample_rows": 50})
        self.environments = {("pandas-demo", "v1"): {"script": [ENVIRONMENT_READY], "cursor": 0,
                                                     "created_at": "2026-10-10T00:00:00Z", "content": "pandas==2.2.3\n",
                                                     "nonce": "seed"}}
        self.audit = [{"audit_id": "aud_%03d" % index, "space_id": "analysis", "generation": GENERATION,
                       "at": "2026-10-18T01:%02d:00Z" % (59 - index), "action": AUDIT_ACTIONS[index % len(AUDIT_ACTIONS)],
                       "actor": {"kind": "API_KEY", "principal_id": "principal_admin", "key_id": "key_admin0000",
                                 "fingerprint": "admin0000"},
                       "target": {"kind": "api_key", "id": "key_%d" % index}, "request_id": "req_%d" % index,
                       "outcome": "COMMITTED", "details": {"note": "<b>detail</b>" if index == 0 else index}}
                      for index in range(60)]
        self.retention = {"revision": 0, "state": "DISABLED",
                          "policy": {name: bounds[0] for name, bounds in RETENTION_POLICY.items()}}
        self.holds, self.collections = [], {}
        self.routes = {
            ("GET", "/admin/spaces/{space_id}/status"): self.status,
            ("GET", "/admin/spaces/{space_id}/metrics"): self.metrics,
            ("GET", "/admin/spaces/{space_id}/api-keys"): lambda call: {"keys": copy.deepcopy(self.keys)},
            ("POST", "/admin/spaces/{space_id}/api-keys"): self.key_create,
            ("POST", "/admin/spaces/{space_id}/api-keys/{key_id}/revoke"): self.key_revoke,
            ("POST", "/admin/spaces/{space_id}/api-keys/{key_id}/rotate"): self.key_rotate,
            ("GET", "/spaces/{space_id}/pipelines"): self.pipeline_list,
            ("POST", "/spaces/{space_id}/pipelines"): self.pipeline_create,
            ("GET", "/spaces/{space_id}/pipelines/{pipeline_id}"): self.pipeline_get,
            ("PUT", "/spaces/{space_id}/pipelines/{pipeline_id}/draft"): self.draft_put,
            ("GET", "/spaces/{space_id}/pipelines/{pipeline_id}/draft/artifacts"): self.artifact_get,
            ("POST", "/spaces/{space_id}/pipelines/{pipeline_id}/artifacts"): self.artifact_inline,
            ("POST", "/spaces/{space_id}/pipelines/{pipeline_id}/validate"): self.pipeline_validate,
            ("POST", "/spaces/{space_id}/pipelines/{pipeline_id}/versions"): self.version_publish,
            ("GET", "/spaces/{space_id}/pipelines/{pipeline_id}/versions"): self.version_list,
            ("POST", "/spaces/{space_id}/pipelines/{pipeline_id}/activate"): self.activate,
            ("PUT", "/spaces/{space_id}/pipelines/{pipeline_id}/schedule"): self.schedule,
            ("GET", "/spaces/{space_id}/pipelines/{pipeline_id}/runs"): self.run_list,
            ("POST", "/spaces/{space_id}/pipelines/{pipeline_id}/runs"): self.run_start,
            ("GET", "/spaces/{space_id}/pipelines/{pipeline_id}/runs/{run_id}"): self.run_get,
            ("POST", "/spaces/{space_id}/pipelines/{pipeline_id}/runs/{run_id}/cancel"): self.run_cancel,
            ("POST", "/spaces/{space_id}/pipelines/{pipeline_id}/runs/{run_id}/retry"): self.run_retry,
            ("POST", "/spaces/{space_id}/pipelines/{pipeline_id}/trial-runs"): self.trial_start,
            ("GET", "/spaces/{space_id}/pipelines/{pipeline_id}/runs/{run_id}/preview"): self.run_preview,
            ("GET", "/spaces/{space_id}/runtime-environments"): self.environment_list,
            ("POST", "/spaces/{space_id}/runtime-environments"): self.environment_create,
            ("GET", "/spaces/{space_id}/runtime-environments/{environment_id}/versions/{version}"): self.environment_get,
            ("POST", "/spaces/{space_id}/runtime-environments/{environment_id}/versions/{version}/build"):
                self.environment_build,
            ("GET", "/admin/spaces/{space_id}/audit"): self.audit_list,
            ("GET", "/admin/spaces/{space_id}/trace"): self.trace,
            ("GET", "/admin/spaces/{space_id}/retention"): lambda call: self.retention_status(),
            ("PUT", "/admin/spaces/{space_id}/retention"): self.retention_update,
            ("POST", "/admin/spaces/{space_id}/retention/holds"): self.hold_place,
            ("POST", "/admin/spaces/{space_id}/retention/holds/{hold_id}/release"): self.hold_release,
            ("POST", "/admin/spaces/{space_id}/retention/collections"): self.collection_request,
            ("GET", "/admin/spaces/{space_id}/retention/collections/{collection_id}"): self.collection_get,
            ("GET", "/admin/spaces/{space_id}/quotas"): self.quotas,
            ("GET", "/admin/spaces/{space_id}/costs"): self.costs,
        }
        assert set(self.routes) <= {(method, suffix) for method, suffix, *_ in ROUTES}

    # -- contract-driven dispatch --------------------------------------------------------------

    def handle(self, method, raw, key, headers, body):
        with self.lock:
            return self._handle(method, raw, key, headers, body)

    def _handle(self, method, raw, key, headers, body):
        parsed = urlsplit(raw)
        options = parse_qs(parsed.query, keep_blank_values=True)
        self.requests.append({"method": method, "path": parsed.path, "query": options, "key": key,
                              "headers": dict(headers), "body": copy.deepcopy(body)})
        found = None
        for entry in TABLE:
            match = entry[1].match(parsed.path)
            if entry[0] == method and match:
                found = entry, [unquote(value) for value in match.groups()]
                break
        if found is None:
            self.violations.append("route " + method + " " + parsed.path)
            return 404, error("ROUTE_NOT_FOUND")
        (_, _, suffix, request, response, success, allowed, conditional), params = found
        if params[0] != "analysis" or key not in KEYS:
            return 403, error("FORBIDDEN", "Space or key mismatch")
        scopes = KEYS[key]
        permitted = ({"pipeline:run", "query:read"} <= scopes if response == "run_preview"
                     else any(scope in scopes for scope in allowed))
        if not permitted:
            return 403, error("FORBIDDEN", "Missing scope " + allowed[0])
        unknown = set(options) - set(QUERY_PARAMETERS.get(response, {}) if method == "GET" else {})
        if unknown or any(len(values) != 1 for values in options.values()):
            self.violations.append("query " + parsed.path + " " + repr(options))
        if conditional:
            value = headers.get(conditional, "")
            pattern = r'"[A-Za-z_][A-Za-z0-9_.-]*"' if conditional == "If-Match" else r"[A-Za-z0-9_.:-]{1,128}"
            if not re.fullmatch(pattern, value):
                self.violations.append(conditional + " " + parsed.path + " " + repr(value))
                return 428, error("CONDITION_REQUIRED")
        if request:
            try:
                validate(request, body)
            except Exception as exception:  # noqa: BLE001 - every rejection is a console defect
                self.violations.append("request " + parsed.path + " " + str(exception))
                return 400, error("INVALID_REQUEST")
            if body is not None and headers.get("Content-Type") != "application/json":
                self.violations.append("content-type " + parsed.path)
        elif body is not None:
            self.violations.append("unexpected body " + parsed.path)
        call = {"params": params[1:], "query": {name: values[0] for name, values in options.items()},
                "headers": headers, "body": body, "key": key}
        result = self.routes[(method, suffix)](call)
        status, value = result if isinstance(result, tuple) else (success, result)
        try:
            validate(response if 200 <= status < 300 else "error", value)
        except Exception as exception:  # noqa: BLE001 - the fixture itself must match the contract
            self.violations.append("response " + parsed.path + " " + str(exception))
        if (method, suffix) in self.lose:
            self.lose.discard((method, suffix))
            return None, value
        if (method, suffix) in self.gateway_timeout:
            self.gateway_timeout.discard((method, suffix))
            return 504, "<html><body>Gateway Timeout</body></html>"
        return status, value

    # -- overview -------------------------------------------------------------------------------

    def status(self, call):
        writer = observation("OK", "2026-10-18T01:59:57Z", 2.5, "WRITER_HEARTBEAT", {
            "permit_state": "ACTIVE", "epoch": 7, "task_arn": "arn:aws:ecs:us-west-2:123456789012:task/colvenna/abc",
            "heartbeat_age_seconds": 2.5, "last_commit_sequence": None})
        if self.writer_state == "STOPPED":
            writer = observation("STOPPED", "2026-10-18T01:30:00Z", 1800, "WRITER_PERMIT",
                                 {"permit_state": "STOPPED", "epoch": 7}, "WRITER_STOPPED_CONFIRMED")
        return {"space_id": "analysis", "generation": GENERATION, "observed_at": NOW, "writer": writer,
                "queue": observation("UNKNOWN", "2026-10-18T01:44:48Z", 912.5, "SQS_ATTRIBUTES+CLOUDWATCH",
                                     {"pending_messages": 3, "oldest_seconds": None}, "QUEUE_AGE_UNKNOWN"),
                "dlq": observation("OK", NOW, 0, "SQS_ATTRIBUTES_DLQ", {"messages": 0}),
                "layers": {
                    "ods": observation("OK", NOW, 1, "NATIVE_LEDGER", {"datasets": 1}),
                    "dwd": observation("DEGRADED", NOW, 1, "NATIVE_LEDGER", {"lagging": 1}, "RELEASE_LAGGING"),
                    "dws": observation("UNKNOWN", None, None, "NATIVE_LEDGER", {}, "WRITER_AUTHORITY_UNAVAILABLE"),
                    "ads": observation("OK", NOW, 1, "NATIVE_LEDGER", {})},
                "datasets": [
                    {"dataset": "ods.orders", "layer": "ods", "write_owner": "ingestion", "status": "OK",
                     "commit_sequence": 42, "release_id": None, "published_at": None, "snapshot_sequence": 42,
                     "observed_at": NOW},
                    {"dataset": "dwd.orders", "layer": "dwd", "write_owner": "pipeline", "status": "NOT_PUBLISHED",
                     "commit_sequence": None, "release_id": None, "published_at": None, "snapshot_sequence": None,
                     "observed_at": None}],
                "jobs": observation("OK", NOW, 0, "RUN_RECORDS", {"active_runs": 1}),
                "queries": observation("OK", NOW, 0, "QUERY_QUEUE", {"pending": 0, "running": 1}),
                "scheduler": observation("OK", NOW, 0, "WRITER_SCHEDULER", {"pending": 0, "max_pending": 32})}

    def metrics(self, call):
        if call["query"].get("window_seconds") != "3600":
            self.violations.append("metrics window " + repr(call["query"]))
        return {"space_id": "analysis", "observed_at": NOW, "window_seconds": 3600, "series": [
            {"name": "ingestion.commit_latency_p95_ms", "value": 120.5, "unit": "ms", "status": "OK",
             "observed_at": NOW, "window_seconds": 3600, "samples": 12, "source": "CLOUDWATCH"},
            {"name": "query.latency_p95_ms", "value": None, "unit": "ms", "status": "UNKNOWN", "observed_at": None,
             "window_seconds": 3600, "samples": 0, "source": "QUERY_RECORDS", "reason": "NO_SAMPLES"}]}

    # -- keys ------------------------------------------------------------------------------------

    def issue(self, body, replaces=None):
        identity = "key_" + uuid.uuid4().hex[:16]
        token = "colvenna_token_" + uuid.uuid4().hex + uuid.uuid4().hex
        self.tokens.append(token)
        value = {"key_id": identity, "name": body["name"], "fingerprint": identity[-12:], "scopes": sorted(body["scopes"]),
                 "created_at": NOW, "expires_at": body["expires_at"], "status": "ACTIVE", "last_used_at": None,
                 "creation_request_id": "req_" + identity}
        if replaces:
            value["replaces_key_id"] = replaces
        self.keys.append(value)
        return 201, {"key": copy.deepcopy(value), "token": token}

    def key_create(self, call):
        return self.issue(call["body"])

    def find_key(self, identity):
        return next((key for key in self.keys if key["key_id"] == identity), None)

    def key_revoke(self, call):
        key = self.find_key(call["params"][0])
        if key is None:
            return 404, error("KEY_NOT_FOUND")
        key["status"] = "REVOKED"
        return copy.deepcopy(key)

    def key_rotate(self, call):
        previous = self.find_key(call["params"][0])
        if previous is None or previous.get("replacement_key_id"):
            return 409, error("ROTATION_ALREADY_ISSUED")
        status, value = self.issue(call["body"], previous["key_id"])
        previous["replacement_key_id"] = value["key"]["key_id"]
        return status, value

    # -- pipelines -------------------------------------------------------------------------------

    def metadata(self, identity):
        record = self.pipelines[identity]
        return {"pipeline_id": identity, "revision": record["revision"], "draft_revision": record["draft_revision"],
                "status": record["status"], "active_version": record["active_version"],
                "schedule_owner": record["schedule"]["owner"], "definition": copy.deepcopy(record["definition"]),
                "artifacts": copy.deepcopy(record["artifacts"]), "schedule": copy.deepcopy(record["schedule"])}

    def pipeline_list(self, call):
        return {"pipelines": [{"pipeline_id": identity, "revision": record["revision"],
                               "draft_revision": record["draft_revision"], "status": record["status"],
                               "active_version": record["active_version"], "schedule": copy.deepcopy(record["schedule"]),
                               "name": record["definition"]["name"], "tasks": len(record["definition"]["tasks"])}
                              for identity, record in self.pipelines.items()]}

    def pipeline_create(self, call):
        body = call["body"]
        identity = body["definition"]["pipeline_id"]
        if identity in self.pipelines:
            return 409, error("PIPELINE_ALREADY_EXISTS")
        current = revision()
        self.pipelines[identity] = {"revision": current, "draft_revision": current, "status": "PAUSED",
                                    "active_version": None, "schedule": {**body["definition"]["schedule"], "state": "PAUSED"},
                                    "definition": copy.deepcopy(body["definition"]),
                                    "artifacts": copy.deepcopy(body["artifacts"])}
        self.versions[identity] = []
        self.created[identity] = current
        return 201, {"pipeline_id": identity, "revision": current, "status": "DRAFT"}

    def pipeline(self, call):
        return self.pipelines.get(call["params"][0])

    def pipeline_get(self, call):
        record = self.pipeline(call)
        return (404, error("PIPELINE_NOT_FOUND")) if record is None else self.metadata(call["params"][0])

    def expected(self, call, record):
        if self.conflict or call["headers"].get("If-Match") != '"' + record["revision"] + '"':
            self.conflict = False
            return 412, error("PIPELINE_REVISION_CHANGED", "Pipeline revision changed")
        return None

    def draft_put(self, call):
        record = self.pipeline(call)
        rejected = self.expected(call, record)
        if rejected:
            return rejected
        for artifact in call["body"]["artifacts"]:
            upload = self.uploads.get(artifact["upload_id"])
            if upload is None or {name: upload[name] for name in ["path", "sha256", "bytes"]} != {
                    name: artifact[name] for name in ["path", "sha256", "bytes"]}:
                return 422, error("PIPELINE_UPLOAD_BINDING_MISMATCH")
        current = revision()
        record.update(revision=current, draft_revision=current, definition=copy.deepcopy(call["body"]["definition"]),
                      artifacts=copy.deepcopy(call["body"]["artifacts"]))
        return {"pipeline_id": call["params"][0], "revision": current, "status": "DRAFT"}

    def artifact_get(self, call):
        record = self.pipeline(call)
        entry = next((item for item in record["artifacts"] if item["path"] == call["query"].get("path")), None)
        if entry is None:
            return 404, error("PIPELINE_ARTIFACT_NOT_FOUND")
        content = self.uploads[entry["upload_id"]]["content"]
        return {**entry, "encoding": "utf-8" if content is not None else "unavailable", "content": content}

    def artifact_inline(self, call):
        body = call["body"]
        if sha(body["content"]) != body["sha256"]:
            return 422, error("ARTIFACT_CONTENT_MISMATCH")
        identity = "upload_" + uuid.uuid4().hex
        self.uploads[identity] = {"path": body["path"], "sha256": body["sha256"],
                                  "bytes": len(body["content"].encode()), "content": body["content"]}
        return 201, {"upload_id": identity, "path": body["path"], "sha256": body["sha256"],
                     "bytes": len(body["content"].encode())}

    def pipeline_validate(self, call):
        record = self.pipeline(call)
        if call["body"]["draft_revision"] != record["draft_revision"]:
            return 412, error("PIPELINE_DRAFT_CHANGED")
        if self.diagnostics:
            return {"valid": False, "diagnostics": [{"path": "/tasks/1/output/schema", "code": "OUTPUT_SCHEMA_MISMATCH",
                                                     "message": "<i>schema</i> differs from dwd.order_scores"}]}
        return {"valid": True, "diagnostics": []}

    def version_publish(self, call):
        identity, record = call["params"][0], self.pipeline(call)
        nonce = ("version", call["headers"]["Idempotency-Key"])
        if nonce in self.nonces:
            return 201, self.nonces[nonce]
        if call["body"]["draft_revision"] != record["draft_revision"]:
            return 412, error("PIPELINE_DRAFT_CHANGED")
        version = "pv_" + sha(nonce[1])[:16]
        self.versions[identity].append({"pipeline_version": version, "draft_revision": record["draft_revision"],
                                        "created_by": "principal_admin", "created_at": NOW,
                                        "code_hashes": {item["path"]: item["sha256"] for item in record["artifacts"]}})
        self.nonces[nonce] = {"pipeline_id": identity, "pipeline_version": version, "status": "PUBLISHED"}
        return 201, self.nonces[nonce]

    def version_list(self, call):
        record = self.pipeline(call)
        return {"versions": [{**version, "active": version["pipeline_version"] == record["active_version"]}
                             for version in reversed(self.versions[call["params"][0]])]}

    def activate(self, call):
        record = self.pipeline(call)
        rejected = self.expected(call, record)
        if rejected:
            return rejected
        if call["body"]["pipeline_version"] not in [item["pipeline_version"] for item in self.versions[call["params"][0]]]:
            return 404, error("PIPELINE_VERSION_NOT_FOUND")
        record.update(active_version=call["body"]["pipeline_version"], revision=revision())
        return self.metadata(call["params"][0])

    def schedule(self, call):
        record = self.pipeline(call)
        rejected = self.expected(call, record)
        if rejected:
            return rejected
        record.update(schedule=copy.deepcopy(call["body"]), status=call["body"]["state"], revision=revision())
        return {"revision": record["revision"], "schedule": copy.deepcopy(call["body"])}

    # -- runs ------------------------------------------------------------------------------------

    def add_run(self, identity, mode, script, trial):
        self.runs[identity] = {"run_id": identity, "pipeline_id": "orders", "pipeline_version": "pv_orders_1",
                               "mode": mode, "script": copy.deepcopy(script), "cursor": 0, "served": None,
                               "created_at": "2026-10-18T01:58:00Z", "cancel_requested": False, "projection": 1,
                               "parameters": {"high_value_threshold": "500.00"}, "trial": trial, "partitions": None}
        return self.runs[identity]

    def receipt(self, run):
        return {"run_id": run["run_id"], "pipeline_version": run["pipeline_version"], "status": "QUEUED",
                "status_url": "/v1/spaces/analysis/pipelines/orders/runs/" + run["run_id"]}

    def snapshot(self, run):
        return run["served"] or run["script"][0]

    def run_value(self, run, snapshot):
        value = {"run_id": run["run_id"], "pipeline_version": run["pipeline_version"], "status": snapshot["status"],
                 "observed_at": NOW, "projection_version": run["projection"], "tasks": copy.deepcopy(snapshot["tasks"]),
                 "trigger": "api", "parameters": copy.deepcopy(run["parameters"]), "snapshot_sha256": "a" * 64,
                 "created_at": run["created_at"], "cancel_requested": run["cancel_requested"], "mode": run["mode"],
                 "pipeline_id": run["pipeline_id"]}
        for name in ["finished_at", "error_code", "root_inputs", "waiting"]:
            if name in snapshot:
                value[name] = copy.deepcopy(snapshot[name])
        if run["partitions"]:
            value["partitions"] = copy.deepcopy(run["partitions"])
        if run["mode"] == "trial":
            # Downstream trials bind upstream task releases; this single-task fixture has none.
            value["trial"] = {**run["trial"], "publication": "NOT_PUBLISHED", "upstream": {},
                              "validation": copy.deepcopy(snapshot["validation"])}
        return value

    def run_list(self, call):
        if call["query"].get("limit") != "20":
            self.violations.append("run list limit " + repr(call["query"]))
        runs = []
        for run in reversed(list(self.runs.values())):
            current = self.snapshot(run)
            runs.append({"run_id": run["run_id"], "pipeline_version": run["pipeline_version"], "status": current["status"],
                         "trigger": "api", "mode": run["mode"], "created_at": run["created_at"],
                         "finished_at": current.get("finished_at"), "error_code": current.get("error_code"),
                         "task_id": run["trial"].get("task_id") if run["mode"] == "trial" else None})
        return {"runs": runs, "next_page_token": None}

    def admit(self, kind, call, mode, script, trial):
        nonce = (kind, call["headers"]["Idempotency-Key"])
        if nonce not in self.nonces:
            run = self.add_run("run_" + sha(repr(nonce))[:40], mode, script, trial)
            run["parameters"].update(call["body"].get("parameters", {}))
            run["partitions"] = call["body"].get("partitions")
            run["created_at"] = NOW
            self.nonces[nonce] = run["run_id"]
        return 202, self.receipt(self.runs[self.nonces[nonce]])

    def run_start(self, call):
        return self.admit("run", call, call["body"]["mode"], [RUNNING, PUBLISHED], {})

    def trial_start(self, call):
        body = call["body"]
        return self.admit("trial", call, "trial", [TRIAL_PENDING, TRIAL_DONE],
                          {"task_id": body["task_id"], "sample_rows": body["sample_rows"]})

    def run(self, call):
        return self.runs.get(call["params"][1])

    def run_get(self, call):
        run = self.run(call)
        if run is None:
            return 404, error("RUN_NOT_FOUND")
        run["served"] = run["script"][min(run["cursor"], len(run["script"]) - 1)]
        run["cursor"] += 1
        run["projection"] += 1
        return self.run_value(run, run["served"])

    def run_cancel(self, call):
        run = self.run(call)
        run["cancel_requested"] = True
        if self.cancel_race:
            # Publication won the race; the cancel reply carries the actual terminal state.
            run.update(script=[PUBLISHED], cursor=0, served=PUBLISHED)
            return {"run_id": run["run_id"], "status": "PUBLISHED"}
        cancelling = {"status": "CANCELLING", "tasks": copy.deepcopy(self.snapshot(run)["tasks"])}
        run.update(script=[cancelling, {**cancelling, "status": "CANCELLED", "finished_at": NOW}], cursor=0,
                   served=cancelling)
        return {"run_id": run["run_id"], "status": "CANCELLING"}

    def run_retry(self, call):
        run = self.run(call)
        nonce = ("retry", call["headers"]["Idempotency-Key"])
        if nonce not in self.nonces:
            if self.snapshot(run)["status"] != "FAILED":
                return 409, error("RUN_NOT_RETRYABLE")
            run.update(script=[RETRIED, RUNNING], cursor=0, served=RETRIED)
            self.nonces[nonce] = run["run_id"]
        return 202, self.receipt(run)

    def run_preview(self, call):
        run = self.run(call)
        if run["mode"] != "trial" or self.snapshot(run)["status"] != "VALIDATED":
            return 409, error("TRIAL_NOT_VALIDATED")
        if call["query"].get("rows") != "20":
            self.violations.append("preview rows " + repr(call["query"]))
        return {"run_id": run["run_id"], "task_id": run["trial"]["task_id"], "publication": "NOT_PUBLISHED",
                "columns": [{"name": "order_id", "type": "BIGINT", "encoding": "native_text"},
                            {"name": "note", "type": "VARCHAR", "encoding": "native_json"},
                            {"name": "high_value", "type": "BOOLEAN", "encoding": "native_json"}],
                "rows": [["9007199254740993", HTML, True], ["2", None, False]], "row_count": 50,
                "truncated": True, "observed_at": NOW}

    # -- environments ----------------------------------------------------------------------------

    def environment_value(self, identity, record, snapshot):
        value = {"environment_id": identity[0], "version": identity[1], "created_at": record["created_at"],
                 "dependency_source": {"format": "requirements", "content": record["content"]},
                 "dependency_lock_sha256": None, "image_digest": None, "executor_bindings": []}
        value.update(copy.deepcopy(snapshot))
        return value

    def environment_list(self, call):
        return {"environments": [{"environment_id": key[0], "version": key[1],
                                  "status": record["script"][max(0, record["cursor"] - 1)]["status"],
                                  "build_id": record["script"][max(0, record["cursor"] - 1)].get("build_id"),
                                  "created_at": record["created_at"], "created_by": "principal_admin"}
                                 for key, record in self.environments.items()]}

    def environment_create(self, call):
        body = call["body"]
        key = (body["environment_id"], body["version"])
        if key in self.environments:
            return 409, error("ENVIRONMENT_VERSION_IMMUTABLE")
        self.environments[key] = {"script": [{"status": "DRAFT"}], "cursor": 0, "created_at": NOW,
                                  "content": body["dependency_source"]["content"], "nonce": None}
        return 201, {"environment_id": key[0], "version": key[1], "status": "DRAFT"}

    def environment_get(self, call):
        key = tuple(call["params"])
        record = self.environments.get(key)
        if record is None:
            return 404, error("ENVIRONMENT_NOT_FOUND")
        snapshot = record["script"][min(record["cursor"], len(record["script"]) - 1)]
        record["cursor"] += 1
        return self.environment_value(key, record, snapshot)

    def environment_build(self, call):
        record = self.environments[tuple(call["params"])]
        nonce = call["headers"]["Idempotency-Key"]
        if record["nonce"] not in {None, nonce}:
            return 409, error("ENVIRONMENT_BUILD_ALREADY_ADMITTED")
        if record["nonce"] is None:
            build = "build_" + sha(nonce)[:20]
            record.update(nonce=nonce, cursor=0, script=[
                {"status": "RESOLVING", "build_id": build},
                {"status": "FAILED", "build_id": build, "codebuild_id": "colvenna-env:" + build, "failure": FAILURE}])
        return 202, {"build_id": record["script"][0]["build_id"], "status": "QUEUED"}

    # -- audit and trace ------------------------------------------------------------------------

    def audit_list(self, call):
        records = [record for record in self.audit
                   if call["query"].get("action") in {None, record["action"]}]
        start = 50 if call["query"].get("page_token") == "p2" else 0
        limit = int(call["query"].get("limit", "50"))
        return {"records": records[start:start + limit],
                "next_page_token": "p2" if start == 0 and len(records) > limit else None}

    def trace(self, call):
        kind, identity, pipeline = (call["query"].get(name) for name in ["kind", "id", "pipeline_id"])
        nodes = []
        if kind == "run":
            if pipeline != "orders":
                return 400, error("TRACE_PIPELINE_REQUIRED")
            nodes = [
                {"kind": "run", "id": identity, "status": "PUBLISHED", "authority": "CONTROL_RECORD", "observed_at": NOW,
                 "attributes": {"pipeline_id": "orders", "mode": "snapshot", "root_inputs": '{"ods.orders":42}'},
                 "links": [{"kind": "pipeline_version", "id": "pv_orders_1"}, {"kind": "job", "id": "job_clean"}]},
                {"kind": "job", "id": "job_clean", "status": "PUBLISHED", "authority": "NATIVE_LEDGER", "observed_at": NOW,
                 "attributes": {"task_id": "clean_orders"}, "links": [{"kind": "release", "id": "release_fixture"}],
                 "conflict": {"projection": "RUNNING", "authority": "PUBLISHED"}},
                {"kind": "object", "id": "s3://colvenna-data/spaces/analysis/descriptor.json", "status": "UNKNOWN",
                 "authority": "UNAVAILABLE", "observed_at": None, "attributes": {"reason": "S3_UNAVAILABLE"},
                 "links": []}]
        elif kind == "release":
            nodes = [{"kind": "release", "id": identity, "status": "PUBLISHED", "authority": "NATIVE_LEDGER",
                      "observed_at": NOW, "attributes": {"pipeline_id": "orders", "dataset": "dwd.orders"},
                      "links": [{"kind": "run", "id": "run_trace"}, {"kind": "completion", "id": "s3://colvenna-data/x"}]}]
        elif kind == "query":
            nodes = [{"kind": "query", "id": identity, "status": "SUCCEEDED", "authority": "QUERY_RECORD",
                      "observed_at": NOW, "attributes": {"sql_visibility": "AUDIT_READ", "sql": "SELECT '<b>x</b>' AS html",
                                                         "result_visibility": "OWNER_ONLY"},
                      "links": [{"kind": "object", "id": "spaces/analysis/results/q.json"}]}]
        return {"space_id": "analysis", "kind": kind, "id": identity, "observed_at": NOW,
                "status": "PARTIAL" if any(item["status"] == "UNKNOWN" for item in nodes) else "COMPLETE",
                "nodes": nodes}

    # -- retention -------------------------------------------------------------------------------

    def collection_summary(self, record):
        current = record["script"][max(0, record["cursor"] - 1)]
        return {"collection_id": record["collection_id"], "status": current["status"], "dry_run": record["dry_run"],
                "requested_at": NOW, "finished_at": current.get("finished_at"), "collected_objects": 0,
                "collected_bytes": 0, "error_code": None}

    def retention_status(self):
        return {"space_id": "analysis", "generation": GENERATION, "observed_at": NOW,
                "revision": "ret_" + str(self.retention["revision"]), "state": self.retention["state"],
                "policy": copy.deepcopy(self.retention["policy"]), "probe_minimum": False,
                "categories": [{"category": "releases", "rule": "keep newest release_keep releases",
                                "collector": "NATIVE_PROTOCOL"},
                               {"category": "uploads", "rule": "unbound uploads after upload_seconds",
                                "collector": "RETENTION_COLLECTOR"}],
                "holds": copy.deepcopy(self.holds),
                "collections": [self.collection_summary(record) for record in self.collections.values()]}

    def retention_update(self, call):
        if call["headers"].get("If-Match") != '"ret_' + str(self.retention["revision"]) + '"':
            return 412, error("RETENTION_REVISION_CHANGED")
        self.retention.update(revision=self.retention["revision"] + 1, state=call["body"]["state"],
                              policy=copy.deepcopy(call["body"]["policy"]))
        return self.retention_status()

    def hold_place(self, call):
        body = call["body"]
        hold = {"hold_id": "hold_" + uuid.uuid4().hex, "target": copy.deepcopy(body["target"]), "reason": body["reason"],
                "created_at": NOW, "expires_at": body["expires_at"], "status": "ACTIVE", "created_by": "principal_admin"}
        self.holds.append(hold)
        return 201, copy.deepcopy(hold)

    def hold_release(self, call):
        hold = next((item for item in self.holds if item["hold_id"] == call["params"][0]), None)
        if hold is None:
            return 404, error("RETENTION_HOLD_NOT_FOUND")
        hold["status"] = "RELEASED"
        return copy.deepcopy(hold)

    def collection_request(self, call):
        nonce = ("collection", call["headers"]["Idempotency-Key"])
        if nonce not in self.nonces:
            identity = "col_" + sha(repr(nonce))[:40]
            done = {"status": "COMPLETED", "started_at": NOW, "finished_at": NOW,
                    "summary": {"releases": {"candidates": 3, "collected": 0, "retained": 3, "bytes": 0},
                                "uploads": {"candidates": 2, "collected": 0, "retained": 0, "bytes": 4096}},
                    "receipt": {"s3_uri": "s3://colvenna-data/spaces/analysis/retention/receipt.json",
                                "object_version": "receipt_v1", "sha256": "7" * 64, "bytes": 512},
                    "objects": [{"category": "uploads", "key": "spaces/analysis/uploads/a", "version_id": "v1",
                                 "bytes": 4096, "action": "WOULD_COLLECT", "reason": "UNBOUND_UPLOAD_EXPIRED"}],
                    "truncated": False}
            if not call["body"]["dry_run"]:
                # A real pass that failed part-way still reports its receipt and the failing call.
                done = {**done, "status": "FAILED", "error_code": "RETENTION_DEPENDENCY_UNAVAILABLE",
                        "failure": {"operation": "DeleteObject", "code": "AccessDenied"}}
            self.collections[identity] = {"collection_id": identity, "dry_run": call["body"]["dry_run"], "cursor": 0,
                                          "script": [{"status": "RUNNING", "started_at": NOW}, done]}
            self.nonces[nonce] = identity
        identity = self.nonces[nonce]
        return 202, {"collection_id": identity, "status": "QUEUED",
                     "status_url": "/v1/admin/spaces/analysis/retention/collections/" + identity}

    def collection_get(self, call):
        record = self.collections.get(call["params"][0])
        if record is None:
            return 404, error("RETENTION_COLLECTION_NOT_FOUND")
        current = record["script"][min(record["cursor"], len(record["script"]) - 1)]
        record["cursor"] += 1
        value = {"collection_id": record["collection_id"], "space_id": "analysis", "generation": GENERATION,
                 "status": "RUNNING", "dry_run": record["dry_run"], "requested_at": NOW, "started_at": None,
                 "finished_at": None, "error_code": None, "summary": {}, "receipt": None, "objects": [],
                 "truncated": False, "failure": None}
        value.update(copy.deepcopy(current))
        return value

    # -- quotas and costs ------------------------------------------------------------------------

    def quotas(self, call):
        return {"space_id": "analysis", "generation": GENERATION, "observed_at": NOW,
                "policy": {"max_active_runs": 4, "max_concurrent_builds": 2, "allowed_profiles": ["writer_short"]},
                "usage": [
                    {"name": "runs.active", "limit": 4, "used": 1, "unit": "count", "status": "OK", "observed_at": NOW,
                     "enforcement": "WAIT", "source": "RUN_RECORDS"},
                    {"name": "builds.concurrent", "limit": 2, "used": 2, "unit": "count", "status": "EXHAUSTED",
                     "observed_at": NOW, "enforcement": "REJECT", "source": "ENVIRONMENT_RECORDS",
                     "code": "SPACE_BUILD_QUOTA"},
                    {"name": "storage.code_bytes", "limit": 536870912, "used": None, "unit": "bytes",
                     "status": "UNKNOWN", "observed_at": None, "enforcement": "REJECT", "source": "S3_LISTING",
                     "code": "USAGE_UNAVAILABLE"}]}

    def costs(self, call):
        return {"space_id": "analysis", "generation": GENERATION, "since": call["query"].get("since", "2026-10-11T02:00:00Z"),
                "until": call["query"].get("until", NOW), "observed_at": NOW, "basis": "MEASURED_USAGE_X_LIST_PRICE",
                "prices": {"region": "us-west-2", "currency": "USD", "retrieved": "2026-10-01",
                           "sources": ["https://aws.amazon.com/lambda/pricing/"]},
                "lines": [
                    {"category": "lambda_compute", "workload": {"kind": "run", "id": "run_failed_fixture"},
                     "usage": 12.5, "unit": "GB-second", "unit_price_usd": 0.0000133334, "estimated_usd": 0.000166667,
                     "observed_at": NOW, "source": "LAMBDA_REPORT", "attributes": {"memory_mb": 1024}},
                    {"category": "s3_storage", "workload": {"kind": "space", "id": "analysis"}, "usage": 2.0,
                     "unit": "GB-month", "unit_price_usd": 0.023, "estimated_usd": 0.046, "observed_at": None,
                     "source": "S3_LISTING", "attributes": {}}],
                "truncated": False, "totals": {"lambda_compute": 0.000166667, "s3_storage": 0.046},
                "excluded": ["data transfer", "CloudWatch"], "status": "PARTIAL",
                "unmeasured": [{"kind": "environment_build", "id": "build_old", "reason": "BUILD_TIME_UNRECORDED"}]}


def handler(fixture):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def dispatch(self):
            path = urlsplit(self.path).path
            if path.startswith("/v1/"):
                self.server.cookies.append(self.headers.get("Cookie"))
                size = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(size) if size else b""
                body = json.loads(raw) if raw else None
                key = self.headers.get("Authorization", "").removeprefix("Bearer ")
                headers = {name: self.headers[name] for name in ["If-Match", "Idempotency-Key", "Content-Type"]
                           if self.headers.get(name) is not None}
                status, value = fixture.handle(self.command, self.path, key, headers, body)
                if status is None:
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                    return
                raw, content_type = (value.encode(), "text/html") if isinstance(value, str) else (
                    json.dumps(value).encode(), "application/json")
            else:
                file = (WEB / (path.lstrip("/") or "index.html")).resolve()
                if not file.is_relative_to(WEB) or not file.is_file():
                    status, raw, content_type = 404, b"", "text/plain"
                else:
                    status, raw = 200, file.read_bytes()
                    content_type = {".html": "text/html", ".js": "application/javascript",
                                    ".css": "text/css"}[file.suffix]
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Security-Policy", CSP)
            self.end_headers()
            self.wfile.write(raw)

        do_GET = dispatch
        do_POST = dispatch
        do_PUT = dispatch
    return Handler


class AdminBrowser(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.playwright = sync_playwright().start()
        cls.browser = cls.playwright.chromium.launch(headless=True, args=["--no-sandbox"])

    @classmethod
    def tearDownClass(cls):
        cls.browser.close()
        cls.playwright.stop()

    def setUp(self):
        self.fixture = Fixture()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler(self.fixture))
        self.server.cookies = []
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.context = self.browser.new_context(viewport={"width": 1440, "height": 1000})
        self.page = self.context.new_page()
        self.errors, self.policy = [], []
        self.page.on("pageerror", lambda failure: self.errors.append(str(failure)))
        self.page.on("console", lambda message: self.policy.append(message.text)
                     if "Content Security Policy" in message.text or "Refused to" in message.text else None)
        self.url = "http://127.0.0.1:" + str(self.server.server_port)
        self.context.add_cookies([{"name": "unused_cookie", "value": "fixture", "url": self.url}])
        # Polling uses page timers; the fake clock advances them deterministically.
        self.page.clock.install()
        self.page.goto(self.url + "/admin.html")

    def tearDown(self):
        try:
            self.assertEqual(self.errors, [])
            self.assertEqual(self.policy, [])
            self.assertEqual(self.fixture.violations, [])
            self.assertTrue(all(cookie is None for cookie in self.server.cookies))
            self.assertEqual(self.page.evaluate("JSON.stringify(localStorage)"), "{}")
            self.assertEqual(self.page.evaluate("JSON.stringify(sessionStorage)"), "{}")
        finally:
            self.context.close()
            self.server.shutdown()
            self.server.server_close()
            self.thread.join(timeout=2)

    # -- helpers --------------------------------------------------------------------------------

    def connect(self, key="fixture_admin"):
        self.page.fill("#space", "analysis")
        self.page.fill("#api-key", key)
        self.page.click("#connect")
        self.page.wait_for_selector("#workspace", state="visible")
        self.assertEqual(self.page.locator("#api-key").input_value(), "")

    def open(self, tab):
        self.page.click('[data-tab="%s"]' % tab)
        expect(self.page.locator("#panel-" + tab)).to_be_visible()

    def calls(self, method, pattern):
        return [call for call in self.fixture.requests if call["method"] == method and re.search(pattern, call["path"])]

    def wait_for(self, predicate, timeout=5.0):
        deadline = time.monotonic() + timeout
        while not predicate():
            if time.monotonic() > deadline:
                self.fail("condition not reached")
            time.sleep(0.05)

    def open_orders(self, key="fixture_admin"):
        self.connect(key)
        self.open("pipelines")
        self.page.click('#pipeline-list [data-pipeline="orders"]')
        expect(self.page.locator("#pipeline-summary")).to_contain_text("orders")
        expect(self.page.locator("#run-version")).to_have_value("pv_orders_1")

    def file_row(self, path):
        return self.page.locator("#file-list tbody tr", has_text=path)

    def create_key(self, name, expires="2027-01-31T12:30:15"):
        self.page.fill("#key-name", name)
        self.page.fill("#key-expires", expires)
        for scope in ["monitor:read", "query:read"]:
            self.page.check('#key-scopes input[value="%s"]' % scope)
        self.page.click("#key-submit")
        expect(self.page.locator("#token-panel")).to_be_visible()
        token = self.page.locator("#token-value").input_value()
        self.assertEqual(token, self.fixture.tokens[-1])
        self.assertNotIn(token, self.page.locator("#keys-table").inner_text())
        return token

    def assert_token_absent(self, token):
        self.assertTrue(self.page.locator("#token-panel").is_hidden())
        self.assertNotIn(token, self.page.content())
        self.assertFalse(self.page.evaluate(
            "token => [...document.querySelectorAll('input, textarea')].some((field) => field.value.includes(token))"
            " || document.body.innerText.includes(token)", token))

    # -- tests ------------------------------------------------------------------------------------

    def test_navigation_links_between_query_editor_and_admin_console(self):
        expect(self.page.locator('header a[href="/"]')).to_have_text("Query Editor")
        self.page.goto(self.url + "/")
        expect(self.page.locator('header a[href="/admin.html"]')).to_have_text("Admin Console")
        self.page.goto(self.url + "/admin.html")
        self.assertTrue(self.page.locator("#workspace").is_hidden())

    def test_overview_shows_server_observations_verbatim_with_times(self):
        self.connect()
        cards = self.page.locator("#overview-cards")
        writer = cards.locator('[data-section="writer"]')
        expect(writer.locator(".status-badge")).to_have_attribute("data-status", "OK")
        expect(writer).to_contain_text("观测 2026-10-18T01:59:57Z · 距今 2.5 秒")
        expect(writer).to_contain_text("WRITER_HEARTBEAT")
        queue = cards.locator('[data-section="queue"]')
        expect(queue.locator(".status-badge")).to_have_attribute("data-status", "UNKNOWN")
        expect(queue).to_contain_text("原因 QUEUE_AGE_UNKNOWN")
        expect(queue).to_contain_text("观测 2026-10-18T01:44:48Z · 距今 912.5 秒")
        dws = cards.locator('[data-section="layers.dws"]')
        expect(dws).to_contain_text("观测时间：未观测")
        expect(dws.locator(".status-badge")).to_have_attribute("data-status", "UNKNOWN")
        expect(cards.locator('[data-section="layers.dwd"] .status-badge')).to_have_attribute("data-status", "DEGRADED")
        self.assertEqual(cards.locator('.status-badge[data-status="OK"]').count(), 7)
        rows = self.page.locator("#overview-datasets tbody tr")
        expect(rows).to_have_count(2)
        expect(rows.nth(1)).to_contain_text("NOT_PUBLISHED")
        expect(rows.nth(1)).to_contain_text("未观测")
        expect(self.page.locator("#overview-metrics")).to_contain_text("UNKNOWN · NO_SAMPLES")
        expect(self.page.locator("#overview-metrics")).to_contain_text("120.5 ms")
        self.fixture.writer_state = "STOPPED"
        self.page.click("#overview-refresh")
        expect(writer.locator(".status-badge")).to_have_attribute("data-status", "STOPPED")
        expect(writer).to_contain_text("WRITER_STOPPED_CONFIRMED")
        expect(writer).to_contain_text("距今 1800 秒")
        before = len(self.calls("GET", r"/status$"))
        self.page.clock.run_for(20000)
        self.assertEqual(len(self.calls("GET", r"/status$")), before)
        self.page.check("#overview-auto")
        self.page.clock.run_for(15000)
        self.wait_for(lambda: len(self.calls("GET", r"/status$")) == before + 1)

    def test_key_token_is_shown_once_and_removed_on_hide_refresh_tab_switch_and_revoke(self):
        self.connect()
        self.open("keys")
        table = self.page.locator("#keys-table")
        expect(table).to_contain_text("key_monitor000")
        expect(table.locator("tbody tr", has_text="key_monitor000")).to_contain_text("未观测")
        first = self.create_key("analyst")
        posted = self.calls("POST", r"/api-keys$")[-1]["body"]
        self.assertEqual(posted, {"name": "analyst", "scopes": ["query:read", "monitor:read"],
                                  "expires_at": "2027-01-31T12:30:15Z"})
        expect(table).to_contain_text("analyst")
        self.page.click("#token-hide")
        self.assert_token_absent(first)
        second = self.create_key("second")
        self.page.click("#keys-refresh")
        expect(table).to_contain_text("second")
        self.assert_token_absent(second)
        third = self.create_key("third", "2027-02-01T08:00")
        self.assertEqual(self.calls("POST", r"/api-keys$")[-1]["body"]["expires_at"], "2027-02-01T08:00:00Z")
        self.open("overview")
        self.assert_token_absent(third)
        self.open("keys")
        expect(table).to_contain_text("third")
        self.assertTrue(self.page.locator("#token-panel").is_hidden())
        analyst = self.fixture.keys[2]["key_id"]
        table.locator("tbody tr", has_text=analyst).get_by_role("button", name="轮换").click()
        expect(self.page.locator("#key-form-title")).to_have_text("轮换 " + analyst)
        self.assertTrue(self.page.locator('#key-scopes input[value="query:read"]').is_checked())
        self.assertFalse(self.page.locator('#key-scopes input[value="keys:manage"]').is_checked())
        self.page.fill("#key-expires", "2027-03-01T00:00")
        self.page.click("#key-submit")
        expect(self.page.locator("#token-panel")).to_be_visible()
        fourth = self.page.locator("#token-value").input_value()
        rotate = self.calls("POST", r"/api-keys/" + analyst + "/rotate$")
        self.assertEqual(rotate[0]["body"], {"name": "analyst", "scopes": ["query:read", "monitor:read"],
                                             "expires_at": "2027-03-01T00:00:00Z"})
        expect(table).to_contain_text("替换 " + analyst)
        expect(table).to_contain_text("已轮换为 ")
        expect(table.locator("tbody tr", has_text=analyst).first.get_by_role("button", name="轮换")).to_have_count(0)
        self.assertNotIn(fourth, table.inner_text())
        row = table.locator("tbody tr", has_text="second")
        row.get_by_role("button", name="吊销").click()
        self.assertEqual(self.calls("POST", r"/revoke$"), [])
        row.get_by_role("button", name="确认吊销").click()
        expect(row.locator(".status-badge")).to_have_attribute("data-status", "REVOKED")
        self.assertEqual(self.calls("POST", r"/revoke$")[0]["body"], {})
        self.assert_token_absent(fourth)
        for token in self.fixture.tokens:
            self.assertNotIn(token, self.page.content())

    def test_disconnect_and_page_hide_clear_token_state_and_dom(self):
        self.connect()
        self.open("keys")
        token = self.create_key("ephemeral")
        self.page.click("#disconnect")
        self.assert_token_absent(token)
        self.assertTrue(self.page.locator("#workspace").is_hidden())
        self.assertTrue(self.page.evaluate(
            "[...document.querySelectorAll('[data-dynamic]')].every((node) => !node.childElementCount && !node.textContent)"))
        self.connect()
        self.open("keys")
        expect(self.page.locator("#keys-table")).to_contain_text("ephemeral")
        self.assertTrue(self.page.locator("#token-panel").is_hidden())
        self.open("pipelines")
        self.page.click('#pipeline-list [data-pipeline="orders"]')
        expect(self.page.locator("#definition")).to_have_value(re.compile("clean_orders"))
        self.page.goto("about:blank")
        self.page.go_back()
        expect(self.page.locator("#connection")).to_be_visible()
        self.assertTrue(self.page.locator("#workspace").is_hidden())
        self.assertEqual(self.page.locator("#api-key").input_value(), "")
        self.assertEqual(self.page.locator("#definition").input_value(), "")

    def test_monitor_key_sees_server_denials_for_keys_audit_and_retention_writes(self):
        self.connect("fixture_monitor")
        expect(self.page.locator('#overview-cards [data-section="writer"] .status-badge')).to_have_attribute(
            "data-status", "OK")
        self.open("keys")
        expect(self.page.locator("#notice")).to_contain_text("FORBIDDEN")
        expect(self.page.locator("#notice")).to_contain_text("服务端拒绝（权限或空间不匹配）")
        expect(self.page.locator("#keys-table")).to_contain_text("列表尚未取得")
        self.open("audit")
        expect(self.page.locator("#notice")).to_contain_text("服务端拒绝（权限或空间不匹配）")
        self.assertEqual(self.page.locator("#audit-table tbody tr").count(), 0)
        self.open("retention")
        expect(self.page.locator("#retention-policy")).to_contain_text("raw_seconds")
        expect(self.page.locator("#notice")).to_be_hidden()
        self.page.click("#retention-save")
        expect(self.page.locator("#notice")).to_contain_text("服务端拒绝（权限或空间不匹配）")
        self.page.fill("#hold-id", "release_fixture")
        self.page.fill("#hold-reason", "audit")
        self.page.fill("#hold-expires", "2026-12-01T00:00")
        self.page.click("#hold-submit")
        expect(self.page.locator("#notice")).to_contain_text("FORBIDDEN")
        self.assertEqual((self.fixture.retention["revision"], self.fixture.holds), (0, []))

    def test_create_pipeline_uses_placeholders_inline_upload_and_conditional_draft(self):
        self.connect()
        self.open("pipelines")
        self.page.click("#pipeline-new")
        expect(self.page.locator("#pipeline-save")).to_have_text("创建流水线")
        self.page.fill("#definition", json.dumps(DEMO))
        expect(self.page.locator("#dag")).to_contain_text("demo_task")
        self.page.fill("#file-new-path", "sql/demo.sql")
        self.page.click("#file-add")
        content = "SELECT 1 AS id; -- 中文注释\n"
        self.page.fill("#file-content", content)
        expect(self.file_row("sql/demo.sql")).to_contain_text("新文件")
        self.page.click("#pipeline-save")
        expect(self.page.locator("#notice")).to_contain_text("草稿已保存 · demo")
        expect(self.page.locator("#pipeline-summary")).to_contain_text("demo")
        digest = hashlib.sha256(content.encode()).hexdigest()
        created = self.calls("POST", r"/spaces/analysis/pipelines$")
        self.assertEqual(len(created), 1)
        self.assertEqual(created[0]["body"]["definition"], DEMO)
        self.assertEqual(created[0]["body"]["artifacts"], [
            {"path": "sql/demo.sql", "upload_id": "pending", "sha256": digest, "bytes": len(content.encode())}])
        uploads = self.calls("POST", r"/pipelines/demo/artifacts$")
        self.assertEqual([call["body"] for call in uploads], [{"path": "sql/demo.sql", "sha256": digest, "content": content}])
        upload_id = next(identity for identity, value in self.fixture.uploads.items() if value["path"] == "sql/demo.sql")
        draft = self.calls("PUT", r"/pipelines/demo/draft$")
        self.assertEqual(len(draft), 1)
        self.assertEqual(draft[0]["headers"]["If-Match"], '"' + self.fixture.created["demo"] + '"')
        self.assertEqual(draft[0]["body"]["artifacts"], [
            {"path": "sql/demo.sql", "upload_id": upload_id, "sha256": digest, "bytes": len(content.encode())}])
        order = [index for index, call in enumerate(self.fixture.requests) if call in created + uploads + draft]
        self.assertEqual(order, sorted(order))
        self.assertEqual(self.fixture.pipelines["demo"]["artifacts"][0]["upload_id"], upload_id)
        expect(self.page.locator("#pipeline-summary")).to_contain_text(self.fixture.pipelines["demo"]["revision"])

    def test_draft_edit_validate_conflict_publish_activate_and_pause(self):
        self.open_orders()
        original = self.fixture.pipelines["orders"]["revision"]
        seeded = {item["path"]: item for item in copy.deepcopy(self.fixture.pipelines["orders"]["artifacts"])}
        layers = self.page.locator("#dag .dag-layer")
        expect(layers).to_have_count(2)
        expect(layers.nth(0)).to_contain_text("writer · sql · full · writer_short")
        expect(layers.nth(0)).to_contain_text("events ← dataset ods.orders · snapshot")
        expect(layers.nth(0)).to_contain_text("输出 dwd.orders · versioned_replace")
        expect(layers.nth(1)).to_contain_text("orders ← task clean_orders · snapshot")
        expect(layers.nth(1)).to_contain_text("lambda · python · full · python_lambda_standard")
        expect(self.page.locator("#parameters")).to_contain_text("decimal_string")
        expect(self.page.locator("#parameters")).to_contain_text("500.00")
        self.file_row("assets/model.bin").get_by_role("button", name="载入").click()
        expect(self.page.locator("#file-meta")).to_contain_text("不可编辑")
        expect(self.page.locator("#file-meta")).to_contain_text("9" * 64)
        self.assertTrue(self.page.locator("#file-content").is_disabled())
        self.file_row("sql/clean_orders.sql").get_by_role("button", name="载入").click()
        expect(self.page.locator("#file-content")).to_have_value(SOURCES["sql/clean_orders.sql"])
        edited = "SELECT order_id FROM events WHERE order_id > 0;\n"
        self.page.fill("#file-content", edited)
        expect(self.file_row("sql/clean_orders.sql")).to_contain_text("已修改")
        self.page.click("#pipeline-save")
        expect(self.page.locator("#notice")).to_contain_text("草稿已保存 · orders")
        uploads = self.calls("POST", r"/pipelines/orders/artifacts$")
        self.assertEqual([call["body"]["path"] for call in uploads], ["sql/clean_orders.sql"])
        self.assertEqual(uploads[0]["body"]["sha256"], hashlib.sha256(edited.encode()).hexdigest())
        put = self.calls("PUT", r"/pipelines/orders/draft$")[0]
        self.assertEqual(put["headers"]["If-Match"], '"' + original + '"')
        artifacts = {item["path"]: item for item in put["body"]["artifacts"]}
        self.assertEqual(set(artifacts), set(SOURCES))
        for path in ["python/score_orders.py", "assets/model.bin"]:
            self.assertEqual(artifacts[path], seeded[path])
        edited_upload = artifacts["sql/clean_orders.sql"]
        self.assertNotEqual(edited_upload["upload_id"], seeded["sql/clean_orders.sql"]["upload_id"])
        self.assertEqual((edited_upload["sha256"], edited_upload["bytes"]),
                         (hashlib.sha256(edited.encode()).hexdigest(), len(edited.encode())))
        self.assertEqual(put["body"]["definition"], DEFINITION)
        current = self.fixture.pipelines["orders"]["draft_revision"]
        expect(self.page.locator("#pipeline-summary")).to_contain_text(current)
        self.fixture.diagnostics = True
        self.page.click("#pipeline-validate")
        expect(self.page.locator("#validation")).to_contain_text("valid=false")
        expect(self.page.locator("#validation")).to_contain_text("<i>schema</i> differs")
        self.assertEqual(self.page.locator("#validation i").count(), 0)
        self.assertEqual(self.calls("POST", r"/validate$")[0]["body"], {"draft_revision": current})
        self.file_row("sql/clean_orders.sql").get_by_role("button", name="载入").click()
        self.page.fill("#file-content", "SELECT 2 AS order_id;\n")
        self.fixture.conflict = True
        self.page.click("#pipeline-save")
        expect(self.page.locator("#notice")).to_contain_text("修订冲突：已被其他人修改，请重新加载")
        self.assertEqual(self.fixture.pipelines["orders"]["draft_revision"], current)
        self.page.click("#pipeline-publish")
        expect(self.page.locator("#publish-result")).to_contain_text("PUBLISHED")
        publish = self.calls("POST", r"/pipelines/orders/versions$")[0]
        self.assertRegex(publish["headers"]["Idempotency-Key"], r"^console\.[0-9a-f-]{36}$")
        self.assertEqual(publish["body"], {"draft_revision": current})
        version = self.fixture.versions["orders"][-1]["pipeline_version"]
        expect(self.page.locator("#publish-result")).to_contain_text(version)
        before = self.fixture.pipelines["orders"]["revision"]
        self.page.locator("#versions tbody tr", has_text=version).get_by_role("button", name="激活").click()
        expect(self.page.locator("#pipeline-summary")).to_contain_text("激活版本 " + version)
        activate = self.calls("POST", r"/activate$")[0]
        self.assertEqual((activate["headers"]["If-Match"], activate["body"]), ('"' + before + '"', {"pipeline_version": version}))
        self.page.select_option("#schedule-state", "ENABLED")
        self.page.click("#schedule-save")
        expect(self.page.locator("#pipeline-summary .status-badge")).to_have_attribute("data-status", "ENABLED")
        enabled = self.calls("PUT", r"/schedule$")[0]
        self.assertEqual(enabled["body"], {"owner": "colvenna", "expression": "rate(1 hour)", "timezone": "UTC",
                                           "state": "ENABLED"})
        after_enable = self.fixture.pipelines["orders"]["revision"]
        self.page.click("#schedule-pause")
        expect(self.page.locator("#pipeline-summary .status-badge")).to_have_attribute("data-status", "PAUSED")
        paused = self.calls("PUT", r"/schedule$")[1]
        self.assertEqual(paused["headers"]["If-Match"], '"' + after_enable + '"')
        self.assertEqual(paused["body"], {"owner": "colvenna", "expression": "rate(1 hour)", "timezone": "UTC",
                                          "state": "PAUSED"})

    def test_run_polls_to_published_and_cancel_race_is_shown_verbatim(self):
        self.open_orders()
        self.page.fill("#run-parameters", '{"high_value_threshold": "750.00"}')
        self.page.click("#run-start")
        detail = self.page.locator("#run-detail")
        expect(detail.locator(".meta .status-badge").first).to_have_attribute("data-status", "RUNNING")
        expect(detail).to_contain_text("IMPORTING · 1/3 · 观测 " + NOW)
        started = self.calls("POST", r"/pipelines/orders/runs$")[0]
        self.assertEqual(started["body"], {"pipeline_version": "pv_orders_1", "mode": "snapshot",
                                           "parameters": {"high_value_threshold": "750.00"}})
        self.page.clock.run_for(5000)
        expect(detail.locator(".meta .status-badge").first).to_have_attribute("data-status", "PUBLISHED")
        expect(detail).to_contain_text("COMPLETE · 3/3")
        expect(detail).to_contain_text("rel_score_1")
        expect(detail.locator("tr", has_text="daily_orders")).to_contain_text(
            "跳过 · NO_AFFECTED_PARTITIONS（沿用已有 release）")
        expect(detail.locator("tr", has_text="clean_orders").first).not_to_contain_text("跳过")
        run_id = self.fixture.nonces[("run", started["headers"]["Idempotency-Key"])]
        expect(self.page.locator("#runs tbody tr", has_text=run_id).locator(".status-badge")).to_have_attribute(
            "data-status", "PUBLISHED")
        polls = len(self.calls("GET", r"/runs/run_[0-9a-f]{40}$"))
        self.page.clock.run_for(20000)
        self.assertEqual(len(self.calls("GET", r"/runs/run_[0-9a-f]{40}$")), polls)
        self.assertTrue(self.page.locator("#run-cancel").is_disabled())
        self.fixture.cancel_race = True
        self.page.click("#run-start")
        expect(self.page.locator("#notice")).to_contain_text("运行已受理")
        expect(detail.locator(".meta .status-badge").first).to_have_attribute("data-status", "RUNNING")
        self.page.click("#run-cancel")
        expect(self.page.locator("#run-result")).to_contain_text("取消请求返回状态")
        expect(self.page.locator("#run-result .status-badge")).to_have_attribute("data-status", "PUBLISHED")
        expect(self.page.locator("#notice")).to_contain_text("服务端返回 PUBLISHED")
        self.assertEqual(self.calls("POST", r"/cancel$")[0]["body"], {})
        self.page.select_option("#run-mode", "backfill")
        self.page.fill("#run-partitions", '[{"order_date": "2026-10-01"}]')
        self.page.fill("#run-parameters", "")
        self.page.click("#run-start")
        expect(detail).to_contain_text("2026-10-01")
        keys = [call["headers"]["Idempotency-Key"] for call in self.calls("POST", r"/pipelines/orders/runs$")]
        self.assertEqual(len(set(keys)), 3)
        self.assertEqual(self.calls("POST", r"/pipelines/orders/runs$")[-1]["body"], {
            "pipeline_version": "pv_orders_1", "mode": "backfill", "partitions": [{"order_date": "2026-10-01"}]})

    def test_failed_run_detail_lists_attempts_and_retry_keeps_run_identity(self):
        self.open_orders()
        self.page.locator("#runs tbody tr", has_text="run_failed_fixture").get_by_role("button", name="查看").click()
        detail = self.page.locator("#run-detail")
        expect(detail.locator(".meta .status-badge").first).to_have_attribute("data-status", "FAILED")
        expect(detail).to_contain_text("错误 TASK_FAILED")
        attempts = detail.locator("tr.subrow", has_text="score_orders")
        for value in ["att_fail_1", "att_fail_2", "dispatch_att_fail_1", "exec_att_fail_2", "TASK_TIMEOUT",
                      "OUTPUT_VALIDATION_FAILED"]:
            expect(attempts).to_contain_text(value)
        expect(detail).to_contain_text("LAMBDA_THROTTLED")
        expect(self.page.locator("#run-retry")).to_be_enabled()
        self.page.click("#run-retry")
        expect(self.page.locator("#run-result .status-badge")).to_have_attribute("data-status", "QUEUED")
        retry = self.calls("POST", r"/runs/run_failed_fixture/retry$")[0]
        self.assertEqual(retry["body"], {})
        self.assertRegex(retry["headers"]["Idempotency-Key"], r"^console\.")
        expect(detail).to_contain_text("run_failed_fixture")
        expect(detail).to_contain_text("等待 PIPELINE_RUN_ACTIVE（同一流水线已有运行进行中） · 自 " + NOW)
        self.page.clock.run_for(5000)
        expect(detail.locator(".meta .status-badge").first).to_have_attribute("data-status", "RUNNING")
        expect(detail).not_to_contain_text("等待 ")

    def test_trial_run_validates_without_publication_and_previews_text_only(self):
        self.open_orders()
        self.page.select_option("#trial-task", "score_orders")
        self.page.fill("#trial-rows", "50")
        self.page.fill("#trial-parameters", '{"high_value_threshold": "100.00"}')
        self.page.click("#trial-start")
        detail = self.page.locator("#trial-detail")
        expect(detail).to_contain_text("试运行不发布正式数据（publication: NOT_PUBLISHED）")
        expect(detail.locator('.status-badge[data-status="PENDING"]')).to_have_count(1)
        self.assertTrue(self.page.locator("#trial-preview-button").is_disabled())
        trial = self.calls("POST", r"/trial-runs$")[0]
        self.assertEqual(trial["body"], {"pipeline_version": "pv_orders_1", "task_id": "score_orders", "sample_rows": 50,
                                         "parameters": {"high_value_threshold": "100.00"}})
        self.assertRegex(trial["headers"]["Idempotency-Key"], r"^console\.")
        self.page.clock.run_for(5000)
        expect(detail.locator(".meta .status-badge").first).to_have_attribute("data-status", "VALIDATED")
        expect(detail.locator('.status-badge[data-status="PASSED"]')).to_have_count(1)
        expect(detail).to_contain_text("row_count 50")
        expect(detail).to_contain_text("bytes 2048")
        self.page.click("#trial-preview-button")
        preview = self.page.locator("#trial-preview")
        expect(preview.locator("tbody tr")).to_have_count(2)
        expect(preview).to_contain_text(HTML)
        expect(preview).to_contain_text("9007199254740993")
        expect(preview.locator("td.null")).to_have_text("NULL")
        self.assertEqual(preview.locator("img").count(), 0)
        self.assertIsNone(self.page.evaluate("window.PWNED"))
        self.assertEqual(self.calls("GET", r"/preview$")[0]["query"], {"rows": ["20"]})
        self.page.click("#disconnect")
        self.open_orders("fixture_runner")
        self.page.locator("#runs tbody tr", has_text="run_trial_fixture").get_by_role("button", name="查看").click()
        expect(self.page.locator("#trial-preview-button")).to_be_enabled()
        self.page.click("#trial-preview-button")
        expect(self.page.locator("#trial-preview")).to_contain_text("需要 query:read 权限查看样本数据")
        expect(self.page.locator("#notice")).to_contain_text("需要 query:read 权限查看样本数据")

    def test_environment_build_unconfirmed_reply_resends_same_identity_and_failure_is_text(self):
        self.connect()
        self.open("environments")
        listing = self.page.locator("#environments")
        listing.locator("tbody tr", has_text="pandas-demo").get_by_role("button", name="查看").click()
        detail = self.page.locator("#environment-detail")
        expect(detail.locator(".meta .status-badge")).to_have_attribute("data-status", "READY")
        expect(detail).to_contain_text("3.12.7")
        expect(detail).to_contain_text("function:colvenna-env-pandas:3")
        self.page.fill("#environment-id", "sql-demo")
        self.page.fill("#environment-version", "v2")
        self.page.fill("#environment-requirements", "duckdb==1.5.6\n")
        self.page.click("#environment-create")
        expect(detail.locator(".meta .status-badge")).to_have_attribute("data-status", "DRAFT")
        self.assertEqual(self.calls("POST", r"/runtime-environments$")[0]["body"], {
            "environment_id": "sql-demo", "version": "v2", "python_version": "3.12", "platform": "linux_arm64",
            "build_profile": "binary_wheels", "dependency_source": {"format": "requirements", "content": "duckdb==1.5.6\n"}})
        self.fixture.gateway_timeout.add(
            ("POST", "/spaces/{space_id}/runtime-environments/{environment_id}/versions/{version}/build"))
        self.page.click("#environment-build")
        pending = self.page.locator("#pending")
        expect(pending).to_be_visible()
        expect(pending).to_contain_text("请求未确认 · 构建环境 sql-demo / v2")
        first = self.calls("POST", r"/build$")[0]
        expect(pending).to_contain_text("Idempotency-Key " + first["headers"]["Idempotency-Key"])
        expect(self.page.locator("#notice")).to_contain_text("HTTP 504")
        self.page.click("#pending-retry")
        expect(pending).to_be_hidden()
        expect(self.page.locator("#notice")).to_contain_text("构建已受理")
        builds = self.calls("POST", r"/build$")
        self.assertEqual(len(builds), 2)
        self.assertEqual([call["headers"]["Idempotency-Key"] for call in builds], [first["headers"]["Idempotency-Key"]] * 2)
        self.assertEqual([call["body"] for call in builds], [{}, {}])
        expect(detail.locator(".meta .status-badge")).to_have_attribute("data-status", "RESOLVING")
        self.page.clock.run_for(5000)
        expect(detail.locator(".meta .status-badge")).to_have_attribute("data-status", "FAILED")
        expect(detail.locator("pre").first).to_have_text(FAILURE["message"])
        self.assertEqual(detail.locator("b").count(), 0)
        expect(detail).to_contain_text("DEPENDENCY_RESOLUTION_FAILED")

    def test_lost_run_reply_blocks_new_mutations_and_retries_identical_request(self):
        self.open_orders()
        self.fixture.lose.add(("POST", "/spaces/{space_id}/pipelines/{pipeline_id}/runs"))
        self.page.click("#run-start")
        pending = self.page.locator("#pending")
        expect(pending).to_contain_text("请求未确认 · 启动运行")
        first = self.calls("POST", r"/pipelines/orders/runs$")[0]
        expect(pending).to_contain_text(first["headers"]["Idempotency-Key"])
        self.page.click("#run-start")
        expect(self.page.locator("#notice")).to_contain_text("请先核对原请求")
        self.page.click("#pipeline-publish")
        expect(self.page.locator("#notice")).to_contain_text("请先核对原请求")
        self.assertEqual(len(self.calls("POST", r"/pipelines/orders/(runs|versions)$")), 1)
        self.page.click("#pending-retry")
        expect(pending).to_be_hidden()
        posts = self.calls("POST", r"/pipelines/orders/runs$")
        self.assertEqual([(call["headers"], call["body"]) for call in posts], [(first["headers"], first["body"])] * 2)
        created = [identity for identity in self.fixture.runs if identity not in {"run_failed_fixture", "run_trial_fixture"}]
        self.assertEqual(len(created), 1)
        expect(self.page.locator("#run-detail")).to_contain_text(created[0])

    def test_audit_filter_and_next_page(self):
        self.connect()
        self.open("audit")
        rows = self.page.locator("#audit-table tbody tr")
        expect(rows).to_have_count(50)
        expect(self.page.locator("#audit-page")).to_have_text("第 1 页 · 本页 50 条")
        expect(self.page.locator("#audit-table")).to_contain_text("note=<b>detail</b>")
        self.assertEqual(self.page.locator("#audit-table b").count(), 0)
        expect(rows.first).to_contain_text("API_KEY · 指纹 admin0000 · principal_admin")
        self.assertEqual(self.calls("GET", r"/audit$")[0]["query"], {"limit": ["50"]})
        self.page.click("#audit-next")
        expect(rows).to_have_count(10)
        self.assertEqual(self.calls("GET", r"/audit$")[-1]["query"], {"limit": ["50"], "page_token": ["p2"]})
        expect(self.page.locator("#audit-next")).to_be_disabled()
        self.page.select_option("#audit-action", "KEY_CREATED")
        self.page.click("#audit-search")
        expect(rows).to_have_count(3)
        self.assertEqual(self.calls("GET", r"/audit$")[-1]["query"], {"limit": ["50"], "action": ["KEY_CREATED"]})
        for index in range(3):
            expect(rows.nth(index).locator("td").nth(1)).to_have_text("KEY_CREATED")

    def test_trace_highlights_conflict_and_follows_links(self):
        self.connect()
        self.open("trace")
        self.page.fill("#trace-id", "run_trace")
        self.page.fill("#trace-pipeline", "orders")
        self.page.click("#trace-submit")
        nodes = self.page.locator("#trace-nodes .trace-node")
        expect(nodes).to_have_count(3)
        expect(self.page.locator("#trace-meta .status-badge")).to_have_attribute("data-status", "PARTIAL")
        conflict = self.page.locator("#trace-nodes .trace-node.has-conflict")
        expect(conflict).to_have_count(1)
        expect(conflict.locator(".conflict")).to_contain_text("projection RUNNING · authority PUBLISHED")
        expect(nodes.nth(2)).to_contain_text("观测时间：未观测")
        expect(nodes.nth(2).locator(".status-badge")).to_have_attribute("data-status", "UNKNOWN")
        self.assertEqual(self.calls("GET", r"/trace$")[0]["query"],
                         {"kind": ["run"], "id": ["run_trace"], "pipeline_id": ["orders"]})
        self.assertEqual(nodes.nth(0).locator(".links button").count(), 0)
        conflict.get_by_role("button", name="release · release_fixture").click()
        expect(nodes).to_have_count(1)
        self.assertEqual(self.calls("GET", r"/trace$")[-1]["query"], {"kind": ["release"], "id": ["release_fixture"]})
        expect(nodes.first.locator(".link-text")).to_contain_text("completion · s3://colvenna-data/x")
        nodes.first.get_by_role("button", name="run · run_trace").click()
        expect(nodes).to_have_count(3)
        self.assertEqual(self.calls("GET", r"/trace$")[-1]["query"],
                         {"kind": ["run"], "id": ["run_trace"], "pipeline_id": ["orders"]})
        self.page.select_option("#trace-kind", "query")
        self.page.fill("#trace-id", "query_fixture")
        self.page.fill("#trace-pipeline", "")
        self.page.click("#trace-submit")
        expect(nodes).to_have_count(1)
        expect(nodes.first).to_contain_text("原始 SQL（audit:read）")
        expect(nodes.first.locator("pre")).to_have_text("SELECT '<b>x</b>' AS html")
        self.assertEqual(self.page.locator("#trace-nodes b").count(), 0)
        self.assertEqual(self.calls("GET", r"/trace$")[-1]["query"], {"kind": ["query"], "id": ["query_fixture"]})

    def test_retention_update_conflict_hold_and_collection_report(self):
        self.connect()
        self.open("retention")
        expect(self.page.locator("#retention-meta")).to_contain_text("revision ret_0")
        expect(self.page.locator("#retention-policy")).to_contain_text("3888000 秒")
        expect(self.page.locator("#retention-upload_seconds")).to_have_value("86400")
        self.page.select_option("#retention-state", "ENABLED")
        self.page.fill("#retention-upload_seconds", "7200")
        self.page.click("#retention-save")
        expect(self.page.locator("#retention-meta")).to_contain_text("revision ret_1")
        expect(self.page.locator("#retention-meta .status-badge")).to_have_attribute("data-status", "ENABLED")
        update = self.calls("PUT", r"/retention$")[0]
        policy = {name: bounds[0] for name, bounds in RETENTION_POLICY.items()}
        policy["upload_seconds"] = 7200
        self.assertEqual((update["headers"]["If-Match"], update["body"]), ('"ret_0"', {"state": "ENABLED", "policy": policy}))
        self.fixture.retention["revision"] += 1
        self.page.fill("#retention-staging_seconds", "7200")
        self.page.click("#retention-save")
        expect(self.page.locator("#notice")).to_contain_text("修订冲突：已被其他人修改，请重新加载")
        self.page.select_option("#hold-kind", "run")
        self.page.fill("#hold-id", "run_failed_fixture")
        self.page.fill("#hold-pipeline", "orders")
        self.page.fill("#hold-reason", "incident <b>review</b>")
        self.page.fill("#hold-expires", "2026-12-01T00:00")
        self.page.click("#hold-submit")
        holds = self.page.locator("#holds tbody tr")
        expect(holds).to_have_count(1)
        expect(holds.first.locator(".status-badge")).to_have_attribute("data-status", "ACTIVE")
        expect(holds.first).to_contain_text("incident <b>review</b>")
        self.assertEqual(self.calls("POST", r"/retention/holds$")[0]["body"], {
            "target": {"kind": "run", "id": "run_failed_fixture", "pipeline_id": "orders"},
            "reason": "incident <b>review</b>", "expires_at": "2026-12-01T00:00:00Z"})
        holds.first.get_by_role("button", name="解除").click()
        expect(holds.first.locator(".status-badge")).to_have_attribute("data-status", "RELEASED")
        self.assertEqual(self.calls("POST", r"/release$")[0]["body"], {})
        self.page.click("#collection-submit")
        detail = self.page.locator("#collection-detail")
        expect(detail.locator(".meta .status-badge")).to_have_attribute("data-status", "RUNNING")
        request = self.calls("POST", r"/retention/collections$")[0]
        self.assertEqual(request["body"], {"dry_run": True})
        self.assertRegex(request["headers"]["Idempotency-Key"], r"^console\.")
        self.page.clock.run_for(5000)
        expect(detail.locator(".meta .status-badge")).to_have_attribute("data-status", "COMPLETED")
        expect(detail).to_contain_text("演练（不删除）")
        expect(detail.locator("table").first.locator("tbody tr")).to_have_count(2)
        expect(detail).to_contain_text("s3://colvenna-data/spaces/analysis/retention/receipt.json")
        expect(detail.locator('.status-badge[data-status="WOULD_COLLECT"]')).to_have_count(1)
        expect(self.page.locator("#collections tbody tr .status-badge")).to_have_attribute("data-status", "COMPLETED")
        polls = len(self.calls("GET", r"/retention/collections/"))
        self.page.clock.run_for(20000)
        self.assertEqual(len(self.calls("GET", r"/retention/collections/")), polls)
        self.page.uncheck("#collection-dry-run")
        self.page.click("#collection-submit")
        expect(detail.locator(".meta .status-badge")).to_have_attribute("data-status", "RUNNING")
        self.page.clock.run_for(5000)
        expect(detail.locator(".meta .status-badge")).to_have_attribute("data-status", "FAILED")
        expect(detail).to_contain_text("错误 RETENTION_DEPENDENCY_UNAVAILABLE")
        expect(detail).to_contain_text("失败调用 DeleteObject · AccessDenied")
        expect(detail).to_contain_text("s3://colvenna-data/spaces/analysis/retention/receipt.json")

    def test_quotas_and_cost_estimate_show_basis_and_exclusions(self):
        self.connect()
        self.open("quotas")
        usage = self.page.locator("#quota-usage tbody tr")
        expect(usage).to_have_count(3)
        expect(usage.nth(1).locator(".status-badge")).to_have_attribute("data-status", "EXHAUSTED")
        expect(usage.nth(2)).to_contain_text("未观测")
        expect(usage.nth(2).locator(".status-badge")).to_have_attribute("data-status", "UNKNOWN")
        expect(self.page.locator("#quota-policy")).to_contain_text('["writer_short"]')
        expect(self.page.locator("#cost-meta")).to_contain_text("估算：实测用量 × 公开单价（2026-10-01）")
        expect(self.page.locator("#cost-meta .status-badge")).to_have_attribute("data-status", "PARTIAL")
        expect(self.page.locator("#cost-lines tbody tr")).to_have_count(2)
        expect(self.page.locator("#cost-lines")).to_contain_text("0.0000133334")
        expect(self.page.locator("#cost-excluded")).to_contain_text("data transfer")
        expect(self.page.locator("#cost-excluded tbody tr", has_text="build_old")).to_contain_text(
            "BUILD_TIME_UNRECORDED")
        expect(self.page.locator("#cost-excluded")).to_contain_text("https://aws.amazon.com/lambda/pricing/")
        self.assertEqual(self.page.locator("#cost-excluded a").count(), 0)
        self.page.fill("#cost-since", "2026-10-01T00:00")
        self.page.fill("#cost-until", "2026-10-18T00:00")
        self.page.click("#cost-submit")
        expect(self.page.locator("#cost-meta")).to_contain_text("区间 2026-10-01T00:00:00Z — 2026-10-18T00:00:00Z")
        self.assertEqual(self.calls("GET", r"/costs$")[-1]["query"],
                         {"since": ["2026-10-01T00:00:00Z"], "until": ["2026-10-18T00:00:00Z"]})


if __name__ == "__main__":
    unittest.main()
