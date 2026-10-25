"""Real HTTP sockets and OS children qualify asynchronous integration seams.

The executor and atomic S3/DynamoDB double are explicit component seams. These
tests do not establish native SQL, AWS IAM or public Gateway acceptance.
"""

import copy
import http.client
import json
import socket
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from urllib.parse import urlsplit

from botocore.exceptions import ReadTimeoutError

from colvenna_isle.contracts.validation import validate
from colvenna_isle.control.registry import ControlError, Principal
from colvenna_isle.query.backend import Backend
from colvenna_isle.query.jobs import Jobs
from colvenna_isle.query.process import run
from colvenna_isle.query.results import Results
from colvenna_isle.query.runtime import Server
from colvenna_isle.query.service import IDENTITY_HEADERS, Service
from test_jobs import EXECUTOR, IMAGE, SOURCE, Storage


class Keys:
    def __init__(self):
        self.actors = {}
        for owner, space, manage in [
            ("owner", "fixture", False), ("other", "fixture", False),
            ("admin", "fixture", True), ("elsewhere", "elsewhere", False),
        ]:
            self.actors[owner] = Principal(owner, frozenset([space]),
                frozenset(["query:read"] + (["query:manage"] if manage else [])),
                "key_" + owner, "gen_fixture", "2099-01-01T00:00:00Z")
        self.revoked = set()

    def authenticate(self, token):
        owner = token.removeprefix("Bearer synthetic_")
        return self.authenticate_id("key_" + owner)

    def authenticate_id(self, identity):
        owner = identity.removeprefix("key_")
        if owner not in self.actors or owner in self.revoked:
            raise ControlError(403, "FORBIDDEN")
        return self.actors[owner]


class Registry:
    def __init__(self):
        self.configurations = {space: {"space_id": space, "generation": "gen_fixture", "revision": 1}
                               for space in ["fixture", "elsewhere"]}

    def _read(self, space):
        return copy.deepcopy(self.configurations[space])

    def _authorize_generation(self, actor, configuration):
        if actor.generation != configuration["generation"]:
            raise ControlError(403, "SPACE_GENERATION_CHANGED")


class AsyncHTTP(unittest.TestCase):
    def setUp(self):
        self.storage, self.keys, self.registry = Storage(), Keys(), Registry()
        for space in self.registry.configurations:
            self.storage.space(space)
        self.jobs = Jobs(self.storage, "table", self.storage, "bucket", "123456789012", SOURCE, IMAGE)
        self.results = Results(self.storage, "bucket", "123456789012")
        self.directory = tempfile.TemporaryDirectory()
        self.events, self.executions = [], []
        self.value = {"columns": [
            {"name": "same", "type": "DECIMAL(30,2)", "encoding": "native_json"},
            {"name": "same", "type": "BIGINT", "encoding": "native_json"}],
            "rows": [[f"{index}.01", "9007199254740993"] for index in range(7)],
            "data_versions": [{"dataset": "dwd.orders", "release_id": None, "generation": "gen_fixture",
                              "commit_sequence": 1, "snapshot_sequence": 1}], "truncated": False}
        self.backend = Backend(self.keys, self.registry, self.jobs, self.results, EXECUTOR,
            self.directory.name, "https://private.example", executor=self.execute, emit=self.events.append)
        self.server = Server(("127.0.0.1", 0), Service(self.keys, self.registry, self.backend),
                             source_commit=SOURCE, emit=self.events.append)
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.01})
        self.thread.start()

    def tearDown(self):
        self.backend.close()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)
        self.directory.cleanup()

    def execute(self, configuration, statements, directory, *, timeout, cancel=lambda: False,
                guard=lambda: None, on_started=lambda identity: None, **options):
        directory = Path(directory)
        directory.mkdir(mode=0o700)
        if options.get("catalog_only"):
            terminal = run([sys.executable, "-c", "pass"], directory / "process",
                           timeout=timeout, guard=guard)
            return {"terminal": terminal, "result": {"catalog": [
                {"dataset": "dwd.orders", "schema": [{"name": "order_id", "type": "BIGINT",
                                                    "nullable": False}]}]}}
        sql = statements[0]["sql"]
        self.executions.append((configuration["space_id"], str(directory), sql))
        command = "import time; time.sleep(60)" if "long private" in sql else \
            "raise SystemExit(42)" if "failure private" in sql else "import time; time.sleep(0.15)"
        terminal = run([sys.executable, "-c", command], directory / "process", timeout=timeout,
                       guard=guard, cancel=cancel, on_started=on_started)
        return {"terminal": terminal, "result": {"results": [copy.deepcopy(self.value)]},
                "failure": {"code": "ORIGINAL_NATIVE_FAILURE"} if terminal["exit_code"] == 42 else None}

    def headers(self, owner):
        actor = self.keys.actors[owner]
        headers = {"authorization": "Bearer synthetic_" + owner, "content-type": "application/json"}
        for name, header in IDENTITY_HEADERS.items():
            value = getattr(actor, name)
            headers[header] = json.dumps(sorted(value)) if isinstance(value, frozenset) else value
        return headers

    def request(self, method, suffix, body=None, owner="owner", space="fixture"):
        connection = http.client.HTTPConnection(*self.server.server_address, timeout=3)
        try:
            connection.request(method, "/v1/spaces/" + space + "/" + suffix,
                               json.dumps(body) if body is not None else None, self.headers(owner))
            response = connection.getresponse()
            return response.status, json.loads(response.read())
        finally:
            connection.close()

    def submit(self, nonce="original", sql="SELECT 'original private SQL'", **kwargs):
        status, receipt = self.request("POST", "queries", {
            "sql": sql, "client_request_id": nonce, "consistency": "current",
            **kwargs.pop("options", {})}, **kwargs)
        self.assertEqual(status, 202)
        validate("query_receipt", receipt)
        return receipt["query_id"]

    def wait(self, predicate, timeout=5):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            value = predicate()
            if value:
                return value
            time.sleep(0.02)
        self.fail("Original query state did not reach expected observation")

    def job(self, identity, owner="owner", space="fixture"):
        return self.jobs.get(self.keys.actors[owner], space, "gen_fixture", identity)

    def terminal(self, identity, owner="owner", space="fixture"):
        return self.wait(lambda: (job if job["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"} else None)
                         if (job := self.job(identity, owner, space)) else None)

    def test_disconnected_original_completes_once_with_stable_pages_owner_export_and_sql_views(self):
        admitted = threading.Event()
        original = self.backend.submit
        def submit(*args):
            result = original(*args)
            admitted.set()
            return result
        self.backend.submit = submit
        body = json.dumps({"sql": "SELECT 'original private SQL'", "client_request_id": "original",
                           "consistency": "current"}).encode()
        with socket.create_connection(self.server.server_address, timeout=3) as connection:
            raw = ("POST /v1/spaces/fixture/queries HTTP/1.1\r\nHost: private\r\n"
                   + "".join(f"{name}: {value}\r\n" for name, value in self.headers("owner").items())
                   + f"Content-Length: {len(body)}\r\n\r\n").encode() + body
            connection.sendall(raw)
            self.assertTrue(admitted.wait(3))
        identity = self.submit()
        self.backend.start()
        job = self.terminal(identity)
        self.assertEqual(job["status"], "SUCCEEDED")
        self.assertEqual(self.submit(), identity)
        rows, token = [], None
        while True:
            suffix = "queries/" + identity + "?page_size=3" + ("&page_token=" + token if token else "")
            status, page = self.request("GET", suffix)
            self.assertEqual(status, 200)
            validate("query_status", page)
            rows.extend(page["rows"])
            token = page["next_page_token"]
            if token is None:
                break
        self.assertEqual(rows, self.value["rows"])
        self.assertEqual(len(self.executions), 1)
        export = urlsplit(page["export_url"])
        suffix = "queries/" + identity + "/export?" + export.query
        status, value = self.request("GET", suffix)
        self.assertEqual((status, value), (200, self.value))
        self.assertEqual(self.request("GET", suffix, owner="other")[0], 403)
        self.assertEqual(self.request("GET", "queries/" + identity, owner="other")[0], 403)
        status, metadata = self.request("GET", "queries/" + identity, owner="admin")
        self.assertEqual((status, metadata["view"]), (200, "metadata"))
        for view in ["sql", "result"]:
            self.assertEqual(self.request("GET", "queries/" + identity + "?view=" + view, owner="admin")[0], 403)
        self.assertEqual(self.request("GET", "queries/" + identity + "?view=sql")[1]["sql"],
                         "SELECT 'original private SQL'")
        self.assertEqual(self.request("GET", "queries", owner="other"), (200, {"queries": []}))
        self.assertEqual(self.request("GET", "queries")[1]["queries"][0]["query_id"], identity)
        self.assertFalse(Path("/proc/" + str(job["terminal"]["pid"])).exists())
        self.assertNotIn("original private SQL", json.dumps(self.events))
        self.assertNotIn("synthetic_owner", json.dumps(self.events))

    def test_unpublished_original_after_confirmation_failure_is_collected_without_reexecution(self):
        original_get = self.storage.get_object
        fault = [False]
        def get(**request):
            if "/query-results/" in request["Key"] and not fault[0]:
                fault[0] = True
                raise ReadTimeoutError(endpoint_url="original-result-confirmation-unavailable")
            return original_get(**request)
        self.storage.get_object = get
        identity = self.submit("original-confirmation-fault")
        self.backend._run(self.jobs.claim(EXECUTOR))
        job = self.job(identity)
        self.assertEqual(job["status"], "FAILED")
        self.assertEqual(job["error_code"], "QUERY_RESULT_UNCONFIRMED")
        self.assertEqual(job["terminal"]["state"], "REAPED")
        self.assertNotIn("result", job)
        self.assertEqual(len(self.jobs._queue()["retained"]), 1)
        self.assertTrue(any("/query-results/" in key for key in self.storage.objects))
        self.backend._collect()
        self.assertTrue(any("/query-results/" in key for key in self.storage.objects))
        now = [job["result_intent"]["expires_unix"] + 1]
        self.jobs.clock = lambda: now[0]
        self.results.clock = self.jobs.clock
        self.backend._collect()
        self.assertFalse(any("/query-results/" in key for key in self.storage.objects))
        self.assertEqual(self.jobs._queue()["retained"], [])
        self.assertEqual(self.jobs.history(self.keys.actors["owner"], "fixture", "gen_fixture")[0]["status"],
                         "FAILED")
        now[0] = job["history_expires_unix"] + 1
        self.backend.last_collect = 0
        self.backend._collect()
        self.assertEqual(self.jobs.expired_history(), [])
        self.assertEqual(self.submit("original-confirmation-fault"), identity)
        self.assertEqual(len(self.executions), 1)
        self.assertEqual(self.job(identity)["status"], "FAILED")

    def test_unconfirmed_unpublished_identity_blocks_cleanup_and_keeps_its_original_charge(self):
        original_get = self.storage.get_object
        fault = [False]
        def get(**request):
            if "/query-results/" in request["Key"] and not fault[0]:
                fault[0] = True
                raise ReadTimeoutError(endpoint_url="original-result-confirmation-unavailable")
            return original_get(**request)
        self.storage.get_object = get
        identity = self.submit("unpublished-identity-fault")
        self.backend._run(self.jobs.claim(EXECUTOR))
        job = self.job(identity)
        key = self.results.key(job)
        original = copy.deepcopy(self.storage.objects[key])
        self.storage.objects[key]["Body"] += b" "
        self.jobs.clock = lambda: job["result_intent"]["expires_unix"] + 1
        self.results.clock = self.jobs.clock
        with self.assertRaises(ControlError):
            self.backend._collect()
        self.assertIn(key, self.storage.objects)
        self.assertEqual(len(self.jobs._queue()["retained"]), 1)
        self.storage.objects[key] = original
        self.backend._collect()
        self.assertNotIn(key, self.storage.objects)
        self.assertEqual(self.jobs._queue()["retained"], [])
        self.assertEqual(len(self.executions), 1)
        self.assertEqual(self.job(identity)["status"], "FAILED")

    def test_queued_repeat_cancel_never_starts_and_explicit_admin_cannot_obtain_sql(self):
        identity = self.submit()
        self.assertEqual(self.request("POST", "queries/" + identity + "/cancel", owner="other")[0], 403)
        status, cancelled = self.request("POST", "queries/" + identity + "/cancel", owner="admin")
        self.assertEqual((status, cancelled["status"]), (200, "CANCELLED"))
        self.assertEqual(self.request("POST", "queries/" + identity + "/cancel")[1], cancelled)
        self.backend.start()
        self.assertEqual(self.job(identity)["terminal"]["state"], "NOT_STARTED")
        self.assertEqual(self.executions, [])
        self.assertEqual(self.jobs._queue()["pending"], [])

    def test_running_cancel_reaps_same_child_and_other_space_completes(self):
        identity = self.submit(sql="SELECT 'long private SQL'")
        self.backend.start()
        running = self.wait(lambda: job if (job := self.job(identity)).get("process") else None)
        other = self.submit("other", owner="elsewhere", space="elsewhere")
        status, cancelled = self.request("POST", "queries/" + identity + "/cancel")
        self.assertEqual((status, cancelled["status"]), (200, "CANCELLING"))
        terminal = self.terminal(identity)
        self.assertEqual(terminal["status"], "CANCELLED")
        self.assertEqual(terminal["terminal"]["pid"], running["process"]["pid"])
        self.assertEqual(terminal["terminal"]["start_ticks"], running["process"]["start_ticks"])
        self.assertFalse(Path("/proc/" + str(terminal["terminal"]["pid"])).exists())
        self.assertEqual(self.terminal(other, "elsewhere", "elsewhere")["status"], "SUCCEEDED")
        self.assertEqual(self.jobs._queue()["running"], [])

    def test_timeout_and_original_failure_have_reaped_identity_and_traceable_reason(self):
        timeout = self.submit("timeout", sql="SELECT 'long private SQL'", options={"timeout_ms": 100})
        failure = self.submit("failure", sql="SELECT 'failure private SQL'")
        self.backend.start()
        first, second = self.terminal(timeout), self.terminal(failure)
        self.assertEqual((first["status"], first["error_code"]), ("FAILED", "QUERY_TIMEOUT"))
        self.assertEqual((second["status"], second["error_code"]), ("FAILED", "ORIGINAL_NATIVE_FAILURE"))
        for job in [first, second]:
            self.assertEqual(job["terminal"]["state"], "REAPED")
            self.assertFalse(Path("/proc/" + str(job["terminal"]["pid"])).exists())
        self.assertEqual(len(self.executions), 2)
        self.assertEqual(self.submit("failure", sql="SELECT 'failure private SQL'"), failure)

    def test_catalog_uses_native_metadata_seam_and_invalid_options_are_denied(self):
        status, value = self.request("GET", "catalog")
        self.assertEqual(status, 200)
        validate("catalog", value)
        self.assertEqual(self.executions, [])
        self.assertEqual(self.request("GET", "catalog", owner="elsewhere")[0], 403)
        identity = self.submit()
        for options in ["page_size=10001", "page_size=1&page_size=2", "view=invalid",
                        "token=unexpected"]:
            self.assertEqual(self.request("GET", "queries/" + identity + "?" + options)[0], 400)
        self.assertEqual(self.request("GET", "queries/" + identity + "/export")[0], 400)

    def test_idle_background_polling_leaves_capacity_for_native_catalog(self):
        idle, observations = threading.Event(), []
        original = self.backend._collect
        def collect():
            original()
            observations.append(True)
            if len(observations) >= 2:
                idle.set()
        self.backend._collect = collect
        self.backend.start()
        self.assertTrue(idle.wait(3))
        status, value = self.request("GET", "catalog")
        self.assertEqual(status, 200)
        self.assertEqual(value["datasets"][0]["dataset"], "dwd.orders")


if __name__ == "__main__":
    unittest.main()
