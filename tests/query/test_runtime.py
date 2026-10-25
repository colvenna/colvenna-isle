"""Actual sockets verify framing, timeout, health, correlation and secret-free logs."""

import http.client
import json
import socket
import threading
import time
import unittest
from unittest.mock import patch

from colvenna_isle.control.registry import ControlError
from colvenna_isle.contracts.validation import validate
from colvenna_isle.query.runtime import Server


class Backend:
    def __init__(self):
        self.requests = []

    def handle(self, method, path, headers, body):
        self.requests.append((method, path, headers, body))
        raise ControlError(403, "FORBIDDEN")


class Transport(unittest.TestCase):
    def setUp(self):
        self.backend = Backend()
        self.events = []
        self.timeout = patch("colvenna_isle.query.runtime.SOCKET_TIMEOUT", 0.2)
        self.timeout.start()
        self.server = Server(("127.0.0.1", 0), self.backend, source_commit="1" * 40,
                             emit=self.events.append)
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.01})
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)
        self.timeout.stop()

    def request(self, method, path, body=None, headers=None):
        client = http.client.HTTPConnection(*self.server.server_address, timeout=3)
        try:
            client.request(method, path, body, headers or {})
            result = client.getresponse()
            return result.status, dict(result.getheaders()), json.loads(result.read())
        finally:
            client.close()

    def test_health_and_denial_have_correlated_source_instance_without_secret_logs(self):
        status, _, value = self.request("GET", "/health")
        self.assertEqual((status, value), (200, {"status": "READY"}))
        status, headers, value = self.request("POST", "/v1/spaces/a/queries",
            b'{"sql":"SELECT secret_customer_data"}', {"authorization": "Bearer super_secret"})
        self.assertEqual(status, 403)
        validate("error", value)
        self.assertEqual(value["request_id"], headers["X-Colvenna-Request-Id"])
        self.assertEqual(headers["X-Colvenna-Source-Commit"], "1" * 40)
        self.assertEqual(headers["X-Colvenna-Instance-Id"], self.server.instance_id)
        # The server records completion after it has written the response; wait for that event.
        deadline = time.monotonic() + 2
        while True:
            finished = [event for event in self.events if event["event"] == "QUERY_REQUEST_FINISHED"
                        and event["request_id"] == value["request_id"]]
            if finished or time.monotonic() > deadline:
                break
            time.sleep(0.01)
        self.assertEqual(len(finished), 1)
        self.assertEqual(finished[0]["status"], 403)
        serialized = json.dumps(self.events)
        self.assertNotIn("super_secret", serialized)
        self.assertNotIn("secret_customer_data", serialized)

    def test_duplicate_identity_headers_are_rejected_before_backend(self):
        client = http.client.HTTPConnection(*self.server.server_address, timeout=3)
        try:
            client.putrequest("GET", "/v1/spaces/a/catalog")
            client.putheader("X-Colvenna-Principal-Id", "alice")
            client.putheader("x-colvenna-principal-id", "bob")
            client.endheaders()
            result = client.getresponse()
            self.assertEqual(result.status, 400)
            self.assertEqual(json.loads(result.read())["code"], "DUPLICATE_HEADER")
        finally:
            client.close()
        self.assertEqual(self.backend.requests, [])

    def test_ambiguous_and_excessive_body_framing_is_rejected(self):
        for headers, expected in [
            ({"transfer-encoding": "chunked"}, 400), ({"content-length": "-1"}, 400),
            ({"content-length": "99999999"}, 413),
        ]:
            status, _, _ = self.request("POST", "/v1/spaces/a/queries", headers=headers)
            self.assertEqual(status, expected)
        self.assertEqual(self.backend.requests, [])

    def test_stalled_request_times_out_while_other_requests_are_healthy(self):
        with socket.create_connection(self.server.server_address, timeout=3) as client:
            client.sendall(b"POST /v1/spaces/a/queries HTTP/1.1\r\nHost: private\r\n"
                           b"Content-Length: 50\r\n\r\n{")
            self.assertEqual(self.request("GET", "/health")[0], 200)
            self.assertIn(b"408 Request Timeout", client.recv(4096))
        self.assertEqual(self.request("GET", "/health")[0], 200)
        self.assertEqual(self.backend.requests, [])


if __name__ == "__main__":
    unittest.main()
