"""Actual non-JSON HTTP responses remain available after a probe fails."""

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import urlopen

from colvenna_isle.query.evidence import response_record


class Gateway(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        body = b"<html>400 Bad Request diagnostic-secret</html>"
        self.send_response(400)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Diagnostic", "diagnostic-secret")
        self.end_headers()
        self.wfile.write(body)


class ResponseEvidence(unittest.TestCase):
    def test_non_json_response_is_saved_before_failure_with_secrets_redacted(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), Gateway)
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        thread.start()
        captured = []
        try:
            with self.assertRaisesRegex(RuntimeError, "HTTP_RESPONSE_NOT_JSON"):
                try:
                    response = urlopen("http://127.0.0.1:" + str(server.server_port), timeout=3)
                except HTTPError as error:
                    response = error
                with response:
                    response_record(response, captured.append, secrets=["diagnostic-secret"])
            self.assertEqual(len(captured), 1)
            self.assertEqual(captured[0]["status"], 400)
            self.assertEqual(captured[0]["headers"]["Content-Type"], "text/html")
            self.assertIn("400 Bad Request", captured[0]["body_utf8"])
            self.assertNotIn("diagnostic-secret", json.dumps(captured))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)
