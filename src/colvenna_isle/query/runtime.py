"""Bounded private HTTP runtime; never logs SQL, credentials or identity headers."""

import json
import os
import signal
import socket
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

import boto3
from botocore.config import Config

from colvenna_isle.region import region
from colvenna_isle.contracts.model import LIMITS
from colvenna_isle.control.keys import KeyStore
from colvenna_isle.control.registry import ControlError, Registry
from .service import Service
from .backend import Backend
from .jobs import Jobs
from .results import Results

MAX_CONNECTIONS = 32
SOCKET_TIMEOUT = 5


class Server(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False

    def __init__(self, address, service, *, source_commit, emit=None):
        self.service = service
        self.source_commit = source_commit
        self.instance_id = uuid.uuid4().hex
        self.connections = threading.BoundedSemaphore(MAX_CONNECTIONS)
        self.emit = emit or (lambda value: print(json.dumps(value), flush=True))
        super().__init__(address, Handler)

    def get_request(self):
        request, address = super().get_request()
        request.settimeout(SOCKET_TIMEOUT)
        return request, address

    def process_request(self, request, address):
        if not self.connections.acquire(blocking=False):
            self.emit({"event": "QUERY_CONNECTION_REFUSED", "instance_id": self.instance_id})
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, address)
        except Exception:
            self.connections.release()
            raise

    def process_request_thread(self, request, address):
        try:
            super().process_request_thread(request, address)
        finally:
            self.connections.release()

    def handle_error(self, request, address):
        # Default BaseServer output includes exception text and remote addresses.
        self.emit({"event": "QUERY_CONNECTION_ERROR", "instance_id": self.instance_id})


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "ColvennaQuery"
    sys_version = ""

    def log_message(self, *args):
        pass

    def _reply(self, status, value, request_id):
        content = json.dumps(value, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.send_header("X-Colvenna-Request-Id", request_id)
        self.send_header("X-Colvenna-Instance-Id", self.server.instance_id)
        self.send_header("X-Colvenna-Source-Commit", self.server.source_commit)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(content)

    def _body(self):
        if self.headers.get("transfer-encoding"):
            raise ControlError(400, "INVALID_REQUEST_FRAMING")
        lengths = self.headers.get_all("content-length", [])
        if len(lengths) > 1 or (lengths and not lengths[0].isascii()):
            raise ControlError(400, "INVALID_REQUEST_FRAMING")
        text = lengths[0] if lengths else "0"
        if not text.isdecimal() or len(text) > 10:
            raise ControlError(400, "INVALID_REQUEST_FRAMING")
        length = int(text)
        if length > LIMITS["api_body_bytes"]:
            raise ControlError(413, "BODY_TOO_LARGE")
        body = self.rfile.read(length)
        if len(body) != length:
            raise ControlError(400, "INCOMPLETE_REQUEST_BODY")
        return body

    def _handle(self):
        request_id = uuid.uuid4().hex
        started = time.monotonic()
        self.close_connection = True
        status = 503
        self.server.emit({"event": "QUERY_REQUEST_STARTED", "request_id": request_id,
                          "instance_id": self.server.instance_id})
        try:
            if self.command == "GET" and self.path == "/health":
                status, value = 200, {"status": "READY"}
            else:
                names = [name.lower() for name in self.headers.keys()]
                if len(set(names)) != len(names):
                    raise ControlError(400, "DUPLICATE_HEADER")
                parsed = urlsplit(self.path)
                if parsed.scheme or parsed.netloc or parsed.fragment:
                    raise ControlError(400, "INVALID_REQUEST_TARGET")
                headers = {name.lower(): value for name, value in self.headers.items()}
                body = self._body()
                if parsed.query:
                    status, value = self.server.service.handle(
                        self.command, parsed.path, headers, body, parsed.query)
                else:
                    status, value = self.server.service.handle(self.command, parsed.path, headers, body)
        except ControlError as error:
            status, value = error.status, {"code": error.code, "message": error.safe_message,
                                          "request_id": request_id}
        except (TimeoutError, socket.timeout):
            status, value = 408, {"code": "REQUEST_TIMEOUT", "message": "Request timed out",
                                 "request_id": request_id}
        except Exception:
            status, value = 503, {"code": "QUERY_SERVICE_UNAVAILABLE",
                                 "message": "Query service unavailable", "request_id": request_id}
        try:
            self._reply(status, value, request_id)
        except (OSError, TimeoutError):
            status = 499
        finally:
            self.server.emit({"event": "QUERY_REQUEST_FINISHED", "request_id": request_id,
                              "instance_id": self.server.instance_id, "status": status,
                              "elapsed_ms": round((time.monotonic() - started) * 1000)})

    do_GET = _handle
    do_POST = _handle
    do_PUT = _handle
    do_DELETE = _handle
    do_PATCH = _handle
    do_HEAD = _handle
    do_OPTIONS = _handle


def main():
    source = os.environ["COLVENNA_SOURCE_COMMIT"]
    if len(source) != 40 or any(character not in "0123456789abcdef" for character in source):
        raise RuntimeError("Invalid query source identity")
    current = boto3.Session(region_name=region())
    if current.get_credentials().method != "container-role":
        raise RuntimeError("ECS task-role credentials required")
    from pathlib import Path
    from colvenna_isle.writer.runtime import identity
    task = identity()
    if json.loads(Path("/opt/colvenna/application/colvenna-source.json").read_text())["source_commit"] != source:
        raise RuntimeError("Query source differs from immutable application provenance")
    if current.client("sts").get_caller_identity()["Account"] != os.environ["COLVENNA_ACCOUNT"]:
        raise RuntimeError("Query account differs")
    ddb = current.client("dynamodb", config=Config(connect_timeout=2, read_timeout=3,
                        retries={"mode": "standard", "total_max_attempts": 2}))
    table = os.environ["COLVENNA_CONTROL_TABLE"]
    bucket, account = os.environ["COLVENNA_DATA_BUCKET"], os.environ["COLVENNA_ACCOUNT"]
    keys, registry = KeyStore(ddb, table), Registry(ddb, table, bucket)
    s3 = current.client("s3", config=Config(connect_timeout=2, read_timeout=5,
                        retries={"mode": "standard", "total_max_attempts": 2}))
    jobs = Jobs(ddb, table, s3, bucket, account, source, task["image_digest"])
    results = Results(s3, bucket, account)
    backend = Backend(keys, registry, jobs, results, task, "/work/queries",
        os.environ["COLVENNA_QUERY_API_URL"], emit=lambda value: print(json.dumps(value), flush=True),
        ecs=current.client("ecs", config=Config(connect_timeout=2, read_timeout=3,
                           retries={"mode": "standard", "total_max_attempts": 2})),
        cluster=os.environ["COLVENNA_ECS_CLUSTER"])
    service = Service(keys, registry, backend)
    server = Server(("0.0.0.0", 8080), service, source_commit=source)

    def stop(*args):
        backend.stopping.set()
        server.emit({"event": "QUERY_SERVICE_STOPPING", "instance_id": server.instance_id})
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    server.emit({"event": "QUERY_SERVICE_STARTED", "instance_id": server.instance_id,
                 "source_commit": source, "maximum_connections": MAX_CONNECTIONS})
    backend.start()
    try:
        server.serve_forever(poll_interval=0.2)
    finally:
        server.server_close()
        backend.close()
        server.emit({"event": "QUERY_SERVICE_STOPPED", "instance_id": server.instance_id})


if __name__ == "__main__":
    main()
