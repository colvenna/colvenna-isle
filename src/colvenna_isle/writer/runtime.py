"""Private listener in the sole writer process; every operation binds a permit."""

import ipaddress
import json
import os
import re
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import urlopen
from urllib.parse import urlparse

import boto3
from botocore.config import Config

from colvenna_isle.region import region
from colvenna_isle.control.registry import ControlError, Registry
from colvenna_isle.control.keys import KeyStore
from colvenna_isle.control.http import decode_json
from colvenna_isle.contracts.model import LIMITS
from colvenna_isle.contracts.validation import ContractError
from colvenna_isle.control.status import Projection
from colvenna_isle.writer.database import Database
from colvenna_isle.writer.consumer import Consumer, archived_ingestion
from colvenna_isle.writer.dlq import DeadLetters
from colvenna_isle.writer.ingestion import CommitUnknown
from colvenna_isle.writer.ownership import Ownership, matches
from colvenna_isle.writer.direct import DirectWrites
from colvenna_isle.writer.intents import Intents
from colvenna_isle.writer.scheduler import Scheduler
from colvenna_isle.writer.write_qualification import LoseReply, WriteQualification
from colvenna_isle.writer.qualification_import import QualificationImports
from colvenna_isle.writer.inputs import Inputs
from colvenna_isle.writer.publications import Publisher, OPERATIONS as PUBLICATION_OPERATIONS
from colvenna_isle.writer.pipeline_reads import PipelineReads, OPERATIONS as PIPELINE_READ_OPERATIONS
from colvenna_isle.writer.tasks import WriterTasks, OPERATIONS as WRITER_TASK_OPERATIONS
from colvenna_isle.writer.governance import Governance, OPERATIONS as GOVERNANCE_OPERATIONS
from colvenna_isle.writer.backups import Backups, OPERATIONS as BACKUP_OPERATIONS
from colvenna_isle.writer.query_reads import QueryReads
from colvenna_isle.query.jobs import Jobs


def identity():
    endpoint = os.environ["ECS_CONTAINER_METADATA_URI_V4"]
    parsed = urlparse(endpoint)
    if parsed.scheme != "http" or parsed.hostname != "169.254.170.2":
        raise RuntimeError("Expected the ECS local task metadata endpoint")
    with urlopen(endpoint + "/task", timeout=5) as response:
        task = json.load(response)
    container = [entry for entry in task["Containers"] if entry["Name"] == "baseline"]
    if len(container) != 1:
        raise RuntimeError("Expected one essential Colvenna Isle process")
    container = container[0]
    address = container["Networks"][0]["IPv4Addresses"][0]
    if not ipaddress.ip_address(address).is_private:
        raise RuntimeError("Writer address must be private")
    return {
        "task_arn": task["TaskARN"],
        "task_definition": task["TaskARN"].split(":task/", 1)[0] + ":task-definition/"
                           + task["Family"] + ":" + str(task["Revision"]),
        "image_digest": container["ImageID"], "source_commit": os.environ["COLVENNA_SOURCE_COMMIT"],
        "process_nonce": uuid.uuid4().hex, "pid": os.getpid(),
        "start_ticks": int(Path("/proc/self/stat").read_text().rsplit(")", 1)[1].split()[19]),
        "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(), "address": address,
    }


class Runtime:
    def __init__(self):
        self.identity = identity()
        embedded = json.loads(Path("/opt/colvenna/application/colvenna-source.json").read_text())
        if embedded["source_commit"] != self.identity["source_commit"]:
            raise RuntimeError("Task source differs from immutable application provenance")
        current = boto3.Session(region_name=region())
        if current.get_credentials().method != "container-role":
            raise RuntimeError("ECS task-role credential chain required")
        client = current.client("dynamodb", config=Config(
            connect_timeout=3, read_timeout=5, retries={"mode": "standard", "total_max_attempts": 2}))
        self.ownership = Ownership(client, os.environ["COLVENNA_CONTROL_TABLE"])
        self.registry = Registry(client, os.environ["COLVENNA_CONTROL_TABLE"], os.environ["COLVENNA_DATA_BUCKET"])
        self.ownership.register(self.identity)
        self.lock = threading.Lock()
        self.scheduler = Scheduler(self.lock)
        self.databases = {}
        self.last_commit = None
        self.probe_space = os.environ.get("COLVENNA_WRITER_PROBE_SPACE", "")
        self.probe_generation = os.environ.get("COLVENNA_WRITER_PROBE_GENERATION", "")
        options = Config(connect_timeout=3, read_timeout=10,
                         retries={"mode": "standard", "total_max_attempts": 2})
        self.consumer = Consumer(self, current.client("s3", config=options),
            current.client("sqs", config=options), os.environ["COLVENNA_DATA_BUCKET"],
            os.environ["COLVENNA_INGRESS_QUEUE"], os.environ["COLVENNA_ACCOUNT"],
            os.environ["COLVENNA_INTAKE_SOURCE_COMMIT"])
        self.dead_letters = DeadLetters(
            self, self.consumer.s3, self.consumer.sqs, os.environ["COLVENNA_DATA_BUCKET"],
            os.environ["COLVENNA_DLQ"], os.environ["COLVENNA_ACCOUNT"])
        self.projection = Projection(client, os.environ["COLVENNA_CONTROL_TABLE"])
        self.projector_paused = False
        self.direct = DirectWrites(self, KeyStore(client, os.environ["COLVENNA_CONTROL_TABLE"]),
            Intents(self.consumer.s3, self.consumer.bucket, self.consumer.account, self.identity["source_commit"]))
        self.write_qualification = WriteQualification(self)
        self.qualification_imports = QualificationImports(self)
        self.inputs = Inputs(self)
        query_source, query_image = os.environ.get("COLVENNA_QUERY_SOURCE_COMMIT", ""), os.environ.get("COLVENNA_QUERY_IMAGE", "")
        self.query_reads = (QueryReads(self, Jobs(client, os.environ["COLVENNA_CONTROL_TABLE"], self.consumer.s3,
            self.consumer.bucket, self.consumer.account, query_source, query_image.split("@")[-1]))
            if re.fullmatch("[0-9a-f]{40}", query_source) and "@" in query_image else None)
        self.publisher = Publisher(self, decode_json(os.environ["COLVENNA_ENVIRONMENT_POLICY"].encode()))
        self.pipeline_reads = PipelineReads(self)
        self.tasks = WriterTasks(self)
        self.governance = Governance(self)
        self.backups = Backups(self, self.consumer.s3, os.environ.get("COLVENNA_BACKUP_BUCKET", ""),
                               os.environ["COLVENNA_ACCOUNT"])
        print(json.dumps({"event": "REGISTERED", "identity": self.identity}), flush=True)

    def log(self, event, fields=None):
        print(json.dumps({"event": event, **(fields or {})}), flush=True)

    def guard(self, epoch=None):
        return self.ownership.require(self.identity, epoch)

    def database(self, configuration):
        permit = self.guard()
        key = configuration["space_id"] + "/" + configuration["generation"]
        if key not in self.databases:
            if len(self.databases) >= 2:
                raise ControlError(503, "WRITER_DATABASE_LIMIT")
            try:
                self.databases[key] = Database(configuration, lambda: self.guard(permit["epoch"]))
            except Exception as error:
                self.consumer.paused = True
                self.log("NATIVE_OPEN_UNKNOWN", {"exception_type": type(error).__name__,
                    "space_id": configuration["space_id"], "generation": configuration["generation"]})
                raise CommitUnknown("NATIVE_OPEN_UNKNOWN") from None
        self.databases[key].ingestions.configuration = configuration
        return self.databases[key]

    def heartbeat(self):
        while True:
            try:
                permit = self.ownership.read()
                state = permit["state"] if matches(permit["identity"] or {}, self.identity) else "WAITING"
                self.ownership.heartbeat(self.identity, state, list(self.databases), self.last_commit)
            except Exception:
                # Missing heartbeats are UNKNOWN. No lease timeout confers a permit.
                print(json.dumps({"event": "HEARTBEAT_UNKNOWN"}), flush=True)
            time.sleep(3)

    def project(self):
        cursors = {}
        while True:
            self.project_once(cursors)
            time.sleep(2)

    def project_once(self, cursors):
        """One projection pass; one database's failure never stalls the projection of the others."""
        with self.lock:
            keys = [] if self.projector_paused else list(self.databases)
        for key in keys:
            try:
                # Native reads share the connection lock. DynamoDB projection
                # calls run outside it and never gate a business COMMIT or ACK.
                with self.lock:
                    database = self.databases.get(key)
                    page = database.ingestions.observations(cursors.get(key, 0)) if database else []
                for value in page:
                    state = self.projection.write(value)
                    if state == "CONFLICT":
                        self.log("PROJECTION_CONFLICT", {"ingestion_id": value["ingestion_id"]})
                        break
                    cursors[key] = value["projection_version"]
            except Exception as error:
                self.log("PROJECTION_UNAVAILABLE", {"exception_type": type(error).__name__,
                                                    "code": getattr(error, "code", None), "database": key})

    def execute(self, request):
        if request.get("operation") == "query_snapshot":
            if self.query_reads is None:
                raise ControlError(503, "QUERY_SOURCE_NOT_READY")
            result = self.query_reads.execute(request)
            return {"identity": self.identity, "epoch": request["epoch"], "result": result}
        if request.get("operation") == "probe_publication_fault":
            result = self.publisher.qualification.execute(request)
            return {"identity": self.identity, "epoch": request["epoch"], "result": result}
        if request.get("operation") in BACKUP_OPERATIONS:
            result = self.backups.execute(request)
            return {"identity": self.identity, "epoch": request["epoch"], "result": result}
        if request.get("operation") in GOVERNANCE_OPERATIONS:
            result = self.governance.execute(request)
            return {"identity": self.identity, "epoch": request["epoch"], "result": result}
        if request.get("operation") in PUBLICATION_OPERATIONS:
            result = self.publisher.execute(request)
            return {"identity": self.identity, "epoch": request["epoch"], "result": result}
        if request.get("operation") in PIPELINE_READ_OPERATIONS:
            result = self.pipeline_reads.execute(request)
            return {"identity": self.identity, "epoch": request["epoch"], "result": result}
        if request.get("operation") in WRITER_TASK_OPERATIONS:
            result = self.tasks.execute(request)
            return {"identity": self.identity, "epoch": request["epoch"], "result": result}
        if request.get("operation") in {"input_prepare", "input_status", "input_manifest", "input_retain",
                                        "input_release"}:
            result = self.inputs.execute(request)
            return {"identity": self.identity, "epoch": request["epoch"], "result": result}
        if request.get("operation") == "probe_import":
            # Fixed preparation runs outside the native lock; each import chunk
            # subsequently competes with FIFO/direct work on the one scheduler.
            result = self.qualification_imports.execute(request)
            return {"identity": self.identity, "epoch": request["epoch"], "result": result}
        with self.lock:
            expected = request.get("identity", {})
            if not matches(expected, self.identity):
                raise ControlError(409, "WRITER_IDENTITY_CHANGED")
            epoch = request.get("epoch")
            if not isinstance(epoch, int) or isinstance(epoch, bool) or epoch <= 0:
                raise ControlError(400, "WRITER_EPOCH_REQUIRED")
            permit = self.guard(epoch)
            operation = request.get("operation")
            if operation == "identity":
                return {"identity": self.identity, "permit": permit,
                        "opened_databases": list(self.databases),
                        "consumer_paused": self.consumer.paused, "last_delivery": self.consumer.last_delivery}
            space, generation = request.get("space_id"), request.get("generation")
            configuration = self.registry._read(space)
            if generation != configuration["generation"]:
                raise ControlError(409, "SPACE_GENERATION_CHANGED")
            if operation not in {"catalog", "checkpoint", "probe_commit", "probe_read", "probe_open", "probe_abort",
                                 "ingestion_status", "ingestion_capacity", "ingestion_diagnostics",
                                 "probe_ingestions", "probe_consumer", "probe_projection", "probe_writes",
                                 "probe_write_fault", "probe_write_abort", "probe_write_expiry",
                                 "probe_write_not_null", "probe_after_write", "probe_resources"}:
                raise ControlError(400, "UNKNOWN_WRITER_OPERATION")
            qualification = (space == self.probe_space and generation == self.probe_generation)
            if operation in {"probe_ingestions", "probe_writes"} and self.probe_space.startswith("probe_"):
                qualification = qualification or space == self.probe_space + "_other"
            if operation.startswith("probe_") and (
                    not self.probe_space.startswith("probe_")
                    or not qualification):
                raise ControlError(403, "QUALIFICATION_DISABLED")
            if operation in {"probe_commit", "probe_read", "probe_open", "probe_abort"} and (
                    not self.probe_space.startswith("probe_")):
                raise ControlError(403, "QUALIFICATION_DISABLED")
            database = (None if operation in {"probe_consumer", "probe_projection", "ingestion_capacity", "probe_write_fault",
                                              "probe_resources"}
                        else self.database(configuration))
            if operation == "ingestion_capacity":
                result = self.consumer.capacity()
            elif operation == "probe_projection":
                if (not self.probe_space.startswith("probe_")
                        or type(request.get("paused")) is not bool):
                    raise ControlError(403, "QUALIFICATION_DISABLED")
                self.projector_paused = request["paused"]
                result = {"projector_paused": self.projector_paused}
                ingestion_id = request.get("ingestion_id")
                if ingestion_id is not None:
                    if not re.fullmatch(r"ing_[0-9a-f]{64}", ingestion_id):
                        raise ControlError(400, "INVALID_INGESTION_ID")
                    existing = self.database(configuration).ingestions.status(ingestion_id)
                    if existing["status"] == "UNKNOWN":
                        raise ControlError(400, "QUALIFICATION_OBSERVATION_REQUIRED")
                    result["write_result"] = self.projection.write(existing)
            elif operation == "probe_consumer":
                if not self.probe_space.startswith("probe_"):
                    raise ControlError(403, "QUALIFICATION_DISABLED")
                paused, fault = request.get("paused"), request.get("fault")
                if type(paused) is not bool or fault not in {None, "before_commit", "after_commit"}:
                    raise ControlError(400, "INVALID_CONSUMER_PROBE")
                ingestion_id = request.get("ingestion_id")
                if fault and not re.fullmatch(r"ing_[0-9a-f]{64}", ingestion_id or ""):
                    raise ControlError(400, "INVALID_INGESTION_ID")
                # The archived identity must be in this synthetic generation.
                if fault and not archived_ingestion(self.consumer.s3, self.consumer.bucket, self.consumer.account,
                                                    space, generation, configuration["datasets"], ingestion_id):
                    raise ControlError(403, "QUALIFICATION_IDENTITY_REQUIRED")
                self.consumer.fault = (ingestion_id, fault) if fault else None
                self.consumer.paused = paused
                result = {"paused": paused, "fault": fault, "last_delivery": self.consumer.last_delivery}
            elif operation in {"ingestion_status", "ingestion_diagnostics"}:
                ingestion_id = request.get("ingestion_id")
                if not re.fullmatch(r"ing_[0-9a-f]{64}", ingestion_id or ""):
                    raise ControlError(400, "INVALID_INGESTION_ID")
                result = (database.ingestions.status(ingestion_id) if operation == "ingestion_status"
                          else database.ingestions.diagnostics(ingestion_id))
            elif operation == "probe_ingestions":
                result = database.ingestions.probe(request.get("dataset"))
                result.update(cache_initial_entries=database.cache_initial_entries,
                              last_delivery=self.consumer.last_delivery)
            elif operation == "probe_writes":
                if not self.probe_space.startswith("probe_"):
                    raise ControlError(403, "QUALIFICATION_DISABLED")
                result = database.writes.probe(request.get("dataset"))
                result.update(cache_initial_entries=database.cache_initial_entries,
                              scheduler=self.scheduler.observation())
            elif operation in {"probe_write_fault", "probe_write_abort", "probe_write_expiry",
                               "probe_write_not_null", "probe_after_write"}:
                result = self.write_qualification.execute(operation, configuration, request, database)
            elif operation == "probe_resources":
                # Qualification capacity evidence: memory peaks, spill, objfs cache/I/O per open database.
                from colvenna_isle.timing import process_resources
                result = {"process": process_resources(), "scheduler": self.scheduler.observation(),
                          "consumer": self.consumer.capacity(), "databases": {
                              key: process_resources(value.connection) for key, value in self.databases.items()}}
            elif operation == "catalog":
                result = database.catalog()
            elif operation == "checkpoint":
                result = database.checkpoint()
            elif operation == "probe_commit":
                row_id, text = request.get("id"), request.get("value")
                if (not isinstance(row_id, int) or isinstance(row_id, bool) or not 1 <= row_id <= 100
                        or not isinstance(text, str) or not 1 <= len(text) <= 128):
                    raise ControlError(400, "INVALID_PROBE_ROW")
                result = database.probe_commit(row_id, text)
                self.last_commit = {**result, "space_id": space, "generation": generation,
                                    "observed_at": time.time()}
            elif operation == "probe_read":
                result = database.probe_read()
            elif operation == "probe_open":
                result = database.probe_open()
            else:
                # Abrupt process death exercises native committed/WAL recovery.
                print(json.dumps({"event": "QUALIFICATION_ABORT", "identity": self.identity}), flush=True)
                os._exit(137)
            return {"identity": self.identity, "epoch": permit["epoch"], "result": result}


def main():
    runtime = Runtime()
    threading.Thread(target=runtime.heartbeat, daemon=True).start()
    threading.Thread(target=runtime.consumer.run, daemon=True).start()
    threading.Thread(target=runtime.project, daemon=True).start()
    threading.Thread(target=runtime.dead_letters.run, daemon=True).start()

    class DirectHandler(BaseHTTPRequestHandler):
        """8082 is reachable only from the independent synchronous proxy SG."""
        slots = threading.BoundedSemaphore(32)

        def log_message(self, *args):
            pass

        def do_POST(self):
            acquired = self.slots.acquire(blocking=False)
            status, result, request = 503, {"error": "WRITER_RESULT_UNKNOWN"}, {}
            try:
                if not acquired:
                    raise ControlError(503, "DIRECT_RPC_LIMIT")
                length = int(self.headers.get("Content-Length", "0"))
                if self.path != "/writes" or not 0 < length <= LIMITS["api_body_bytes"] + 8192:
                    raise ControlError(400, "INVALID_DIRECT_REQUEST")
                request = decode_json(self.rfile.read(length))
                if not isinstance(request, dict):
                    raise ControlError(400, "INVALID_DIRECT_REQUEST")
                value = runtime.direct.execute(request)
                status, result = value["status"], {
                    "identity": runtime.identity, "epoch": request["epoch"], "result": value["result"],
                    "timing": value.get("timing"),
                }
            except ControlError as error:
                status, result = error.status, {"error": error.code}
            except LoseReply:
                self.close_connection = True
                self.connection.close()
                return
            except ContractError:
                status, result = 422, {"error": "INVALID_WRITE_REQUEST"}
            except Exception as error:
                runtime.log("DIRECT_OPERATION_UNKNOWN", {"exception_type": type(error).__name__})
            finally:
                if acquired:
                    self.slots.release()
            body = json.dumps(result, separators=(",", ":")).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass  # A lost public reply cannot roll back an already durable result.

    threading.Thread(target=ThreadingHTTPServer(("0.0.0.0", 8082), DirectHandler).serve_forever,
                     daemon=True, name="direct-writes-listener").start()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            status, result = 200, None
            try:
                length = int(self.headers.get("Content-Length", "0"))
                # Partition-bound input requests can name thousands of partitions.
                if self.path != "/internal" or not 0 < length <= 262144:
                    raise ControlError(400, "INVALID_INTERNAL_REQUEST")
                request = decode_json(self.rfile.read(length))
                if not isinstance(request, dict):
                    raise ControlError(400, "INVALID_INTERNAL_REQUEST")
                result = runtime.execute(request)
            except ControlError as error:
                status, result = error.status, {"error": error.code}
            except (ContractError, json.JSONDecodeError, UnicodeDecodeError):
                status, result = 422, {"error": "INVALID_INTERNAL_REQUEST"}
            except Exception as error:
                print(json.dumps({"event": "WRITER_OPERATION_UNKNOWN",
                                  "exception_type": type(error).__name__}), flush=True)
                status, result = 503, {"error": "WRITER_RESULT_UNKNOWN"}
            body = json.dumps(result, separators=(",", ":")).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    ThreadingHTTPServer(("0.0.0.0", 8081), Handler).serve_forever()


if __name__ == "__main__":
    main()
