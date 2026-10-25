"""Fresh query authority, one private capture invocation, and verified local staging."""

import os
import re

from botocore.exceptions import BotoCoreError, ClientError

from colvenna_isle.region import region
from colvenna_isle.control.http import decode_json
from colvenna_isle.control.registry import ControlError, canonical
from colvenna_isle.writer.ownership import matches
from .snapshots import load

MAX_REPLY_BYTES = 3145728


class Reader:
    def __init__(self, jobs, keys, registry, ownership, client, function, writer_source, writer_image):
        self.jobs, self.keys, self.registry, self.ownership = jobs, keys, registry, ownership
        self.client, self.function = client, function
        self.writer_source, self.writer_image = writer_source, writer_image

    @classmethod
    def from_environment(cls):
        import boto3
        from botocore.config import Config
        from colvenna_isle.control.keys import KeyStore
        from colvenna_isle.control.registry import Registry
        from colvenna_isle.writer.ownership import Ownership
        from .jobs import Jobs

        current = boto3.Session(region_name=region())
        if current.get_credentials().method != "container-role":
            raise ControlError(503, "QUERY_SOURCE_IDENTITY_UNKNOWN")
        options = Config(connect_timeout=2, read_timeout=3,
                         retries={"mode": "standard", "total_max_attempts": 2})
        ddb = current.client("dynamodb", config=options)
        s3 = current.client("s3", config=options)
        table, bucket, account = (os.environ[name] for name in [
            "COLVENNA_CONTROL_TABLE", "COLVENNA_DATA_BUCKET", "COLVENNA_ACCOUNT"])
        jobs = Jobs(ddb, table, s3, bucket, account, os.environ["COLVENNA_SOURCE_COMMIT"],
                    os.environ["COLVENNA_QUERY_IMAGE"].split("@")[-1])
        # A lost response must never invoke the capture operation again.
        client = current.client("lambda", config=Config(connect_timeout=2, read_timeout=25,
            retries={"mode": "standard", "total_max_attempts": 1}))
        return cls(jobs, KeyStore(ddb, table), Registry(ddb, table, bucket), Ownership(ddb, table),
                   client, os.environ["COLVENNA_WRITER_FUNCTION"], os.environ["COLVENNA_WRITER_SOURCE_COMMIT"],
                   os.environ["COLVENNA_WRITER_IMAGE"].split("@")[-1])

    def stage(self, origin, configuration, target, guard, datasets):
        guard()
        actor = self.keys.authenticate_id(origin["key_id"])
        actor.authorize(origin["space_id"], "query:read")
        job = self.jobs.get(actor, origin["space_id"], origin["generation"], origin["query_id"], data=True)
        original = self.jobs.input(job)
        if (actor.principal_id != origin["principal_id"] or actor.generation != origin["generation"]
                or job.get("execution_id") != origin["execution_id"] or job["key_id"] != actor.key_id
                or job["source_commit"] != self.jobs.source or job["image_digest"] != self.jobs.image
                or original["configuration"] != configuration):
            raise ControlError(503, "QUERY_EXECUTION_CUSTODY_UNKNOWN")

        def fresh():
            guard()
            if self.keys.authenticate_id(actor.key_id) != actor:
                raise ControlError(403, "FORBIDDEN")
            current = self.registry._read(configuration["space_id"])
            if any(current.get(name) != configuration.get(name) for name in [
                    "generation", "route_version", "database_uri", "datasets"]):
                raise ControlError(409, "SPACE_CONFIGURATION_CHANGED")
            value = self.jobs.get(actor, origin["space_id"], origin["generation"], origin["query_id"], data=True)
            self.jobs._execution(value, job)
            if value["status"] != "RUNNING":
                raise ControlError(409, "QUERY_CANCELLED")
            return value

        fresh()
        permit = self.ownership.read()
        producer = permit.get("identity") or {}
        if (permit["state"] != "ACTIVE" or not self.function
                or producer.get("source_commit") != self.writer_source
                or producer.get("image_digest") != self.writer_image):
            raise ControlError(503, "QUERY_SOURCE_NOT_READY")
        request = {**origin, "operation": "query_snapshot", "identity": producer, "epoch": permit["epoch"],
                   "datasets": sorted(datasets)}
        try:
            response = self.client.invoke(FunctionName=self.function, InvocationType="RequestResponse",
                                          Payload=canonical(request).encode())
            with response["Payload"] as stream:
                raw = stream.read(MAX_REPLY_BYTES + 1)
            if len(raw) > MAX_REPLY_BYTES or response.get("FunctionError"):
                raise ValueError("Capture reply exceeds bound")
            reply = decode_json(raw)
            status, body = reply["statusCode"], reply["body"]
        except (BotoCoreError, ClientError, ValueError, KeyError, TypeError):
            raise ControlError(503, "QUERY_SOURCE_UNCONFIRMED") from None
        current = fresh()
        observed = self.ownership.read()
        if (observed["state"] != "ACTIVE" or observed["epoch"] != permit["epoch"]
                or not matches(observed.get("identity") or {}, producer)):
            raise ControlError(503, "QUERY_SOURCE_IDENTITY_CHANGED")
        if status != 200:
            code = body.get("error", "") if isinstance(body, dict) else ""
            if type(status) is int and status in {400, 403, 404, 409, 413, 429, 503, 504} \
                    and isinstance(code, str) and re.fullmatch("[A-Z][A-Z0-9_]{0,100}", code):
                raise ControlError(status, code)
            raise ControlError(503, "QUERY_SOURCE_UNCONFIRMED")
        try:
            value, source = body["result"], current["source_read"]
            if (not matches(body["identity"], producer) or body["epoch"] != permit["epoch"]
                    or source["producer"] != {**producer, "epoch": permit["epoch"]}
                    or source["state"] != "CLOSED" or source["proof"]["outcome"] != "CAPTURED"
                    or value["capture_id"] != source["capture_id"]
                    or source["proof"]["sha256"] != value["sha256"]
                    or source["proof"]["bytes"] != value["bytes"]
                    or source["proof"]["data_versions"] != [
                        table["version"] for table in value["snapshot"]["tables"]]):
                raise ValueError("Capture custody differs")
        except (KeyError, TypeError, ValueError):
            raise ControlError(503, "QUERY_SOURCE_UNCONFIRMED") from None
        load(value, configuration, target, fresh, request["datasets"])
        fresh()
