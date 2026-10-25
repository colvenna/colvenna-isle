"""Gateway intake integration and IAM-only fault qualification entry point."""

import json
import os
import re
from functools import lru_cache

import boto3
from botocore.exceptions import BotoCoreError, ClientError, ReadTimeoutError

from colvenna_isle.region import region
from colvenna_isle.contracts.validation import ContractError, validate
from colvenna_isle.control.http import body_bytes, decode_json, response, verified_principal
from colvenna_isle.control.keys import KeyStore
from colvenna_isle.control.registry import ControlError, Registry
from colvenna_isle.timing import Timing
from .service import Intake, IntakeError
from .backpressure import Admission, Backpressure
from .replay import Replay


def handle(event, intake, keys, replay=None):
    """One timing line per batch submission (the replay and diagnostics routes are not timed)."""
    request_id = event.get("requestContext", {}).get("requestId", "unavailable")
    timing = Timing("ingestion_accept", request_id=request_id)
    result = respond(event, intake, keys, replay, request_id, timing)
    if "space_id" in timing.identity:
        try:
            body = json.loads(result["body"])
            outcome = str(result["statusCode"]) + ":" + str(body.get("status") or body.get("code"))
        except (KeyError, TypeError, ValueError):
            outcome = str(result.get("statusCode"))
        timing.emit(outcome, "intake")
    return result


def respond(event, intake, keys, replay, request_id, timing):
    try:
        with timing.segment("authenticate"):
            principal = verified_principal(event, keys)
        extra = re.fullmatch(
            r"/v1/spaces/([A-Za-z_][A-Za-z0-9_.-]*)/ingestions/(ing_[0-9a-f]{64})/(diagnostics|replay)",
            event.get("rawPath", ""))
        method = event.get("requestContext", {}).get("http", {}).get("method")
        if extra:
            space, ingestion_id, operation = extra.groups()
            if replay is None:
                raise ControlError(503, "REPLAY_UNAVAILABLE")
            if operation == "diagnostics" and method == "GET":
                return response(200, replay.diagnostics(principal, space, ingestion_id))
            if operation == "replay" and method == "POST":
                validate("empty_request", decode_json(body_bytes(event)) if event.get("body") else {})
                status, value = replay.submit(principal, space, ingestion_id, request_id)
                return response(status, value)
            raise ControlError(404, "ROUTE_NOT_FOUND")
        match = re.fullmatch(r"/v1/spaces/([A-Za-z_][A-Za-z0-9_.-]*)/ingestions", event.get("rawPath", ""))
        method = event.get("requestContext", {}).get("http", {}).get("method")
        if not match or method != "POST":
            raise ControlError(404, "ROUTE_NOT_FOUND")
        space = match[1]
        principal.authorize(space, "ingest:write")
        raw = body_bytes(event)
        headers = {name.lower(): value for name, value in event.get("headers", {}).items()}
        value = intake.submit(principal, space, decode_json(raw), raw, headers.get("idempotency-key", ""), timing)
        return response(202, value)
    except IntakeError as error:
        value = {"code": error.code, "message": error.safe_message, "request_id": request_id,
                 "ingestion_id": error.ingestion_id, "status": "UNKNOWN",
                 "status_url": f"/v1/spaces/{space}/ingestions/{error.ingestion_id}"}
        return response(error.status, value)
    except Backpressure as error:
        value = {"code": error.code, "message": error.safe_message,
                 "request_id": request_id, "retry_after_ms": error.retry_after_ms}
        result = response(429, value)
        result["headers"]["retry-after"] = str(error.retry_after_ms // 1000)
        return result
    except ControlError as error:
        return response(error.status, {"code": error.code, "message": error.safe_message, "request_id": request_id})
    except (ContractError, ValueError, TypeError, KeyError, RecursionError):
        return response(400, {"code": "INVALID_REQUEST", "message": "Invalid request", "request_id": request_id})
    except (ClientError, BotoCoreError):
        return response(503, {"code": "CONTROL_UNAVAILABLE", "message": "Retry the same Idempotency-Key",
                              "request_id": request_id})


@lru_cache(maxsize=1)
def sdk_clients():
    from botocore.config import Config

    options = Config(connect_timeout=3, read_timeout=5, retries={"total_max_attempts": 2})
    return (boto3.client("dynamodb", region_name=region(), config=options), boto3.client("s3", region_name=region(), config=options),
            boto3.client("sqs", region_name=region(), config=options), boto3.client("cloudwatch", region_name=region(), config=options),
            boto3.client("lambda", region_name=region(), config=Config(
                connect_timeout=3, read_timeout=23, retries={"total_max_attempts": 1})))


def clients():
    from colvenna_isle.control.status import Ledger
    from colvenna_isle.writer.ownership import Ownership

    ddb, s3, sqs, cloudwatch, lam = sdk_clients()
    registry = Registry(ddb, os.environ["COLVENNA_CONTROL_TABLE"], os.environ["COLVENNA_DATA_BUCKET"])
    ledger = Ledger(Ownership(ddb, os.environ["COLVENNA_CONTROL_TABLE"]), lam,
                    os.environ["COLVENNA_WRITER_CONTROL_FUNCTION"], os.environ["COLVENNA_SOURCE_COMMIT"])
    admission = Admission(sqs, cloudwatch, ledger,
                          os.environ["COLVENNA_INGRESS_QUEUE"], os.environ["COLVENNA_DLQ"])
    intake = Intake(registry, s3, sqs,
                  os.environ["COLVENNA_DATA_BUCKET"], os.environ["COLVENNA_INGRESS_QUEUE"],
                  os.environ["COLVENNA_ACCOUNT"], os.environ["COLVENNA_SOURCE_COMMIT"], admission)
    from colvenna_isle.control.audit import Audit

    return intake, KeyStore(ddb, os.environ["COLVENNA_CONTROL_TABLE"]), Replay(
        intake, ledger, Audit(ddb, os.environ["COLVENNA_CONTROL_TABLE"]))


def handler(event, context):
    if os.environ.get("COLVENNA_INTAKE_ENABLED") != "true":
        return response(503, {"code": "INTAKE_DISABLED", "message": "Intake is disabled",
                              "request_id": event.get("requestContext", {}).get("requestId", "unavailable")})
    return handle(event, *clients())


class LostReply:
    """Private qualification: perform the actual AWS write, then lose its reply."""
    def __init__(self, client, operation):
        self.client, self.operation = client, operation

    def __getattr__(self, name):
        method = getattr(self.client, name)
        if name != self.operation:
            return method

        def dispatch(**kwargs):
            method(**kwargs)
            raise ReadTimeoutError(endpoint_url="qualification-reply-lost")
        return dispatch


class UnavailableDestination:
    """Private qualification: make a real AWS request to a disallowed destination."""
    def __init__(self, client, operation, overrides):
        self.client, self.operation, self.overrides = client, operation, overrides

    def __getattr__(self, name):
        method = getattr(self.client, name)
        if name != self.operation:
            return method
        return lambda **kwargs: method(**{**kwargs, **self.overrides})


def probe_handler(event, context):
    """IAM-only Lambda, no Gateway resource policy; fault flags never enter public handlers."""
    intake, keys, replay = clients()
    fault = event.get("_probe_fault")
    if fault == "archive_reply_lost":
        intake.s3 = LostReply(intake.s3, "put_object")
    elif fault == "queue_reply_lost":
        intake.sqs = LostReply(intake.sqs, "send_message")
    elif fault == "archive_rejected":
        intake.s3 = UnavailableDestination(intake.s3, "put_object", {
            "Bucket": "colvenna-intake-unavailable-probe"})
    elif fault == "queue_rejected":
        intake.sqs = UnavailableDestination(intake.sqs, "send_message", {
            "QueueUrl": "https://sqs." + region() + ".amazonaws.com/" + os.environ["COLVENNA_ACCOUNT"]
                        + "/colvenna-intake-unavailable-probe.fifo"})
    elif fault not in {None, "response_lost"}:
        return response(400, {"code": "INVALID_PROBE_FAULT", "message": "Invalid private qualification fault",
                              "request_id": "qualification"})
    result = handle(event, intake, keys, replay)
    if fault == "response_lost" and result["statusCode"] == 202:
        raise RuntimeError("Private qualification: response lost after confirmed queue send")
    return result
