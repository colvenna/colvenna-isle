"""API Gateway integration for the independent VPC synchronous Lambda."""

import json
import os
import re
import time
from functools import lru_cache

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from colvenna_isle.region import region
from colvenna_isle.contracts.validation import ContractError
from colvenna_isle.control.http import body_bytes, decode_json, response, verified_principal
from colvenna_isle.control.keys import KeyStore
from colvenna_isle.control.registry import ControlError, Registry
from colvenna_isle.timing import NOT_TIMED, Timing
from colvenna_isle.writer.ownership import Ownership
from .service import Proxy, WriteUnknown


def handle(event, proxy, keys):
    """One line per public write: the proxy's own segments with the writer's nested under its RPC."""
    request_id = event.get("requestContext", {}).get("requestId", "unavailable")
    writing = event.get("requestContext", {}).get("http", {}).get("method") == "POST"
    timing = Timing("sync_write", request_id=request_id) if writing else NOT_TIMED
    result = respond(event, proxy, keys, request_id, timing)
    if writing:
        try:
            body = json.loads(result["body"])
            outcome = str(result["statusCode"]) + ":" + str(body.get("status") or body.get("code"))
        except (KeyError, TypeError, ValueError):
            outcome = str(result.get("statusCode"))
        timing.emit(outcome, "sync")
    return result


def respond(event, proxy, keys, request_id, timing):
    space = None
    try:
        started = event.get("requestContext", {}).get("timeEpoch", int(time.time() * 1000))
        if type(started) is not int or started > int(time.time() * 1000):
            raise ControlError(400, "INVALID_REQUEST_TIME")
        with timing.segment("authenticate"):
            principal = verified_principal(event, keys)
        path = event.get("rawPath", "")
        match = re.fullmatch(
            r"/v1/spaces/([A-Za-z_][A-Za-z0-9_.-]*)/writes(?:/(write_[0-9a-f]{64}))?", path)
        method = event.get("requestContext", {}).get("http", {}).get("method")
        if not match or event.get("rawQueryString"):
            raise ControlError(404, "ROUTE_NOT_FOUND")
        space, identity = match.groups()
        headers = {name.lower(): value for name, value in event.get("headers", {}).items()}
        if any(name.startswith("x-colvenna-") for name in headers):
            raise ControlError(400, "INTERNAL_IDENTITY_FORBIDDEN")
        if identity is not None and method == "GET":
            if event.get("body"):
                raise ControlError(400, "GET_BODY_FORBIDDEN")
            status, value = proxy.status(principal, space, identity, started)
        elif identity is None and method == "POST":
            status, value = proxy.submit(
                principal, space, decode_json(body_bytes(event)), headers.get("idempotency-key", ""), started, timing)
        else:
            raise ControlError(404, "ROUTE_NOT_FOUND")
        result = response(status, value)
        if status == 429:
            result["headers"]["retry-after"] = "1"
        return result
    except WriteUnknown as error:
        return response(error.status, {
            "write_id": error.write_id, "status": "UNKNOWN", "code": error.code,
            "message": error.safe_message, "request_id": request_id,
            "status_url": f"/v1/spaces/{space}/writes/{error.write_id}",
        })
    except ControlError as error:
        return response(error.status, {"code": error.code, "message": error.safe_message, "request_id": request_id})
    except ContractError:
        return response(422, {"code": "INVALID_WRITE_REQUEST", "message": "Invalid write request",
                              "request_id": request_id})
    except (ValueError, TypeError, KeyError, RecursionError):
        return response(400, {"code": "INVALID_REQUEST", "message": "Invalid request", "request_id": request_id})
    except (ClientError, BotoCoreError):
        return response(503, {"code": "CONTROL_UNAVAILABLE", "message": "Reconcile the original write",
                              "request_id": request_id})


@lru_cache(maxsize=1)
def clients():
    ddb = boto3.client("dynamodb", region_name=region(), config=Config(
        connect_timeout=2, read_timeout=3, retries={"total_max_attempts": 1}))
    table = os.environ["COLVENNA_CONTROL_TABLE"]
    registry = Registry(ddb, table, os.environ["COLVENNA_DATA_BUCKET"])
    return Proxy(registry, Ownership(ddb, table), os.environ["COLVENNA_SOURCE_COMMIT"]), KeyStore(ddb, table)


def handler(event, context):
    if os.environ.get("COLVENNA_SYNC_ENABLED") != "true":
        return response(503, {"code": "SYNC_DISABLED", "message": "Synchronous writes are disabled",
                              "request_id": event.get("requestContext", {}).get("requestId", "unavailable")})
    return handle(event, *clients())
