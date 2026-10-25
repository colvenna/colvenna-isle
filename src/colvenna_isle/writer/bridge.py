"""IAM-invoked private control bridge; no API Gateway permission or public URL."""

import json
import os
from functools import lru_cache
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import boto3
from botocore.config import Config

from colvenna_isle.region import region
from colvenna_isle.control.registry import ControlError
from colvenna_isle.writer.ownership import Ownership, matches

# The public status and diagnostics routes must answer inside the API's 30 s integration limit. Their read-only
# observations give up early on an unreachable writer (a task lost before its heartbeat goes stale) and
# report it as unknown; operations that change state keep the full wait.
OBSERVATIONS = {"space_overview", "governance_read"}
OBSERVATION_TIMEOUT, OPERATION_TIMEOUT = 8, 20


@lru_cache(maxsize=1)
def clients():
    """Reuse SDK models/pools; no ownership or approval response is cached."""
    current = boto3.Session(region_name=region())
    options = Config(connect_timeout=3, read_timeout=5,
                     retries={"mode": "standard", "total_max_attempts": 2})
    return current.client("dynamodb", config=options), current.client("ecs", config=options)


def handler(event, context):
    ddb, ecs = clients()
    ownership = Ownership(ddb, os.environ["COLVENNA_CONTROL_TABLE"])
    try:
        permit = ownership.read()
        target = permit["identity"]
        if event.get("probe_target"):
            # Qualification can contact a revoked/waiting original, to verify its
            # independent denial. Caller cannot select an arbitrary URL/address.
            if not os.environ.get("COLVENNA_WRITER_PROBE_SPACE", "").startswith("probe_"):
                raise ControlError(403, "QUALIFICATION_DISABLED")
            registration = ownership.registration(event["probe_target"])
            if not registration:
                raise ControlError(503, "WRITER_REGISTRATION_UNKNOWN")
            target = registration["identity"]
            result = ecs.describe_tasks(cluster=os.environ["COLVENNA_CLUSTER"], tasks=[target["task_arn"]])
            if (result.get("failures") or len(result["tasks"]) != 1
                    or result["tasks"][0]["lastStatus"] != "RUNNING"
                    or result["tasks"][0]["taskDefinitionArn"] != target["task_definition"]):
                raise ControlError(503, "WRITER_TASK_UNKNOWN")
            addresses = [entry["value"] for attachment in result["tasks"][0]["attachments"]
                         for entry in attachment.get("details", []) if entry["name"] == "privateIPv4Address"]
            if addresses != [target["address"]]:
                raise ControlError(503, "WRITER_ADDRESS_UNKNOWN")
        elif permit["state"] != "ACTIVE" or not target:
            raise ControlError(503, "WRITER_OWNERSHIP_UNKNOWN")
        if not matches(event.get("identity", {}), target) or event.get("epoch") != permit["epoch"]:
            raise ControlError(409, "WRITER_IDENTITY_CHANGED")
        payload = {key: value for key, value in event.items() if key != "probe_target"}
        request = Request("http://" + target["address"] + ":8081/internal",
                          data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"})
        try:
            timeout = OBSERVATION_TIMEOUT if payload.get("operation") in OBSERVATIONS else OPERATION_TIMEOUT
            with urlopen(request, timeout=timeout) as response:
                body, status = json.load(response), response.status
        except HTTPError as error:
            body, status = json.load(error), error.code
        except (URLError, TimeoutError):
            raise ControlError(503, "WRITER_RESULT_UNKNOWN") from None
        if status == 200 and (not matches(body.get("identity", {}), target)
                              or body.get("epoch", body.get("permit", {}).get("epoch")) != permit["epoch"]):
            raise ControlError(503, "WRITER_RESPONSE_IDENTITY_UNKNOWN")
        return {"statusCode": status, "body": body}
    except ControlError as error:
        return {"statusCode": error.status, "body": {"error": error.code}}
    except Exception:
        return {"statusCode": 503, "body": {"error": "WRITER_RESULT_UNKNOWN"}}
