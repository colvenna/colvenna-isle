"""Uncached HTTP API Lambda Authorizer; never log or return credentials/digests."""

import json
import os

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from .keys import KeyStore
from .registry import ControlError

from colvenna_isle.region import region


def authorize(event, keys):
    try:
        headers = {name.lower(): value for name, value in event.get("headers", {}).items()}
        principal = keys.authenticate(headers.get("authorization", ""))
        return {"isAuthorized": True, "context": {
            "principal_id": principal.principal_id, "key_id": principal.key_id,
            "spaces": json.dumps(sorted(principal.spaces)), "scopes": json.dumps(sorted(principal.scopes)),
            "generation": principal.generation, "expires_at": principal.expires_at,
        }}
    except (ControlError, ClientError, BotoCoreError, ValueError, TypeError, KeyError):
        return {"isAuthorized": False}


def handler(event, context):
    if os.environ.get("COLVENNA_AUTH_ENABLED") != "true":
        return {"isAuthorized": False}
    return probe_handler(event, context)


def probe_handler(event, context):
    """IAM-only qualification function; no API Gateway invocation permission."""
    return authorize(event, KeyStore(boto3.client("dynamodb", region_name=region()), os.environ["COLVENNA_CONTROL_TABLE"]))
