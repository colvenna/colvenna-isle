"""Shared HTTP parsing and verified Gateway identity for Lambda services."""

import base64
import json

from colvenna_isle.contracts.model import LIMITS
from colvenna_isle.contracts.validation import ContractError, finite_json
from .registry import ControlError, Principal


def unique_pairs(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ContractError("Duplicate JSON key")
        value[key] = item
    return value


def body_bytes(event):
    content = event.get("body") or ""
    value = base64.b64decode(content, validate=True) if event.get("isBase64Encoded") else content.encode()
    if len(value) > LIMITS["api_body_bytes"]:
        raise ControlError(413, "BODY_TOO_LARGE")
    headers = {name.lower(): value for name, value in event.get("headers", {}).items()}
    if headers.get("content-type", "").split(";")[0].lower() != "application/json":
        raise ControlError(415, "JSON_REQUIRED")
    return value


def decode_json(content):
    value = json.loads(content.decode(), object_pairs_hook=unique_pairs)
    finite_json(value)
    return value


def parse_body(event):
    return decode_json(body_bytes(event))


def principal_from_event(event):
    trusted = event.get("requestContext", {}).get("authorizer", {}).get("lambda", {})
    try:
        principal_id = trusted["principal_id"]
        spaces, scopes = json.loads(trusted["spaces"]), json.loads(trusted["scopes"])
        if (not isinstance(principal_id, str) or not principal_id
                or not isinstance(spaces, list) or not isinstance(scopes, list)
                or not all(isinstance(item, str) for item in spaces + scopes)):
            raise ValueError("Invalid context")
        return Principal(principal_id, frozenset(spaces), frozenset(scopes),
                         trusted.get("key_id"), trusted.get("generation"), trusted.get("expires_at"))
    except (KeyError, TypeError, ValueError):
        raise ControlError(403, "FORBIDDEN") from None


def verified_principal(event, keys):
    principal = principal_from_event(event)
    headers = {name.lower(): value for name, value in event.get("headers", {}).items()}
    if keys.authenticate(headers.get("authorization", "")) != principal:
        raise ControlError(403, "FORBIDDEN")
    return principal


def response(status, value):
    return {"statusCode": status, "headers": {"content-type": "application/json", "cache-control": "no-store"},
            "body": json.dumps(value, separators=(",", ":"))}
