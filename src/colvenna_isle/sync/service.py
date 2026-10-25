"""Fixed private RPC from the synchronous proxy to the approved writer."""

import re
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from colvenna_isle.contracts.validation import validate
from colvenna_isle.control.http import decode_json
from colvenna_isle.control.registry import ControlError, canonical
from colvenna_isle.timing import NOT_TIMED
from colvenna_isle.writer.ownership import matches
from colvenna_isle.writer.writes import write_id


class WriteUnknown(ControlError):
    def __init__(self, identity, code="WRITE_RESULT_UNKNOWN", status=503):
        super().__init__(status, code, "Reconcile or retry the same Idempotency-Key")
        self.write_id = identity


class Proxy:
    def __init__(self, registry, ownership, source_commit, connect=urlopen):
        self.registry, self.ownership = registry, ownership
        self.source_commit, self.connect = source_commit, connect

    def target(self, identity):
        permit = self.ownership.read()
        if permit["state"] != "ACTIVE" or not permit["identity"]:
            raise WriteUnknown(identity, "WRITER_OWNERSHIP_UNKNOWN")
        if permit["identity"]["source_commit"] != self.source_commit:
            raise WriteUnknown(identity, "WRITER_SOURCE_CHANGED")
        return permit

    def invoke(self, principal, configuration, identity, operation, deadline, timing=NOT_TIMED, **fields):
        with timing.segment("writer_target"):
            permit = self.target(identity)
        target = permit["identity"]
        remaining = deadline / 1000 - time.time()
        if remaining <= 0:
            raise WriteUnknown(identity, "WRITE_WAIT_TIMEOUT", 504)
        message = {
            "identity": target, "epoch": permit["epoch"], "space_id": configuration["space_id"],
            "generation": configuration["generation"], "principal_id": principal.principal_id,
            "key_id": principal.key_id, "operation": operation, **fields,
        }
        if timing.identity.get("request_id"):
            message["request_id"] = timing.identity["request_id"]
        request = Request("http://" + target["address"] + ":8082/writes",
                          data=canonical(message).encode(), headers={"Content-Type": "application/json"})
        try:
            with timing.segment("writer_rpc"):
                try:
                    with self.connect(request, timeout=remaining) as response:
                        content, status = response.read(65537), response.status
                except HTTPError as response:
                    content, status = response.read(65537), response.code
        except (URLError, TimeoutError, OSError):
            raise WriteUnknown(identity) from None
        try:
            if len(content) > 65536:
                raise ValueError("Oversized writer response")
            value = decode_json(content)
            timing.adopt(value.get("timing"), "writer_rpc")
            if "error" in value:
                if status in {400, 403, 404, 409, 422, 428}:
                    raise ControlError(status, value["error"])
                raise WriteUnknown(identity, value["error"])
            if not matches(value.get("identity", {}), target) or value.get("epoch") != permit["epoch"]:
                raise WriteUnknown(identity, "WRITER_RESPONSE_IDENTITY_UNKNOWN")
            result = value["result"]
            if result["write_id"] != identity:
                raise WriteUnknown(identity, "WRITE_RESPONSE_IDENTITY_UNKNOWN")
            if result["status"] in {"COMMITTED", "REJECTED"}:
                validate("write_receipt", result)
                if (result["space_id"] != configuration["space_id"]
                        or result["generation"] != configuration["generation"]
                        or status not in {200, 422}
                        or result["status"] == "COMMITTED" and status != 200):
                    raise WriteUnknown(identity, "WRITE_RESPONSE_IDENTITY_UNKNOWN")
            elif result["status"] == "UNKNOWN":
                raise WriteUnknown(identity, result.get("code", "WRITE_RESULT_UNKNOWN"),
                                   504 if status == 504 else 503)
            elif result["status"] != "NOT_SUBMITTED" or status != 429:
                raise WriteUnknown(identity)
            else:
                validate("write_not_submitted", result)
            return status, result
        except (KeyError, TypeError, ValueError):
            raise WriteUnknown(identity, "WRITE_RESPONSE_UNCONFIRMED") from None

    def submit(self, principal, space, payload, idempotency_key, started_ms, timing=NOT_TIMED):
        principal.authorize(space, "data:write")
        validate("write_request", payload)
        if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", idempotency_key or ""):
            raise ControlError(428, "IDEMPOTENCY_KEY_REQUIRED")
        with timing.segment("dataset"):
            configuration, registered = self.registry.dataset(principal, space, payload["dataset"], "data:write")
        definition = registered["definition"]
        if definition.get("write_owner") != "direct":
            raise ControlError(403, "DIRECT_WRITE_TARGET_REQUIRED")
        if payload["schema_version"] != definition["schema_version"]:
            raise ControlError(409, "SCHEMA_VERSION_MISMATCH")
        with timing.segment("validate"):
            validate("write_request", payload, definition=definition)
        identity = write_id(space, configuration["generation"], payload["dataset"], idempotency_key)
        timing.bind(space_id=space, generation=configuration["generation"], write_id=identity,
                    dataset=payload["dataset"], operation=payload["operation"])
        deadline = started_ms + payload.get("timeout_ms", 10000)
        status, value = self.invoke(principal, configuration, identity, "write", deadline, timing,
            payload=payload, idempotency_key=idempotency_key, deadline_unix_ms=deadline)
        if value["status"] in {"COMMITTED", "REJECTED"} and (
                value["dataset"] != payload["dataset"] or value["schema_version"] != payload["schema_version"]):
            raise WriteUnknown(identity, "WRITE_RESPONSE_IDENTITY_UNKNOWN")
        return status, value

    def status(self, principal, space, identity, started_ms):
        principal.authorize(space, "datasets:manage" if "datasets:manage" in principal.scopes else "data:write")
        if not re.fullmatch(r"write_[0-9a-f]{64}", identity or ""):
            raise ControlError(400, "INVALID_WRITE_ID")
        configuration = self.registry._read(space)
        self.registry._authorize_generation(principal, configuration)
        return self.invoke(principal, configuration, identity, "write_status", started_ms + 10000, write_id=identity)
