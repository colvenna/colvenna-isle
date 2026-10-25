"""Principal-bound private writer commands through the IAM-only control bridge."""

import json

from botocore.exceptions import BotoCoreError, ClientError

from .registry import ControlError, canonical


class WriterUnknown(ControlError):
    """The command may or may not have executed; reconcile the original request."""

    def __init__(self, code="WRITER_RESULT_UNKNOWN"):
        super().__init__(503, code)


class WriterClient:
    def __init__(self, ownership, client, function):
        self.ownership, self.client, self.function = ownership, client, function

    def call(self, principal, configuration, operation, **fields):
        """Send one command to the currently approved writer for this principal.

        Every response is checked against the same permit identity/epoch by the
        bridge. Transport loss, bridge 5xx and identity changes are UNKNOWN; a
        deterministic writer rejection is returned as its own ControlError.
        """
        permit = self.ownership.read()
        identity = permit["identity"]
        if not self.function or permit["state"] != "ACTIVE" or not identity:
            raise ControlError(503, "WRITER_AUTHORITY_UNAVAILABLE")
        request = {"identity": identity, "epoch": permit["epoch"], "operation": operation,
                   "space_id": configuration["space_id"], "generation": configuration["generation"],
                   "key_id": principal.key_id, "principal_id": principal.principal_id, **fields}
        try:
            response = self.client.invoke(FunctionName=self.function, Payload=canonical(request).encode())
            result = json.load(response["Payload"])
        except (ClientError, BotoCoreError, ValueError, KeyError, TypeError):
            raise WriterUnknown() from None
        if response.get("FunctionError") or not isinstance(result, dict):
            raise WriterUnknown()
        status, body = result.get("statusCode"), result.get("body") or {}
        if status == 200 and isinstance(body.get("result"), dict):
            return body["result"]
        code = body.get("error") if isinstance(body.get("error"), str) else "WRITER_RESULT_UNKNOWN"
        if type(status) is not int or status >= 500 or code in {"WRITER_RESULT_UNKNOWN", "WRITER_IDENTITY_CHANGED"}:
            raise WriterUnknown(code)
        raise ControlError(status, code)
