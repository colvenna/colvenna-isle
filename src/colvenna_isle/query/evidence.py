"""Capture an HTTP probe response without exposing its temporary credentials."""

import base64
import json

MAX_RESPONSE_BYTES = 4194304


def response_record(response, save, *, secrets=()):
    raw = response.read(MAX_RESPONSE_BYTES + 1)
    replacements = [value for value in secrets if value]

    def redact(value):
        for secret in replacements:
            value = value.replace(secret, "<REDACTED>")
        return value

    safe_raw = raw[:MAX_RESPONSE_BYTES]
    for secret in replacements:
        safe_raw = safe_raw.replace(secret.encode(), b"<REDACTED>")
    result = {"status": response.status,
              "headers": {name: redact(value) for name, value in response.headers.items()},
              "body_utf8": safe_raw.decode(errors="replace"),
              "body_base64": base64.b64encode(safe_raw).decode(),
              "body_truncated": len(raw) > MAX_RESPONSE_BYTES,
              "redacted": safe_raw != raw[:MAX_RESPONSE_BYTES]}
    # Persist the transport evidence even if decoding/contract assertions fail.
    save(result)
    if result["body_truncated"]:
        raise RuntimeError("HTTP_RESPONSE_TOO_LARGE")
    try:
        body = json.loads(safe_raw)
    except (ValueError, UnicodeError):
        raise RuntimeError("HTTP_RESPONSE_NOT_JSON: raw response retained") from None
    return {**result, "body": body}
