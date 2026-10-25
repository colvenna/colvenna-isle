"""Source-local native execution behind a bounded, independently owned process."""

import hashlib
import json
import os
import sys
from pathlib import Path

from colvenna_isle.control.registry import ControlError
from colvenna_isle.query.process import run


def execute(configuration, statements, directory, *, timeout, cancel=lambda: False,
            guard=lambda: None, open_only=False, on_started=lambda identity: None,
             catalog_only=False, consistency="current", condition=None, source=None):
    """Server-only execution; no HTTP/job lifecycle or publication claim.

    A terminal timeout/cancel never publishes a child result. The parent checks
    authorization throughout staging/execution and once more before returning.
    """
    guard()
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=False, mode=0o700)
    request = {"configuration": configuration, "statements": statements, "open_only": open_only,
               "catalog_only": catalog_only, "consistency": consistency, "condition": condition,
               "source": source}
    raw = json.dumps(request, separators=(",", ":"), ensure_ascii=False).encode()
    identity = hashlib.sha256(raw).hexdigest()
    path = directory / "input.json"
    with path.open("xb") as stream:
        os.chmod(path, 0o600)
        stream.write(raw)

    def admitted():
        guard()
        if hashlib.sha256(path.read_bytes()).hexdigest() != identity:
            raise ControlError(409, "QUERY_INPUT_CHANGED")

    terminal = run([sys.executable, "-m", "colvenna_isle.query.worker",
                   str(path), identity, str(directory / "process")],
                   directory / "process", timeout=timeout, cancel=cancel, guard=admitted,
                   on_started=on_started)
    if terminal["outcome"] != "SUCCEEDED":
        failure = directory / "process" / "failure.json"
        return {"terminal": terminal, "result": None,
                "failure": json.loads(failure.read_text()) if failure.exists() else None}
    admitted()
    result_path = directory / "process" / "result.json"
    if result_path.stat().st_size > 16777216:
        raise ControlError(413, "QUERY_RESULT_TOO_LARGE")
    return {"terminal": terminal, "result": json.loads(result_path.read_text())}
