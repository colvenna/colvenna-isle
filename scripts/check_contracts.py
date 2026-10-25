#!/usr/bin/env python3
"""Run contract tests and bind an optional receipt to sources, locks and environment."""

import argparse
import hashlib
import importlib.metadata
import json
import platform
import re
import subprocess
import sys
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from colvenna_isle.contracts.model import artifacts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--source-commit", help="explicit identity when checking a git archive")
    args = parser.parse_args()
    if args.source_commit and not re.fullmatch(r"[0-9a-f]{40}", args.source_commit):
        parser.error("--source-commit must be a full lowercase SHA")
    started = time.time_ns()
    generated = ROOT / "src/colvenna_isle/contracts/v2"
    for name, value in artifacts().items():
        expected = json.dumps(value, sort_keys=True, indent=2) + "\n"
        if (generated / name).read_text() != expected:
            raise RuntimeError(f"Generated contract differs: {name}")
    suite = unittest.defaultTestLoader.discover(str(ROOT / "tests/contracts"))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    paths = sorted(set(
        list((ROOT / "src/colvenna_isle").rglob("*.py"))
        + list(generated.glob("*.json"))
        + list((ROOT / "tests/contracts").rglob("*.py"))
        + [ROOT / "scripts/check_contracts.py", ROOT / "runtime/contracts-requirements.lock",
           ROOT / "examples/orders/pipeline.yaml", ROOT / "examples/orders/requirements.lock",
           ROOT / "examples/orders/run_local.py", ROOT / "examples/orders/python/score_orders.py"]
    ))
    receipt = {
        "schema_version": 1, "status": "PASS" if result.wasSuccessful() else "FAIL",
        "scope": "OFFLINE_CONTRACT_ONLY", "test_groups": result.testsRun,
        "errors": len(result.errors), "failures": len(result.failures),
        "started_unix_ns": started, "finished_unix_ns": time.time_ns(),
        "base_commit": args.source_commit or subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, check=True,
                                       capture_output=True, text=True).stdout.strip(),
        "environment": {"python": platform.python_version(), "architecture": platform.machine(),
                        "packages": {name: importlib.metadata.version(name) for name in [
                            "jsonschema", "attrs", "jsonschema-specifications", "referencing",
                            "rpds-py", "typing-extensions", "PyYAML"]}},
        "sources": {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths},
        "not_run": ["live API authentication/authorization", "SQL sandbox", "AWS execution",
                    "production adapter/publication", "performance/recovery/cost release acceptance"],
    }
    if args.receipt:
        args.receipt.parent.mkdir(parents=True, exist_ok=True)
        if args.receipt.exists():
            raise RuntimeError("Evidence receipt already exists; preserve the original")
        args.receipt.write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps({"status": receipt["status"], "test_groups": result.testsRun, "scope": receipt["scope"]}))
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
