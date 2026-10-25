#!/usr/bin/env python3
"""Build a source-bound Lambda ZIP using the hash-locked Python 3.12 arm64 closure."""

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_REGION = json.loads((ROOT / "tests/fixture-region.json").read_text())["region"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if output.exists() or output == ROOT or ROOT in output.parents:
        parser.error("use a new external artifact directory")
    if sys.version_info[:2] != (3, 12) or platform.machine() != "aarch64":
        parser.error("current Lambda target is Python 3.12 / Linux arm64")
    if subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip():
        parser.error("commit source before building")
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    tree = subprocess.check_output(["git", "rev-parse", "HEAD^{tree}"], cwd=ROOT, text=True).strip()
    output.mkdir(parents=True)
    package = output / "package"
    command = [sys.executable, "-m", "pip", "install", "--require-hashes", "--only-binary=:all:",
               "--no-compile", "--target", str(package), "-r", str(ROOT / "runtime/contracts-requirements.lock"),
               "-r", str(ROOT / "runtime/aws-requirements.lock")]
    with (output / "install.log").open("x") as stream:
        subprocess.run(command, check=True, stdout=stream, stderr=subprocess.STDOUT)
    # Dependency CLI scripts bind to the build host's interpreter. Services
    # import these packages as modules, so those host-specific entrypoints
    # do not belong in the Lambda ZIP or the writer/query application.
    if (package / "bin").exists():
        shutil.rmtree(package / "bin")
    shutil.copytree(ROOT / "src/colvenna_isle", package / "colvenna_isle", ignore=shutil.ignore_patterns("__pycache__"))
    sources = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
               for path in sorted((ROOT / "src/colvenna_isle").rglob("*"))
               if path.is_file() and "__pycache__" not in path.parts}
    (package / "colvenna-source.json").write_text(json.dumps({
        "source_commit": head, "source_tree": tree, "sources": sources}, sort_keys=True) + "\n")
    # Suites run without per-request timing lines (timing tests enable it themselves), against the AWS
    # documentation example account.
    env = dict(os.environ, PYTHONPATH=str(package), COLVENNA_TIMING="off", COLVENNA_ACCOUNT="123456789012",
               COLVENNA_REGION=FIXTURE_REGION, AWS_REGION=FIXTURE_REGION, AWS_DEFAULT_REGION=FIXTURE_REGION)
    checks = [
        [sys.executable, "-m", "colvenna_isle.contracts", "generate", "--check"],
        [sys.executable, "-m", "unittest", "discover", "-s", str(ROOT / "tests/control"), "-v"],
        [sys.executable, "-m", "unittest", "discover", "-s", str(ROOT / "tests/intake"), "-v"],
        [sys.executable, "-m", "unittest", "discover", "-s", str(ROOT / "tests/writer"), "-v"],
        [sys.executable, "-m", "unittest", "discover", "-s", str(ROOT / "tests/query"), "-v"],
        *[[sys.executable, "-m", "unittest", "discover", "-s", str(ROOT / "tests/pipeline"),
           "-p", name, "-v"] for name in [
            "test_output_validation.py", "test_workspace.py", "test_task_adapter.py", "test_candidates.py",
            "test_sql_tasks.py", "test_environment_check.py"]],
    ]
    with (output / "validation.log").open("x") as stream:
        for command in checks:
            subprocess.run(command, cwd=output, env=env, check=True, stdout=stream, stderr=subprocess.STDOUT)
    target = output / "control.zip"
    with zipfile.ZipFile(target, "x", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(package.rglob("*")):
            if path.is_file() and "__pycache__" not in path.parts:
                info = zipfile.ZipInfo(str(path.relative_to(package)), (2026, 10, 15, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = 0o644 << 16
                archive.writestr(info, path.read_bytes())
    (output / "manifest.json").write_text(json.dumps({
        "status": "PASS", "scope": "LAMBDA_SERVICE_ARTIFACT_NOT_AWS_ACCEPTANCE",
        "source_commit": head, "source_tree": tree, "zip": target.name,
        "zip_sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
        "locks": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in [
            "runtime/contracts-requirements.lock", "runtime/aws-requirements.lock"]},
        "not_run": ["AWS control service", "Bearer authentication"],
    }, indent=2) + "\n")
    print(json.dumps({"status": "PASS", "source_commit": head, "artifact": str(target)}))


if __name__ == "__main__":
    main()
