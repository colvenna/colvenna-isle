#!/usr/bin/env python3
"""Package clean, source-bound trusted CodeBuild code, never user dependencies."""

import argparse
import hashlib
import json
import subprocess
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BUILDSPEC = """version: 0.2
phases:
  install:
    runtime-versions:
      python: "3.12"
    commands:
      - 'python3 -m pip install --require-hashes --only-binary=:all: --no-compile -r runtime/contracts-requirements.lock -r runtime/aws-requirements.lock'
  build:
    commands:
      - 'python3 scripts/build_environment.py --output /tmp/colvenna-environment-build'
"""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if output.exists() or output == ROOT or ROOT in output.parents:
        parser.error("use a new external source directory")
    if subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip():
        parser.error("commit source before packaging")
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    tree = subprocess.check_output(["git", "rev-parse", "HEAD^{tree}"], cwd=ROOT, text=True).strip()
    names = subprocess.check_output([
        "git", "ls-files", "-z", "--", "src", "runtime", "scripts/build_environment.py", "LICENSE"],
        cwd=ROOT).decode().split("\0")
    content = {name: (ROOT / name).read_bytes() for name in names if name}
    content["buildspec.yml"] = BUILDSPEC.encode()
    manifest = {
        "source_commit": head, "source_tree": tree,
        "adapter_sha256": hashlib.sha256(content[
            "src/colvenna_isle/pipeline/environment_adapter.py"]).hexdigest(),
        "files": {name: hashlib.sha256(raw).hexdigest() for name, raw in content.items()},
    }
    raw_manifest = canonical(manifest).encode()
    content["builder-source.json"] = raw_manifest
    output.mkdir(parents=True)
    archive_path = output / "builder.zip"
    with zipfile.ZipFile(archive_path, "x", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, raw in sorted(content.items()):
            info = zipfile.ZipInfo(name, (2026, 10, 16, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            archive.writestr(info, raw)
    (output / "manifest.json").write_text(canonical({
        "status": "PACKAGED_NOT_RUNTIME_QUALIFIED", "source_commit": head, "source_tree": tree,
        "zip": archive_path.name, "zip_sha256": hashlib.sha256(archive_path.read_bytes()).hexdigest(),
        "builder_source_sha256": hashlib.sha256(raw_manifest).hexdigest(),
        "adapter_sha256": manifest["adapter_sha256"],
    }) + "\n")
    print(canonical({"status": "PACKAGED_NOT_RUNTIME_QUALIFIED", "source_commit": head}))


if __name__ == "__main__":
    main()
