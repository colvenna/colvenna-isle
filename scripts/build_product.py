#!/usr/bin/env python3
"""Build every deployable component of one clean source commit into one new release directory.

The release manifest binds the product version, exact source/tree, each
component manifest's SHA-256 and the local writer/query image IDs. Each
component builder runs its own checks; any failure leaves no PASS manifest.
Installation (scripts/aws/install.py) accepts only a release of the deploy
worktree's own commit.
"""

import argparse
import hashlib
import json
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
STEPS = [
    ("control", ["build_control.py"]),
    ("writer", ["build_writer.py", "--service", "writer", "--bundle", "{control}"]),
    ("query", ["build_writer.py", "--service", "query", "--bundle", "{control}"]),
    ("builder-source", ["build_environment_source.py"]),
    ("editor", ["build_editor.py"]),
]


def git(*arguments):
    return subprocess.check_output(["git", *arguments], cwd=ROOT, text=True).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--storage-image", help="local storage image for offline qualification; identity remains locked")
    args = parser.parse_args()
    output = args.output.resolve()
    if output.exists() or output == ROOT or ROOT in output.parents:
        parser.error("use a new external release directory")
    if git("status", "--porcelain"):
        parser.error("commit source before building a release")
    head, tree = git("rev-parse", "HEAD"), git("rev-parse", "HEAD^{tree}")
    version = re.fullmatch(r'__version__ = "([^"]+)"\n', (ROOT / "src/colvenna_isle/_version.py").read_text())[1]
    lock = json.loads((ROOT / "runtime/writer-lock.json").read_text())
    output.mkdir(parents=True)
    manifest = {"format_version": 1, "product": "colvenna-isle", "version": version, "source_commit": head,
                "source_tree": tree, "status": "BUILDING", "started_unix": time.time(),
                "storage_image": {name: lock["storage_" + name] for name in ["repository", "digest"]},
                "components": {}}
    for name, (script, *arguments) in STEPS:
        command = [sys.executable, str(ROOT / "scripts" / script),
                   *[value.format(control=output / "control") for value in arguments], "--output", str(output / name)]
        if name in {"writer", "query"} and args.storage_image:
            command.extend(["--storage-image", args.storage_image])
        started = time.monotonic()
        with (output / (name + ".log")).open("x") as stream:
            result = subprocess.run(command, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT)
        if result.returncode:
            manifest.update(status="FAIL", failed_component=name)
            (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
            raise SystemExit(name + " build failed; see " + str(output / (name + ".log")))
        raw = (output / name / "manifest.json").read_bytes()
        component = json.loads(raw)
        if component["source_commit"] != head:
            raise SystemExit(name + " built a different source")
        manifest["components"][name] = {"manifest_sha256": hashlib.sha256(raw).hexdigest(),
                                        "status": component["status"],
                                        "seconds": round(time.monotonic() - started)}
        if name in {"writer", "query"}:
            manifest["components"][name].update(image_id=component["image_id"], tag=component["tag"])
    if git("rev-parse", "HEAD") != head or git("status", "--porcelain"):
        raise SystemExit("Source changed while building")
    manifest.update(status="PASS", finished_unix=time.time())
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({"status": "PASS", "version": version, "source_commit": head, "release": str(output)}))


if __name__ == "__main__":
    main()
