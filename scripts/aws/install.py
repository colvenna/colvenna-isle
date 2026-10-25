#!/usr/bin/env python3
"""Install or upgrade one Colvenna Isle environment from one fixed release build, in dependency order.

COLVENNA_ENVIRONMENT (required) names the environment. `fresh` creates every stack
of a new environment; `upgrade` first closes admission of an installed
environment (old writer stopped with a terminal receipt), then replaces every
component with the release source. Each step runs the repository's own scoped
deployment tool with a new receipt directory; a failed step stops the sequence
for reconciliation of its original operation (no replay).
"""

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT), str(Path(__file__).parent)]
from manage import REGION, REPOSITORY, STACKS, encode, session, source, storage_image

PLANS = {
    # Bearer admission precedes the writer; control binds the writer source afterwards.
    "fresh": ["vault", "baseline", "environments", "control", "auth", "writer", "control-registry", "intake",
              "query", "sync", "editor", "open"],
    "upgrade": ["close", "vault", "environments", "writer", "control", "auth", "intake", "query", "sync", "editor",
                "open"],
}
COMPONENTS = ["control", "writer", "query", "builder-source", "editor"]


def plan_steps(plan, requested=None):
    """The plan's steps, or a requested subset in plan order. Control and writer read the builder configuration
    that the environments step writes into this new receipt directory, so a resume that needs it reruns that
    step; an unchanged builder stack is a confirmed no-op."""
    steps = PLANS[plan]
    if not requested:
        return list(steps)
    selected = set(requested.split(","))
    if not selected <= set(steps):
        raise ValueError("steps must belong to the " + plan + " plan")
    if selected & {"control", "writer", "control-registry"}:
        selected.add("environments")
    return [step for step in steps if step in selected]


def verify_release(release, head):
    """Every component manifest of the release names the deploy worktree's exact source."""
    manifest = json.loads((release / "manifest.json").read_text())
    if manifest.get("format_version") != 1 or manifest["source_commit"] != head:
        raise RuntimeError("Release source differs from the deploy worktree")
    for name in COMPONENTS:
        component = json.loads((release / name / "manifest.json").read_text())
        recorded = manifest["components"][name]
        if (component["source_commit"] != head
                or hashlib.sha256((release / name / "manifest.json").read_bytes()).hexdigest()
                != recorded["manifest_sha256"]):
            raise RuntimeError("Release component manifest differs: " + name)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("plan", choices=sorted(PLANS))
    parser.add_argument("--release", type=Path, required=True, help="release build directory (build_product.py)")
    parser.add_argument("--output", type=Path, required=True, help="new external receipt directory")
    parser.add_argument("--steps", help="comma-separated subset, in plan order (resume after reconciliation)")
    parser.add_argument("--query-count", default="1")
    parser.add_argument("--writer-probe-space", default="",
                        help="qualification only: bind writer fault probes to one provisioned probe_* space")
    parser.add_argument("--writer-probe-generation", default="")
    args = parser.parse_args()
    receipts = args.output.resolve()
    if receipts.exists() or receipts == ROOT or ROOT in receipts.parents:
        parser.error("use a new external receipt directory")
    head = source()
    release = args.release.resolve()
    manifest = verify_release(release, head)
    try:
        steps = plan_steps(args.plan, args.steps)
    except ValueError as error:
        parser.error(str(error))
    current, identity = session()
    lock = json.loads((ROOT / "runtime/writer-lock.json").read_text())
    digest = lock["storage_digest"]
    storage = storage_image({"repository": lock["storage_repository"], "digest": digest})
    image = current.client("ecr").describe_images(repositoryName=REPOSITORY, imageIds=[{"imageDigest": digest}])
    if image["imageDetails"][0]["imageDigest"] != digest:
        raise RuntimeError("The locked native storage image is not published; publish it first")
    receipts.mkdir(parents=True)
    record = {"plan": args.plan, "environment": STACKS, "source_commit": head, "release": str(release),
              "release_manifest_sha256": hashlib.sha256((release / "manifest.json").read_bytes()).hexdigest(),
              "version": manifest["version"], "identity": identity, "steps": []}
    (receipts / "install.json").write_text(encode(record))
    python, configuration = sys.executable, receipts / "environments/configuration.json"
    commands = {
        "vault": ["backup.py", "vault"],
        "baseline": ["manage.py", "create", "--stack", STACKS["baseline"], "--image", storage],
        "close": ["admission.py", "close"],
        "environments": ["environments.py", "--source-bundle", str(release / "builder-source")],
        "control": ["control.py", "deploy", "--bundle", str(release / "control"),
                    "--environment-configuration", str(configuration)],
        "auth": ["control.py", "enable-auth", "--bundle", str(release / "control")],
        "writer": ["writer.py", "deploy", "--bundle", str(release / "control"), "--image-build",
                   str(release / "writer"), "--environment", str(configuration),
                   "--probe-space", args.writer_probe_space, "--probe-generation", args.writer_probe_generation],
        "control-registry": ["control.py", "deploy", "--registry-only", "--bundle", str(release / "control"),
                             "--environment-configuration", str(configuration)],
        "intake": ["intake.py", "--bundle", str(release / "control")],
        "query": ["query.py", "--image-build", str(release / "query")],
        "sync": ["sync.py", "--bundle", str(release / "control")],
        "editor": ["editor.py", "--artifact", str(release / "editor")],
        "open": ["admission.py", "open", "--query-count", args.query_count],
    }
    environment = {key: value for key, value in os.environ.items() if key not in {"AWS_PROFILE", "AWS_DEFAULT_PROFILE"}}
    environment.update(COLVENNA_REGION=REGION, AWS_REGION=REGION, AWS_DEFAULT_REGION=REGION, COLVENNA_ENVIRONMENT=STACKS["environment"])
    for step in steps:
        started = time.monotonic()
        tool, *arguments = commands[step]
        command = [python, str(ROOT / "scripts/aws" / tool), *arguments, "--output", str(receipts / step)]
        with (receipts / (step + ".log")).open("x") as stream:
            result = subprocess.run(command, cwd=ROOT, env=environment, stdout=stream, stderr=subprocess.STDOUT)
        record["steps"].append({"step": step, "command": command, "exit": result.returncode,
                                "seconds": round(time.monotonic() - started)})
        (receipts / "install.json").write_text(encode(record))
        print(json.dumps(record["steps"][-1]), flush=True)
        if result.returncode:
            raise SystemExit(step + " failed; reconcile its original operation before resuming with --steps")
    record["status"] = "INSTALLED" if steps[-1] == "open" else "PARTIAL_STEPS_COMPLETED"
    (receipts / "install.json").write_text(encode(record))
    print(encode({"status": record["status"], "environment": STACKS["environment"], "source_commit": head}))


if __name__ == "__main__":
    main()
