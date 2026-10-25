#!/usr/bin/env python3
"""Deploy source-bound intake through its retained CloudFormation owner, initially disabled."""

import argparse
import hashlib
import json
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT), str(ROOT / "src"), str(Path(__file__).parent)]
from infra.template import template
from manage import STACKS, encode, outputs, owned_stack, publish_artifact, session, source, template_arguments, update_stack, wait
from control import stopped


def deploy_intake(current, baseline, artifact, head, directory, *, enabled=False):
    directory.mkdir()
    values = outputs(baseline)
    (directory / "stopped.json").write_text(encode(stopped(current, values)))
    previous = {entry["ParameterKey"]: entry["ParameterValue"] for entry in baseline["Parameters"]}
    definitions = template()
    supplied = {
        "SourceCommit": head, "QueryDesired": "0", "IntakeCodeBucket": values["DataBucket"],
        "IntakeCodeKey": artifact["key"], "IntakeCodeVersion": artifact["version_id"],
        "IntakeSourceCommit": head, "IntakeEnabled": "true" if enabled else "false",
    }
    parameters = [{"ParameterKey": name, "ParameterValue": value} for name, value in supplied.items()]
    parameters += [{"ParameterKey": name, "UsePreviousValue": True}
                   for name in previous if name not in supplied and name in definitions["Parameters"]]
    if previous.get("AuthEnabled") != "true" or not previous.get("AuthSourceCommit"):
        raise RuntimeError("Require the previously qualified Bearer authentication")
    body = json.dumps(definitions, separators=(",", ":"))
    cf = current.client("cloudformation")
    (directory / "template.json").write_text(body + "\n")
    (directory / "validation.json").write_text(encode(cf.validate_template(**template_arguments(current, body))))
    token = "colvenna-intake-" + uuid.uuid4().hex
    (directory / "admission.json").write_text(encode({
        "stack_id": baseline["StackId"], "source_commit": head, "artifact": artifact,
        "intake_enabled": enabled, "client_request_token": token,
        "storage_image": previous["StorageImage"], "auth_source_commit": previous["AuthSourceCommit"],
        "template_sha256": hashlib.sha256(body.encode()).hexdigest(), "parameters": parameters}))
    result = update_stack(
        cf, StackName=baseline["StackId"], **template_arguments(current, body), Capabilities=["CAPABILITY_IAM"],
        Parameters=parameters, ClientRequestToken=token,
        Tags=[{"Key": "Project", "Value": "colvenna-isle"}, {"Key": "Component", "Value": "core"},
              {"Key": "SourceCommit", "Value": head}])
    (directory / "dispatch.json").write_text(encode(result))
    if wait(cf, result["StackId"], directory / "terminal.json") != "UPDATE_COMPLETE":
        raise RuntimeError("Intake deployment incomplete; reconcile original operation")
    final = owned_stack(cf, STACKS["baseline"])
    retained = {entry["ParameterKey"]: entry["ParameterValue"] for entry in final["Parameters"]}
    if (retained["StorageImage"] != previous["StorageImage"]
            or retained["AuthSourceCommit"] != previous["AuthSourceCommit"]
            or retained["AuthCodeVersion"] != previous["AuthCodeVersion"]):
        raise RuntimeError("Independent source identities were not preserved")
    (directory / "stopped-after.json").write_text(encode(stopped(current, outputs(final))))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    directory = args.output.resolve()
    if directory.exists() or directory == ROOT or ROOT in directory.parents:
        parser.error("preserve originals; use a new external receipt directory")
    head = source()
    manifest = json.loads((args.bundle / "manifest.json").read_text())
    artifact = (args.bundle / manifest["zip"]).resolve()
    if (args.bundle.resolve() not in artifact.parents or manifest["status"] != "PASS"
            or manifest["source_commit"] != head
            or hashlib.sha256(artifact.read_bytes()).hexdigest() != manifest["zip_sha256"]):
        raise RuntimeError("Artifact/source identity mismatch")
    current, identity = session()
    baseline = owned_stack(current.client("cloudformation"), STACKS["baseline"])
    if baseline["StackStatus"] != "UPDATE_COMPLETE":
        raise RuntimeError("Reconcile the original baseline operation")
    values = outputs(baseline)
    directory.mkdir(parents=True)
    (directory / "identity.json").write_text(encode(identity))
    (directory / "baseline.json").write_text(encode(baseline))
    (directory / "stopped.json").write_text(encode(stopped(current, values)))
    key = f"artifacts/intake/{head}/{manifest['zip_sha256']}/intake.zip"
    (directory / "artifact-admission.json").write_text(encode({
        "source_commit": head, "bucket": values["DataBucket"], "key": key,
        "sha256": manifest["zip_sha256"], "conditional_creation": True}))
    binding = {"bucket": values["DataBucket"], "key": key, "version_id": publish_artifact(
        current.client("s3"), values["DataBucket"], key, artifact.read_bytes(), manifest["zip_sha256"]),
        "sha256": manifest["zip_sha256"]}
    (directory / "artifact.json").write_text(encode(binding))
    deploy_intake(current, baseline, binding, head, directory / "intake", enabled=False)
    print(json.dumps({"status": "PASS", "source_commit": head, "intake_enabled": False}))


if __name__ == "__main__":
    main()
