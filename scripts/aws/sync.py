#!/usr/bin/env python3
"""Install the source-bound synchronous proxy, disabled, with physical idle proof."""

import argparse
import hashlib
import json
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT), str(ROOT / "src"), str(Path(__file__).parent)]
from infra.template import template
from manage import ACCOUNT, STACKS, encode, outputs, owned_stack, publish_artifact, session, source, template_arguments, update_stack, wait
from control import stopped


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    directory = args.output.resolve()
    if directory.exists() or directory == ROOT or ROOT in directory.parents:
        parser.error("use a fresh external receipt directory; preserve uncertain originals")
    head = source()
    manifest = json.loads((args.bundle / "manifest.json").read_text())
    artifact = (args.bundle / manifest["zip"]).resolve()
    if (args.bundle.resolve() not in artifact.parents or manifest["status"] != "PASS"
            or manifest["source_commit"] != head
            or hashlib.sha256(artifact.read_bytes()).hexdigest() != manifest["zip_sha256"]):
        raise RuntimeError("Synchronous artifact/source identity mismatch")
    current, identity = session()
    cf = current.client("cloudformation")
    baseline = owned_stack(cf, STACKS["baseline"])
    if baseline["StackStatus"] != "UPDATE_COMPLETE":
        raise RuntimeError("Reconcile the original baseline operation")
    directory.mkdir(parents=True)

    def save(name, value):
        with (directory / (name + ".json")).open("x") as stream:
            stream.write(encode(value))

    values = outputs(baseline)
    save("identity", identity)
    save("baseline", baseline)
    save("stopped", stopped(current, values))
    previous = {entry["ParameterKey"]: entry["ParameterValue"] for entry in baseline["Parameters"]}
    if previous.get("AuthEnabled") != "true" or previous.get("IntakeEnabled") != "false":
        raise RuntimeError("Require Bearer authorization and paused business admission")
    queues = {}
    for name in ["IngressQueue", "Dlq"]:
        queues[name] = current.client("sqs").get_queue_attributes(QueueUrl=values[name], AttributeNames=[
            "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed"])["Attributes"]
        if any(int(count) for count in queues[name].values()):
            raise RuntimeError("Reconcile original queued work before installing a new source")
    save("queues", queues)
    key = f"artifacts/sync/{head}/{manifest['zip_sha256']}/sync.zip"
    save("artifact-admission", {"source_commit": head, "bucket": values["DataBucket"], "key": key,
                                "sha256": manifest["zip_sha256"], "conditional_creation": True})
    s3 = current.client("s3")
    created = {"VersionId": publish_artifact(s3, values["DataBucket"], key, artifact.read_bytes(),
                                             manifest["zip_sha256"])}
    binding = {"bucket": values["DataBucket"], "key": key, "version_id": created["VersionId"],
               "sha256": manifest["zip_sha256"]}
    save("artifact", binding)
    readback = s3.get_object(Bucket=values["DataBucket"], Key=key, VersionId=created["VersionId"],
                            ExpectedBucketOwner=ACCOUNT)
    with readback["Body"] as stream:
        sha = hashlib.sha256(stream.read()).hexdigest()
    if sha != binding["sha256"] or readback.get("VersionId") != binding["version_id"]:
        raise RuntimeError("Fixed-version synchronous artifact readback differs")
    save("artifact-readback", {"status": "PASS", **binding})
    definition = template()
    supplied = {"SourceCommit": head, "QueryDesired": "0", "SyncCodeBucket": values["DataBucket"],
                "SyncCodeKey": key, "SyncCodeVersion": binding["version_id"],
                "SyncSourceCommit": head, "SyncEnabled": "false"}
    parameters = [{"ParameterKey": name, "ParameterValue": value} for name, value in supplied.items()]
    parameters += [{"ParameterKey": name, "UsePreviousValue": True}
                   for name in previous if name not in supplied and name in definition["Parameters"]]
    body = json.dumps(definition, separators=(",", ":"))
    (directory / "template.json").write_text(body + "\n")
    save("validation", cf.validate_template(**template_arguments(current, body)))
    token = "colvenna-sync-" + uuid.uuid4().hex
    save("deployment-admission", {
        "stack_id": baseline["StackId"], "source_commit": head, "artifact": binding,
        "client_request_token": token, "parameters": parameters,
        "template_sha256": hashlib.sha256(body.encode()).hexdigest(), "sync_enabled": False})
    dispatched = update_stack(
        cf, StackName=baseline["StackId"], **template_arguments(current, body), Capabilities=["CAPABILITY_IAM"],
        Parameters=parameters, ClientRequestToken=token,
        Tags=[{"Key": "Project", "Value": "colvenna-isle"}, {"Key": "Component", "Value": "core"},
              {"Key": "SourceCommit", "Value": head}])
    save("deployment-dispatch", dispatched)
    if wait(cf, dispatched["StackId"], directory / "deployment-terminal.json") != "UPDATE_COMPLETE":
        raise RuntimeError("Reconcile the original synchronous deployment")
    final = owned_stack(cf, STACKS["baseline"])
    retained = {entry["ParameterKey"]: entry["ParameterValue"] for entry in final["Parameters"]}
    for name in ["StorageImage", "AuthCodeVersion", "AuthSourceCommit", "IntakeCodeVersion",
                 "IntakeSourceCommit", "WriterImage", "WriterSourceCommit", "WriterCodeVersion"]:
        if retained.get(name) != previous.get(name):
            raise RuntimeError("Independent source changed: " + name)
    save("final", final)
    save("stopped-after", stopped(current, outputs(final)))
    print(json.dumps({"status": "PASS", "source_commit": head, "sync_enabled": False}))


if __name__ == "__main__":
    main()
