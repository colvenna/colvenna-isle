#!/usr/bin/env python3
"""Install exact browser assets on the positively owned, idle baseline stack."""
import argparse
import base64
import hashlib
import json
import sys
import time
import uuid
import zipfile
from pathlib import Path

from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT), str(ROOT / "src"), str(ROOT / "scripts"), str(Path(__file__).parent)]
from build_editor import FILES, validate_artifact
from infra.template import template
from manage import ACCOUNT, STACKS, encode, outputs, owned_stack, session, source, template_arguments, wait
from control import stopped

WEB_RESOURCES = {"Headers", "BrowserApiCache", "BrowserApiForwarding", "Distribution", "WebPolicy"}


def check_template(original, candidate):
    """Reject unrelated infrastructure changes before creating a change set."""
    for section in set(original) | set(candidate):
        old, new = original.get(section), candidate.get(section)
        if old == new:
            continue
        allowed = (WEB_RESOURCES if section == "Resources" else {"BrowserSourceCommit"}
                   if section in {"Parameters", "Outputs"} else set())
        if not isinstance(old, dict) or not isinstance(new, dict):
            raise RuntimeError("Unrelated template section changed: " + section)
        changed = {name for name in set(old) | set(new) if old.get(name) != new.get(name)}
        if not changed <= allowed:
            raise RuntimeError("Unrelated template changes: " + section + " " + ", ".join(sorted(changed)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    directory = args.output.resolve()
    if directory.exists() or directory == ROOT or ROOT in directory.parents:
        parser.error("use a fresh external receipt directory; reconcile uncertain originals")
    head = source()
    artifact = validate_artifact(args.artifact.resolve(), head)
    current, identity = session()
    cf = current.client("cloudformation", config=Config(retries={"total_max_attempts": 1}))
    baseline = owned_stack(cf, STACKS["baseline"])
    if baseline["StackStatus"] != "UPDATE_COMPLETE":
        raise RuntimeError("Reconcile the original stack operation")
    values = outputs(baseline)
    previous = {item["ParameterKey"]: item["ParameterValue"] for item in baseline["Parameters"]}
    if (previous["AuthEnabled"] != "true" or previous["IntakeEnabled"] != "false"
            or previous["SyncEnabled"] != "false"):
        raise RuntimeError("Require Bearer authentication and paused write admissions")
    definition = template()
    original = cf.get_template(StackName=baseline["StackId"], TemplateStage="Original")["TemplateBody"]
    if isinstance(original, str):
        original = json.loads(original)
    check_template(original, definition)
    directory.mkdir(parents=True)

    def save(name, value):
        with (directory / (name + ".json")).open("x") as stream:
            stream.write(encode(value))

    save("identity", identity)
    save("artifact", artifact)
    save("baseline", baseline)
    save("stopped", stopped(current, values))
    for name in ["IngressQueue", "Dlq"]:
        queue = current.client("sqs").get_queue_attributes(QueueUrl=values[name], AttributeNames=[
            "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed"])["Attributes"]
        save(name, queue)
        if any(int(count) for count in queue.values()):
            raise RuntimeError("Reconcile original queue work before browser deployment")
    body = json.dumps(definition, separators=(",", ":"))
    (directory / "template.json").write_text(body + "\n")
    save("validation", cf.validate_template(**template_arguments(current, body)))
    parameters = [{"ParameterKey": "BrowserSourceCommit", "ParameterValue": head}]
    parameters += [{"ParameterKey": name, "UsePreviousValue": True} for name in previous
                   if name != "BrowserSourceCommit"]
    token = "colvenna-editor-" + uuid.uuid4().hex
    save("change-set-admission", {"stack_id": baseline["StackId"], "source_commit": head,
        "client_request_token": token, "parameters": parameters,
        "template_sha256": hashlib.sha256(body.encode()).hexdigest()})
    change = cf.create_change_set(StackName=baseline["StackId"], ChangeSetName=token,
        ChangeSetType="UPDATE", **template_arguments(current, body), Parameters=parameters,
        Capabilities=["CAPABILITY_IAM"], ClientToken=token)
    save("change-set-dispatch", change)
    deadline = time.monotonic() + 180
    while True:
        plan = cf.describe_change_set(ChangeSetName=change["Id"], StackName=baseline["StackId"])
        (directory / "change-set-observation.json").write_text(encode(plan))
        if plan["Status"] == "CREATE_COMPLETE":
            break
        if plan["Status"] == "FAILED" or time.monotonic() >= deadline:
            raise RuntimeError("Reconcile the original browser change set")
        time.sleep(2)
    if (plan.get("NextToken") or any(item["ResourceChange"]["LogicalResourceId"] not in WEB_RESOURCES
            or item["ResourceChange"]["Action"] not in {"Add", "Modify"}
            or item["ResourceChange"].get("Replacement", "False") != "False"
            for item in plan["Changes"])):
        raise RuntimeError("Refuse unrelated or replacing browser change set")
    save("stopped-before-execute", stopped(current, values))
    live = owned_stack(cf, STACKS["baseline"])
    if (live["StackStatus"] != "UPDATE_COMPLETE"
            or {item["ParameterKey"]: item["ParameterValue"] for item in live["Parameters"]} != previous
            or live.get("LastUpdatedTime") != baseline.get("LastUpdatedTime")):
        raise RuntimeError("Stack changed after browser admission; reconcile without execution")
    save("execute-admission", {"stack_id": baseline["StackId"], "change_set_id": change["Id"],
                              "client_request_token": token})
    save("execute-dispatch", cf.execute_change_set(ChangeSetName=change["Id"],
        StackName=baseline["StackId"], ClientRequestToken=token))
    if wait(cf, baseline["StackId"], directory / "deployment-terminal.json") != "UPDATE_COMPLETE":
        raise RuntimeError("Reconcile the original browser deployment")
    final = owned_stack(cf, STACKS["baseline"])
    retained = {item["ParameterKey"]: item["ParameterValue"] for item in final["Parameters"]}
    if (retained["BrowserSourceCommit"] != head
            or any(retained.get(name) != value for name, value in previous.items()
                   if name != "BrowserSourceCommit")):
        raise RuntimeError("Independent service/source parameters changed")
    save("final", final)
    s3 = current.client("s3", config=Config(retries={"total_max_attempts": 1}))
    installed = {}
    with zipfile.ZipFile(args.artifact / "editor.zip") as archive:
        # Install HTML last so a new page never advertises missing script files.
        for name in sorted(FILES, key=lambda value: (value.endswith(".html"), value)):
            raw = archive.read(name)
            digest = hashlib.sha256(raw).hexdigest()
            save("upload-admission-" + name.replace("/", "-"), {
                "bucket": values["WebBucket"], "key": name, "source_commit": head, "sha256": digest})
            version = None
            try:
                reply = s3.put_object(Bucket=values["WebBucket"], Key=name, Body=raw,
                    ExpectedBucketOwner=ACCOUNT, ContentType=FILES[name], CacheControl="no-store",
                    Metadata={"sha256": digest, "source-commit": head},
                    ChecksumSHA256=base64.b64encode(hashlib.sha256(raw).digest()).decode())
                version = reply.get("VersionId")
            except (BotoCoreError, ClientError):
                pass  # Read the original key once; an uncertain put never dispatches a replacement.
            observed = s3.get_object(Bucket=values["WebBucket"], Key=name,
                ExpectedBucketOwner=ACCOUNT, **({"VersionId": version} if version else {}))
            original_version = observed["VersionId"]
            with observed["Body"] as stream:
                stored = stream.read(len(raw) + 1)
            if (stored != raw or not original_version or original_version == "null"
                    or version is not None and original_version != version
                    or observed["Metadata"] != {"sha256": digest, "source-commit": head}):
                raise RuntimeError("Original browser object upload not confirmed")
            installed[name] = {"sha256": digest, "version_id": original_version, "bytes": len(raw)}
            save("upload-receipt-" + name.replace("/", "-"), installed[name])
    cloudfront = current.client("cloudfront", config=Config(retries={"total_max_attempts": 1}))
    invalidation = cloudfront.create_invalidation(DistributionId=values["Distribution"],
        InvalidationBatch={"Paths": {"Quantity": len(FILES) + 1, "Items": ["/"] + ["/" + name for name in FILES]},
                           "CallerReference": token})
    save("invalidation-dispatch", invalidation)
    deadline = time.monotonic() + 900
    while invalidation["Invalidation"]["Status"] != "Completed":
        if time.monotonic() >= deadline:
            raise RuntimeError("Reconcile original browser invalidation")
        time.sleep(5)
        invalidation = cloudfront.get_invalidation(
            DistributionId=values["Distribution"], Id=invalidation["Invalidation"]["Id"])
        (directory / "invalidation-observation.json").write_text(encode(invalidation))
    save("stopped-after", stopped(current, outputs(final)))
    receipt = {"status": "PASS_STATIC_INSTALLATION_NOT_BROWSER_API_ACCEPTANCE",
        "source_commit": head, "artifact_sha256": artifact["zip_sha256"], "installed": installed,
        "web_url": values["WebUrl"], "invalidation_id": invalidation["Invalidation"]["Id"],
        "invalidation_status": invalidation["Invalidation"]["Status"]}
    save("receipt", receipt)
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()
