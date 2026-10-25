#!/usr/bin/env python3
"""Deploy only the owned Colvenna Isle ARM environment builder; never dispatch a build."""

import argparse
import hashlib
import json
import re
import sys
import uuid
from pathlib import Path

from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT), str(ROOT / "src"), str(Path(__file__).parent)]
from infra.environments import template
from manage import REGION, ACCOUNT, STACKS, encode, outputs, owned_stack, publish_artifact, session, source, template_arguments, update_stack, wait
from control import stopped


def lambda_image_access(current, repository, prefix, directory):
    ecr = current.client("ecr")
    actual = ecr.describe_repositories(registryId=ACCOUNT, repositoryNames=[repository])["repositories"][0]
    tags = {x["Key"]: x["Value"] for x in ecr.list_tags_for_resource(
        resourceArn=actual["repositoryArn"])["tags"]}
    if actual["imageTagMutability"] != "IMMUTABLE" or tags.get("Project") != "colvenna-isle":
        raise RuntimeError("ENVIRONMENT_REPOSITORY_OWNERSHIP_UNCONFIRMED")
    try:
        previous = json.loads(ecr.get_repository_policy(
            registryId=ACCOUNT, repositoryName=repository)["policyText"])
    except ecr.exceptions.RepositoryPolicyNotFoundException:
        previous = {"Version": "2012-10-17", "Statement": []}
    # The repository is shared; each environment adds its own runtime-prefix statement.
    suffix = STACKS["environment"].capitalize()
    statement = {
        "Sid": "ColvennaIsleEnvironmentLambdaImageRetrieval" + suffix, "Effect": "Allow",
        "Principal": {"Service": "lambda.amazonaws.com"},
        "Action": ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"],
        "Condition": {"ArnLike": {"aws:SourceArn":
            f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{prefix}*"}}}
    existing = [x for x in previous["Statement"] if x.get("Sid") == statement["Sid"]]
    if existing and existing != [statement]:
        raise RuntimeError("ENVIRONMENT_REPOSITORY_POLICY_CHANGED")
    (directory / "repository-policy-before.json").write_text(encode(previous))
    if not existing:
        policy = {**previous, "Statement": [*previous["Statement"], statement]}
        (directory / "repository-policy-admission.json").write_text(encode(policy))
        result = ecr.set_repository_policy(
            registryId=ACCOUNT, repositoryName=repository, policyText=json.dumps(policy))
        (directory / "repository-policy-dispatch.json").write_text(encode(result))
        confirmed = json.loads(ecr.get_repository_policy(
            registryId=ACCOUNT, repositoryName=repository)["policyText"])
        if confirmed != policy:
            raise RuntimeError("ENVIRONMENT_REPOSITORY_POLICY_UNCONFIRMED")
        (directory / "repository-policy-confirmed.json").write_text(encode(confirmed))


def private_wheels(current, bucket, path):
    if path is None:
        return {}
    values = json.loads(path.read_text())
    if not isinstance(values, dict) or len(values) > 8:
        raise RuntimeError("PRIVATE_WHEEL_POLICY_INVALID")
    for name, approved in values.items():
        if (not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,127}", name)
                or set(approved) != {"s3_uri", "object_version", "sha256", "bytes",
                                     "filename", "version", "import_name"}
                or not approved["s3_uri"].startswith(f"s3://{bucket}/artifacts/private-wheels/")
                or not re.fullmatch(r"[a-zA-Z0-9_.+-]+\.whl", approved["filename"])
                or not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", approved["import_name"])
                or not re.fullmatch(r"[0-9a-f]{64}", approved["sha256"])
                or not approved["object_version"] or approved["object_version"] == "null"
                or type(approved["bytes"]) is not int or not 0 < approved["bytes"] <= 134217728):
            raise RuntimeError("PRIVATE_WHEEL_POLICY_INVALID")
        key = approved["s3_uri"][len(f"s3://{bucket}/"):]
        if ".." in key.split("/"):
            raise RuntimeError("PRIVATE_WHEEL_POLICY_INVALID")
        result = current.client("s3").get_object(
            Bucket=bucket, Key=key, VersionId=approved["object_version"], ExpectedBucketOwner=ACCOUNT)
        with result["Body"] as stream:
            raw = stream.read(134217729)
        if (len(raw) != approved["bytes"] or result["VersionId"] != approved["object_version"]
                or hashlib.sha256(raw).hexdigest() != approved["sha256"]):
            raise RuntimeError("PRIVATE_WHEEL_POLICY_UNCONFIRMED")
    return values


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-bundle", type=Path, required=True)
    parser.add_argument("--private-wheel-policy", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    directory = args.output.resolve()
    if directory.exists() or directory == ROOT or ROOT in directory.parents:
        parser.error("use a new external deployment receipt directory")
    head = source()
    current, identity = session()
    cf = current.client("cloudformation")
    baseline = owned_stack(cf, STACKS["baseline"])
    if baseline["StackStatus"] not in {"CREATE_COMPLETE", "UPDATE_COMPLETE"}:
        raise RuntimeError("Reconcile baseline before environment deployment")
    values = outputs(baseline) | {"StackId": baseline["StackId"]}
    manifest = json.loads((args.source_bundle / "manifest.json").read_text())
    archive = (args.source_bundle / manifest["zip"]).resolve()
    if (args.source_bundle.resolve() not in archive.parents or manifest["source_commit"] != head
            or manifest["status"] != "PACKAGED_NOT_RUNTIME_QUALIFIED"
            or hashlib.sha256(archive.read_bytes()).hexdigest() != manifest["zip_sha256"]):
        raise RuntimeError("BUILDER_SOURCE_UNCONFIRMED")
    policy = json.loads((ROOT / "runtime/environment-lock.json").read_text())
    curated = current.client("codebuild").list_curated_environment_images()
    versions = {version for platform in curated["platforms"] for language in platform["languages"]
                for image in language["images"] for version in image["versions"]}
    if policy["builder_image"] not in versions:
        raise RuntimeError("BUILDER_IMAGE_NOT_APPROVED")
    policy.update(
        source_commit=head, adapter_sha256=manifest["adapter_sha256"],
        builder_source_sha256=manifest["builder_source_sha256"], bucket=values["DataBucket"],
        repository="colvenna-isle-images", runtime_prefix=STACKS["runtime_prefix"],
        private_wheels=private_wheels(current, values["DataBucket"], args.private_wheel_policy))
    directory.mkdir(parents=True)
    (directory / "identity.json").write_text(encode(identity))
    (directory / "baseline.json").write_text(encode(baseline))
    (directory / "stopped.json").write_text(encode(stopped(current, values)))
    lambda_image_access(current, policy["repository"], policy["runtime_prefix"], directory)
    stack_name = STACKS["environments"]
    try:
        existing = cf.describe_stacks(StackName=stack_name)["Stacks"][0]
    except ClientError as error:
        if "does not exist" not in error.response["Error"].get("Message", ""):
            raise
        existing = None
    if existing:
        tags = {x["Key"]: x["Value"] for x in existing["Tags"]}
        if (tags.get("Project") != "colvenna-isle" or tags.get("Component") != "builder"
                or outputs(existing)["BaselineStackId"] != baseline["StackId"]
                or existing["StackStatus"] not in {"CREATE_COMPLETE", "UPDATE_COMPLETE"}):
            raise RuntimeError("BUILDER_STACK_OWNERSHIP_UNCONFIRMED")
    key = f"artifacts/environment-builder/{head}/{manifest['zip_sha256']}/builder.zip"
    binding = {"bucket": values["DataBucket"], "key": key, "version_id": publish_artifact(
        current.client("s3"), values["DataBucket"], key, archive.read_bytes(), manifest["zip_sha256"],
        {"sha256": manifest["zip_sha256"]}), "sha256": manifest["zip_sha256"]}
    if binding["version_id"] in {"", "null"}:
        raise RuntimeError("BUILDER_SOURCE_VERSION_UNCONFIRMED")
    original = current.client("s3").get_object(
        Bucket=values["DataBucket"], Key=key, VersionId=binding["version_id"], ExpectedBucketOwner=ACCOUNT)
    with original["Body"] as stream:
        raw = stream.read(len(archive.read_bytes()) + 1)
    if (original["VersionId"] != binding["version_id"]
            or hashlib.sha256(raw).hexdigest() != manifest["zip_sha256"]):
        raise RuntimeError("BUILDER_SOURCE_VERSION_UNCONFIRMED")
    policy.update(source_location=values["DataBucket"] + "/" + key, source_version=binding["version_id"])
    (directory / "artifact.json").write_text(encode(binding))
    body = json.dumps(template(values, binding, policy, stack_name, account=ACCOUNT), separators=(",", ":"))
    (directory / "template.json").write_text(body + "\n")
    (directory / "validation.json").write_text(encode(cf.validate_template(**template_arguments(current, body))))
    token = "colvenna-environments-" + uuid.uuid4().hex
    (directory / "admission.json").write_text(encode({
        "stack": stack_name, "source_commit": head, "artifact": binding,
        "baseline_stack_id": baseline["StackId"], "client_request_token": token,
        "template_sha256": hashlib.sha256(body.encode()).hexdigest()}))
    operation = (lambda **arguments: update_stack(cf, **arguments)) if existing else cf.create_stack
    result = operation(
        StackName=stack_name, **template_arguments(current, body), Capabilities=["CAPABILITY_IAM"],
        ClientRequestToken=token, Tags=[
            {"Key": "Project", "Value": "colvenna-isle"}, {"Key": "Component", "Value": "builder"},
            {"Key": "SourceCommit", "Value": head}])
    (directory / "dispatch.json").write_text(encode(result))
    if wait(cf, result["StackId"], directory / "terminal.json") not in {"CREATE_COMPLETE", "UPDATE_COMPLETE"}:
        raise RuntimeError("BUILDER_DEPLOYMENT_UNCONFIRMED_RECONCILE_ORIGINAL")
    live = cf.describe_stacks(StackName=result["StackId"])["Stacks"][0]
    live_values = outputs(live)
    policy.update(runtime_boundary_arn=live_values["RuntimeBoundaryArn"],
                  execution_role_arn=live_values["ExecutionRoleArn"],
                  runtime_log_group=live_values["RuntimeLogGroup"],
                  notification_queue_arn=live_values["PipelineEventsQueueArn"],
                  notification_queue_url=live_values["PipelineEventsQueueUrl"],
                  runtime_log_arn=f"arn:aws:logs:{REGION}:{ACCOUNT}:log-group:{live_values['RuntimeLogGroup']}")
    (directory / "configuration.json").write_text(encode({"project": live_values["Project"], "policy": policy}))
    (directory / "baseline-preserved.json").write_text(encode(owned_stack(cf, STACKS["baseline"])))
    print(encode({"status": "BUILDER_DEPLOYED_NOT_ENVIRONMENT_READY", "project": live_values["Project"]}))


if __name__ == "__main__":
    main()
