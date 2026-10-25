#!/usr/bin/env python3
"""Deploy, inspect, stop and clean only the tagged Colvenna Isle baseline (core) stack."""

import argparse
import base64
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT), str(ROOT / "src")]
from infra.names import names
from infra.template import template

from colvenna_isle.region import region

REGION = region()
REPOSITORY = "colvenna-isle-images"
# One environment per process, named by the required COLVENNA_ENVIRONMENT.
STACKS = names()


def account():
    """The account to operate on is the operator's explicit choice; it is never part of the source."""
    value = os.environ.get("COLVENNA_ACCOUNT", "")
    if not re.fullmatch(r"[0-9]{12}", value):
        raise RuntimeError("Set COLVENNA_ACCOUNT to the 12-digit AWS account to operate on")
    return value


def __getattr__(name):
    if name == "ACCOUNT":
        return account()
    raise AttributeError(name)


def storage_image(locked):
    """The locked native storage image ({repository, digest}) in this account's registry."""
    return f"{account()}.dkr.ecr.{REGION}.amazonaws.com/{locked['repository']}@{locked['digest']}"


def operator_role(identity):
    """The operator's role name, from its assumed-role session ARN."""
    return identity["Arn"].split(":assumed-role/", 1)[1].split("/", 1)[0]


def encode(value):
    return json.dumps(value, indent=2, default=str) + "\n"


def source():
    if subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip():
        raise RuntimeError("Commit source changes before AWS mutations")
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()


def session():
    current = boto3.Session(region_name=REGION)
    identity = current.client("sts").get_caller_identity()
    if identity["Account"] != account() or not identity["Arn"].startswith(
            f"arn:aws:sts::{account()}:assumed-role/"):
        raise RuntimeError("Expected an assumed role in COLVENNA_ACCOUNT")
    if current.get_credentials().method != "iam-role":
        raise RuntimeError("Use the EC2 role credential chain; no static keys")
    return current, {key: identity[key] for key in ["Account", "Arn"]} | {
        "region": REGION, "credential_method": "iam-role"}


def owned_stack(cf, name):
    value = cf.describe_stacks(StackName=name)["Stacks"][0]
    tags = {tag["Key"]: tag["Value"] for tag in value.get("Tags", [])}
    if tags.get("Project") != "colvenna-isle" or tags.get("Component") != "core":
        raise RuntimeError("Refuse operations on a stack without the project's core-stack tags")
    if f":{REGION}:{account()}:stack/{name}/" not in value["StackId"]:
        raise RuntimeError("Stack identity does not match the scoped account/region/name")
    return value


def vault_bucket(cf):
    """The account-level backup vault bucket, or "" before the vault is deployed."""
    from infra.backups import STACK

    try:
        stack = cf.describe_stacks(StackName=STACK)["Stacks"][0]
    except ClientError as error:
        if "does not exist" in error.response["Error"].get("Message", ""):
            return ""
        raise
    tags = {tag["Key"]: tag["Value"] for tag in stack.get("Tags", [])}
    if (tags.get("Project") != "colvenna-isle" or tags.get("Environment") != "backups"
            or f":{REGION}:{account()}:stack/{STACK}/" not in stack["StackId"]
            or stack["StackStatus"] not in {"CREATE_COMPLETE", "UPDATE_COMPLETE"}):
        raise RuntimeError("Backup vault stack not positively owned and terminal")
    return outputs(stack)["Bucket"]


def outputs(stack):
    return {item["OutputKey"]: item["OutputValue"] for item in stack.get("Outputs", [])}


def update_stack(cf, **arguments):
    """UpdateStack; redeploying a release onto a stack that already runs exactly this template, these
    parameters and tags is a confirmed no-op. The caller then observes the stack's existing terminal status."""
    try:
        return cf.update_stack(**arguments)
    except ClientError as error:
        if error.response["Error"]["Message"] != "No updates are to be performed.":
            raise
        return {"StackId": cf.describe_stacks(StackName=arguments["StackName"])["Stacks"][0]["StackId"],
                "NoUpdates": True}


def wait(cf, stack_id, output):
    deadline = time.monotonic() + 1800
    history = []
    while time.monotonic() < deadline:
        try:
            stack = cf.describe_stacks(StackName=stack_id)["Stacks"][0]
            status = stack["StackStatus"]
        except ClientError as error:
            if "does not exist" not in error.response["Error"]["Message"]:
                raise
            stack, status = None, "DELETE_COMPLETE"
        if not history or history[-1]["status"] != status:
            history.append({"status": status, "observed_unix": time.time()})
            print(status, flush=True)
        events = cf.describe_stack_events(StackName=stack_id)["StackEvents"] if stack else []
        output.write_text(encode({"history": history, "stack": stack, "events": events}))
        if not status.endswith("_IN_PROGRESS"):
            return status
        time.sleep(10)
    raise RuntimeError("Observer timeout: retain stack identity and reconcile; do not replay deployment")


def publish_artifact(s3, bucket, key, raw, digest, metadata=None):
    """Conditionally publish one immutable deployment artifact; return its exact, re-read version.

    Keys embed the source commit and SHA-256, so redeploying the same release
    finds its original object and reuses that version only after re-reading
    identical bytes. Different bytes under the key are refused.
    """
    try:
        version = s3.put_object(Bucket=bucket, Key=key, Body=raw, ExpectedBucketOwner=account(), IfNoneMatch="*",
                                ChecksumSHA256=base64.b64encode(bytes.fromhex(digest)).decode(),
                                **({"Metadata": metadata} if metadata else {}))["VersionId"]
    except ClientError as error:
        if error.response["Error"]["Code"] != "PreconditionFailed":
            raise
        version = s3.head_object(Bucket=bucket, Key=key, ExpectedBucketOwner=account())["VersionId"]
    if version in {None, "", "null"}:
        raise RuntimeError("Artifact version unconfirmed")
    original = s3.get_object(Bucket=bucket, Key=key, VersionId=version, ExpectedBucketOwner=account())
    with original["Body"] as stream:
        content = stream.read(len(raw) + 1)
    if original["VersionId"] != version or hashlib.sha256(content).hexdigest() != digest or content != raw:
        raise RuntimeError("Artifact bytes under the immutable key differ from the release")
    return version


def template_arguments(current, body):
    """CloudFormation template arguments: inline below the API body limit, otherwise one immutable vault object.

    The key is the template's SHA-256, so the referenced content can never change after publication.
    """
    if len(body.encode()) <= 51200:
        return {"TemplateBody": body}
    bucket = vault_bucket(current.client("cloudformation"))
    if not bucket:
        raise RuntimeError("A template above the inline limit needs the account vault; deploy it first")
    digest = hashlib.sha256(body.encode()).hexdigest()
    key = "templates/" + digest + ".json"
    publish_artifact(current.client("s3"), bucket, key, body.encode(), digest)
    return {"TemplateURL": f"https://{bucket}.s3.{REGION}.amazonaws.com/{key}"}


def push_image(current, local_tag, image_id, tag, log):
    """Publish one qualified local image under an immutable tag; an identical earlier push is reused.

    Environments share the private repository, so another environment may have
    published the same source already. A tag bound to a different digest fails.
    """
    ecr = current.client("ecr")
    uri = f"{account()}.dkr.ecr.{REGION}.amazonaws.com/{REPOSITORY}:{tag}"
    try:
        existing = ecr.describe_images(repositoryName=REPOSITORY, imageIds=[{"imageTag": tag}])["imageDetails"]
    except ecr.exceptions.ImageNotFoundException:
        existing = []
    if not existing:
        authorization = ecr.get_authorization_token()["authorizationData"][0]
        username, password = base64.b64decode(authorization["authorizationToken"]).decode().split(":", 1)
        with tempfile.TemporaryDirectory(prefix="colvenna-image-docker-") as configuration:
            environment = dict(os.environ, DOCKER_CONFIG=configuration)
            subprocess.run(["docker", "login", "--username", username, "--password-stdin",
                            authorization["proxyEndpoint"]], input=password.encode(), env=environment,
                           check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run(["docker", "tag", local_tag, uri], check=True, env=environment)
            with log.open("x") as stream:
                subprocess.run(["docker", "push", uri], check=True, env=environment,
                               stdout=stream, stderr=subprocess.STDOUT)
    else:
        log.write_text("REUSED_IMMUTABLE_TAG " + existing[0]["imageDigest"] + "\n")
    published = ecr.describe_images(repositoryName=REPOSITORY, imageIds=[{"imageTag": tag}])["imageDetails"][0]
    if published["imageDigest"] != image_id:
        raise RuntimeError("Published immutable image digest differs from the qualified local OCI image")
    return uri.split(":", 1)[0] + "@" + published["imageDigest"], published


def empty_bucket(s3, bucket, expected_account):
    # Called only for buckets discovered from this positively owned stack.
    for page in s3.get_paginator("list_object_versions").paginate(Bucket=bucket, ExpectedBucketOwner=expected_account):
        entries = [{"Key": entry["Key"], "VersionId": entry["VersionId"]}
                   for entry in page.get("Versions", []) + page.get("DeleteMarkers", [])]
        if entries:
            result = s3.delete_objects(Bucket=bucket, ExpectedBucketOwner=expected_account,
                                       Delete={"Objects": entries, "Quiet": True})
            if result.get("Errors"):
                raise RuntimeError("Owned bucket cleanup failed")


def cleanup_guardduty(current, stack, values, directory):
    """Remove only GuardDuty network resources attached to this owned, stopped VPC."""
    ecs = current.client("ecs")
    try:
        if ecs.list_tasks(cluster=values["Cluster"], desiredStatus="RUNNING")["taskArns"]:
            raise RuntimeError("Stop and reconcile owned compute before GuardDuty network cleanup")
    except ecs.exceptions.ClusterNotFoundException:
        resources = current.client("cloudformation").list_stack_resources(StackName=stack["StackId"])["StackResourceSummaries"]
        if not any(r["LogicalResourceId"] == "Cluster" and r["ResourceStatus"] == "DELETE_COMPLETE" for r in resources):
            raise RuntimeError("Missing cluster has no positive deletion evidence")
    ec2 = current.client("ec2")
    vpc = ec2.describe_vpcs(VpcIds=[values["Vpc"]])["Vpcs"][0]
    tags = {tag["Key"]: tag["Value"] for tag in vpc.get("Tags", [])}
    if tags.get("aws:cloudformation:stack-id") != stack["StackId"] or tags.get("Project") != "colvenna-isle":
        raise RuntimeError("VPC ownership does not match this stack")
    endpoints = ec2.describe_vpc_endpoints(Filters=[{"Name": "vpc-id", "Values": [vpc["VpcId"]]}])["VpcEndpoints"]
    matched = [endpoint for endpoint in endpoints
               if endpoint["ServiceName"] == f"com.amazonaws.{REGION}.guardduty-data"
               and endpoint["OwnerId"] == account()
               and {tag["Key"]: tag["Value"] for tag in endpoint.get("Tags", [])}.get("GuardDutyManaged") == "true"]
    record = {"vpc": vpc, "endpoints": matched, "groups": [], "scope": "OWNED_STOPPED_VPC_ONLY"}
    (directory / "guardduty-admission.json").write_text(encode(record))
    group_ids = sorted({group["GroupId"] for endpoint in matched for group in endpoint["Groups"]})
    for group_id in group_ids:
        group = ec2.describe_security_groups(GroupIds=[group_id])["SecurityGroups"][0]
        if group["VpcId"] != vpc["VpcId"] or group["OwnerId"] != account() or group["GroupName"] != (
                "GuardDutyManagedSecurityGroup-"+vpc["VpcId"]):
            raise RuntimeError("GuardDuty security group identity does not match the owned VPC")
        record["groups"].append(group)
    (directory / "guardduty-admission.json").write_text(encode(record))
    delete_ids = [endpoint["VpcEndpointId"] for endpoint in matched if endpoint["State"] != "deleting"]
    if delete_ids:
        dispatched = ec2.delete_vpc_endpoints(VpcEndpointIds=delete_ids)
        (directory / "guardduty-dispatch.json").write_text(encode(dispatched))
        if dispatched["Unsuccessful"]:
            raise RuntimeError("Owned GuardDuty endpoint deletion refused; inspect original receipt")
    deadline = time.monotonic()+600
    ids = {endpoint["VpcEndpointId"] for endpoint in matched}
    while ids:
        remaining = ec2.describe_vpc_endpoints(Filters=[{"Name": "vpc-id", "Values": [vpc["VpcId"]]}])["VpcEndpoints"]
        live = [endpoint for endpoint in remaining if endpoint["VpcEndpointId"] in ids and endpoint["State"] != "deleted"]
        if not live:
            break
        if time.monotonic() > deadline:
            raise RuntimeError("Endpoint deletion UNKNOWN; reconcile original IDs without replay")
        time.sleep(5)
    for group_id in group_ids:
        try:
            ec2.delete_security_group(GroupId=group_id)
        except ClientError as error:
            if error.response["Error"]["Code"] != "InvalidGroup.NotFound":
                raise
    (directory / "guardduty-terminal.json").write_text(encode({
        "status": "DELETED", "endpoint_ids": sorted(ids), "security_group_ids": group_ids,
        "vpc_id": vpc["VpcId"], "account_monitoring": "UNCHANGED"}))
    print("Deleted owned GuardDuty endpoint/security group; account monitoring unchanged", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["publish", "create", "update", "status", "stop", "delete", "cleanup-network"])
    parser.add_argument("--stack", default=STACKS["baseline"])
    parser.add_argument("--output", type=Path, required=True, help="new external operation receipt directory")
    parser.add_argument("--image", help="digest-pinned ECR image for create/update")
    parser.add_argument("--bundle", type=Path, help="source-bound development bundle for publish")
    parser.add_argument("--query-desired", type=int, choices=[0, 1], default=0)
    args = parser.parse_args()
    if not re.fullmatch(r"colvenna-isle-[a-z][a-z0-9]{1,11}-core", args.stack):
        parser.error("stack name must be an environment's Colvenna Isle core stack (colvenna-isle-<environment>-core)")
    directory = args.output.resolve()
    if directory.exists() or directory == ROOT or ROOT in directory.parents:
        parser.error("use a new external receipt directory")
    current, identity = session()
    directory.mkdir(parents=True)
    (directory / "identity.json").write_text(encode(identity))
    cf = current.client("cloudformation")
    if args.operation == "publish":
        if not args.bundle:
            parser.error("--bundle is required")
        manifest = json.loads((args.bundle / "manifest.json").read_text())
        if manifest["status"] != "PASS" or not manifest["storage_image"]:
            raise RuntimeError("Use a passing development bundle with verified storage image")
        for name, expected in manifest["artifacts"].items():
            path = (args.bundle / name).resolve()
            if args.bundle.resolve() not in path.parents or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                raise RuntimeError("Artifact checksum/path mismatch")
        image_id = manifest["storage_image"]["id"]
        actual = json.loads(subprocess.check_output(["docker", "image", "inspect", image_id], text=True))[0]
        if actual["Id"] != image_id:
            raise RuntimeError("Local image differs from the manifest")
        ecr = current.client("ecr")
        try:
            repository = ecr.describe_repositories(repositoryNames=[REPOSITORY])["repositories"][0]
        except ecr.exceptions.RepositoryNotFoundException:
            repository = ecr.create_repository(repositoryName=REPOSITORY, imageTagMutability="IMMUTABLE",
                imageScanningConfiguration={"scanOnPush": True},
                encryptionConfiguration={"encryptionType": "AES256"},
                tags=[{"Key": "Project", "Value": "colvenna-isle"}, {"Key": "Component", "Value": "core"}])["repository"]
        if repository["imageTagMutability"] != "IMMUTABLE":
            raise RuntimeError("Expected immutable private artifact repository")
        image_tag = "source-" + manifest["source_commit"]
        reference = repository["repositoryUri"] + ":" + image_tag
        auth = ecr.get_authorization_token()["authorizationData"][0]
        username, password = base64.b64decode(auth["authorizationToken"]).decode().split(":", 1)
        config = directory / "temporary-docker-auth"
        config.mkdir()
        env = dict(os.environ, DOCKER_CONFIG=str(config))
        try:
            subprocess.run(["docker", "login", "--username", username, "--password-stdin", auth["proxyEndpoint"]],
                           input=password, text=True, check=True, env=env, capture_output=True)
            subprocess.run(["docker", "tag", image_id, reference], check=True, env=env)
            with (directory / "push.log").open("x") as stream:
                subprocess.run(["docker", "push", reference], check=True, env=env, stdout=stream, stderr=subprocess.STDOUT)
        finally:
            shutil.rmtree(config)
        detail = ecr.describe_images(repositoryName=REPOSITORY, imageIds=[{"imageTag": image_tag}])["imageDetails"][0]
        (directory / "publication.json").write_text(encode({
            "source_commit": manifest["source_commit"], "bundle_manifest_sha256":
                hashlib.sha256((args.bundle / "manifest.json").read_bytes()).hexdigest(),
            "local_image_id": image_id, "image": repository["repositoryUri"] + "@" + detail["imageDigest"],
            "registry": "PRIVATE_ECR", "repository": repository, "image_details": detail,
        }))
        print(repository["repositoryUri"] + "@" + detail["imageDigest"])
        return
    token = "colvenna-core-" + uuid.uuid4().hex
    if args.operation in {"create", "update"}:
        head = source()
        if not args.image or not re.fullmatch(
            f"{account()}\\.dkr\\.ecr\\.{REGION}\\.amazonaws\\.com/{REPOSITORY}@sha256:[0-9a-f]{{64}}", args.image):
            parser.error("use the verified private ECR digest")
        value = template()
        body = json.dumps(value, separators=(",", ":"))
        (directory / "template.json").write_text(body + "\n")
        template_source = template_arguments(current, body)
        validation = cf.validate_template(**template_source)
        (directory / "template-validation.json").write_text(encode(validation))
        kwargs = {"StackName": args.stack, **template_source, "Capabilities": ["CAPABILITY_IAM"],
                  "Parameters": [{"ParameterKey": "SourceCommit", "ParameterValue": head},
                                 {"ParameterKey": "StorageImage", "ParameterValue": args.image},
                                 {"ParameterKey": "QueryDesired", "ParameterValue": str(args.query_desired)}],
                  "Tags": [{"Key": "Project", "Value": "colvenna-isle"}, {"Key": "Component", "Value": "core"},
                           {"Key": "SourceCommit", "Value": head}], "ClientRequestToken": token}
        if args.operation == "update":
            existing = owned_stack(cf, args.stack)
            supplied = {entry["ParameterKey"] for entry in kwargs["Parameters"]}
            kwargs["Parameters"].extend(
                {"ParameterKey": entry["ParameterKey"], "UsePreviousValue": True}
                for entry in existing["Parameters"]
                if entry["ParameterKey"] not in supplied and entry["ParameterKey"] in value["Parameters"])
        (directory / "admission.json").write_text(encode({
            "source_commit": head, "stack_name": args.stack, "client_request_token": token,
            "template_sha256": hashlib.sha256(body.encode()).hexdigest(), "image": args.image}))
        result = getattr(cf, args.operation+"_stack")(**kwargs)
        (directory / "dispatch.json").write_text(encode(result))
        status = wait(cf, result["StackId"], directory / "terminal.json")
        print(encode({"status": status, "stack_id": result["StackId"]}))
        if status not in {"CREATE_COMPLETE", "UPDATE_COMPLETE"}:
            raise RuntimeError("Deployment did not complete; preserve and reconcile original stack")
        return
    stack = owned_stack(cf, args.stack)
    values = outputs(stack)
    (directory / "stack.json").write_text(encode(stack))
    if args.operation == "status":
        resources = cf.list_stack_resources(StackName=stack["StackId"])["StackResourceSummaries"]
        (directory / "resources.json").write_text(encode(resources))
        print(encode({"status": stack["StackStatus"], "outputs": values}))
    elif args.operation == "stop":
        ecs = current.client("ecs")
        for name in ["WriterService", "QueryService"]:
            result = ecs.update_service(cluster=values["Cluster"], service=values[name], desiredCount=0)
            (directory / (name+".json")).write_text(encode(result))
        ecs.get_waiter("services_stable").wait(cluster=values["Cluster"],
                                               services=[values["WriterService"], values["QueryService"]],
                                               WaiterConfig={"Delay": 10, "MaxAttempts": 60})
        running = ecs.list_tasks(cluster=values["Cluster"], desiredStatus="RUNNING")["taskArns"]
        if running:
            raise RuntimeError("Owned tasks remain; reconcile before delete/rebuild")
        (directory / "terminal.json").write_text(encode({"status": "STOPPED", "running_tasks": []}))
        print("STOPPED")
    elif args.operation == "delete":
        # Refuse teardown until all known compute is positively terminal.
        if "Cluster" in values:
            ecs = current.client("ecs")
            services = ecs.describe_services(cluster=values["Cluster"],
                services=[values["WriterService"], values["QueryService"]])["services"]
            if any(s["desiredCount"] or s["runningCount"] or s["pendingCount"] for s in services):
                raise RuntimeError("Stop services and reconcile tasks before teardown")
            if ecs.list_tasks(cluster=values["Cluster"], desiredStatus="RUNNING")["taskArns"]:
                raise RuntimeError("Running tasks prevent teardown")
            s3 = current.client("s3")
            for name in ["DataBucket", "WebBucket"]:
                empty_bucket(s3, values[name], account())
            cleanup_guardduty(current, stack, values, directory)
        cf.delete_stack(StackName=stack["StackId"], ClientRequestToken=token)
        (directory / "dispatch.json").write_text(encode({"stack_id": stack["StackId"], "token": token}))
        status = wait(cf, stack["StackId"], directory / "terminal.json")
        if status != "DELETE_COMPLETE":
            raise RuntimeError("Teardown incomplete; reconcile original resource identities")
        print("DELETE_COMPLETE")
    elif args.operation == "cleanup-network":
        (directory / "recovery-source.json").write_text(encode({"source_commit": source()}))
        if stack["StackStatus"] not in {"DELETE_IN_PROGRESS", "DELETE_FAILED"}:
            raise RuntimeError("Recovery cleanup is only allowed for this positively deleting stack")
        cleanup_guardduty(current, stack, values, directory)


if __name__ == "__main__":
    main()
