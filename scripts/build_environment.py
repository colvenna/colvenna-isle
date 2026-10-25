#!/usr/bin/env python3
"""Trusted CodeBuild orchestration; user wheels execute only in isolated containers."""

import argparse
import base64
import copy
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from colvenna_isle.account import account
from colvenna_isle.control.registry import canonical
from colvenna_isle.control.pipelines import DEFINITION_EPOCH

from colvenna_isle.region import region

REGION = region()


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


# The failing stage and its last log, for a bounded failure receipt.
PROGRESS = {"stage": "SPECIFICATION", "log": None}
STAGE_CODES = {"SPECIFICATION": "BUILD_SPECIFICATION_FAILED", "RESOLUTION": "DEPENDENCY_RESOLUTION_FAILED",
               "IMAGE": "ENVIRONMENT_IMAGE_BUILD_FAILED", "CHECK": "ENVIRONMENT_IMPORT_CHECK_FAILED",
               "PUBLISH": "ENVIRONMENT_PUBLICATION_FAILED"}


def stage(name):
    PROGRESS.update(stage=name, log=None)


def run(command, log, *, environment=None, timeout=600):
    PROGRESS["log"] = Path(log)
    with Path(log).open("x") as stream:
        subprocess.run(command, cwd=ROOT, env=environment, timeout=timeout, check=True,
                       stdout=stream, stderr=subprocess.STDOUT)


def failure_receipt(error, identity):
    """Stage, code and a sanitized log tail; never environment, credentials or URLs with userinfo."""
    code = str(error) if isinstance(error, RuntimeError) and re.fullmatch(r"[A-Z][A-Z0-9_]{2,63}", str(error)) \
        else STAGE_CODES[PROGRESS["stage"]]
    message = None
    if PROGRESS["log"] is not None and PROGRESS["log"].exists():
        lines = PROGRESS["log"].read_text(errors="replace").splitlines()
        reported = resolver_failure(lines) if PROGRESS["stage"] == "RESOLUTION" else None
        if reported:
            # The resolver's own terminal replaces image-pull noise in the log tail.
            code, message = reported
        else:
            message = "\n".join(lines[-12:])
        message = re.sub(r"(://)[^/@\s]+@", r"\1<redacted>@", message)
        message = re.sub(r"\b(AKIA|ASIA)[A-Z0-9]{16}\b", "<redacted>", message)[-1024:]
    return {"format_version": 1, "build_id": identity, "stage": PROGRESS["stage"], "code": code,
            "message": message}


RESOLVER_MESSAGES = {
    "DEPENDENCY_NOT_FOUND": "No linux_arm64 CPython 3.12 binary wheel on PyPI satisfies: {}.",
    "DEPENDENCY_RESOLUTION_FAILED":
        "pip could not resolve the requirements to linux_arm64 CPython 3.12 binary wheels "
        "(a version conflict, a source-only distribution or an unreachable index).",
    "DEPENDENCY_LICENSE_NOT_APPROVED": "A resolved distribution's license is not on the approved list.",
    "DEPENDENCY_SOURCE_NOT_APPROVED": "A requirement or download points outside the approved sources.",
    "DEPENDENCY_PACKAGE_QUOTA": "The resolution exceeds the package-count limit.",
    "DEPENDENCY_WHEEL_QUOTA": "The wheels exceed the download or unpacked-size limit.",
}


def resolver_failure(lines):
    """The resolver's final FAILED line as (code, message), if it printed one."""
    for line in reversed(lines):
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if not isinstance(value, dict) or value.get("status") != "FAILED" or not isinstance(
                value.get("code"), str) or not re.fullmatch(r"[A-Z][A-Z0-9_]{2,63}", value["code"]):
            return None
        requirements = [x for x in value.get("requirements") or [] if isinstance(x, str)][:16]
        message = RESOLVER_MESSAGES.get(value["code"], "Dependency resolution was rejected.")
        return value["code"], message.format(", ".join(requirements) or "a submitted requirement")
    return None


def read_original(s3, reference, maximum):
    bucket, key = reference["s3_uri"][5:].split("/", 1)
    if (not reference["s3_uri"].startswith("s3://") or reference["object_version"] in {"", "null"}
            or not 0 < reference["bytes"] <= maximum):
        raise RuntimeError("BUILD_INPUT_INVALID")
    response = s3.get_object(Bucket=bucket, Key=key, VersionId=reference["object_version"],
                             ExpectedBucketOwner=account())
    with response["Body"] as stream:
        content = stream.read(maximum + 1)
    if (response["VersionId"] != reference["object_version"] or len(content) != reference["bytes"]
            or hashlib.sha256(content).hexdigest() != reference["sha256"]):
        raise RuntimeError("BUILD_INPUT_UNCONFIRMED")
    return content


def platform_dependencies():
    requirements, hashes = [], {}
    for name in ["contracts-requirements.lock", "aws-requirements.lock", "columnar-requirements.lock"]:
        for line in (ROOT / "runtime" / name).read_text().splitlines():
            if not line or line.startswith("#"):
                continue
            value = re.fullmatch(r"([a-z0-9-]+)==(\S+) --hash=sha256:([0-9a-f]{64})", line)
            if not value:
                raise RuntimeError("PLATFORM_LOCK_INVALID")
            package, version, hash_value = value.groups()
            if package in hashes and hashes[package] != hash_value:
                raise RuntimeError("PLATFORM_LOCK_CONFLICT")
            requirements.append(package + "==" + version)
            hashes[package] = hash_value
    return sorted(set(requirements)), hashes


def confirm_source(policy):
    raw = (ROOT / "builder-source.json").read_bytes()
    manifest = json.loads(raw)
    if (manifest["source_commit"] != policy["source_commit"]
            or manifest["adapter_sha256"] != policy["adapter_sha256"]
            or hashlib.sha256(raw).hexdigest() != policy["builder_source_sha256"]
            or not re.fullmatch(r"[0-9a-f]{40}", manifest["source_commit"])):
        raise RuntimeError("BUILD_SOURCE_MISMATCH")
    for name, expected in manifest["files"].items():
        path = ROOT / name
        if (Path(name).is_absolute() or ".." in Path(name).parts
                or path.is_symlink() or sha(path) != expected):
            raise RuntimeError("BUILD_SOURCE_MISMATCH")
    actual = {str(path.relative_to(ROOT)) for prefix in ["src", "runtime", "scripts"]
              for path in (ROOT / prefix).rglob("*")
              if path.is_file() and "__pycache__" not in path.parts}
    if actual | {"buildspec.yml", "LICENSE"} != set(manifest["files"]):
        raise RuntimeError("BUILD_SOURCE_MISMATCH")
    return manifest


def build_images(specification, identity, output, *, private_directory=None):
    policy = specification["policy"]
    if platform.machine() != "aarch64" or policy["platform"] != "linux_arm64":
        raise RuntimeError("BUILD_PLATFORM_MISMATCH")
    requirements, hashes = platform_dependencies()
    work = output / "resolution"
    work.mkdir()
    (work / "private").mkdir()
    private = policy.get("private_wheels", {})
    if private:
        if private_directory is None:
            raise RuntimeError("PRIVATE_WHEELS_UNAVAILABLE")
        for approved in private.values():
            filename = approved["filename"]
            if Path(filename).name != filename:
                raise RuntimeError("PRIVATE_WHEEL_INVALID")
            source = private_directory / filename
            if sha(source) != approved["sha256"] or source.stat().st_size != approved["bytes"]:
                raise RuntimeError("PRIVATE_WHEEL_HASH_MISMATCH")
            shutil.copyfile(source, work / "private" / filename)
    (work / "request.json").write_text(canonical({
        "requirements": specification["requirements"], "platform_requirements": requirements,
        "platform_hashes": hashes, "private_wheels": private,
        **{name: policy[name] for name in [
            "python_version", "maximum_wheel_bytes", "maximum_packages",
            "maximum_image_bytes", "approved_licenses", "approved_license_classifiers"]}}))
    # No role/metadata environment or host Docker socket reaches dependency code.
    stage("RESOLUTION")
    run(["docker", "run", "--rm", "--platform", "linux/arm64", "--cap-drop", "ALL",
         "--user", f"{os.getuid()}:{os.getgid()}",
         "--security-opt", "no-new-privileges", "--pids-limit", "128", "--memory", "1024m",
         "--cpus", "1", "--read-only", "--tmpfs", "/tmp:rw,size=128m",
         "--mount", f"type=bind,src={work},dst=/resolution",
         "--mount", f"type=bind,src={ROOT / 'runtime/environment_resolve.py'},dst=/resolve.py,readonly",
         "--env", "PIP_CONFIG_FILE=/dev/null", "--entrypoint", "python",
         policy["base_image"], "/resolve.py", "/resolution"],
        output / "resolve.log", timeout=600)
    resolution = json.loads((work / "resolution.json").read_text())
    if resolution["status"] != "RESOLVED":
        raise RuntimeError("DEPENDENCY_RESOLUTION_UNCONFIRMED")
    adapter = ROOT / "src/colvenna_isle/pipeline/environment_adapter.py"
    if sha(adapter) != policy["adapter_sha256"]:
        raise RuntimeError("ADAPTER_SOURCE_MISMATCH")
    context = output / "context"
    context.mkdir()
    shutil.copytree(ROOT / "src/colvenna_isle", context / "application/colvenna_isle",
                    ignore=shutil.ignore_patterns("__pycache__"))
    shutil.copytree(work / "wheels", context / "wheels")
    shutil.copyfile(work / "requirements.lock", context / "requirements.lock")
    shutil.copyfile(ROOT / "LICENSE", context / "LICENSE")
    environment = {
        "source_commit": policy["source_commit"], "adapter_sha256": policy["adapter_sha256"],
        "dependency_lock_sha256": resolution["dependency_lock_sha256"],
        "python_version": resolution["python_version"], "packages": resolution["packages"],
        "specification_sha256": hashlib.sha256(canonical(specification).encode()).hexdigest(),
        "environment_id": specification["environment_id"], "version": specification["version"],
        "space_id": specification["space_id"], "generation": specification["generation"],
        "bucket": policy["bucket"], "definition_epoch": DEFINITION_EPOCH,
    }
    dockerfile = (ROOT / "runtime/Dockerfile.environment").read_text()
    tags, checks = {}, {}
    sql = {"duckdb", "pyarrow"} <= {package["name"] for package in resolution["packages"]}
    for executor in ["lambda", "ecs_task"]:
        environment["executor"] = executor
        # The controlled SQL runner needs DuckDB and PyArrow in the same locked image.
        environment["profiles"] = profiles(executor, sql)
        (context / "environment.json").write_text(canonical(environment))
        footer = (
            f'\nLABEL io.colvenna.source="{policy["source_commit"]}" '
            f'io.colvenna.adapter="{policy["adapter_sha256"]}" '
            f'io.colvenna.dependency-lock="{resolution["dependency_lock_sha256"]}" '
            f'io.colvenna.executor="{executor}"\n')
        footer += ('ENTRYPOINT ["/lambda-entrypoint.sh"]\n'
                   'CMD ["colvenna_isle.pipeline.environment_adapter.lambda_handler"]\n'
                   if executor == "lambda" else
                   'USER root\nRUN chown 10001:10001 /tmp && chmod 0700 /tmp\n'
                   'VOLUME ["/tmp"]\nUSER 10001:10001\n'
                   'ENTRYPOINT ["python", "-m", "colvenna_isle.pipeline.environment_adapter"]\nCMD []\n')
        (context / ("Dockerfile." + executor)).write_text(dockerfile + footer)
        tag = "colvenna-isle-environment:" + identity + "-" + executor
        stage("IMAGE")
        run(["docker", "build", "--platform", "linux/arm64", "--network", "none",
             "--provenance=false", "--sbom=false",
             "--build-arg", "BASE_IMAGE=" + policy["base_image"],
             "-f", str(context / ("Dockerfile." + executor)), "-t", tag, str(context)],
            output / (executor + "-build.log"))
        actual = json.loads(subprocess.check_output(["docker", "image", "inspect", tag], text=True))[0]
        if (actual["Architecture"] != "arm64" or actual["Config"]["User"] != "10001:10001"
                or actual["Size"] > policy["maximum_image_bytes"]):
            raise RuntimeError("ENVIRONMENT_IMAGE_QUOTA_OR_PLATFORM")
        common = [
            "docker", "run", "--rm", "--network", "none", "--read-only", "--cap-drop", "ALL",
            "--security-opt", "no-new-privileges", "--pids-limit", "128", "--memory", "1024m",
            "--cpus", "1"]
        if executor == "lambda":
            common += ["--tmpfs", "/tmp:rw,size=128m"]
        common += ["--entrypoint", "python", tag]
        stage("CHECK")
        run(common + ["-m", "colvenna_isle.pipeline.environment_adapter", "--environment-check"],
            output / (executor + "-import.log"), timeout=300)
        check = json.loads((output / (executor + "-import.log")).read_text())
        if (check["status"] != "ENVIRONMENT_IMPORT_AND_CONTRACT_PASS"
                or check["dependency_lock_sha256"] != resolution["dependency_lock_sha256"]):
            raise RuntimeError("ENVIRONMENT_IMPORT_UNCONFIRMED")
        run(common + ["-m", "colvenna_isle.contracts", "generate", "--check"],
            output / (executor + "-contracts.log"))
        run(common + ["-m", "pip", "check"], output / (executor + "-pip-check.log"))
        tags[executor], checks[executor] = tag, check
    return resolution, tags, checks


def profiles(executor, sql):
    names = ["python_lambda_standard"] if executor == "lambda" else ["python_ecs_medium"]
    if sql:
        names.append("sql_lambda_standard" if executor == "lambda" else "sql_ecs_medium")
    return names


def publish(s3, bucket, key, content):
    digest = hashlib.sha256(content).hexdigest()
    result = s3.put_object(Bucket=bucket, Key=key, Body=content, ExpectedBucketOwner=account(),
        IfNoneMatch="*", Metadata={"sha256": digest},
        ChecksumSHA256=base64.b64encode(bytes.fromhex(digest)).decode())
    reference = {"s3_uri": f"s3://{bucket}/{key}", "object_version": result["VersionId"],
                 "sha256": digest, "bytes": len(content)}
    if read_original(s3, reference, max(len(content), 1)) != content:
        raise RuntimeError("BUILD_ARTIFACT_UNCONFIRMED")
    return reference


def runtime_role(session, specification):
    """The installed user role has no builder, private-package or warehouse rights."""
    space, generation = specification["space_id"], specification["generation"]
    bucket = specification["policy"]["bucket"]
    name = specification["policy"]["runtime_prefix"] + hashlib.sha256((space + "/" + generation).encode()).hexdigest()[:16]
    iam = session.client("iam")
    trust = {"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"Service": ["ecs-tasks.amazonaws.com", "lambda.amazonaws.com"]},
        "Action": "sts:AssumeRole"}]}
    try:
        role = iam.get_role(RoleName=name)["Role"]
    except iam.exceptions.NoSuchEntityException:
        role = iam.create_role(RoleName=name, AssumeRolePolicyDocument=canonical(trust),
            PermissionsBoundary=specification["policy"]["runtime_boundary_arn"],
            Tags=[{"Key": "Project", "Value": "colvenna-isle"},
                  {"Key": "SpaceId", "Value": space}, {"Key": "Generation", "Value": generation}])["Role"]
    tags = {x["Key"]: x["Value"] for x in role["Tags"]}
    # IAM can reorder the service-principal array when returning an existing role.
    # Normalize that array only; every other trust field must still match exactly.
    actual_trust = copy.deepcopy(role["AssumeRolePolicyDocument"])
    try:
        services = actual_trust["Statement"][0]["Principal"]["Service"]
        if not isinstance(services, list) or any(not isinstance(value, str) for value in services):
            raise ValueError()
        actual_trust["Statement"][0]["Principal"]["Service"] = sorted(services)
    except (KeyError, IndexError, TypeError, ValueError):
        raise RuntimeError("ENVIRONMENT_ROLE_OWNERSHIP_MISMATCH") from None
    if (tags != {"Project": "colvenna-isle", "SpaceId": space, "Generation": generation}
            or role.get("PermissionsBoundary", {}).get("PermissionsBoundaryArn")
            != specification["policy"]["runtime_boundary_arn"]
            or actual_trust != trust
            or iam.list_attached_role_policies(RoleName=name)["AttachedPolicies"]):
        raise RuntimeError("ENVIRONMENT_ROLE_OWNERSHIP_MISMATCH")
    statements = [
        {"Effect": "Allow", "Action": ["s3:GetObjectVersion"], "Resource": [
            f"arn:aws:s3:::{bucket}/spaces/{space}/inputs/{generation}/*",
            f"arn:aws:s3:::{bucket}/spaces/{space}/pipeline-code/{generation}/*",
            f"arn:aws:s3:::{bucket}/spaces/{space}/task-contexts/{generation}/*"]},
        {"Effect": "Allow", "Action": ["s3:PutObject", "s3:GetObjectVersion"],
         "Resource": f"arn:aws:s3:::{bucket}/spaces/{space}/staging/{generation}/*"},
        {"Effect": "Allow", "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
         "Resource": specification["policy"]["runtime_log_arn"] + ":log-stream:*"},
    ]
    if specification["policy"].get("notification_queue_arn"):
        statements.append({"Effect": "Allow", "Action": ["sqs:SendMessage"],
                           "Resource": specification["policy"]["notification_queue_arn"]})
    policy = {"Version": "2012-10-17", "Statement": statements}
    existing = iam.list_role_policies(RoleName=name)["PolicyNames"]
    if existing:
        if existing != ["RuntimeFiles"]:
            raise RuntimeError("ENVIRONMENT_ROLE_PERMISSIONS_CHANGED")
        actual = iam.get_role_policy(RoleName=name, PolicyName="RuntimeFiles")["PolicyDocument"]
        legacy = {"Version": "2012-10-17", "Statement": [
            statement for statement in statements if statement["Action"] != ["sqs:SendMessage"]]}
        if actual == legacy and actual != policy:
            # The only approved change: add the notification-queue send right.
            iam.put_role_policy(RoleName=name, PolicyName="RuntimeFiles", PolicyDocument=canonical(policy))
            time.sleep(10)
        elif actual != policy:
            raise RuntimeError("ENVIRONMENT_ROLE_PERMISSIONS_CHANGED")
    else:
        iam.put_role_policy(RoleName=name, PolicyName="RuntimeFiles", PolicyDocument=canonical(policy))
        # IAM policy propagation precedes publishing the first runtime function.
        time.sleep(10)
    return role["Arn"]


def create_function(lam, **arguments):
    """IAM is eventually consistent: a role created moments earlier, possibly by a concurrent build of the same
    space, is not yet assumable by Lambda. Only that refusal is retried, for a bounded time; the refused call
    created nothing."""
    deadline = time.monotonic() + 120
    while True:
        try:
            return lam.create_function(**arguments)
        except lam.exceptions.InvalidParameterValueException as error:
            if "cannot be assumed by Lambda" not in str(error) or time.monotonic() >= deadline:
                raise
            time.sleep(5)


def publish_executors(session, specification, identity, tags, output, *, sql=False):
    policy = specification["policy"]
    ecr = session.client("ecr")
    auth = ecr.get_authorization_token()["authorizationData"][0]
    user, password = base64.b64decode(auth["authorizationToken"]).decode().split(":", 1)
    images = {}
    with tempfile.TemporaryDirectory(prefix="colvenna-environment-docker-") as configuration:
        environment = dict(os.environ, DOCKER_CONFIG=configuration)
        subprocess.run(["docker", "login", "--username", user, "--password-stdin", auth["proxyEndpoint"]],
                       input=password.encode(), env=environment, check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for executor, tag in tags.items():
            name = "environment-" + identity + "-" + executor
            uri = f"{account()}.dkr.ecr.{REGION}.amazonaws.com/{policy['repository']}:{name}"
            subprocess.run(["docker", "tag", tag, uri], env=environment, check=True)
            run(["docker", "push", uri], output / (executor + "-push.log"), environment=environment)
            image = ecr.describe_images(registryId=account(), repositoryName=policy["repository"],
                                       imageIds=[{"imageTag": name}])["imageDetails"][0]
            images[executor] = uri.rsplit(":", 1)[0] + "@" + image["imageDigest"]
    role = runtime_role(session, specification)
    name = policy["runtime_prefix"] + identity.removeprefix("build_")
    lam = session.client("lambda")
    function = create_function(
        lam, FunctionName=name, PackageType="Image", Code={"ImageUri": images["lambda"]},
        Role=role, Architectures=["arm64"], Timeout=900, MemorySize=2048,
        EphemeralStorage={"Size": 1024}, Publish=True,
        LoggingConfig={"LogGroup": policy["runtime_log_group"]},
        Tags={"Project": "colvenna-isle", "BuildId": identity})
    version = function["Version"]
    if not re.fullmatch(r"[1-9][0-9]*", version):
        raise RuntimeError("LAMBDA_VERSION_NOT_NUMERIC")
    arn = function["FunctionArn"].split(":function:", 1)[0] + ":function:" + name + ":" + version
    lam.get_waiter("function_active_v2").wait(FunctionName=name, Qualifier=version)
    queue = policy.get("notification_queue_arn")
    if queue:
        # One Invoke is one execution attempt: Lambda's own async retries are
        # disabled and both outcomes notify the scheduler (a hint only).
        lam.put_function_event_invoke_config(
            FunctionName=name, Qualifier=version, MaximumRetryAttempts=0, MaximumEventAgeInSeconds=60,
            DestinationConfig={"OnSuccess": {"Destination": queue}, "OnFailure": {"Destination": queue}})
    task = session.client("ecs").register_task_definition(
        family=name, requiresCompatibilities=["FARGATE"], networkMode="awsvpc",
        cpu="1024", memory="4096", taskRoleArn=role, executionRoleArn=policy["execution_role_arn"],
        runtimePlatform={"cpuArchitecture": "ARM64", "operatingSystemFamily": "LINUX"},
        containerDefinitions=[{
            "name": "runtime", "image": images["ecs_task"], "essential": True,
            "environment": [{"name": name, "value": REGION} for name in [
                "COLVENNA_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"]],
            "user": "10001:10001", "readonlyRootFilesystem": True,
            "mountPoints": [{"sourceVolume": "work", "containerPath": "/tmp", "readOnly": False}],
            "logConfiguration": {"logDriver": "awslogs", "options": {
                "awslogs-group": policy["runtime_log_group"], "awslogs-region": REGION,
                "awslogs-stream-prefix": identity}}}],
        volumes=[{"name": "work"}], tags=[{"key": "Project", "value": "colvenna-isle"},
                                        {"key": "BuildId", "value": identity}])["taskDefinition"]
    return images, [
        *[{"executor": "lambda", "profile": profile, "lambda_mode": "standard", "function_arn": arn}
          for profile in profiles("lambda", sql)],
        *[{"executor": "ecs_task", "profile": profile, "task_definition_arn": task["taskDefinitionArn"],
           "image": images["ecs_task"]} for profile in profiles("ecs_task", sql)]]


def main():
    try:
        build()
    except Exception as error:
        identity = os.environ.get("COLVENNA_ENVIRONMENT_BUILD_ID", "")
        reference = os.environ.get("COLVENNA_ENVIRONMENT_SPECIFICATION_REF")
        if re.fullmatch(r"build_[0-9a-f]{32}", identity) and reference:
            # Best effort: the CodeBuild FAILED status remains authoritative.
            try:
                session = boto3.Session(region_name=REGION)
                specification = json.loads(read_original(session.client("s3"), json.loads(reference), 262144))
                prefix = (f"spaces/{specification['space_id']}/environment-builds/"
                          f"{specification['generation']}/{identity}/")
                publish(session.client("s3"), specification["policy"]["bucket"], prefix + "failure.json",
                        canonical(failure_receipt(error, identity)).encode())
            except Exception:
                pass
        raise


def build():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local-specification", type=Path)
    parser.add_argument("--private-wheels", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if output.exists() or output == ROOT or ROOT in output.parents:
        parser.error("use a new external build directory")
    output.mkdir(parents=True)
    session = None
    if args.local_specification:
        specification = json.loads(args.local_specification.read_text())
        identity = "build_" + hashlib.sha256(canonical(specification).encode()).hexdigest()[:32]
        private_directory = args.private_wheels
        codebuild_id = None
    else:
        session = boto3.Session(region_name=REGION)
        if session.client("sts").get_caller_identity()["Account"] != account():
            raise RuntimeError("BUILD_ACCOUNT_MISMATCH")
        s3 = session.client("s3")
        reference = json.loads(os.environ["COLVENNA_ENVIRONMENT_SPECIFICATION_REF"])
        content = read_original(s3, reference, 262144)
        expected = os.environ["COLVENNA_ENVIRONMENT_SPECIFICATION_SHA256"]
        if hashlib.sha256(content).hexdigest() != expected:
            raise RuntimeError("BUILD_SPECIFICATION_MISMATCH")
        specification = json.loads(content)
        source_manifest = confirm_source(specification["policy"])
        identity, codebuild_id = os.environ["COLVENNA_ENVIRONMENT_BUILD_ID"], os.environ["CODEBUILD_BUILD_ID"]
        if (not re.fullmatch(r"build_[0-9a-f]{32}", identity)
                or not codebuild_id.startswith(specification["build_project"] + ":")):
            raise RuntimeError("BUILD_IDENTITY_MISMATCH")
        private_directory = output / "private"
        private_directory.mkdir()
        approved_wheels = specification["policy"].get("private_wheels", {})
        if sum(value["bytes"] for value in approved_wheels.values()) > specification["policy"]["maximum_wheel_bytes"]:
            raise RuntimeError("PRIVATE_WHEEL_QUOTA")
        for approved in approved_wheels.values():
            filename = approved["filename"]
            if Path(filename).name != filename:
                raise RuntimeError("PRIVATE_WHEEL_INVALID")
            raw = read_original(s3, {k: approved[k] for k in [
                "s3_uri", "object_version", "sha256", "bytes"]}, 134217728)
            (private_directory / filename).write_bytes(raw)
    (output / "specification.json").write_text(canonical(specification))
    resolution, tags, checks = build_images(
        specification, identity, output, private_directory=private_directory)
    receipt = {
        "format_version": 1, "build_id": identity, "codebuild_id": codebuild_id,
        "specification_sha256": hashlib.sha256(canonical(specification).encode()).hexdigest(),
        **{k: specification["policy"][k] for k in ["source_commit", "adapter_sha256", "base_image"]},
        **{k: resolution[k] for k in ["python_version", "python_abi", "platform", "packages"]},
        "checks": {"imports": "PASS", "contract": "PASS"},
        "build_tools": {"pip": resolution["pip_version"], "docker": subprocess.check_output(
            ["docker", "version", "--format", "{{.Server.Version}}"], text=True).strip()},
        "image_import_receipts": checks,
    }
    if session:
        stage("PUBLISH")
        policy = specification["policy"]
        images, bindings = publish_executors(session, specification, identity, tags, output,
            sql={"duckdb", "pyarrow"} <= {package["name"] for package in resolution["packages"]})
        receipt.update(images=images, executor_bindings=bindings)
        receipt["builder_source"] = source_manifest
        prefix = f"spaces/{specification['space_id']}/environment-builds/{specification['generation']}/{identity}/"
        receipt["dependency_lock"] = publish(
            s3, policy["bucket"], prefix + "requirements.lock",
            (output / "resolution/requirements.lock").read_bytes())
        for package in resolution["packages"]:
            path = output / "resolution/wheels" / package["filename"]
            package["artifact"] = publish(s3, policy["bucket"], prefix + "wheels/" + path.name, path.read_bytes())
        raw = canonical(receipt).encode()
        if len(raw) > 262144:
            raise RuntimeError("BUILD_RECEIPT_QUOTA")
        publish(s3, policy["bucket"], prefix + "receipt.json", raw)
    else:
        receipt.update(status="PASS_LOCAL_TARGET_IMAGES_NOT_AWS_ACCEPTANCE", local_tags=tags,
                       not_run=["CodeBuild", "ECR publication", "Lambda/ECS deployment", "environment API"])
    (output / "receipt.json").write_text(canonical(receipt))
    print(json.dumps({"status": receipt.get("status", "ARTIFACTS_PUBLISHED_NOT_CONTROL_READY"),
                      "build_id": identity, "package_count": len(resolution["packages"])}))


if __name__ == "__main__":
    main()
