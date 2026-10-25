"""Confirm immutable original build artifacts and real AWS executor bindings."""

import base64
import hashlib
import json
import re
from urllib.error import URLError
from urllib.request import urlopen

from botocore.exceptions import ClientError

from colvenna_isle.region import region
from colvenna_isle.account import account
from .registry import ControlError, canonical
from .http import decode_json


MAX_RECEIPT_BYTES = 262144
MAX_LOCK_BYTES = 65536


def require(condition):
    if not condition:
        raise ControlError(503, "ENVIRONMENT_ARTIFACTS_UNVERIFIED")


def image_configuration(url):
    require(isinstance(url, str) and url.startswith("https://"))
    try:
        with urlopen(url, timeout=5) as response:
            content = response.read(MAX_RECEIPT_BYTES + 1)
        require(0 < len(content) <= MAX_RECEIPT_BYTES)
        return content
    except (URLError, TimeoutError, OSError):
        raise ControlError(503, "ENVIRONMENT_ARTIFACTS_UNVERIFIED") from None


def binding_shape(bindings, packages):
    """Environments with DuckDB and PyArrow also bind the controlled SQL profiles,
    to the same immutable function version and task definition revision."""
    sql = {"duckdb", "pyarrow"} <= {package["name"] for package in packages}
    expected = {("lambda", "python_lambda_standard"), ("ecs_task", "python_ecs_medium")} | (
        {("lambda", "sql_lambda_standard"), ("ecs_task", "sql_ecs_medium")} if sql else set())
    return (isinstance(bindings, list) and len(bindings) == len(expected)
            and {(x["executor"], x["profile"]) for x in bindings} == expected
            and len({x.get("function_arn") for x in bindings if x["executor"] == "lambda"}) == 1
            and len({x.get("task_definition_arn") for x in bindings if x["executor"] == "ecs_task"}) == 1)


class EnvironmentArtifacts:
    def __init__(self, s3, ecr, lam, ecs, bucket, fetch_configuration=image_configuration):
        self.s3, self.ecr, self.lam, self.ecs, self.bucket = s3, ecr, lam, ecs, bucket
        self.fetch_configuration = fetch_configuration

    def _prefix(self, record):
        specification = record["specification"]
        return (f"spaces/{specification['space_id']}/environment-builds/"
                f"{specification['generation']}/{record['build']['build_id']}/")

    def _original(self, key, maximum):
        response = self.s3.list_object_versions(
            Bucket=self.bucket, Prefix=key, MaxKeys=2, ExpectedBucketOwner=account())
        versions = response.get("Versions", [])
        require(response.get("IsTruncated") is False and not response.get("DeleteMarkers")
                and len(versions) == 1 and versions[0]["Key"] == key)
        version = versions[0]["VersionId"]
        require(version and version != "null")
        response = self.s3.get_object(
            Bucket=self.bucket, Key=key, VersionId=version, ExpectedBucketOwner=account())
        require(response["VersionId"] == version and 0 < response["ContentLength"] <= maximum)
        with response["Body"] as stream:
            content = stream.read(maximum + 1)
        sha = hashlib.sha256(content).hexdigest()
        require(len(content) == response["ContentLength"]
                and response.get("Metadata", {}).get("sha256") == sha)
        return content, {"s3_uri": f"s3://{self.bucket}/{key}", "object_version": version,
                         "sha256": sha, "bytes": len(content)}

    def image_bytes(self, ready):
        """Total ECR size of the READY environment's executor images (cost observation only)."""
        total = 0
        for image in sorted({binding["image"] for binding in ready["executor_bindings"] if binding.get("image")}):
            repository, digest = image.split("/", 1)[1].split("@", 1)
            described = self.ecr.describe_images(repositoryName=repository, imageIds=[{"imageDigest": digest}])
            total += sum(value.get("imageSizeInBytes", 0) for value in described.get("imageDetails", []))
        return total or None

    def failure(self, record):
        """The builder's original failure receipt, if it wrote one before exiting."""
        key = self._prefix(record) + "failure.json"
        listed = self.s3.list_object_versions(Bucket=self.bucket, Prefix=key, MaxKeys=2,
                                              ExpectedBucketOwner=account())
        if not listed.get("Versions"):
            return None
        content, _ = self._original(key, 16384)
        value = decode_json(content)
        if (set(value) != {"format_version", "build_id", "stage", "code", "message"}
                or value["build_id"] != record["build"]["build_id"]
                or not re.fullmatch(r"[A-Z][A-Z0-9_]{2,63}", value["code"])
                or not re.fullmatch(r"[A-Z_]{1,32}", value["stage"])):
            raise ControlError(503, "ENVIRONMENT_FAILURE_RECEIPT_INVALID")
        return value

    def publish_specification(self, record):
        content = canonical(record["specification"]).encode()
        sha = hashlib.sha256(content).hexdigest()
        require(sha == record["specification_sha256"] and len(content) <= MAX_RECEIPT_BYTES)
        key = self._prefix(record) + "specification.json"
        try:
            self.s3.put_object(
                Bucket=self.bucket, Key=key, Body=content, ExpectedBucketOwner=account(),
                IfNoneMatch="*", Metadata={"sha256": sha},
                ChecksumSHA256=base64.b64encode(bytes.fromhex(sha)).decode())
        except ClientError as error:
            if error.response["Error"]["Code"] not in {"PreconditionFailed", "412"}:
                raise
        actual, reference = self._original(key, MAX_RECEIPT_BYTES)
        require(actual == content)
        return reference

    def _reference(self, record, reference, maximum):
        require(isinstance(reference, dict)
                and set(reference) == {"s3_uri", "object_version", "sha256", "bytes"})
        uri = reference["s3_uri"]
        prefix = f"s3://{self.bucket}/{self._prefix(record)}"
        require(isinstance(uri, str) and uri.startswith(prefix)
                and reference["object_version"] not in {"", "null"})
        key = uri[len(f"s3://{self.bucket}/"):]
        require(".." not in key.split("/") and not key.endswith("/"))
        # Confirm the actual exact original version, including complete listing.
        content, actual = self._original(key, maximum)
        require(actual == reference)
        return content

    def _wheel(self, record, reference, expected_sha):
        prefix = f"s3://{self.bucket}/{self._prefix(record)}wheels/"
        require(set(reference) == {"s3_uri", "object_version", "sha256", "bytes"}
                and reference["s3_uri"].startswith(prefix)
                and "/" not in reference["s3_uri"][len(prefix):]
                and reference["s3_uri"].endswith(".whl")
                and reference["object_version"] not in {"", "null"}
                and reference["sha256"] == expected_sha
                and type(reference["bytes"]) is int and 0 < reference["bytes"] <= 134217728)
        key = reference["s3_uri"][len(f"s3://{self.bucket}/"):]
        listed = self.s3.list_object_versions(
            Bucket=self.bucket, Prefix=key, MaxKeys=2, ExpectedBucketOwner=account())
        require(listed.get("IsTruncated") is False and not listed.get("DeleteMarkers")
                and len(listed.get("Versions", [])) == 1
                and listed["Versions"][0]["Key"] == key
                and listed["Versions"][0]["VersionId"] == reference["object_version"])
        actual = self.s3.head_object(
            Bucket=self.bucket, Key=key, VersionId=reference["object_version"],
            ExpectedBucketOwner=account(), ChecksumMode="ENABLED")
        require(actual["VersionId"] == reference["object_version"]
                and actual["ContentLength"] == reference["bytes"]
                and actual["Metadata"]["sha256"] == expected_sha
                and actual["ChecksumSHA256"] == base64.b64encode(bytes.fromhex(expected_sha)).decode())

    def _image(self, image, policy, lock_sha256, executor):
        prefix = f"{account()}.dkr.ecr.{region()}.amazonaws.com/{policy['repository']}@sha256:"
        require(isinstance(image, str) and image.startswith(prefix)
                and re.fullmatch(r"[0-9a-f]{64}", image[len(prefix):]))
        sha = "sha256:" + image[len(prefix):]
        response = self.ecr.batch_get_image(
            registryId=account(), repositoryName=policy["repository"],
            imageIds=[{"imageDigest": sha}])
        require(not response.get("failures") and len(response.get("images", [])) == 1)
        actual = response["images"][0]
        require(actual["imageId"]["imageDigest"] == sha
                and "sha256:" + hashlib.sha256(actual["imageManifest"].encode()).hexdigest() == sha)
        manifest = decode_json(actual["imageManifest"].encode())
        require(manifest["schemaVersion"] == 2 and "config" in manifest and "manifests" not in manifest)
        config_sha = manifest["config"]["digest"]
        require(re.fullmatch(r"sha256:[0-9a-f]{64}", config_sha))
        url = self.ecr.get_download_url_for_layer(
            registryId=account(), repositoryName=policy["repository"], layerDigest=config_sha)["downloadUrl"]
        configuration_bytes = self.fetch_configuration(url)
        require(0 < len(configuration_bytes) <= MAX_RECEIPT_BYTES
                and "sha256:" + hashlib.sha256(configuration_bytes).hexdigest() == config_sha)
        configuration = decode_json(configuration_bytes)
        require(configuration["architecture"] == "arm64" and configuration["os"] == "linux")
        require(configuration["config"]["User"] == "10001:10001")
        labels = configuration["config"]["Labels"]
        require(labels["io.colvenna.source"] == policy["source_commit"]
                and labels["io.colvenna.adapter"] == policy["adapter_sha256"]
                and labels["io.colvenna.dependency-lock"] == lock_sha256
                and labels["io.colvenna.executor"] == executor)
        require(configuration["config"].get("Entrypoint") == (
            ["/lambda-entrypoint.sh"] if executor == "lambda"
            else ["python", "-m", "colvenna_isle.pipeline.environment_adapter"]))
        command = configuration["config"].get("Cmd")
        require(command == ["colvenna_isle.pipeline.environment_adapter.lambda_handler"]
                if executor == "lambda" else command in (None, []))
        require(not any(value.split("=", 1)[0] in {
            "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "PIP_INDEX_URL",
            "PIP_EXTRA_INDEX_URL", "COLVENNA_PRIVATE_PACKAGE_TOKEN",
        } for value in configuration["config"].get("Env", [])))
        metadata = self.ecr.describe_images(
            registryId=account(), repositoryName=policy["repository"],
            imageIds=[{"imageDigest": sha}])["imageDetails"]
        require(len(metadata) == 1 and metadata[0]["imageDigest"] == sha
                and 0 < metadata[0]["imageSizeInBytes"] <= policy["maximum_image_bytes"])
        return sha

    def verify(self, record, actual_build):
        policy = record["specification"]["policy"]
        require(actual_build["buildStatus"] == "SUCCEEDED"
                and actual_build["id"] == record["build"]["codebuild_id"]
                and actual_build["projectName"] == record["build"]["project"]
                and actual_build["projectName"] == record["specification"]["build_project"]
                and actual_build["sourceVersion"] == policy["source_version"]
                and actual_build["source"]["type"] == "S3"
                and actual_build["source"]["location"] == policy["source_location"]
                and actual_build["source"]["buildspec"] == "buildspec.yml")
        variables = actual_build["environment"]["environmentVariables"]
        require(len({x["name"] for x in variables}) == len(variables))
        variables = {x["name"]: x["value"] for x in variables}
        require(variables.get("COLVENNA_ENVIRONMENT_BUILD_ID") == record["build"]["build_id"]
                and variables.get("COLVENNA_ENVIRONMENT_SPECIFICATION_SHA256")
                == record["specification_sha256"]
                and json.loads(variables["COLVENNA_ENVIRONMENT_SPECIFICATION_REF"])
                == record["build"]["specification_ref"])
        content, provenance = self._original(self._prefix(record) + "receipt.json", MAX_RECEIPT_BYTES)
        receipt = decode_json(content)
        require(receipt["format_version"] == 1 and receipt["build_id"] == record["build"]["build_id"]
                and receipt["codebuild_id"] == actual_build["id"]
                and receipt["specification_sha256"] == record["specification_sha256"]
                and receipt["source_commit"] == policy["source_commit"]
                and receipt["adapter_sha256"] == policy["adapter_sha256"]
                and receipt["base_image"] == policy["base_image"]
                and receipt["python_version"] == policy["python_version"]
                and receipt["python_abi"] == "cp312" and receipt["platform"] == "linux_arm64"
                and receipt["checks"] == {"imports": "PASS", "contract": "PASS"})
        builder = receipt["builder_source"]
        require(hashlib.sha256(canonical(builder).encode()).hexdigest() == policy["builder_source_sha256"]
                and builder["source_commit"] == policy["source_commit"]
                and builder["adapter_sha256"] == policy["adapter_sha256"])
        lock = self._reference(record, receipt["dependency_lock"], MAX_LOCK_BYTES)
        packages = receipt["packages"]
        require(isinstance(packages, list) and 0 < len(packages) <= policy["maximum_packages"])
        require(len({x["name"] for x in packages}) == len(packages))
        private = policy.get("private_wheels", {})
        for package in packages:
            require(re.fullmatch(r"[a-z0-9][a-z0-9.-]*", package["name"])
                    and re.fullmatch(r"[0-9a-f]{64}", package["wheel_sha256"])
                    and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.+!_-]*", package["version"]))
            if package["name"] in private:
                require(package["source"] == private[package["name"]]
                        and package["wheel_sha256"] == private[package["name"]]["sha256"])
            else:
                require(isinstance(package["source"], str)
                        and package["source"].startswith("https://files.pythonhosted.org/")
                        and "@" not in package["source"])
            require(package["license"] in policy["approved_licenses"]
                    or any(value in policy["approved_license_classifiers"]
                           for value in package["license_classifiers"]))
            self._wheel(record, package["artifact"], package["wheel_sha256"])
        require(sum(package["artifact"]["bytes"] for package in packages) <= policy["maximum_wheel_bytes"])
        expected = "".join(
            f"{x['name']}=={x['version']} --hash=sha256:{x['wheel_sha256']}\n"
            for x in sorted(packages, key=lambda x: x["name"])).encode()
        require(lock == expected)
        bindings = receipt["executor_bindings"]
        require(binding_shape(bindings, packages))
        images = receipt["images"]
        require(set(images) == {"lambda", "ecs_task"} and images["lambda"] != images["ecs_task"])
        image_digests = {name: self._image(value, policy, receipt["dependency_lock"]["sha256"], name)
                         for name, value in images.items()}
        space_binding = record["specification"]["space_id"] + "/" + record["specification"]["generation"]
        prefix = re.escape(policy["runtime_prefix"])
        runtime_role = (f"arn:aws:iam::{account()}:role/{policy['runtime_prefix']}"
                        + hashlib.sha256(space_binding.encode()).hexdigest()[:16])
        verified = set()
        for binding in bindings:
            identity = binding.get("function_arn") or binding.get("task_definition_arn")
            if identity in verified:
                continue
            verified.add(identity)
            if binding["executor"] == "lambda":
                arn = binding["function_arn"]
                require(re.fullmatch(
                    rf"arn:aws:lambda:{region()}:{account()}:function:{prefix}[a-f0-9]{{32}}:[1-9][0-9]*",
                    arn) and binding["lambda_mode"] == "standard")
                function = self.lam.get_function(FunctionName=arn)
                configuration = function["Configuration"]
                require(configuration["FunctionArn"] == arn and configuration["Version"] == arn.rsplit(":", 1)[1]
                        and configuration["PackageType"] == "Image"
                        and configuration["Architectures"] == ["arm64"]
                        and configuration["State"] == "Active" and configuration["Role"] == runtime_role
                        and function["Code"]["ResolvedImageUri"] == images["lambda"]
                        and not configuration.get("Environment", {}).get("Variables")
                        and not configuration.get("Environment", {}).get("Error")
                        and not configuration.get("ImageConfigResponse", {}).get("Error")
                        and not configuration.get("ImageConfigResponse", {}).get("ImageConfig")
                        and configuration["Timeout"] == 900 and configuration["MemorySize"] == 2048)
                if policy.get("notification_queue_arn"):
                    # One Invoke is one attempt; outcomes only notify the scheduler.
                    invoke = self.lam.get_function_event_invoke_config(FunctionName=arn)
                    require(invoke.get("MaximumRetryAttempts") == 0
                            and invoke.get("MaximumEventAgeInSeconds") == 60
                            and invoke.get("DestinationConfig") == {
                                "OnSuccess": {"Destination": policy["notification_queue_arn"]},
                                "OnFailure": {"Destination": policy["notification_queue_arn"]}})
            else:
                arn = binding["task_definition_arn"]
                require(binding["image"] == images["ecs_task"]
                        and re.fullmatch(
                            rf"arn:aws:ecs:{region()}:{account()}:task-definition/{prefix}[a-f0-9]{{32}}:[1-9][0-9]*",
                            arn))
                task = self.ecs.describe_task_definition(taskDefinition=arn)["taskDefinition"]
                require(task["taskDefinitionArn"] == arn and task["status"] == "ACTIVE"
                        and task["runtimePlatform"] == {"cpuArchitecture": "ARM64",
                                                       "operatingSystemFamily": "LINUX"}
                        and task["taskRoleArn"] == runtime_role
                        and task["executionRoleArn"] == policy["execution_role_arn"]
                        and task["cpu"] == "1024" and task["memory"] == "4096"
                        and task["networkMode"] == "awsvpc"
                        and task["requiresCompatibilities"] == ["FARGATE"]
                        and len(task["containerDefinitions"]) == 1)
                container = task["containerDefinitions"][0]
                require(container["image"] == images["ecs_task"]
                        and container["readonlyRootFilesystem"]
                        and container["user"] == "10001:10001"
                        and not container.get("secrets") and not container.get("privileged")
                        and len(container.get("environment", [])) == 3
                        and sorted((item["name"], item["value"]) for item in container["environment"])
                        == [(name, region()) for name in ["AWS_DEFAULT_REGION", "AWS_REGION", "COLVENNA_REGION"]]
                        and not container.get("entryPoint") and not container.get("command")
                        and not container.get("environmentFiles"))
        return {"dependency_lock_sha256": receipt["dependency_lock"]["sha256"],
                "image_digest": image_digests["lambda"], "executor_bindings": bindings,
                "dependency_lock": receipt["dependency_lock"], "provenance": provenance,
                "runtime": {name: receipt[name] for name in [
                    "python_version", "python_abi", "platform", "base_image", "adapter_sha256", "source_commit"]}}
