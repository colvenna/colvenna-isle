"""Independent original-version, package-lock and real-resource confirmation seam."""

import copy
import base64
import hashlib
import io
import json
import unittest

from botocore.exceptions import ClientError

from colvenna_isle.control.environment_artifacts import EnvironmentArtifacts
from colvenna_isle.control.registry import ControlError, canonical

ACCOUNT = "123456789012"
BUCKET = "test-bucket"
PREFIX = "spaces/space_one/environment-builds/gen_original/build_original/"
ROLE = ("arn:aws:iam::123456789012:role/colvenna-isle-env-"
        + hashlib.sha256(b"space_one/gen_original").hexdigest()[:16])
RESOURCE_NAME = "colvenna-isle-env-" + "1" * 32
FUNCTION = "arn:aws:lambda:us-west-2:123456789012:function:" + RESOURCE_NAME + ":7"
TASK = "arn:aws:ecs:us-west-2:123456789012:task-definition/" + RESOURCE_NAME + ":3"
WHEEL = b"actual synthetic wheel fixture bytes"
WHEEL_HASH = hashlib.sha256(WHEEL).hexdigest()


class Objects:
    def __init__(self):
        self.objects = {}
        self.ambiguous = False

    def put_object(self, **request):
        key = request["Key"]
        if key in self.objects and request.get("IfNoneMatch") == "*":
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.objects[key] = (request["Body"], request["Metadata"], "original-version")
        return {"VersionId": "original-version"}

    def list_object_versions(self, **request):
        key = request["Prefix"]
        versions = [{"Key": key, "VersionId": self.objects[key][2]}] if key in self.objects else []
        return {"IsTruncated": self.ambiguous, "Versions": versions, "DeleteMarkers": []}

    def get_object(self, **request):
        body, metadata, version = self.objects[request["Key"]]
        if request["VersionId"] != version:
            raise AssertionError("Consumer read a different version")
        return {"Body": io.BytesIO(body), "ContentLength": len(body), "Metadata": metadata,
                "VersionId": version}

    def save(self, name, content):
        sha = hashlib.sha256(content).hexdigest()
        self.put_object(Key=PREFIX + name, Body=content, Metadata={"sha256": sha})
        return {"s3_uri": f"s3://{BUCKET}/{PREFIX}{name}", "object_version": "original-version",
                "sha256": sha, "bytes": len(content)}

    def head_object(self, **request):
        content, metadata, version = self.objects[request["Key"]]
        return {"ContentLength": len(content), "VersionId": version, "Metadata": metadata,
                "ChecksumSHA256": base64.b64encode(hashlib.sha256(content).digest()).decode()}


class Images:
    def __init__(self):
        lock = ("pandas==2.2.3 --hash=sha256:" + WHEEL_HASH + "\n").encode()
        self.manifests, self.configurations = {}, {}
        for executor in ["lambda", "ecs_task"]:
            configuration = canonical({
                "architecture": "arm64", "os": "linux", "config": {
                    "User": "10001:10001", "Env": [],
                    "Entrypoint": ["/lambda-entrypoint.sh"] if executor == "lambda"
                                  else ["python", "-m", "colvenna_isle.pipeline.environment_adapter"],
                    "Cmd": ["colvenna_isle.pipeline.environment_adapter.lambda_handler"]
                           if executor == "lambda" else [],
                    "Labels": {
                        "io.colvenna.source": "d" * 40, "io.colvenna.adapter": "e" * 64,
                        "io.colvenna.dependency-lock": hashlib.sha256(lock).hexdigest(),
                        "io.colvenna.executor": executor,
                    }}}).encode()
            config_sha = "sha256:" + hashlib.sha256(configuration).hexdigest()
            manifest = canonical({"schemaVersion": 2, "config": {"digest": config_sha}})
            sha = "sha256:" + hashlib.sha256(manifest.encode()).hexdigest()
            self.configurations[config_sha] = configuration
            self.manifests[sha] = manifest
        self.digest, self.ecs_digest = list(self.manifests)
        self.size = 512 * 1024 * 1024

    def batch_get_image(self, **request):
        sha = request["imageIds"][0]["imageDigest"]
        return {"images": [{"imageId": {"imageDigest": sha}, "imageManifest": self.manifests[sha]}]}

    def describe_images(self, **request):
        return {"imageDetails": [{"imageDigest": request["imageIds"][0]["imageDigest"],
                                  "imageSizeInBytes": self.size}]}

    def get_download_url_for_layer(self, **request):
        return {"downloadUrl": request["layerDigest"]}

    def fetch(self, digest):
        return self.configurations[digest]


class Function:
    def __init__(self, image):
        self.value = {
            "Configuration": {"FunctionArn": FUNCTION, "Version": "7", "PackageType": "Image",
                              "Architectures": ["arm64"], "State": "Active", "Role": ROLE,
                              "Timeout": 900, "MemorySize": 2048},
            "Code": {"ResolvedImageUri": image},
        }

    def get_function(self, **request):
        return copy.deepcopy(self.value)


class Tasks:
    def __init__(self, image):
        self.value = {
            "taskDefinitionArn": TASK, "status": "ACTIVE",
            "runtimePlatform": {"cpuArchitecture": "ARM64", "operatingSystemFamily": "LINUX"},
            "taskRoleArn": ROLE, "executionRoleArn": "arn:aws:iam::123456789012:role/approved-execution",
            "cpu": "1024", "memory": "4096", "networkMode": "awsvpc",
            "requiresCompatibilities": ["FARGATE"],
            "containerDefinitions": [{"image": image, "readonlyRootFilesystem": True,
                                      "environment": [{"name": name, "value": "us-west-2"} for name in [
                                          "COLVENNA_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"]],
                                      "user": "10001:10001"}],
        }

    def describe_task_definition(self, **request):
        return {"taskDefinition": copy.deepcopy(self.value)}


class ArtifactTests(unittest.TestCase):
    def setUp(self):
        self.objects, self.images = Objects(), Images()
        image = f"{ACCOUNT}.dkr.ecr.us-west-2.amazonaws.com/colvenna-isle-images@" + self.images.digest
        ecs_image = f"{ACCOUNT}.dkr.ecr.us-west-2.amazonaws.com/colvenna-isle-images@" + self.images.ecs_digest
        self.lam, self.ecs = Function(image), Tasks(ecs_image)
        self.artifacts = EnvironmentArtifacts(
            self.objects, self.images, self.lam, self.ecs, BUCKET, self.images.fetch)
        policy = {
            "source_version": "original-source-version", "source_commit": "d" * 40,
            "source_location": "test-bucket/artifacts/builders/original.zip",
            "adapter_sha256": "e" * 64, "base_image": "public.ecr.aws/lambda/python@sha256:" + "f" * 64,
            "repository": "colvenna-isle-images", "runtime_prefix": "colvenna-isle-env-",
            "maximum_image_bytes": 1024 * 1024 * 1024,
            "execution_role_arn": "arn:aws:iam::123456789012:role/approved-execution",
            "python_version": "3.12.13", "maximum_packages": 128, "maximum_wheel_bytes": 536870912,
            "approved_licenses": ["BSD-3-Clause"], "approved_license_classifiers": [],
        }
        builder = {"source_commit": policy["source_commit"], "adapter_sha256": policy["adapter_sha256"],
                   "source_tree": "0" * 40, "files": {"buildspec.yml": "f" * 64}}
        policy["builder_source_sha256"] = hashlib.sha256(canonical(builder).encode()).hexdigest()
        specification = {"environment_id": "analysis", "version": "v1", "space_id": "space_one",
                         "generation": "gen_original", "policy": policy, "build_project": "approved"}
        self.record = {"specification": specification,
                       "specification_sha256": hashlib.sha256(canonical(specification).encode()).hexdigest(),
                       "build": {"build_id": "build_original", "codebuild_id": "approved:original",
                                 "project": "approved"}}
        self.record["build"]["specification_ref"] = self.artifacts.publish_specification(self.record)
        self.actual = {
            "id": "approved:original", "projectName": "approved", "buildStatus": "SUCCEEDED",
            "source": {"type": "S3", "location": "test-bucket/artifacts/builders/original.zip",
                       "buildspec": "buildspec.yml"},
            "sourceVersion": "original-source-version", "environment": {"environmentVariables": [
                {"name": "COLVENNA_ENVIRONMENT_BUILD_ID", "value": "build_original"},
                {"name": "COLVENNA_ENVIRONMENT_SPECIFICATION_SHA256",
                 "value": self.record["specification_sha256"]},
                {"name": "COLVENNA_ENVIRONMENT_SPECIFICATION_REF",
                 "value": canonical(self.record["build"]["specification_ref"])},
            ]},
        }
        lock = self.objects.save("requirements.lock", (
            "pandas==2.2.3 --hash=sha256:" + WHEEL_HASH + "\n").encode())
        self.receipt = {
            "format_version": 1, "build_id": "build_original", "codebuild_id": "approved:original",
            "specification_sha256": self.record["specification_sha256"],
            "source_commit": policy["source_commit"], "adapter_sha256": policy["adapter_sha256"],
            "base_image": policy["base_image"], "python_version": "3.12.13", "python_abi": "cp312",
            "platform": "linux_arm64", "checks": {"imports": "PASS", "contract": "PASS"},
            "builder_source": builder,
            "dependency_lock": lock, "packages": [{
                "name": "pandas", "version": "2.2.3", "wheel_sha256": WHEEL_HASH,
                "source": "https://files.pythonhosted.org/packages/test/pandas.whl",
                "artifact": self.objects.save("wheels/pandas-2.2.3-py3-none-any.whl", WHEEL),
                "license": "BSD-3-Clause", "license_classifiers": []}],
            "images": {"lambda": image, "ecs_task": ecs_image}, "executor_bindings": [
                {"executor": "lambda", "profile": "python_lambda_standard", "lambda_mode": "standard",
                 "function_arn": FUNCTION},
                {"executor": "ecs_task", "profile": "python_ecs_medium",
                 "task_definition_arn": TASK, "image": ecs_image},
            ],
        }
        self.save_receipt()

    def save_receipt(self):
        self.objects.save("receipt.json", canonical(self.receipt).encode())

    def verify(self):
        return self.artifacts.verify(self.record, self.actual)

    def test_sql_capable_environments_bind_sql_profiles_to_the_same_versions(self):
        from colvenna_isle.control.environment_artifacts import binding_shape

        python = [{"name": "pandas"}]
        sql = python + [{"name": "duckdb"}, {"name": "pyarrow"}]
        lambda_binding, ecs_binding = self.receipt["executor_bindings"]
        four = [lambda_binding, {**lambda_binding, "profile": "sql_lambda_standard"},
                ecs_binding, {**ecs_binding, "profile": "sql_ecs_medium"}]
        self.assertTrue(binding_shape(self.receipt["executor_bindings"], python))
        self.assertTrue(binding_shape(four, sql))
        self.assertFalse(binding_shape(self.receipt["executor_bindings"], sql))
        self.assertFalse(binding_shape(four, python))
        other_version = copy.deepcopy(four)
        other_version[1]["function_arn"] = FUNCTION.rsplit(":", 1)[0] + ":8"
        self.assertFalse(binding_shape(other_version, sql))

    def test_complete_original_proof_confirms_both_numeric_bindings(self):
        value = self.verify()
        self.assertEqual(value["executor_bindings"], self.receipt["executor_bindings"])
        self.assertEqual(value["runtime"]["python_version"], "3.12.13")
        self.assertEqual(value["dependency_lock"]["object_version"], "original-version")
        self.assertEqual(value["provenance"]["object_version"], "original-version")

    def test_source_manifest_license_and_exact_wheel_identity_are_mandatory(self):
        original = copy.deepcopy(self.receipt)
        for mutation in ["source", "license", "wheel_version", "wheel_hash"]:
            self.receipt = copy.deepcopy(original)
            if mutation == "source":
                self.receipt["builder_source"]["source_commit"] = "0" * 40
            elif mutation == "license":
                self.receipt["packages"][0]["license"] = "UNAPPROVED"
            elif mutation == "wheel_version":
                self.receipt["packages"][0]["artifact"]["object_version"] = "replacement"
            else:
                self.receipt["packages"][0]["artifact"]["sha256"] = "0" * 64
            self.save_receipt()
            with self.subTest(mutation=mutation), self.assertRaises(ControlError):
                self.verify()

    def test_oci_empty_ecs_command_can_be_omitted_but_cannot_be_overridden(self):
        original = json.loads(self.images.configurations[next(reversed(self.images.configurations))])
        for command in [None, ["unapproved"]]:
            ecs_configuration = copy.deepcopy(original)
            ecs_configuration["config"].pop("Cmd")
            if command:
                ecs_configuration["config"]["Cmd"] = command
            raw = canonical(ecs_configuration).encode()
            config_sha = "sha256:" + hashlib.sha256(raw).hexdigest()
            manifest = canonical({"schemaVersion": 2, "config": {"digest": config_sha}})
            digest = "sha256:" + hashlib.sha256(manifest.encode()).hexdigest()
            self.images.configurations[config_sha] = raw
            self.images.manifests[digest] = manifest
            image = self.receipt["images"]["ecs_task"].split("@", 1)[0] + "@" + digest
            self.receipt["images"]["ecs_task"] = image
            self.receipt["executor_bindings"][1]["image"] = image
            self.ecs.value["containerDefinitions"][0]["image"] = image
            self.save_receipt()
            if command:
                with self.assertRaises(ControlError):
                    self.verify()
            else:
                self.verify()

    def test_specification_uses_conditional_original_key_and_exact_version(self):
        first = self.record["build"]["specification_ref"]
        self.assertEqual(self.artifacts.publish_specification(self.record), first)
        self.assertEqual(len(self.objects.objects), 4)
        self.objects.ambiguous = True
        with self.assertRaises(ControlError):
            self.artifacts.publish_specification(self.record)

    def test_wrong_source_adapter_abi_architecture_or_contract_never_verifies(self):
        original = copy.deepcopy(self.receipt)
        for name, value in [
            ("source_commit", "0" * 40), ("adapter_sha256", "0" * 64), ("python_version", "3.11.9"),
            ("python_abi", "cp311"), ("platform", "linux_amd64"),
            ("checks", {"imports": "PASS", "contract": "FAIL"}),
            ("specification_sha256", "0" * 64), ("codebuild_id", "foreign:build"),
        ]:
            self.receipt = {**original, name: value}
            self.save_receipt()
            with self.subTest(field=name), self.assertRaises(ControlError):
                self.verify()

    def test_actual_function_architecture_image_role_or_state_cannot_be_forged_in_receipt(self):
        original = copy.deepcopy(self.lam.value)
        for name, value in [("Architectures", ["x86_64"]), ("State", "Pending"),
                            ("Role", "arn:aws:iam::123456789012:role/warehouse-writer"),
                            ("Version", "$LATEST")]:
            self.lam.value = copy.deepcopy(original)
            self.lam.value["Configuration"][name] = value
            with self.subTest(field=name), self.assertRaises(ControlError):
                self.verify()
        self.lam.value = copy.deepcopy(original)
        self.lam.value["Code"]["ResolvedImageUri"] += "_changed"
        with self.assertRaises(ControlError):
            self.verify()

    def test_actual_task_platform_image_permissions_and_writable_root_are_rejected(self):
        original = copy.deepcopy(self.ecs.value)
        for mutate in [
            lambda t: t.update(status="INACTIVE"),
            lambda t: t.update(taskRoleArn="arn:aws:iam::123456789012:role/warehouse-writer"),
            lambda t: t["runtimePlatform"].update(cpuArchitecture="X86_64"),
            lambda t: t["containerDefinitions"][0].update(readonlyRootFilesystem=False),
            lambda t: t["containerDefinitions"][0].update(image="mutable:latest"),
            lambda t: t["containerDefinitions"][0].update(secrets=[{"name": "PRIVATE_INDEX", "valueFrom": "secret"}]),
            lambda t: t["containerDefinitions"][0].update(environment=[{"name": "AWS_ACCESS_KEY_ID", "value": "bad"}]),
        ]:
            self.ecs.value = copy.deepcopy(original)
            mutate(self.ecs.value)
            with self.assertRaises(ControlError):
                self.verify()

    def test_manifest_lock_change_or_incomplete_listing_is_rejected(self):
        self.objects.save("requirements.lock", b"pandas==2.2.2 --hash=sha256:" + b"a" * 64 + b"\n")
        with self.assertRaises(ControlError):
            self.verify()
        self.setUp()
        self.objects.ambiguous = True
        with self.assertRaises(ControlError):
            self.verify()

    def test_nonapproved_package_source_and_oversized_image_are_rejected(self):
        self.receipt["packages"][0]["source"] = "https://unapproved.example/pandas.whl"
        self.save_receipt()
        with self.assertRaises(ControlError):
            self.verify()
        self.setUp()
        self.images.size = 2 * 1024 * 1024 * 1024
        with self.assertRaises(ControlError):
            self.verify()

    def test_actual_image_configuration_hash_architecture_or_lock_binding_is_required(self):
        for mutate in [
            lambda c: c.update(architecture="amd64"),
            lambda c: c["config"].update(User="root"),
            lambda c: c["config"]["Labels"].update({"io.colvenna.dependency-lock": "0" * 64}),
            lambda c: c["config"].update(Env=["AWS_SECRET_ACCESS_KEY=forbidden"]),
        ]:
            self.setUp()
            key = next(iter(self.images.configurations))
            configuration = json.loads(self.images.configurations[key])
            mutate(configuration)
            raw = canonical(configuration).encode()
            config_sha = "sha256:" + hashlib.sha256(raw).hexdigest()
            manifest = canonical({"schemaVersion": 2, "config": {"digest": config_sha}})
            image_sha = "sha256:" + hashlib.sha256(manifest.encode()).hexdigest()
            self.images.configurations[config_sha] = raw
            self.images.manifests[image_sha] = manifest
            image = f"{ACCOUNT}.dkr.ecr.us-west-2.amazonaws.com/colvenna-isle-images@" + image_sha
            self.receipt["images"]["lambda"] = image
            self.lam.value["Code"]["ResolvedImageUri"] = image
            self.save_receipt()
            # All original hashes and resource images match; the physical config is invalid.
            with self.assertRaises(ControlError):
                self.verify()

    def test_configuration_bytes_cannot_change_under_the_original_digest(self):
        key = next(iter(self.images.configurations))
        self.images.configurations[key] += b"\n"
        with self.assertRaises(ControlError):
            self.verify()

    def test_lambda_build_credentials_and_entrypoint_overrides_are_rejected(self):
        for extra in [
            {"Environment": {"Variables": {"PIP_INDEX_URL": "https://user:secret@example.invalid"}}},
            {"Environment": {"Error": {"ErrorCode": "KMSAccessDeniedException"}}},
            {"ImageConfigResponse": {"ImageConfig": {"Command": ["unapproved.handler"]}}},
        ]:
            self.setUp()
            self.lam.value["Configuration"].update(extra)
            with self.assertRaises(ControlError):
                self.verify()

    def test_swapping_executor_images_cannot_change_the_entrypoint_contract(self):
        images = self.receipt["images"]
        images["lambda"], images["ecs_task"] = images["ecs_task"], images["lambda"]
        self.receipt["executor_bindings"][1]["image"] = images["ecs_task"]
        self.lam.value["Code"]["ResolvedImageUri"] = images["lambda"]
        self.ecs.value["containerDefinitions"][0]["image"] = images["ecs_task"]
        self.save_receipt()
        with self.assertRaises(ControlError):
            self.verify()

    def test_missing_version_listing_completion_is_not_positive_custody(self):
        original = self.objects.list_object_versions

        def incomplete(**request):
            value = original(**request)
            del value["IsTruncated"]
            return value

        self.objects.list_object_versions = incomplete
        with self.assertRaises(ControlError):
            self.verify()

    def test_actual_source_location_version_and_buildspec_are_fixed(self):
        for mutate in [
            lambda b: b.update(sourceVersion="current-version"),
            lambda b: b["source"].update(location="other-bucket/other.zip"),
            lambda b: b["source"].update(type="GITHUB"),
            lambda b: b["source"].update(buildspec="other-buildspec.yml"),
        ]:
            self.setUp()
            mutate(self.actual)
            with self.assertRaises(ControlError):
                self.verify()


if __name__ == "__main__":
    unittest.main()
