"""Environment API behaviour, admission limits and lost original build replies."""

import copy
import json
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

from botocore.exceptions import ClientError, ReadTimeoutError

from colvenna_isle.control.environments import Environments
from colvenna_isle.control.handler import handle
from colvenna_isle.control.registry import ControlError, Principal, Registry, canonical


class Table:
    """Atomic versioned DynamoDB seam used without an AWS account."""

    def __init__(self):
        self.items = {}
        self.lock = threading.Lock()

    def key(self, value):
        return value["PK"]["S"], value["SK"]["S"]

    def get_item(self, **request):
        with self.lock:
            value = self.items.get(self.key(request["Key"]))
            return {"Item": copy.deepcopy(value)} if value else {}

    def put_item(self, **request):
        with self.lock:
            key = self.key(request["Item"])
            if key in self.items:
                raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem")
            self.items[key] = copy.deepcopy(request["Item"])

    @staticmethod
    def passes(condition, previous, expected, unconditional):
        """The few condition expressions the control services use."""
        if condition is None:
            return unconditional
        if condition == "attribute_not_exists(PK)":
            return previous is None
        if condition == "#revision = :revision":
            return previous is not None and previous["revision"] == expected[":revision"]
        if condition == "generation = :generation":
            return previous is not None and previous["generation"] == expected[":generation"]
        if condition == "#revision = :previous AND generation = :generation":
            return (previous is not None and previous["revision"] == expected[":previous"]
                    and previous["generation"] == expected[":generation"])
        if condition == "attribute_not_exists(stored_bytes) OR stored_bytes <= :room":
            return (previous is None or "stored_bytes" not in previous
                    or int(previous["stored_bytes"]["N"]) <= int(expected[":room"]["N"]))
        if condition == "stored_bytes >= :size":
            return previous is not None and int(previous["stored_bytes"]["N"]) >= int(expected[":size"]["N"])
        return False

    @staticmethod
    def updated(previous, update, key):
        """SET stored_bytes = [if_not_exists(stored_bytes, :zero) +|-] :size counters only."""
        values = update["ExpressionAttributeValues"]
        item = copy.deepcopy(previous) if previous else {"PK": {"S": key[0]}, "SK": {"S": key[1]}}
        current = int(item.get("stored_bytes", values.get(":zero", {"N": "0"}))["N"])
        delta = int(values[":size"]["N"])
        item["stored_bytes"] = {"N": str(current - delta if "- :size" in update["UpdateExpression"]
                                         else current + delta)}
        return item

    def update_item(self, **request):
        with self.lock:
            key = self.key(request["Key"])
            previous = self.items.get(key)
            if not self.passes(request.get("ConditionExpression"), previous,
                               request.get("ExpressionAttributeValues", {}), True):
                raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "UpdateItem")
            self.items[key] = self.updated(previous, request, key)

    def transact_write_items(self, **request):
        with self.lock:
            pending = []
            for entry in request["TransactItems"]:
                if "Delete" in entry:
                    pending.append((self.key(entry["Delete"]["Key"]), None))
                    continue
                value = entry.get("Put", entry.get("ConditionCheck", entry.get("Update")))
                key = self.key(value.get("Item", value.get("Key")))
                previous = self.items.get(key)
                expected = value.get("ExpressionAttributeValues", {})
                if not self.passes(value.get("ConditionExpression"), previous, expected,
                                   "Put" in entry or "Update" in entry):
                    raise ClientError({
                        "Error": {"Code": "TransactionCanceledException"},
                        "CancellationReasons": [{"Code": "ConditionalCheckFailed"}],
                    }, "TransactWriteItems")
                if "Put" in entry:
                    pending.append((key, copy.deepcopy(value["Item"])))
                elif "Update" in entry:
                    pending.append((key, self.updated(previous, value, key)))
            for key, value in pending:
                if value is None:
                    self.items.pop(key, None)
                else:
                    self.items[key] = value


class Builds:
    def __init__(self):
        self.started = []
        self.status = "IN_PROGRESS"
        self.phase = "BUILD"
        self.lost_reply = False
        self.unavailable = False

    def start_build(self, **request):
        self.started.append(copy.deepcopy(request))
        if self.lost_reply:
            raise ReadTimeoutError(endpoint_url="https://codebuild.us-west-2.amazonaws.com")
        return {"build": {"id": "approved:" + request["idempotencyToken"],
                          "projectName": request["projectName"]}}

    def batch_get_builds(self, **request):
        if self.unavailable:
            raise ReadTimeoutError(endpoint_url="https://codebuild.us-west-2.amazonaws.com")
        return {"builds": [{"id": request["ids"][0], "projectName": "approved",
                           "buildStatus": self.status, "currentPhase": self.phase}]}


class Artifacts:
    def __init__(self):
        self.published = []
        self.reject = False

    def publish_specification(self, record):
        self.published.append(copy.deepcopy(record))
        return {"s3_uri": "s3://test-bucket/environment-builds/" + record["build"]["build_id"] + "/spec.json",
                "object_version": "original-spec", "sha256": record["specification_sha256"],
                "bytes": 512}

    def verify(self, record, actual):
        if self.reject:
            raise ControlError(503, "ENVIRONMENT_ARTIFACTS_UNVERIFIED")
        return {"dependency_lock_sha256": "a" * 64, "image_digest": "sha256:" + "b" * 64,
                "dependency_lock": {"s3_uri": "s3://test-bucket/lock.txt", "object_version": "lock-version",
                                    "sha256": "a" * 64, "bytes": 512},
                "runtime": {"source_commit": "a" * 40, "adapter_sha256": "c" * 64,
                    "python_version": "3.12.12", "python_abi": "cp312", "platform": "linux_arm64",
                    "base_image": "approved@sha256:" + "a" * 64},
                "executor_bindings": [{
                    "executor": "ecs_task", "profile": "python_ecs_medium",
                    "task_definition_arn":
                        "arn:aws:ecs:us-west-2:123456789012:task-definition/colvenna-isle-env-test:1",
                    "image": "123456789012.dkr.ecr.us-west-2.amazonaws.com/colvenna-isle-environments@sha256:"
                             + "b" * 64}]}


def specification(version="v1", content="pandas==2.2.3\npyarrow==24.0.0"):
    return {"environment_id": "analysis", "version": version, "python_version": "3.12",
            "platform": "linux_arm64", "build_profile": "binary_wheels",
            "dependency_source": {"format": "requirements", "content": content}}


class EnvironmentTests(unittest.TestCase):
    def setUp(self):
        self.table = Table()
        self.registry = Registry(self.table, "control", "test-bucket")
        self.configuration = self.registry.provision("space_one")
        self.actor = Principal("owner", frozenset(["space_one"]), frozenset(["runtime:manage"]),
                               generation=self.configuration["generation"])
        self.builds, self.artifacts = Builds(), Artifacts()
        self.environments = Environments(
            self.registry, self.builds, "approved",
            {"base_image": "approved@sha256:" + "a" * 64, "adapter_sha256": "c" * 64,
             "source_location": "test-bucket/artifacts/builders/original.zip",
             "source_version": "original-source-version"},
            self.artifacts)

    def create(self, version="v1", content=None, actor=None, space="space_one"):
        request = specification(version)
        if content is not None:
            request["dependency_source"]["content"] = content
        return self.environments.create(actor or self.actor, space, request)

    def build(self, version="v1", nonce="original", actor=None, space="space_one"):
        return self.environments.build(actor or self.actor, space, "analysis", version, nonce)

    def get(self, version="v1", actor=None, space="space_one"):
        return self.environments.get(actor or self.actor, space, "analysis", version)

    def test_version_is_immutable_and_upgrade_creates_another_version(self):
        original = self.create()
        self.assertEqual(original, self.create())
        with self.assertRaises(ControlError) as error:
            self.create(content="pandas==2.2.2")
        self.assertEqual(error.exception.code, "ENVIRONMENT_VERSION_IMMUTABLE")
        self.create("v2", "pandas==2.2.2")
        self.assertEqual(self.get()["status"], "DRAFT")
        self.assertEqual(self.get("v2")["status"], "DRAFT")

    def test_lost_start_reply_preserves_one_original_without_redispatch(self):
        self.create()
        self.builds.lost_reply = True
        original = self.build()
        self.assertEqual(original, self.build())
        self.assertEqual(original, self.build())
        self.assertEqual(len(self.builds.started), 1)
        self.assertEqual(len(self.artifacts.published), 1)
        self.assertEqual(self.get()["status"], "UNKNOWN")
        with self.assertRaises(ControlError):
            self.build(nonce="replacement")

    def test_concurrent_same_original_starts_once(self):
        self.create()
        with ThreadPoolExecutor(max_workers=4) as pool:
            receipts = list(pool.map(lambda _: self.build(), range(4)))
        self.assertEqual(len({x["build_id"] for x in receipts}), 1)
        self.assertEqual(len(self.builds.started), 1)

    def test_original_specification_reference_is_pinned_before_dispatch(self):
        self.create()
        receipt = self.build()
        overrides = {x["name"]: x["value"] for x in self.builds.started[0]["environmentVariablesOverride"]}
        reference = json.loads(overrides["COLVENNA_ENVIRONMENT_SPECIFICATION_REF"])
        self.assertEqual(reference["object_version"], "original-spec")
        self.assertEqual(self.builds.started[0]["idempotencyToken"], receipt["build_id"])
        self.assertEqual(self.builds.started[0]["sourceVersion"], "original-source-version")
        self.assertEqual(self.builds.started[0]["sourceLocationOverride"],
                         "test-bucket/artifacts/builders/original.zip")
        self.assertEqual(self.builds.started[0]["sourceTypeOverride"], "S3")
        self.assertEqual(self.builds.started[0]["buildspecOverride"], "buildspec.yml")
        self.assertEqual(reference["sha256"], overrides["COLVENNA_ENVIRONMENT_SPECIFICATION_SHA256"])
        self.assertNotIn("COLVENNA_ENVIRONMENT_SPECIFICATION", overrides)

    def test_actual_terminal_and_artifact_proof_are_required_before_ready(self):
        self.create()
        self.build()
        self.assertEqual(self.get()["status"], "BUILDING")
        self.builds.phase = "POST_BUILD"
        self.assertEqual(self.get()["status"], "VERIFYING")
        self.builds.status = "SUCCEEDED"
        self.artifacts.reject = True
        self.assertEqual(self.get()["status"], "UNKNOWN")
        self.artifacts.reject = False
        ready = self.get()
        self.assertEqual(ready["status"], "READY")
        self.builds.unavailable = True
        self.assertEqual(self.get(), ready)
        with self.assertRaises(ControlError):
            self.build(nonce="new")

    def test_wrong_project_or_missing_original_build_is_unknown(self):
        self.create()
        self.build()
        self.builds.batch_get_builds = lambda **request: {
            "builds": [{"id": request["ids"][0], "projectName": "foreign", "buildStatus": "SUCCEEDED"}]}
        self.assertEqual(self.get()["status"], "UNKNOWN")
        self.assertEqual(len(self.builds.started), 1)

    def test_failed_original_remains_failed_and_frees_capacity(self):
        for version in ["v1", "v2", "v3"]:
            self.create(version)
        self.build()
        self.build("v2")
        with self.assertRaises(ControlError) as error:
            self.build("v3")
        self.assertEqual(error.exception.code, "SPACE_BUILD_QUOTA")
        self.builds.status = "FAILED"
        self.assertEqual(self.get()["status"], "FAILED")
        self.build("v3")
        self.assertEqual(self.get()["status"], "FAILED")
        self.assertEqual(len(self.builds.started), 3)

    def test_observing_response_reports_the_recorded_builder_failure(self):
        self.create()
        self.build()
        self.builds.status = "FAILED"
        self.artifacts.failure = lambda record: {
            "format_version": 1, "build_id": record["build"]["build_id"], "stage": "RESOLUTION",
            "code": "DEPENDENCY_NOT_FOUND", "message": "No binary wheel satisfies: missing==1."}
        first, second = self.get(), self.get()
        self.assertEqual(first["failure"], second["failure"])
        self.assertEqual(first["failure"]["code"], "DEPENDENCY_NOT_FOUND")
        self.assertEqual(first["failure"]["phase"], "RESOLUTION")
        self.assertEqual(first["failure"]["aws_status"], "FAILED")

    def test_unknown_original_keeps_capacity(self):
        self.builds.lost_reply = True
        for version in ["v1", "v2", "v3"]:
            self.create(version)
        self.build()
        self.build("v2")
        with self.assertRaises(ControlError) as error:
            self.build("v3")
        self.assertEqual(error.exception.status, 429)
        self.assertEqual(len(self.builds.started), 2)

    def test_global_capacity_bounds_other_spaces(self):
        for space in ["space_one", "space_two", "space_three"]:
            config = self.registry.provision(space)
            actor = Principal("owner", frozenset([space]), frozenset(["runtime:manage"]),
                              generation=config["generation"])
            for version in ["v1", "v2"]:
                self.create(version, actor=actor, space=space)
                if space != "space_three":
                    self.build(version, actor=actor, space=space)
                else:
                    with self.assertRaises(ControlError) as error:
                        self.build(version, actor=actor, space=space)
                    self.assertEqual(error.exception.status, 429)
        self.assertEqual(len(self.builds.started), 4)

    def test_sources_cannot_supply_urls_credentials_paths_or_pip_options(self):
        for content in ["--extra-index-url https://private.example", "pkg @ https://example/wheel.whl",
                        "-e ./local", "https://user:secret@example/wheel.whl", "py arrow==24.0.0",
                        "pkg;python_version>'3.12'", "../wheel.whl", ""]:
            with self.subTest(content=content), self.assertRaises((ControlError, ValueError)):
                self.create(content=content)
        self.assertEqual(self.builds.started, [])

    def test_space_generation_and_management_scope_are_enforced(self):
        for actor in [
            Principal("owner", frozenset(["other"]), self.actor.scopes),
            Principal("owner", self.actor.spaces, frozenset(["pipeline:run"])),
            Principal("owner", self.actor.spaces, self.actor.scopes, generation="gen_" + "0" * 32),
        ]:
            with self.assertRaises(ControlError) as error:
                self.create(actor=actor)
            self.assertEqual(error.exception.status, 403)
        self.create()
        monitor = Principal("monitor", self.actor.spaces, frozenset(["monitor:read"]),
                            generation=self.actor.generation)
        self.assertEqual(self.get(actor=monitor)["status"], "DRAFT")
        with self.assertRaises(ControlError):
            self.build(actor=monitor)

    def test_wrong_platform_cannot_create_an_environment(self):
        for field, value in [("python_version", "3.11"), ("platform", "linux_amd64"),
                             ("build_profile", "unapproved")]:
            request = {**specification(), field: value}
            with self.assertRaises(ValueError):
                self.environments.create(self.actor, "space_one", request)
        with self.assertRaises(ControlError) as error:
            self.get()
        self.assertEqual(error.exception.status, 404)

    def test_catalogue_limit_and_corrupt_binding_fail_closed(self):
        for index in range(64):
            self.create("v" + str(index))
        with self.assertRaises(ControlError) as error:
            self.create("v64")
        self.assertEqual(error.exception.code, "ENVIRONMENT_VERSION_LIMIT")
        for key, item in self.table.items.items():
            if key[0].startswith("ENVIRONMENT#"):
                value = json.loads(item["record"]["S"])
                value["versions"]["analysis/v0"]["specification"]["platform"] = "linux_amd64"
                item["record"]["S"] = canonical(value)
        with self.assertRaises(ControlError) as error:
            self.get("v0")
        self.assertEqual(error.exception.code, "ENVIRONMENT_RECORD_UNAVAILABLE")
        self.assertEqual(error.exception.status, 503)

    def test_deployment_profile_change_cannot_rebind_an_existing_draft(self):
        self.create()
        changed = Environments(
            self.registry, self.builds, "approved", {"base_image": "changed"}, self.artifacts)
        with self.assertRaises(ControlError) as error:
            changed.build(self.actor, "space_one", "analysis", "v1", "original")
        self.assertEqual(error.exception.code, "ENVIRONMENT_BUILD_PROFILE_CHANGED")
        self.assertEqual(self.builds.started, [])

    def test_missing_immutable_builder_source_cannot_admit_or_dispatch(self):
        for changes in [
            {"source_version": "null"}, {"source_version": ""},
            {"source_location": "other-bucket/artifacts/builder.zip"},
            {"source_location": "test-bucket/artifacts/../builder.zip"},
        ]:
            policy = {**self.environments.policy, **changes}
            invalid = Environments(self.registry, self.builds, "approved", policy, self.artifacts)
            with self.assertRaises(ControlError) as error:
                invalid.create(self.actor, "space_one", specification())
            self.assertEqual(error.exception.code, "ENVIRONMENT_BUILDER_UNAVAILABLE")
        self.assertEqual(self.builds.started, [])
        with self.assertRaises(ControlError) as error:
            self.get()
        self.assertEqual(error.exception.status, 404)

    def test_http_routes_and_missing_nonce_use_existing_contract(self):
        event = {
            "rawPath": "/v1/spaces/space_one/runtime-environments",
            "headers": {"content-type": "application/json"},
            "requestContext": {"requestId": "request_one", "http": {"method": "POST"},
                "authorizer": {"lambda": {"principal_id": "owner", "spaces": '["space_one"]',
                                         "scopes": '["runtime:manage"]',
                                         "generation": self.actor.generation}}},
            "body": canonical(specification()),
        }
        result = handle(event, self.registry, environments=self.environments)
        self.assertEqual(result["statusCode"], 201)
        event["rawPath"] += "/analysis/versions/v1/build"
        event["body"] = "{}"
        self.assertEqual(handle(event, self.registry, environments=self.environments)["statusCode"], 428)
        event["headers"]["idempotency-key"] = "original"
        self.assertEqual(handle(event, self.registry, environments=self.environments)["statusCode"], 202)
        event["rawPath"] = event["rawPath"].removesuffix("/build")
        event["requestContext"]["http"]["method"] = "GET"
        result = handle(event, self.registry, environments=self.environments)
        self.assertEqual(result["statusCode"], 200)
        self.assertEqual(json.loads(result["body"])["status"], "BUILDING")


if __name__ == "__main__":
    unittest.main()
