"""Public definition API checks; real AWS custody is qualified separately."""

import copy
import hashlib
import io
import json
import unittest
import zipfile
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import parse_qs, urlparse
from unittest.mock import patch

import boto3
from botocore.exceptions import ClientError, ReadTimeoutError

from colvenna_isle.control.environments import Environments
from colvenna_isle.control.handler import clients, handle
from colvenna_isle.control.pipeline_artifacts import PipelineArtifacts, checksum
from colvenna_isle.control.pipelines import Pipelines
from colvenna_isle.control.registry import ControlError, Principal, Registry, canonical
from test_environments import Artifacts, Builds, Table, specification


class DefinitionTable(Table):
    def __init__(self):
        super().__init__()
        self.lose_publication_reply = False

    def put_item(self, **request):
        with self.lock:
            key = self.key(request["Item"])
            previous = self.items.get(key)
            expected = request.get("ExpressionAttributeValues", {})
            passed = previous is None if request["ConditionExpression"] == "attribute_not_exists(PK)" else (
                previous is not None and previous["revision"] == expected[":previous"]
                and previous["generation"] == expected[":generation"])
            if not passed:
                raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem")
            self.items[key] = copy.deepcopy(request["Item"])

    def transact_write_items(self, **request):
        super().transact_write_items(**request)
        if self.lose_publication_reply and any(
                "Put" in operation and operation["Put"]["Item"]["SK"]["S"].startswith("PIPELINE_VERSION#")
                for operation in request["TransactItems"]):
            self.lose_publication_reply = False
            raise ReadTimeoutError(endpoint_url="https://dynamodb.us-west-2.amazonaws.com")


class Objects:
    def __init__(self):
        self.objects = {}
        self.version = 0
        self.lose_package_reply = False
        self.after_package = None

    def generate_presigned_url(self, operation, **request):
        self.signed = copy.deepcopy(request)
        return "https://test-bucket.s3.us-west-2.amazonaws.com/opaque-upload"

    def put_object(self, **request):
        key = request["Key"]
        if key in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.version += 1
        value = {"VersionId": "object-version-" + str(self.version), "Body": request["Body"],
                 "ContentLength": len(request["Body"]), "ChecksumSHA256": request["ChecksumSHA256"]}
        self.objects[key] = value
        if key.endswith("package.zip"):
            if self.after_package:
                callback, self.after_package = self.after_package, None
                callback()
            if self.lose_package_reply:
                self.lose_package_reply = False
                raise ReadTimeoutError(endpoint_url="https://s3.us-west-2.amazonaws.com")
        return {"VersionId": value["VersionId"]}

    def head_object(self, **request):
        if request["Key"] not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {name: value for name, value in self.objects[request["Key"]].items() if name != "Body"}

    def get_object(self, **request):
        value = self.objects[request["Key"]]
        if request["VersionId"] != value["VersionId"]:
            raise ClientError({"Error": {"Code": "NoSuchVersion"}}, "GetObject")
        return {**value, "Body": io.BytesIO(value["Body"])}


def definition():
    return {"spec_version": 1, "pipeline_id": "demo", "name": "Synthetic pipeline",
            "schedule": {"owner": "none"}, "max_active_runs": 1, "coalesce_pending": True,
            "tasks": [{"task_id": "transform", "language": "python", "executor": "ecs_task",
                       "processing": {"mode": "full"}, "entrypoint": "python/transform.py",
                       "runtime_environment": {"environment_id": "analysis", "version": "v1"},
                       "resource_profile": "python_ecs_medium", "input_mode": "local_files",
                       "timeout_seconds": 600, "max_attempts": 1,
                       "inputs": {"source": {"dataset": "ods.source", "selection": "snapshot"}},
                       "output": {"dataset": "dwd.result", "write_mode": "versioned_replace",
                                  "allow_empty": True, "key_columns": ["id"],
                                  "schema": [{"name": "id", "type": "BIGINT", "nullable": False}]}}]}


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.table = DefinitionTable()
        self.registry = Registry(self.table, "control", "test-bucket")
        self.configuration = self.registry.provision("space_one")
        self.actor = Principal("owner", frozenset(["space_one"]),
                               frozenset(["pipeline:manage", "runtime:manage", "datasets:manage"]),
                               generation=self.configuration["generation"])
        for dataset, owner in [("ods.source", "direct"), ("dwd.result", "pipeline")]:
            value = {"dataset": dataset, "schema_version": 1,
                     "schema": [{"name": "id", "type": "BIGINT", "nullable": False}],
                     "key_columns": ["id"], "partition_columns": [], "write_owner": owner}
            if owner == "direct":
                value["constraints"] = {"primary_key": ["id"]}
            self.registry.register(self.actor, "space_one", value, generation=self.actor.generation)
        self.builds, self.verifier = Builds(), Artifacts()
        self.environments = Environments(self.registry, self.builds, "approved", {
            "source_location": "test-bucket/artifacts/builders/approved.zip",
            "source_version": "source-version"}, self.verifier)
        self.environments.create(self.actor, "space_one", specification())
        self.environments.build(self.actor, "space_one", "analysis", "v1", "original")
        self.builds.status = "SUCCEEDED"
        self.environments.get(self.actor, "space_one", "analysis", "v1")
        self.objects = Objects()
        self.artifacts = PipelineArtifacts(self.objects, "test-bucket")
        self.pipelines = Pipelines(self.registry, self.artifacts, self.environments, writer_source="a" * 40)
        self.code = b"def transform(context):\n    return context\n"
        self.request = {"definition": definition(), "artifacts": [{
            "path": "python/transform.py", "upload_id": "pending",
            "sha256": hashlib.sha256(self.code).hexdigest(), "bytes": len(self.code)}]}
        self.created = self.pipelines.create(self.actor, "space_one", self.request)

    def http(self, method, suffix="", body=None, headers=None, actor=None):
        actor = actor or self.actor
        return handle({
            "rawPath": "/v1/spaces/space_one/pipelines" + suffix,
            "requestContext": {"requestId": "synthetic", "http": {"method": method},
                "authorizer": {"lambda": {"principal_id": actor.principal_id,
                    "spaces": json.dumps(list(actor.spaces)), "scopes": json.dumps(list(actor.scopes)),
                    "generation": actor.generation}}},
            "headers": {"content-type": "application/json", **(headers or {})},
            "body": canonical(body) if body is not None else None,
        }, self.registry, pipelines=self.pipelines)

    def ready_draft(self, code=None):
        code = self.code if code is None else code
        declaration = {"path": "python/transform.py", "bytes": len(code),
                       "sha256": hashlib.sha256(code).hexdigest()}
        receipt = self.pipelines.upload(self.actor, "space_one", "demo", declaration)
        key = self.objects.signed["Params"]["Key"]
        self.objects.put_object(Key=key, Body=code, ChecksumSHA256=checksum(declaration["sha256"]))
        request = {"definition": definition(), "artifacts": [{
            **declaration, "upload_id": receipt["upload_id"]}]}
        current = self.pipelines.get(self.actor, "space_one", "demo")
        self.request = request
        return self.pipelines.replace_draft(
            self.actor, "space_one", "demo", request, '"' + current["revision"] + '"')

    def publish(self, draft=None, nonce="original"):
        draft = draft or self.ready_draft()
        return self.pipelines.publish(self.actor, "space_one", "demo",
                                      {"draft_revision": draft["revision"]}, nonce)

    def test_publish_activate_and_schedule_bind_complete_original_package(self):
        draft = self.ready_draft()
        validated = self.http("POST", "/demo/validate", {"draft_revision": draft["revision"]})
        self.assertEqual(json.loads(validated["body"]), {"valid": True, "diagnostics": []})
        published = self.publish(draft)
        version = self.pipelines.version(self.actor, "space_one", "demo", published["pipeline_version"])
        self.assertEqual(version["environment_versions"]["transform"],
                         {"environment_id": "analysis", "version": "v1"})
        self.assertEqual(version["code_hashes"]["python/transform.py"], hashlib.sha256(self.code).hexdigest())
        key = version["package_uri"].split("test-bucket/", 1)[1]
        with zipfile.ZipFile(io.BytesIO(self.objects.objects[key]["Body"])) as package:
            self.assertEqual(package.read("python/transform.py"), self.code)
            manifest = json.loads(package.read(".colvenna/manifest.json"))
            self.assertEqual(manifest["environments"]["transform"]["executor_binding"]["executor"], "ecs_task")
            self.assertEqual(manifest["definition"], definition())
        activated = self.http("POST", "/demo/activate", published | {}, {
            "if-match": '"' + draft["revision"] + '"'})
        self.assertEqual(activated["statusCode"], 400)  # Exact request shape, no receipt fields.
        activated = self.http("POST", "/demo/activate", {
            "pipeline_version": published["pipeline_version"]}, {"if-match": '"' + draft["revision"] + '"'})
        self.assertEqual(activated["statusCode"], 200)
        current = json.loads(activated["body"])
        scheduled = self.http("PUT", "/demo/schedule", {
            "owner": "external", "state": "ENABLED"}, {"if-match": '"' + current["revision"] + '"'})
        self.assertEqual(scheduled["statusCode"], 200)
        current = self.pipelines.get(self.actor, "space_one", "demo")
        self.assertEqual((current["status"], current["schedule_owner"]), ("ENABLED", "external"))
        self.assertEqual(current["draft_revision"], draft["revision"])
        paused = self.http("PUT", "/demo/schedule", {"owner": "none", "state": "PAUSED"},
                           {"if-match": '"' + current["revision"] + '"'})
        self.assertEqual(paused["statusCode"], 200)

    def test_concurrent_draft_and_activation_have_one_revision_winner(self):
        draft = self.ready_draft()
        published = self.publish(draft)
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: self.http("POST", "/demo/activate", {
                "pipeline_version": published["pipeline_version"]}, {
                "if-match": '"' + draft["revision"] + '"'}), range(4)))
        self.assertEqual(sorted(value["statusCode"] for value in results), [200, 412, 412, 412])
        current = self.pipelines.get(self.actor, "space_one", "demo")
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: self.http("PUT", "/demo/draft", self.request, {
                "if-match": '"' + current["revision"] + '"'}), range(4)))
        self.assertEqual(sorted(value["statusCode"] for value in results), [200, 412, 412, 412])

    def test_publication_retry_after_lost_package_and_database_replies_keeps_identity(self):
        draft = self.ready_draft()
        self.objects.lose_package_reply = True
        self.assertEqual(self.http("POST", "/demo/versions", {"draft_revision": draft["revision"]},
                                  {"idempotency-key": "original"})["statusCode"], 503)
        self.table.lose_publication_reply = True
        self.assertEqual(self.http("POST", "/demo/versions", {"draft_revision": draft["revision"]},
                                  {"idempotency-key": "original"})["statusCode"], 503)
        original = self.publish(draft)
        self.assertEqual(original, self.publish(draft))
        self.assertEqual(sum(key.endswith("package.zip") for key in self.objects.objects), 1)
        changed = self.ready_draft(b"def transform(context):\n    return None\n")
        self.assertNotEqual(changed["revision"], draft["revision"])
        self.assertEqual(self.publish(draft), original)
        with self.assertRaises(ControlError) as error:
            self.publish(changed)
        self.assertEqual(error.exception.code, "IDEMPOTENCY_KEY_CONFLICT")

    def test_concurrent_same_publication_exposes_one_version(self):
        draft = self.ready_draft()
        with ThreadPoolExecutor(max_workers=4) as pool:
            values = list(pool.map(lambda _: self.publish(draft), range(4)))
        self.assertEqual(len({value["pipeline_version"] for value in values}), 1)
        self.assertEqual(sum(key.endswith("package.zip") for key in self.objects.objects), 1)

    def test_invalid_dag_paths_modes_and_aws_overrides_are_rejected(self):
        for change in [
            lambda d: d["tasks"][0].update(depends_on=["transform"]),
            lambda d: d["tasks"][0].update(depends_on=["missing"]),
            lambda d: d["tasks"][0].update(entrypoint="../escape.py"),
            lambda d: d["tasks"][0]["processing"].update(mode="incremental"),
            lambda d: d["tasks"][0].update(function_arn="arbitrary"),
            lambda d: d["tasks"][0].update(resource_profile="sql_ecs_medium"),
            lambda d: d["tasks"][0].update(runtime_environment="unfixed-name"),
        ]:
            request = copy.deepcopy(self.request)
            change(request["definition"])
            self.assertEqual(self.http("PUT", "/demo/draft", request, {
                "if-match": '"' + self.created["revision"] + '"'})["statusCode"], 400)

    def test_nonready_environment_missing_upload_and_wrong_content_reject_publication(self):
        draft = self.ready_draft()
        self.environments.create(self.actor, "space_one", specification("v2"))
        self.request["definition"]["tasks"][0]["runtime_environment"]["version"] = "v2"
        replacement = self.pipelines.replace_draft(
            self.actor, "space_one", "demo", self.request, '"' + draft["revision"] + '"')
        result = self.pipelines.validate(self.actor, "space_one", "demo", {
            "draft_revision": replacement["revision"]})
        self.assertEqual(result["diagnostics"][0]["code"], "ENVIRONMENT_NOT_READY")
        with self.assertRaises(ControlError):
            self.publish(replacement)
        draft = self.ready_draft()
        key = next(key for key in reversed(self.objects.objects) if "/uploads/" in key)
        self.objects.objects[key]["Body"] = b"x" * len(self.code)
        with self.assertRaises(ControlError) as error:
            self.publish(draft)
        self.assertEqual(error.exception.code, "ARTIFACT_CONTENT_MISMATCH")
        self.assertFalse(any(key.endswith("package.zip") for key in self.objects.objects))

    def test_registered_target_schema_owner_and_inputs_are_checked_independently(self):
        for change, code in [
            (lambda d: d["tasks"][0]["inputs"]["source"].update(dataset="ods.missing"),
             "PIPELINE_DATASET_NOT_REGISTERED"),
            (lambda d: d["tasks"][0]["output"]["schema"][0].update(type="VARCHAR"),
             "PIPELINE_TARGET_SCHEMA_OR_OWNER_MISMATCH"),
        ]:
            draft = self.ready_draft()
            change(self.request["definition"])
            draft = self.pipelines.replace_draft(
                self.actor, "space_one", "demo", self.request, '"' + draft["revision"] + '"')
            with self.assertRaises(ControlError) as error:
                self.publish(draft)
            self.assertEqual(error.exception.code, code)

    def test_run_scope_is_separate_and_existing_code_environment_do_not_follow_edits(self):
        draft = self.ready_draft()
        published = self.publish(draft)
        runner = Principal("runner", self.actor.spaces, frozenset(["pipeline:run"]),
                           generation=self.actor.generation)
        original = self.pipelines.bind_run(runner, "space_one", "demo", published["pipeline_version"])
        self.ready_draft(b"def transform(context):\n    return None\n")
        self.assertEqual(self.pipelines.bind_run(runner, "space_one", "demo", published["pipeline_version"]), original)
        self.assertEqual(self.http("GET", "/demo", actor=runner)["statusCode"], 403)
        manager = Principal("manager", self.actor.spaces, frozenset(["pipeline:manage"]),
                            generation=self.actor.generation)
        with self.assertRaises(ControlError):
            self.pipelines.bind_run(manager, "space_one", "demo", published["pipeline_version"])
        self.assertEqual(self.pipelines.version(manager, "space_one", "demo",
                                               published["pipeline_version"])["pipeline_version"],
                         published["pipeline_version"])
        foreign = Principal("foreign", frozenset(["other"]), self.actor.scopes)
        self.assertEqual(self.http("GET", "/demo", actor=foreign)["statusCode"], 403)

    def test_breaking_epoch_and_generation_invalidate_original_candidates(self):
        published = self.publish()
        changed = Pipelines(self.registry, self.artifacts, self.environments,
                            epoch="b" * 64, writer_source="a" * 40)
        with self.assertRaises(ControlError) as error:
            changed.activate(self.actor, "space_one", "demo",
                             {"pipeline_version": published["pipeline_version"]},
                             '"' + self.pipelines.get(self.actor, "space_one", "demo")["revision"] + '"')
        self.assertEqual(error.exception.code, "PIPELINE_CONTRACT_CHANGED")
        with self.assertRaises(ControlError):
            changed.bind_run(Principal("runner", self.actor.spaces, frozenset(["pipeline:run"]),
                                      generation=self.actor.generation),
                             "space_one", "demo", published["pipeline_version"])
        current = self.registry.configuration(self.actor, "space_one")
        self.registry.rebuild("space_one", current)
        with self.assertRaises(ControlError) as error:
            self.pipelines.version(self.actor, "space_one", "demo", published["pipeline_version"])
        self.assertEqual(error.exception.code, "SPACE_GENERATION_CHANGED")

    def test_rebuild_between_package_and_final_record_cannot_publish_to_new_generation(self):
        draft = self.ready_draft()
        self.objects.after_package = lambda: self.registry.rebuild(
            "space_one", self.registry.configuration(self.actor, "space_one"))
        with self.assertRaises(ControlError):
            self.publish(draft)
        self.assertFalse(any(key[1].startswith("PIPELINE_VERSION#") for key in self.table.items))

    def test_missing_headers_unuploaded_artifact_and_invalid_python_fail_closed(self):
        self.assertEqual(self.http("PUT", "/demo/draft", self.request)["statusCode"], 428)
        self.assertEqual(self.http("POST", "/demo/versions",
                                  {"draft_revision": self.created["revision"]})["statusCode"], 428)
        with self.assertRaises(ControlError):
            self.publish(self.created)
        draft = self.ready_draft(b"def invalid(\n")
        with self.assertRaises(ControlError) as error:
            self.publish(draft)
        self.assertEqual(error.exception.code, "ARTIFACT_SOURCE_INVALID")
        collision = copy.deepcopy(self.request)
        collision["artifacts"].append({**collision["artifacts"][0], "path": "python"})
        current = self.pipelines.get(self.actor, "space_one", "demo")
        self.assertEqual(self.http("PUT", "/demo/draft", collision,
                                  {"if-match": '"' + current["revision"] + '"'})["statusCode"], 400)

    def test_upload_is_bound_to_checksum_length_path_and_single_object(self):
        declaration = {name: self.request["artifacts"][0][name] for name in ["path", "sha256", "bytes"]}
        uploaded = self.pipelines.upload(self.actor, "space_one", "demo", declaration)
        self.assertEqual(uploaded["upload_headers"]["if-none-match"], "*")
        self.assertEqual(uploaded["upload_headers"]["x-amz-checksum-sha256"], checksum(declaration["sha256"]))
        self.assertEqual(self.objects.signed["Params"]["ContentLength"], len(self.code))
        self.assertIn(self.actor.generation, self.objects.signed["Params"]["Key"])
        for path in ["../escape.py", "/absolute.py", ".colvenna/manifest.json", "a/./b.py"]:
            with self.assertRaises((ControlError, ValueError)):
                self.pipelines.upload(self.actor, "space_one", "demo", {**declaration, "path": path})

    def test_actual_sigv4_presigner_binds_every_upload_restriction(self):
        # Local cryptographic signing with synthetic credentials, without AWS calls.
        factory = boto3.client
        def synthetic_client(service, **options):
            return factory(service, **{"region_name": "us-west-2", **options},
                           aws_access_key_id="synthetic-access", aws_secret_access_key="synthetic-secret")
        clients.cache_clear()
        with patch("colvenna_isle.control.handler.boto3.client", side_effect=synthetic_client):
            _, s3, _ = clients()
        clients.cache_clear()
        declaration = {name: self.request["artifacts"][0][name] for name in ["path", "sha256", "bytes"]}
        url, headers = PipelineArtifacts(s3, "test-bucket").upload({
            "declaration": declaration, "key": "spaces/space_one/pipeline-code/gen_test/demo/uploads/test"})
        query = parse_qs(urlparse(url).query)
        # Regional virtual host: a new bucket's global name answers 307 until its DNS propagates.
        self.assertEqual(urlparse(url).netloc, "test-bucket.s3.us-west-2.amazonaws.com")
        self.assertEqual(query["X-Amz-Algorithm"], ["AWS4-HMAC-SHA256"])
        signed = set(query["X-Amz-SignedHeaders"][0].split(";"))
        self.assertTrue(set(headers).issubset(signed))
        self.assertEqual(query["X-Amz-Expires"], ["300"])


if __name__ == "__main__":
    unittest.main()
