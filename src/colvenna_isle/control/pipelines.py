"""Revision-guarded definitions and original immutable publication custody."""

import copy
import hashlib
import json
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from botocore.exceptions import ClientError

from colvenna_isle.contracts.model import PROFILES, TASK_BUDGETS, schemas
from colvenna_isle.contracts import validation as contract_validation
from colvenna_isle.contracts.validation import ContractError, safe_relative, validate
from .audit import Audit, key_actor
from .registry import ControlError, canonical

MAX_RECORD_BYTES = 350000
DEFINITION_EPOCH = hashlib.sha256(canonical({
    "publication_protocol": 3, "schemas": schemas(), "profiles": PROFILES, "task_budgets": TASK_BUDGETS,
    "semantic_validator_sha256": hashlib.sha256(
        Path(contract_validation.__file__).read_bytes()).hexdigest(),
}).encode()).hexdigest()


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def revision():
    return "rev_" + uuid.uuid4().hex


# Human-readable validation feedback; messages contain no payload values.
MESSAGES = {
    "PIPELINE_DATASET_NOT_REGISTERED": "A task input or output dataset is not registered in this space.",
    "PIPELINE_TARGET_SCHEMA_OR_OWNER_MISMATCH":
        "A task output must be a pipeline-owned dataset whose schema, keys and partitions match its registration.",
    "ENVIRONMENT_NOT_READY": "The referenced runtime environment version is not READY.",
    "ENVIRONMENT_EXECUTOR_PROFILE_MISMATCH": "The environment does not provide this executor/profile binding.",
    "ENVIRONMENT_BUILD_PROFILE_CHANGED": "The environment was built under a different platform build policy.",
    "PIPELINE_UPLOAD_NOT_FOUND": "A draft artifact names an upload that does not exist.",
    "PIPELINE_UPLOAD_BINDING_MISMATCH": "A draft artifact's path, hash or size differs from its upload.",
    "ARTIFACT_UPLOAD_INCOMPLETE": "A draft artifact upload has no stored object yet.",
    "ARTIFACT_CONTENT_MISMATCH": "A stored artifact's bytes differ from the declared hash or size.",
    "ARTIFACT_SOURCE_INVALID": "A Python, SQL or YAML artifact could not be parsed.",
    "EMPTY_SQL_ARTIFACT": "A SQL artifact is empty.",
    "YAML_DEFINITION_MISMATCH": "The YAML artifact differs from the submitted definition.",
    "SPACE_PROFILE_QUOTA": "A task resource profile exceeds this space's quota.",
    "PIPELINE_WRITER_BINDING_UNAVAILABLE": "The writer executor binding is unavailable.",
}


def diagnostic(error):
    """One bounded diagnostic: a JSON pointer path, a stable code and a fixed explanation."""
    if isinstance(error, ContractError):
        text = str(error)
        path = text.split(":", 1)[0].split("/", 1)
        return {"path": "/definition" + ("/" + path[1] if len(path) == 2 else ""), "code": "INVALID_DEFINITION",
                "message": text[:1024]}
    return {"path": getattr(error, "path", "/definition"), "code": error.code,
            "message": MESSAGES.get(error.code, "Definition or immutable artifact binding rejected: " + error.code)}


class Pipelines:
    """Publication admission fixes one snapshot before any package is written.

    S3 and DynamoDB are not one transaction. A lost package or final-transaction
    reply is reconciled using the original nonce/version and immutable snapshot.
    Activation only references a fully stored and verified PUBLISHED version.
    """

    def __init__(self, registry, artifacts, environments, keys=None, *,
                 epoch=DEFINITION_EPOCH, writer_source=None, quotas=None):
        self.registry, self.artifacts, self.environments, self.keys = registry, artifacts, environments, keys
        self.quotas = quotas
        self.client, self.table = registry.client, registry.table
        self.epoch, self.writer_source = epoch, writer_source
        self.audit = Audit(self.client, self.table)

    def _audited(self, principal, configuration, action, pipeline, identity, target=None, **details):
        return self.audit.put(configuration["space_id"], configuration["generation"], action, key_actor(principal),
                              target or {"kind": "pipeline", "id": pipeline}, identity=identity, details=details)

    def _authorize(self, principal, space, scope="pipeline:manage"):
        principal.authorize(space, scope)
        configuration = self.registry._read(space)
        self.registry._authorize_generation(principal, configuration)
        if self.keys is not None and self.keys.authenticate_id(principal.key_id or "") != principal:
            raise ControlError(403, "FORBIDDEN")
        return configuration

    def _conditions(self, principal, configuration):
        if self.keys is not None:
            return self.keys.operation_conditions(
                principal, configuration["space_id"], configuration["generation"], "pipeline:manage")
        return [{"ConditionCheck": {
            "TableName": self.table, "Key": self.registry._key(configuration["space_id"]),
            "ConditionExpression": "generation = :generation",
            "ExpressionAttributeValues": {":generation": {"S": configuration["generation"]}},
        }}]

    def _key(self, configuration, kind, pipeline, identity=None):
        validate("identifier", pipeline)
        if identity is not None:
            validate("identifier", identity)
        sort = f"{kind}#{configuration['generation']}#{pipeline}"
        return {"PK": {"S": "SPACE#" + configuration["space_id"]},
                "SK": {"S": sort + ("#" + identity if identity else "")}}

    def _read(self, key, *, missing="PIPELINE_NOT_FOUND"):
        item = self.client.get_item(
            TableName=self.table, Key=key, ConsistentRead=True).get("Item")
        if not item:
            if missing is None:
                return None
            raise ControlError(404, missing)
        try:
            content = item["record"]["S"]
            value = json.loads(content)
            if (len(content.encode()) > MAX_RECORD_BYTES or type(value["record_revision"]) is not int
                    or value["record_revision"] < 1
                    or value["record_key"] != key
                    or item["revision"] != {"N": str(value["record_revision"])}
                    or value["integrity_sha256"] != digest({
                        name: member for name, member in value.items() if name != "integrity_sha256"})):
                raise ValueError()
            return value
        except (KeyError, TypeError, ValueError):
            raise ControlError(503, "PIPELINE_RECORD_UNAVAILABLE") from None

    def _put(self, key, value, previous=None):
        record = {**value, "record_key": key,
                  "record_revision": 1 if previous is None else previous["record_revision"] + 1}
        record.pop("integrity_sha256", None)
        record["integrity_sha256"] = digest(record)
        content = canonical(record)
        if len(content.encode()) > MAX_RECORD_BYTES:
            raise ControlError(413, "PIPELINE_RECORD_LIMIT")
        operation = {"TableName": self.table, "Item": {
            **key, "record": {"S": content}, "revision": {"N": str(record["record_revision"])}},
            "ConditionExpression": "attribute_not_exists(PK)"}
        if previous is not None:
            operation.update(
                ConditionExpression="#revision = :revision",
                ExpressionAttributeNames={"#revision": "revision"},
                ExpressionAttributeValues={":revision": {"N": str(previous["record_revision"])}})
        return {"Put": operation}

    def _condition_record(self, key, record):
        return {"ConditionCheck": {
            "TableName": self.table, "Key": key, "ConditionExpression": "#revision = :revision",
            "ExpressionAttributeNames": {"#revision": "revision"},
            "ExpressionAttributeValues": {":revision": {"N": str(record["record_revision"])}},
        }}

    def _transaction(self, operations):
        try:
            self.client.transact_write_items(TransactItems=operations, ClientRequestToken=uuid.uuid4().hex)
            return True
        except ClientError as error:
            if (error.response["Error"]["Code"] == "TransactionCanceledException"
                    and any(value.get("Code") == "ConditionalCheckFailed"
                            for value in error.response.get("CancellationReasons", []))):
                return False
            raise

    def _current(self, configuration, pipeline):
        record = self._read(self._key(configuration, "PIPELINE", pipeline))
        if record["definition_epoch"] != self.epoch:
            raise ControlError(409, "PIPELINE_CONTRACT_CHANGED")
        return record

    def _expected(self, record, expected):
        if not isinstance(expected, str) or not re.fullmatch(r'"rev_[0-9a-f]{32}"', expected):
            raise ControlError(428, "PIPELINE_REVISION_REQUIRED")
        if expected[1:-1] != record["revision"]:
            raise ControlError(412, "PIPELINE_REVISION_CHANGED")

    def _draft(self, pipeline, request):
        validate("draft_request", request)
        if request["definition"]["pipeline_id"] != pipeline:
            raise ControlError(400, "PIPELINE_ID_MISMATCH")
        for artifact in request["artifacts"]:
            if artifact["path"].split("/")[0] == ".colvenna":
                raise ControlError(422, "RESERVED_ARTIFACT_PATH")
        return copy.deepcopy(request)

    def create(self, principal, space, request):
        configuration = self._authorize(principal, space)
        validate("draft_request", request)
        pipeline = request["definition"]["pipeline_id"]
        draft = self._draft(pipeline, request)
        if self.quotas is not None and len(self._records(configuration, "PIPELINE#" + configuration["generation"] + "#")) \
                >= self.quotas.policy(space, configuration["generation"])["max_pipelines"]:
            raise ControlError(429, "SPACE_PIPELINE_QUOTA")
        current_revision = revision()
        record = {"revision": current_revision, "draft_revision": current_revision,
                  "draft": draft, "definition_epoch": self.epoch,
                  "status": "PAUSED", "active_version": None,
                  "schedule": {**draft["definition"]["schedule"], "state": "PAUSED"}}
        key = self._key(configuration, "PIPELINE", pipeline)
        if not self._transaction([self._put(key, record), *self._conditions(principal, configuration),
                                  self._audited(principal, configuration, "PIPELINE_CREATED", pipeline,
                                                pipeline + ":" + current_revision, draft_revision=current_revision,
                                                artifacts=len(draft["artifacts"]))]):
            self._authorize(principal, space)
            raise ControlError(409, "PIPELINE_ALREADY_EXISTS")
        return self._receipt(pipeline, current_revision)

    def _receipt(self, pipeline, draft_revision):
        return {"pipeline_id": pipeline, "revision": draft_revision, "status": "DRAFT"}

    def _metadata(self, pipeline, record):
        result = {"pipeline_id": pipeline, "revision": record["revision"],
                  "draft_revision": record["draft_revision"], "status": record["status"],
                  "active_version": record["active_version"],
                  "schedule_owner": record["schedule"]["owner"], "definition": record["draft"]["definition"],
                  "artifacts": copy.deepcopy(record["draft"]["artifacts"]),
                  "schedule": {name: value for name, value in record["schedule"].items()
                               if name in {"owner", "expression", "timezone", "state"}}}
        validate("pipeline_metadata", result)
        return result

    def _records(self, configuration, prefix, limit=2000):
        """Every record of one kind under the space partition (bounded, consistent)."""
        items, start = [], None
        while True:
            request = {"TableName": self.table, "ConsistentRead": True,
                       "KeyConditionExpression": "PK = :pk AND begins_with(SK, :prefix)",
                       "ExpressionAttributeValues": {":pk": {"S": "SPACE#" + configuration["space_id"]},
                                                     ":prefix": {"S": prefix}}}
            if start:
                request["ExclusiveStartKey"] = start
            page = self.client.query(**request)
            items += page.get("Items", [])
            start = page.get("LastEvaluatedKey")
            if len(items) > limit:
                raise ControlError(413, "PIPELINE_LIST_LIMIT")
            if not start:
                return items

    def list(self, principal, space):
        scope = next((name for name in ["pipeline:manage", "pipeline:run", "monitor:read"]
                      if name in principal.scopes), "pipeline:manage")
        configuration = self._authorize(principal, space, scope)
        pipelines = []
        for item in self._records(configuration, "PIPELINE#" + configuration["generation"] + "#", 512):
            pipeline = item["SK"]["S"].rsplit("#", 1)[1]
            record = self._read(self._key(configuration, "PIPELINE", pipeline))
            if record["definition_epoch"] != self.epoch:
                continue  # A changed contract invalidated this definition.
            definition = record["draft"]["definition"]
            pipelines.append({"pipeline_id": pipeline, "revision": record["revision"],
                              "draft_revision": record["draft_revision"], "status": record["status"],
                              "active_version": record["active_version"],
                              "schedule": {name: value for name, value in record["schedule"].items()
                                           if name in {"owner", "expression", "timezone", "state"}},
                              "name": definition["name"], "tasks": len(definition["tasks"])})
        result = {"pipelines": pipelines}
        validate("pipeline_list", result)
        return result

    def versions(self, principal, space, pipeline):
        scope = "pipeline:manage" if "pipeline:manage" in principal.scopes else "pipeline:run"
        configuration = self._authorize(principal, space, scope)
        record = self._current(configuration, pipeline)
        values = []
        for item in self._records(configuration, f"PIPELINE_VERSION#{configuration['generation']}#{pipeline}#"):
            version = item["SK"]["S"].rsplit("#", 1)[1]
            published = self._read(self._key(configuration, "PIPELINE_VERSION", pipeline, version))
            if published["definition_epoch"] != self.epoch or published.get("collected_unix"):
                continue
            snapshot = published["snapshot"]
            values.append({"pipeline_version": version, "draft_revision": snapshot["draft_revision"],
                           "active": record["active_version"] == version,
                           "created_by": published.get("created_by"),
                           "created_at": datetime.fromtimestamp(published["created_unix"], timezone.utc).isoformat(
                               timespec="seconds").replace("+00:00", "Z") if published.get("created_unix") else None,
                           "code_hashes": {entry["path"]: entry["sha256"] for entry in snapshot["artifacts"]}})
        result = {"versions": sorted(values, key=lambda value: (value["created_at"] or "", value["pipeline_version"]),
                                     reverse=True)}
        validate("version_list", result)
        return result

    def artifact(self, principal, space, pipeline, path):
        """Original draft source by path; only UTF-8 text up to 768 KiB is returned inline."""
        configuration = self._authorize(principal, space)
        record = self._current(configuration, pipeline)
        safe_relative(path)
        entry = next((value for value in record["draft"]["artifacts"] if value["path"] == path), None)
        if entry is None:
            raise ControlError(404, "PIPELINE_ARTIFACT_NOT_FOUND")
        upload = self._read(self._key(configuration, "UPLOAD", pipeline, entry["upload_id"]),
                            missing="PIPELINE_UPLOAD_NOT_FOUND")
        if upload.get("collected_unix"):
            raise ControlError(410, "PIPELINE_UPLOAD_COLLECTED")
        if upload["declaration"] != {name: entry[name] for name in ["path", "sha256", "bytes"]}:
            raise ControlError(422, "PIPELINE_UPLOAD_BINDING_MISMATCH")
        _, content = self.artifacts.verified_upload(upload)
        try:
            text = content.decode("utf-8") if len(content) <= 786432 else None
        except UnicodeError:
            text = None
        result = {"path": path, "upload_id": entry["upload_id"], "sha256": entry["sha256"], "bytes": entry["bytes"],
                  "encoding": "utf-8" if text is not None else "unavailable", "content": text}
        validate("artifact_content", result)
        return result

    def inline_upload(self, principal, space, pipeline, request):
        """Browser source upload through the same admission, quota and audit as presigned uploads."""
        validate("artifact_inline_request", request)
        content = request["content"].encode("utf-8")
        if hashlib.sha256(content).hexdigest() != request["sha256"]:
            raise ControlError(422, "ARTIFACT_CONTENT_MISMATCH")
        receipt = self._admit_upload(principal, space, pipeline, {
            "path": request["path"], "sha256": request["sha256"], "bytes": len(content)})
        self.artifacts.put_inline(receipt["record"], content)
        return {"upload_id": receipt["upload_id"], "path": request["path"], "sha256": request["sha256"],
                "bytes": len(content)}

    def get(self, principal, space, pipeline):
        configuration = self._authorize(principal, space)
        return self._metadata(pipeline, self._current(configuration, pipeline))

    def replace_draft(self, principal, space, pipeline, request, expected):
        configuration = self._authorize(principal, space)
        record = self._current(configuration, pipeline)
        self._expected(record, expected)
        draft = self._draft(pipeline, request)
        new_revision = revision()
        updated = {**record, "revision": new_revision, "draft_revision": new_revision, "draft": draft}
        if not self._transaction([
                self._put(self._key(configuration, "PIPELINE", pipeline), updated, record),
                *self._conditions(principal, configuration),
                self._audited(principal, configuration, "PIPELINE_DRAFT_REPLACED", pipeline,
                              pipeline + ":" + new_revision, draft_revision=new_revision,
                              previous_revision=record["draft_revision"], artifacts=len(draft["artifacts"]))]):
            self._authorize(principal, space)
            raise ControlError(412, "PIPELINE_REVISION_CHANGED")
        return self._receipt(pipeline, new_revision)

    def upload(self, principal, space, pipeline, request):
        receipt = self._admit_upload(principal, space, pipeline, request)
        record, identity = receipt["record"], receipt["upload_id"]
        url, headers = self.artifacts.upload(record)
        return {"upload_id": identity, "upload_url": url, "upload_headers": headers,
                "expires_at": datetime.fromtimestamp(record["expires_unix"], timezone.utc).isoformat().replace("+00:00", "Z")}

    def _admit_upload(self, principal, space, pipeline, request):
        configuration = self._authorize(principal, space)
        self._current(configuration, pipeline)
        validate("upload_request", request)
        safe_relative(request["path"])
        if request["path"].split("/")[0] == ".colvenna":
            raise ControlError(422, "RESERVED_ARTIFACT_PATH")
        identity = "upload_" + uuid.uuid4().hex
        record = {"declaration": request, "definition_epoch": self.epoch,
                  "created_by": principal.principal_id,
                  "key": self.artifacts.upload_key(space, configuration["generation"], pipeline, identity),
                  "expires_unix": time.time() + 300}
        operations = [
            self._put(self._key(configuration, "UPLOAD", pipeline, identity), record),
            *self._conditions(principal, configuration),
            self._audited(principal, configuration, "PIPELINE_ARTIFACT_ADMITTED", pipeline, identity,
                          {"kind": "pipeline_artifact", "id": identity, "pipeline_id": pipeline},
                          path=request["path"], sha256=request["sha256"], bytes=request["bytes"])]
        policy = self.quotas.policy(space, configuration["generation"]) if self.quotas is not None else None
        if policy is not None:
            # Stored code bytes are reserved at admission and released only by retention.
            operations.append(self.quotas.reserve_code(space, configuration["generation"], policy, request["bytes"]))
        if not self._transaction(operations):
            self._authorize(principal, space)
            if policy is not None and self.quotas.code_bytes(space, configuration["generation"]) \
                    + request["bytes"] > policy["max_stored_code_bytes"]:
                raise ControlError(429, "SPACE_ARTIFACT_QUOTA")
            raise ControlError(409, "PIPELINE_UPLOAD_NOT_ADMITTED")
        return {"upload_id": identity, "record": record}

    def _bindings(self, principal, configuration, definition, scope="pipeline:manage"):
        validate("pipeline", definition)
        datasets, environments, executors = {}, {}, {}
        policy = (self.quotas.policy(configuration["space_id"], configuration["generation"])
                  if self.quotas is not None and scope == "pipeline:manage" else None)
        for index, task in enumerate(definition["tasks"]):
            try:
                self._task_binding(principal, configuration, task, policy, datasets, environments, executors, scope)
            except ControlError as error:
                error.path = f"/definition/tasks/{index}"
                raise
        return {"datasets": datasets, "environments": environments, "executors": executors}

    def _task_binding(self, principal, configuration, task, policy, datasets, environments, executors, scope):
        if policy is not None:
            self.quotas.check_profile(policy, task["resource_profile"])
        output = task["output"]
        names = [output["dataset"]] + [binding["dataset"] for binding in task["inputs"].values()
                                      if "dataset" in binding]
        for name in names:
            registered = configuration["datasets"].get(name)
            if registered is None:
                raise ControlError(422, "PIPELINE_DATASET_NOT_REGISTERED")
            datasets[name] = registered["definition_sha256"]
        target = configuration["datasets"][output["dataset"]]["definition"]
        if (target.get("write_owner") != "pipeline" or target["schema"] != output["schema"]
                or target["key_columns"] != output["key_columns"]
                or target["partition_columns"] != output.get("partition_columns", [])):
            raise ControlError(422, "PIPELINE_TARGET_SCHEMA_OR_OWNER_MISMATCH")
        if task["executor"] == "writer":
            if not isinstance(self.writer_source, str) or not re.fullmatch(r"[0-9a-f]{40}", self.writer_source):
                raise ControlError(503, "PIPELINE_WRITER_BINDING_UNAVAILABLE")
            executors[task["task_id"]] = {"executor": "writer", "profile": task["resource_profile"],
                                         "source_commit": self.writer_source}
        else:
            if self.environments is None:
                raise ControlError(503, "ENVIRONMENT_SERVICE_UNAVAILABLE")
            binding = self.environments.pipeline_binding(
                principal, configuration["space_id"], task["runtime_environment"],
                task["executor"], task["resource_profile"], scope)
            environments[task["task_id"]] = binding
            executors[task["task_id"]] = binding["executor_binding"]

    def _snapshot(self, principal, configuration, pipeline, record, version):
        definition = record["draft"]["definition"]
        bindings = self._bindings(principal, configuration, definition)
        artifacts, files = [], {}
        for artifact in record["draft"]["artifacts"]:
            upload = self._read(self._key(configuration, "UPLOAD", pipeline, artifact["upload_id"]),
                                missing="PIPELINE_UPLOAD_NOT_FOUND")
            if upload.get("collected_unix"):
                raise ControlError(410, "PIPELINE_UPLOAD_COLLECTED")
            if (upload["declaration"] != {name: artifact[name] for name in ["path", "sha256", "bytes"]}
                    or upload["definition_epoch"] != self.epoch
                    or upload["key"] != self.artifacts.upload_key(
                        configuration["space_id"], configuration["generation"], pipeline, artifact["upload_id"])):
                raise ControlError(422, "PIPELINE_UPLOAD_BINDING_MISMATCH")
            reference, content = self.artifacts.verified_upload(upload)
            artifacts.append(reference)
            files[artifact["path"]] = content
        self.artifacts.verify_code(definition, files)
        return {"space_id": configuration["space_id"], "generation": configuration["generation"],
                "definition_epoch": self.epoch, "pipeline_id": pipeline, "pipeline_version": version,
                "definition": definition, "draft_revision": record["draft_revision"],
                "artifacts": sorted(artifacts, key=lambda entry: entry["path"]), **bindings}

    def validate(self, principal, space, pipeline, request):
        configuration = self._authorize(principal, space)
        validate("validate_request", request)
        record = self._current(configuration, pipeline)
        if request["draft_revision"] != record["draft_revision"]:
            raise ControlError(412, "PIPELINE_DRAFT_CHANGED")
        try:
            self._snapshot(principal, configuration, pipeline, record, "validation")
        except (ControlError, ContractError) as error:
            return {"valid": False, "diagnostics": [diagnostic(error)]}
        return {"valid": True, "diagnostics": []}

    def publish(self, principal, space, pipeline, request, nonce):
        configuration = self._authorize(principal, space)
        validate("version_request", request)
        if not isinstance(nonce, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", nonce):
            raise ControlError(428, "IDEMPOTENCY_KEY_REQUIRED")
        identity = "pv_" + digest({"actor": principal.principal_id, "nonce": nonce,
                                  "space": space, "generation": configuration["generation"],
                                  "pipeline": pipeline, "epoch": self.epoch})
        intent_key = self._key(configuration, "PIPELINE_PUBLICATION", pipeline, identity)
        for _ in range(8):
            original = self._read(intent_key, missing=None)
            if original is not None:
                if original["request"] != request:
                    raise ControlError(409, "IDEMPOTENCY_KEY_CONFLICT")
                break
            record = self._current(configuration, pipeline)
            if record["draft_revision"] != request["draft_revision"]:
                raise ControlError(412, "PIPELINE_DRAFT_CHANGED")
            if self.quotas is not None and len(self._records(
                    configuration, f"PIPELINE_VERSION#{configuration['generation']}#{pipeline}#")) >= \
                    self.quotas.policy(space, configuration["generation"])["max_pipeline_versions"]:
                raise ControlError(429, "SPACE_PIPELINE_VERSION_QUOTA")
            snapshot = self._snapshot(principal, configuration, pipeline, record, identity)
            intent = {"request": request, "snapshot": snapshot, "phase": "ADMITTED",
                      "snapshot_sha256": digest(snapshot), "created_by": principal.principal_id}
            if self._transaction([
                    self._put(intent_key, intent),
                    self._condition_record(self._key(configuration, "PIPELINE", pipeline), record),
                    *self._conditions(principal, configuration)]):
                original = self._read(intent_key)
                break
            configuration = self._authorize(principal, space)
        else:
            raise ControlError(503, "PIPELINE_PUBLICATION_BUSY")
        snapshot = original["snapshot"]
        if (snapshot["definition_epoch"] != self.epoch
                or snapshot["generation"] != configuration["generation"]
                or snapshot["pipeline_version"] != identity or original["snapshot_sha256"] != digest(snapshot)):
            raise ControlError(409, "PIPELINE_PUBLICATION_INVALIDATED")
        version_key = self._key(configuration, "PIPELINE_VERSION", pipeline, identity)
        if original["phase"] == "PUBLISHED":
            published = self._read(version_key)
            self._verify_published(principal, configuration, published)
            return self._version_receipt(pipeline, identity)
        package = self.artifacts.package(snapshot)
        # Recheck live identity after S3 IO, then atomically expose the complete version.
        configuration = self._authorize(principal, space)
        if configuration["generation"] != snapshot["generation"]:
            raise ControlError(409, "PIPELINE_PUBLICATION_INVALIDATED")
        published = {"snapshot": snapshot, "snapshot_sha256": digest(snapshot), "package": package,
                     "definition_epoch": self.epoch, "created_by": original["created_by"],
                     "created_unix": time.time()}
        for _ in range(8):
            if configuration["generation"] != snapshot["generation"]:
                raise ControlError(409, "PIPELINE_PUBLICATION_INVALIDATED")
            original = self._read(intent_key)
            if original["phase"] == "PUBLISHED":
                self._verify_published(principal, configuration, self._read(version_key))
                return self._version_receipt(pipeline, identity)
            if self._transaction([
                    self._put(version_key, published),
                    self._put(intent_key, {**original, "phase": "PUBLISHED"}, original),
                    *self._conditions(principal, configuration),
                    self._audited(principal, configuration, "PIPELINE_VERSION_PUBLISHED", pipeline, identity,
                                  {"kind": "pipeline_version", "id": identity, "pipeline_id": pipeline},
                                  draft_revision=snapshot["draft_revision"],
                                  package_sha256=package["package_sha256"])]):
                return self._version_receipt(pipeline, identity)
            configuration = self._authorize(principal, space)
        raise ControlError(503, "PIPELINE_PUBLICATION_BUSY")

    def _version_receipt(self, pipeline, version):
        return {"pipeline_id": pipeline, "pipeline_version": version, "status": "PUBLISHED"}

    def _verify_published(self, principal, configuration, published, scope="pipeline:manage"):
        if published.get("collected_unix"):
            # Retention collected this unused version's package; it cannot run or activate again.
            raise ControlError(410, "PIPELINE_VERSION_COLLECTED")
        self._verify_binding(principal, configuration, published, scope)
        self.artifacts.verify_package(published["snapshot"], published["package"])

    def _verify_binding(self, principal, configuration, published, scope):
        snapshot = published["snapshot"]
        if (published["definition_epoch"] != self.epoch or snapshot["definition_epoch"] != self.epoch
                or snapshot["space_id"] != configuration["space_id"]
                or snapshot["generation"] != configuration["generation"]
                or published["snapshot_sha256"] != digest(snapshot)):
            raise ControlError(409, "PIPELINE_VERSION_INVALIDATED")
        if self._bindings(principal, configuration, snapshot["definition"], scope) != {
                name: snapshot[name] for name in ["datasets", "environments", "executors"]}:
            raise ControlError(409, "PIPELINE_VERSION_BINDING_CHANGED")

    def version(self, principal, space, pipeline, version):
        configuration = self._authorize(principal, space)
        record = self._read(self._key(configuration, "PIPELINE_VERSION", pipeline, version),
                            missing="PIPELINE_VERSION_NOT_FOUND")
        self._verify_published(principal, configuration, record)
        snapshot = record["snapshot"]
        result = {"pipeline_id": pipeline, "pipeline_version": version,
                  "definition": snapshot["definition"],
                  **{name: record["package"][name] for name in [
                      "package_uri", "package_object_version", "package_sha256"]},
                  "environment_versions": {task: value["reference"]
                                           for task, value in snapshot["environments"].items()},
                  "definition_epoch": self.epoch,
                  "code_hashes": {entry["path"]: entry["sha256"] for entry in snapshot["artifacts"]}}
        validate("version_metadata", result)
        return result

    def activate(self, principal, space, pipeline, request, expected):
        configuration = self._authorize(principal, space)
        validate("activate_request", request)
        record = self._current(configuration, pipeline)
        self._expected(record, expected)
        published = self._read(self._key(
            configuration, "PIPELINE_VERSION", pipeline, request["pipeline_version"]),
            missing="PIPELINE_VERSION_NOT_FOUND")
        self._verify_published(principal, configuration, published)
        updated = {**record, "revision": revision(), "active_version": request["pipeline_version"]}
        if not self._transaction([
                self._put(self._key(configuration, "PIPELINE", pipeline), updated, record),
                *self._conditions(principal, configuration),
                self._audited(principal, configuration, "PIPELINE_ACTIVATED", pipeline,
                              pipeline + ":" + updated["revision"], pipeline_version=request["pipeline_version"],
                              previous_version=record["active_version"])]):
            self._authorize(principal, space)
            raise ControlError(412, "PIPELINE_REVISION_CHANGED")
        return self._metadata(pipeline, updated)

    def schedule(self, principal, space, pipeline, request, expected):
        configuration = self._authorize(principal, space)
        validate("schedule_request", request)
        record = self._current(configuration, pipeline)
        self._expected(record, expected)
        if request["state"] == "ENABLED":
            if record["active_version"] is None:
                raise ControlError(409, "PIPELINE_ACTIVE_VERSION_REQUIRED")
            published = self._read(self._key(
                configuration, "PIPELINE_VERSION", pipeline, record["active_version"]))
            self._verify_published(principal, configuration, published)
        updated = {**record, "revision": revision(), "schedule": request, "status": request["state"]}
        # The scheduler only enumerates colvenna-owned ENABLED pipelines. Its runs
        # act as the enabling principal, who must also hold pipeline:run.
        index = {"PK": {"S": "PIPELINESCHEDULES#1"},
                 "SK": {"S": "#".join([space, configuration["generation"], pipeline])}}
        if request["state"] == "ENABLED" and request["owner"] == "colvenna":
            if "pipeline:run" not in principal.scopes or not principal.key_id:
                raise ControlError(403, "SCHEDULE_RUN_SCOPE_REQUIRED")
            scheduled = {"Put": {"TableName": self.table, "Item": {
                **index, "run_as_key_id": {"S": principal.key_id},
                "run_as_principal_id": {"S": principal.principal_id},
                "schedule_revision": {"S": updated["revision"]}}}}
        else:
            scheduled = {"Delete": {"TableName": self.table, "Key": index}}
        if not self._transaction([
                self._put(self._key(configuration, "PIPELINE", pipeline), updated, record), scheduled,
                *self._conditions(principal, configuration),
                self._audited(principal, configuration, "PIPELINE_SCHEDULE_CHANGED", pipeline,
                              pipeline + ":" + updated["revision"], owner=request["owner"], state=request["state"],
                              expression=request.get("expression"))]):
            self._authorize(principal, space)
            raise ControlError(412, "PIPELINE_REVISION_CHANGED")
        return {"revision": updated["revision"], "schedule": copy.deepcopy(request)}

    def bind_run(self, principal, space, pipeline, version):
        """Scheduler/task seam: fixed version only, separately authorized for run."""
        configuration = self._authorize(principal, space, "pipeline:run")
        record = self._read(self._key(configuration, "PIPELINE_VERSION", pipeline, version),
                            missing="PIPELINE_VERSION_NOT_FOUND")
        self._verify_published(principal, configuration, record, "pipeline:run")
        return copy.deepcopy({"snapshot": record["snapshot"], "package": record["package"]})

    def confirm_run_binding(self, principal, space, pipeline, version, original):
        """Fresh authority/policy fence during descriptor IO, without re-fetching code."""
        configuration = self._authorize(principal, space, "pipeline:run")
        record = self._read(self._key(configuration, "PIPELINE_VERSION", pipeline, version),
                            missing="PIPELINE_VERSION_NOT_FOUND")
        self._verify_binding(principal, configuration, record, "pipeline:run")
        if original != {"snapshot": record["snapshot"], "package": record["package"]}:
            raise ControlError(409, "TASK_DEFINITION_BINDING_CHANGED")
