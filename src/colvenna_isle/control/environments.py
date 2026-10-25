"""Immutable environment versions and original CodeBuild admission custody."""

import copy
import hashlib
import json
import logging
import re
import time
import uuid

from botocore.exceptions import BotoCoreError, ClientError

from colvenna_isle.contracts.validation import validate
from .audit import Audit, key_actor
from .registry import ControlError, canonical

MAX_VERSIONS = 64
MAX_SPACE_BUILDS = 2
MAX_GLOBAL_BUILDS = 4
MAX_CATALOG_BYTES = 350000
_NAME = r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}"
_VERSION = r"[A-Za-z0-9][A-Za-z0-9.*+!_-]{0,127}"
_REQUIREMENT = re.compile(
    rf"({_NAME})(?:\[({_NAME}(?:,{_NAME})*)\])?"
    rf"((?:===|==|~=|!=|<=|>=|<|>){_VERSION}"
    rf"(?:,(?:===|==|~=|!=|<=|>=|<|>){_VERSION})*)?")
_NONCE = re.compile(r"[A-Za-z0-9_.:-]{1,128}")


def dependencies(content):
    """Approved index requirements only; no URL, credential, file or pip option."""
    result = []
    for raw in content.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        # Keep a deliberately bounded, documented subset of requirements.in.
        line = re.sub(r"\s*(===|==|~=|!=|<=|>=|<|>|,)\s*", r"\1", line)
        if not _REQUIREMENT.fullmatch(line):
            raise ControlError(422, "DEPENDENCY_SOURCE_NOT_APPROVED")
        result.append(line)
    if not result or len(result) > 128:
        raise ControlError(422, "DEPENDENCY_LIMIT")
    return result


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def sanitize(message, limit=1024):
    """Printable diagnostic text without URL credentials or AWS access key IDs."""
    if not isinstance(message, str):
        return None
    text = re.sub(r"(://)[^/@\s]+@", r"\1<redacted>@", message)
    text = re.sub(r"\b(AKIA|ASIA)[A-Z0-9]{16}\b", "<redacted>", text)
    text = "".join(character if character.isprintable() or character == "\n" else " " for character in text)
    return text[-limit:] or None


class Environments:
    """One bounded catalogue per generation; capacity precedes build dispatch.

    A lost StartBuild reply leaves the original admission UNKNOWN. A subsequent
    HTTP retry reads that admission and never starts a replacement build.
    """

    def __init__(self, registry, codebuild, project, policy, verifier, keys=None, quotas=None):
        self.registry, self.client, self.table = registry, registry.client, registry.table
        self.codebuild, self.project = codebuild, project
        self.policy, self.verifier, self.keys = copy.deepcopy(policy), verifier, keys
        self.quotas, self.audit = quotas, Audit(registry.client, registry.table)

    def _key(self, partition):
        return {"PK": {"S": partition}, "SK": {"S": "META"}}

    def _read(self, partition, field):
        item = self.client.get_item(
            TableName=self.table, Key=self._key(partition), ConsistentRead=True).get("Item")
        if not item:
            return {"revision": 0, field: {} if field == "versions" else []}
        try:
            if len(item["record"]["S"].encode()) > MAX_CATALOG_BYTES:
                raise ValueError()
            value = json.loads(item["record"]["S"])
            if (type(value["revision"]) is not int
                    or item["revision"] != {"N": str(value["revision"])} or value["revision"] < 1):
                raise ValueError()
            if not isinstance(value[field], dict if field == "versions" else list):
                raise ValueError()
            if field == "versions":
                if len(value[field]) > MAX_VERSIONS:
                    raise ValueError()
                for identity, record in value[field].items():
                    specification = record["specification"]
                    if (identity != self._version_key(
                            specification["environment_id"], specification["version"])
                            or self._partition(specification["space_id"], specification["generation"]) != partition
                            or record["specification_sha256"] != digest(specification)
                            or record["status"] not in {
                                "DRAFT", "RESOLVING", "BUILDING", "VERIFYING", "READY", "FAILED", "UNKNOWN"}
                            or (record["status"] == "DRAFT") != (record["build"] is None)
                            or (record["status"] == "READY") != (record["ready"] is not None)):
                        raise ValueError()
                    if record["build"] is not None:
                        build = record["build"]
                        if (not re.fullmatch(r"build_[0-9a-f]{32}", build["build_id"])
                                or build["phase"] not in {"ADMITTED", "DISPATCHED", "UNKNOWN", "TERMINAL"}
                                or not isinstance(build["project"], str)
                                or build["codebuild_id"] is not None
                                and not isinstance(build["codebuild_id"], str)):
                            raise ValueError()
            elif (len(value[field]) > MAX_GLOBAL_BUILDS or len(set(value[field])) != len(value[field])
                  or any(not re.fullmatch(r"build_[0-9a-f]{32}", identity) for identity in value[field])):
                raise ValueError()
            return value
        except (ValueError, KeyError, TypeError):
            raise ControlError(503, "ENVIRONMENT_RECORD_UNAVAILABLE") from None

    def _put(self, partition, previous, value):
        record = canonical(value)
        if len(record.encode()) > MAX_CATALOG_BYTES:
            raise ControlError(413, "ENVIRONMENT_CATALOG_LIMIT")
        operation = {
            "TableName": self.table,
            "Item": {**self._key(partition), "record": {"S": record},
                     "revision": {"N": str(value["revision"])}},
            "ConditionExpression": "attribute_not_exists(PK)",
        }
        if previous["revision"]:
            operation.update(
                ConditionExpression="#revision = :revision",
                ExpressionAttributeNames={"#revision": "revision"},
                ExpressionAttributeValues={":revision": {"N": str(previous["revision"])}})
        return {"Put": operation}

    def _authorize(self, principal, space, *, read=False):
        principal.authorize(space, "monitor:read" if read and "runtime:manage" not in principal.scopes
                            else "runtime:manage")
        configuration = self.registry._read(space)
        self.registry._authorize_generation(principal, configuration)
        if self.keys is not None:
            actual = self.keys.authenticate_id(principal.key_id or "")
            if actual != principal:
                raise ControlError(403, "FORBIDDEN")
        return configuration["generation"]

    def _conditions(self, principal, space, generation):
        result = [{"ConditionCheck": {
            "TableName": self.table, "Key": self.registry._key(space),
            "ConditionExpression": "generation = :generation",
            "ExpressionAttributeValues": {":generation": {"S": generation}},
        }}]
        if self.keys is not None:
            actor, secret_digest = self.keys._by_id(principal.key_id or "")
            if (actor["status"] != "ACTIVE" or actor["generation"] != generation
                    or actor["principal_id"] != principal.principal_id
                    or actor["space_id"] != space or frozenset(actor["scopes"]) != principal.scopes):
                raise ControlError(403, "FORBIDDEN")
            result.append(self.keys._condition_actor(actor, secret_digest))
        return result

    def _transaction(self, operations):
        try:
            self.client.transact_write_items(
                TransactItems=operations, ClientRequestToken=uuid.uuid4().hex)
            return True
        except ClientError as error:
            if error.response["Error"]["Code"] == "TransactionCanceledException":
                reasons = error.response.get("CancellationReasons", [])
                if any(x.get("Code") == "ConditionalCheckFailed" for x in reasons):
                    return False
            raise

    def _partition(self, space, generation):
        return f"ENVIRONMENT#1#{space}#{generation}"

    def _version_key(self, environment, version):
        validate("identifier", environment)
        validate("identifier", version)
        return environment + "/" + version

    def _builder_available(self):
        location, version = self.policy.get("source_location"), self.policy.get("source_version")
        if (not self.project or self.codebuild is None or self.verifier is None
                or not isinstance(location, str)
                or not location.startswith(self.registry.bucket + "/artifacts/")
                or any(x in location for x in ["..", "*", "?", "#", "\\"])
                or re.search(r"\s", location)
                or not isinstance(version, str) or version in {"", "null"}
                or len(version) > 1024):
            raise ControlError(503, "ENVIRONMENT_BUILDER_UNAVAILABLE")

    def create(self, principal, space, request):
        generation = self._authorize(principal, space)
        validate("environment_request", request)
        requirements = dependencies(request["dependency_source"]["content"])
        self._builder_available()
        specification = {**request, "requirements": requirements, "generation": generation,
                         "space_id": space, "policy": self.policy, "build_project": self.project}
        binding = digest(specification)
        partition = self._partition(space, generation)
        version_key = self._version_key(request["environment_id"], request["version"])
        for _ in range(8):
            current = self._read(partition, "versions")
            original = current["versions"].get(version_key)
            if original:
                if original["specification_sha256"] != binding:
                    raise ControlError(409, "ENVIRONMENT_VERSION_IMMUTABLE")
                return self._created(request)
            if len(current["versions"]) >= MAX_VERSIONS:
                raise ControlError(429, "ENVIRONMENT_VERSION_LIMIT")
            record = {"specification": specification, "specification_sha256": binding,
                      "status": "DRAFT", "created_unix": time.time(),
                      "created_by": principal.principal_id, "build": None, "ready": None}
            updated = {"revision": current["revision"] + 1,
                       "versions": {**current["versions"], version_key: record}}
            if self._transaction([self._put(partition, current, updated),
                                  *self._conditions(principal, space, generation),
                                  self.audit.put(space, generation, "ENVIRONMENT_CREATED", key_actor(principal),
                                                 {"kind": "environment", "id": version_key}, identity=binding,
                                                 details={"specification_sha256": binding,
                                                          "requirements": len(requirements)})]):
                return self._created(request)
            self._authorize(principal, space)
        raise ControlError(503, "ENVIRONMENT_BUSY")

    def _created(self, request):
        return {"environment_id": request["environment_id"], "version": request["version"],
                "status": "DRAFT"}

    def pipeline_binding(self, principal, space, reference, executor, profile, scope):
        """Read an already verified READY binding using the pipeline's own scope."""
        if scope not in {"pipeline:manage", "pipeline:run"}:
            raise ControlError(403, "FORBIDDEN")
        principal.authorize(space, scope)
        configuration = self.registry._read(space)
        self.registry._authorize_generation(principal, configuration)
        if self.keys is not None and self.keys.authenticate_id(principal.key_id or "") != principal:
            raise ControlError(403, "FORBIDDEN")
        validate("environment_reference", reference)
        record = self._read(self._partition(space, configuration["generation"]), "versions")["versions"].get(
            self._version_key(reference["environment_id"], reference["version"]))
        if not record or record["status"] != "READY":
            raise ControlError(422, "ENVIRONMENT_NOT_READY")
        if record["specification"]["policy"] != self.policy:
            raise ControlError(409, "ENVIRONMENT_BUILD_PROFILE_CHANGED")
        bindings = [value for value in record["ready"]["executor_bindings"]
                    if value["executor"] == executor and value["profile"] == profile]
        if len(bindings) != 1:
            raise ControlError(422, "ENVIRONMENT_EXECUTOR_PROFILE_MISMATCH")
        return copy.deepcopy({"reference": reference, "ready": record["ready"],
                              "executor_binding": bindings[0],
                              "specification_sha256": record["specification_sha256"]})

    def build(self, principal, space, environment, version, nonce):
        generation = self._authorize(principal, space)
        if not isinstance(nonce, str) or not _NONCE.fullmatch(nonce):
            raise ControlError(428, "IDEMPOTENCY_KEY_REQUIRED")
        partition = self._partition(space, generation)
        version_key = self._version_key(environment, version)
        for _ in range(8):
            current = self._read(partition, "versions")
            record = current["versions"].get(version_key)
            if not record:
                raise ControlError(404, "ENVIRONMENT_NOT_FOUND")
            if record["build"]:
                if (record["build"]["nonce"] != nonce
                        or record["build"]["principal_id"] != principal.principal_id):
                    raise ControlError(409, "ENVIRONMENT_BUILD_ALREADY_ADMITTED")
                return self._build_receipt(record)
            if (record["specification"]["policy"] != self.policy
                    or record["specification"]["build_project"] != self.project):
                raise ControlError(409, "ENVIRONMENT_BUILD_PROFILE_CHANGED")
            self._builder_available()
            pool = self._read("ENVIRONMENTBUILDS#1", "builds")
            limit = (min(MAX_GLOBAL_BUILDS, self.quotas.policy(space, generation)["max_concurrent_builds"])
                     if self.quotas is not None else MAX_SPACE_BUILDS)
            if len(pool["builds"]) >= MAX_GLOBAL_BUILDS:
                raise ControlError(429, "ENVIRONMENT_BUILD_LIMIT")
            if sum(bool(x["build"]) and x["status"] not in {"READY", "FAILED"}
                   for x in current["versions"].values()) >= limit:
                raise ControlError(429, "SPACE_BUILD_QUOTA")
            identity = "build_" + uuid.uuid4().hex
            admitted = {**record, "status": "RESOLVING", "build": {
                "build_id": identity, "nonce": nonce, "principal_id": principal.principal_id,
                "phase": "ADMITTED", "codebuild_id": None, "project": self.project,
                "admitted_unix": time.time()}}
            updated = {"revision": current["revision"] + 1,
                       "versions": {**current["versions"], version_key: admitted}}
            capacity = {"revision": pool["revision"] + 1, "builds": pool["builds"] + [identity]}
            if self._transaction([
                    self._put(partition, current, updated),
                    self._put("ENVIRONMENTBUILDS#1", pool, capacity),
                    *self._conditions(principal, space, generation),
                    self.audit.put(space, generation, "ENVIRONMENT_BUILD_ADMITTED", key_actor(principal),
                                   {"kind": "environment", "id": version_key}, identity=identity,
                                   details={"build_id": identity})]):
                break
            self._authorize(principal, space)
        else:
            raise ControlError(503, "ENVIRONMENT_BUSY")
        # Dispatch once, only after durable quota and original specification.
        try:
            specification = self.verifier.publish_specification(admitted)
            self._observe(partition, version_key, identity, "RESOLVING",
                          {"specification_ref": specification})
            result = self.codebuild.start_build(
                projectName=self.project, idempotencyToken=identity,
                sourceTypeOverride="S3",
                sourceLocationOverride=admitted["specification"]["policy"]["source_location"],
                sourceVersion=admitted["specification"]["policy"]["source_version"],
                buildspecOverride="buildspec.yml",
                environmentVariablesOverride=[
                    {"name": "COLVENNA_ENVIRONMENT_SPECIFICATION_REF", "type": "PLAINTEXT",
                     "value": canonical(specification)},
                    {"name": "COLVENNA_ENVIRONMENT_SPECIFICATION_SHA256", "type": "PLAINTEXT",
                     "value": admitted["specification_sha256"]},
                    {"name": "COLVENNA_ENVIRONMENT_BUILD_ID", "type": "PLAINTEXT", "value": identity}],
            )["build"]
            if (not isinstance(result.get("id"), str)
                    or result.get("projectName") != self.project):
                raise ControlError(503, "ENVIRONMENT_BUILD_OBSERVATION_UNKNOWN")
            self._observe(partition, version_key, identity, "RESOLVING",
                          {"phase": "DISPATCHED", "codebuild_id": result["id"]})
        except (ClientError, BotoCoreError, ControlError, KeyError, TypeError):
            # Do not release capacity or redispatch when the effect is uncertain.
            self._observe(partition, version_key, identity, "UNKNOWN", {"phase": "UNKNOWN"})
        return {"build_id": identity, "status": "QUEUED"}

    def _build_receipt(self, record):
        return {"build_id": record["build"]["build_id"], "status": "QUEUED"}

    def _observe(self, partition, version_key, identity, status, build_changes=None, ready=None):
        for _ in range(8):
            current = self._read(partition, "versions")
            original = current["versions"].get(version_key)
            if not original or not original["build"] or original["build"]["build_id"] != identity:
                raise ControlError(503, "ENVIRONMENT_BUILD_IDENTITY_CHANGED")
            if original["status"] in {"READY", "FAILED"}:
                return original
            record = {**original, "status": status,
                      "build": {**original["build"], **(build_changes or {})},
                      "ready": ready}
            updated = {"revision": current["revision"] + 1,
                       "versions": {**current["versions"], version_key: record}}
            operations = [self._put(partition, current, updated)]
            if status in {"READY", "FAILED"}:
                pool = self._read("ENVIRONMENTBUILDS#1", "builds")
                if identity not in pool["builds"]:
                    raise ControlError(503, "ENVIRONMENT_BUILD_CAPACITY_UNKNOWN")
                capacity = {"revision": pool["revision"] + 1,
                            "builds": [x for x in pool["builds"] if x != identity]}
                operations.append(self._put("ENVIRONMENTBUILDS#1", pool, capacity))
            if self._transaction(operations):
                return record
        raise ControlError(503, "ENVIRONMENT_BUSY")

    def get(self, principal, space, environment, version):
        generation = self._authorize(principal, space, read=True)
        partition = self._partition(space, generation)
        version_key = self._version_key(environment, version)
        record = self._read(partition, "versions")["versions"].get(version_key)
        if not record:
            raise ControlError(404, "ENVIRONMENT_NOT_FOUND")
        if record["build"] and record["status"] not in {"READY", "FAILED"}:
            record = self._refresh(partition, version_key, record)
        # The observing response reports the failure it just recorded, not the prior build state.
        build = record["build"]
        result = {"environment_id": environment, "version": version, "status": record["status"],
                  "dependency_lock_sha256": None, "image_digest": None, "executor_bindings": [],
                  "created_at": self._stamp(record["created_unix"])}
        if "runtime:manage" in principal.scopes:
            result["dependency_source"] = copy.deepcopy(record["specification"]["dependency_source"])
        if build:
            result["build_id"] = build["build_id"]
            if build["codebuild_id"]:
                result["codebuild_id"] = build["codebuild_id"]
            if record["status"] == "FAILED":
                result["failure"] = copy.deepcopy(build.get("failure") or {
                    "phase": None, "aws_status": build.get("aws_status"), "code": "ENVIRONMENT_BUILD_FAILED",
                    "message": None})
        if record["ready"]:
            result.update(record["ready"])
        validate("environment_metadata", result)
        return result

    @staticmethod
    def _stamp(value):
        from datetime import datetime, timezone

        return datetime.fromtimestamp(value, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

    def list(self, principal, space):
        generation = self._authorize(principal, space, read=True)
        catalogue = self._read(self._partition(space, generation), "versions")["versions"]
        result = {"environments": [{
            "environment_id": record["specification"]["environment_id"],
            "version": record["specification"]["version"], "status": record["status"],
            "build_id": record["build"]["build_id"] if record["build"] else None,
            "created_at": self._stamp(record["created_unix"]), "created_by": record["created_by"],
        } for _, record in sorted(catalogue.items())]}
        validate("environment_list", result)
        return result

    def _failure(self, record, actual):
        """Bounded, sanitized cause: the builder's own failure receipt, else the failed CodeBuild phase."""
        failed = next((phase for phase in actual.get("phases", [])
                       if phase.get("phaseStatus") not in {None, "SUCCEEDED"}), {})
        contexts = failed.get("contexts") or [{}]
        value = {"phase": failed.get("phaseType"), "aws_status": actual.get("buildStatus"),
                 "code": "ENVIRONMENT_BUILD_FAILED", "message": sanitize(contexts[0].get("message"))}
        reader = getattr(self.verifier, "failure", None)
        if reader is not None:
            try:
                reported = reader(record)
            except (ClientError, BotoCoreError, ControlError, KeyError, TypeError, ValueError):
                reported = None
            if reported:
                value.update(phase=reported["stage"], code=reported["code"],
                             message=sanitize(reported.get("message")))
        return value

    def _refresh(self, partition, version_key, record):
        build = record["build"]
        if not build["codebuild_id"]:
            # Admission may have reached AWS; no replacement is allowed.
            return {**record, "status": "UNKNOWN"}
        try:
            response = self.codebuild.batch_get_builds(ids=[build["codebuild_id"]])
            if response.get("buildsNotFound") or len(response["builds"]) != 1:
                raise ControlError(503, "ENVIRONMENT_BUILD_OBSERVATION_UNKNOWN")
            actual = response["builds"][0]
            if actual["id"] != build["codebuild_id"] or actual["projectName"] != build["project"]:
                raise ControlError(503, "ENVIRONMENT_BUILD_IDENTITY_CHANGED")
            status = actual["buildStatus"]
            # Measured build duration, for cost observation.
            timing = ({"build_seconds": round((actual["endTime"] - actual["startTime"]).total_seconds(), 3)}
                      if actual.get("startTime") and actual.get("endTime") else {})
            if status in {"FAILED", "FAULT", "STOPPED", "TIMED_OUT"}:
                return self._observe(partition, version_key, build["build_id"], "FAILED",
                                     {"phase": "TERMINAL", "aws_status": status,
                                      "failure": self._failure(record, actual), **timing})
            if status == "SUCCEEDED":
                if self.verifier is None:
                    raise ControlError(503, "ENVIRONMENT_ARTIFACTS_UNVERIFIED")
                verified = self.verifier.verify(record, actual)
                validate("environment_metadata", {
                    "environment_id": record["specification"]["environment_id"],
                    "version": record["specification"]["version"], "status": "READY", **verified})
                size = getattr(self.verifier, "image_bytes", None)
                try:
                    timing["image_bytes"] = size(verified) if size else None
                except (ClientError, BotoCoreError, KeyError, ValueError):
                    timing["image_bytes"] = None
                return self._observe(partition, version_key, build["build_id"], "READY",
                                     {"phase": "TERMINAL", "aws_status": status, **timing}, ready=verified)
            if status != "IN_PROGRESS":
                raise ControlError(503, "ENVIRONMENT_BUILD_OBSERVATION_UNKNOWN")
            phase = actual.get("currentPhase", "")
            state = "VERIFYING" if phase in {"POST_BUILD", "UPLOAD_ARTIFACTS", "FINALIZING"} else (
                "BUILDING" if phase == "BUILD" else "RESOLVING")
            return self._observe(partition, version_key, build["build_id"], state,
                                 {"aws_phase": phase})
        except (ClientError, BotoCoreError, ControlError, KeyError, TypeError, ValueError) as error:
            # Safe diagnostics preserve original identity without SDK messages,
            # URLs, dependency metadata, credentials or raw artifact contents.
            code = error.response["Error"]["Code"] if isinstance(error, ClientError) else (
                error.code if isinstance(error, ControlError) else type(error).__name__)
            logging.getLogger(__name__).warning(canonical({
                "event": "environment_observation_unknown", "build_id": build["build_id"],
                "codebuild_id": build["codebuild_id"], "code": code,
                "operation": error.operation_name if isinstance(error, ClientError) else None}))
            # Observation unavailable: preserve the durable original and quota.
            return {**record, "status": "UNKNOWN", "ready": None}
