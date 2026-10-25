"""Current-version retention: per-category policy, reference tracking and safe collection.

The reference set is computed from authoritative records before anything is
deleted: active runs (their exports, staging, code and environments), active
and recently used pipeline versions, current drafts, retention holds, and the
writer's own protected releases. Native custody changes first (an export is
COLLECTING before deletion; a release is dropped in one native transaction).
S3 objects are deleted by exact version, then confirmed absent, and every
collection writes an immutable receipt plus an audit record. SlateDB database
prefixes are never listed or deleted here; their own protocol maintains them.
"""

import base64
import copy
import hashlib
import json
import re
import time
import uuid
from datetime import datetime, timezone

from botocore.exceptions import BotoCoreError, ClientError

from colvenna_isle.account import account
from colvenna_isle.contracts.model import RETENTION_POLICY, RETENTION_PROBE_MINIMUM
from colvenna_isle.contracts.validation import validate
from .audit import SYSTEM_ACTOR, Audit, key_actor
from .registry import ControlError, canonical

QUEUE_INDEX, SPACE_INDEX = "RETENTIONQUEUE#1", "RETENTIONSPACES#1"
LEASE_SECONDS = 600
MAX_LISTED = 5000
MAX_RECORD_OBJECTS = 200
DEFAULTS = {name: bounds[0] for name, bounds in RETENTION_POLICY.items()}
CATEGORIES = [
    ("raw_archive", "Raw ingestion originals: after raw_seconds and replay_grace_seconds past the last replay intent, "
                    "only when natively COMMITTED and the ingress queue and DLQ are empty", "RETENTION_COLLECTOR"),
    ("fixed_inputs", "Fixed input exports: expired with no ACTIVE pin, failed after 1 hour, or abandoned; the writer "
                     "marks COLLECTING before objects are deleted", "RETENTION_COLLECTOR"),
    ("code_uploads", "Pipeline code uploads not referenced by the current draft or a retained version, after "
                     "upload_seconds", "RETENTION_COLLECTOR"),
    ("code_versions", "Pipeline version packages that are not active, held, used by an active run, or newer than "
                      "pipeline_version_seconds", "RETENTION_COLLECTOR"),
    ("environments", "READY runtime environments are retained for the generation; failed build artifacts after "
                     "failed_build_seconds", "RETENTION_COLLECTOR"),
    ("staging", "Task staging data files of terminal runs after staging_seconds; completion and manifest metadata "
                "remain", "RETENTION_COLLECTOR"),
    ("query_results", "Query results after 600 seconds and history/SQL after 24 hours by exact-version GC",
     "QUERY_SERVICE"),
    ("releases", "Published releases beyond release_keep and older than release_seconds, unless current, held, read "
                 "by a task watermark, a live export or an active run", "RETENTION_COLLECTOR"),
    ("native_database", "SlateDB manifest/WAL/SST objects of the open generation follow the native protocol only",
     "NATIVE_PROTOCOL"),
    ("old_generations", "Objects of an invalidated generation, by explicit operator purge after its tasks stop",
     "GENERATION_PURGE"),
]


def stamp(value):
    if value is None:
        return None
    return datetime.fromtimestamp(value, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def epoch(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def probe(space):
    return space.startswith("probe_")


def effective(space, overrides):
    """Defaults overlaid by a request; product minimums apply except for probe_* qualification spaces."""
    policy = {**DEFAULTS, **(overrides or {})}
    for name, (_, minimum, maximum) in RETENTION_POLICY.items():
        floor = minimum if name == "release_keep" or not probe(space) else RETENTION_PROBE_MINIMUM
        if not floor <= policy[name] <= maximum:
            raise ControlError(422, "RETENTION_POLICY_BELOW_MINIMUM")
    return policy


class Retention:
    def __init__(self, registry, keys, runs, *, invoke=None, clock=time.time):
        self.registry, self.keys, self.runs = registry, keys, runs
        self.client, self.table, self.clock = registry.client, registry.table, clock
        self.audit, self.invoke = Audit(self.client, self.table), invoke or (lambda: None)

    # -- records ----------------------------------------------------------------------------------

    @staticmethod
    def _key(space, sort):
        return {"PK": {"S": "SPACE#" + space}, "SK": {"S": sort}}

    def _get(self, key):
        item = self.client.get_item(TableName=self.table, Key=key, ConsistentRead=True).get("Item")
        if not item:
            return None
        value = json.loads(item["record"]["S"])
        if item.get("revision") != {"N": str(value["revision"])}:
            raise ControlError(503, "RETENTION_RECORD_UNAVAILABLE")
        return value

    def _put(self, key, value, previous=None):
        operation = {"TableName": self.table, "Item": {**key, "record": {"S": canonical(value)},
                                                       "revision": {"N": str(value["revision"])}},
                     "ConditionExpression": "attribute_not_exists(PK)"}
        if previous is not None:
            operation.update(ConditionExpression="#revision = :revision",
                             ExpressionAttributeNames={"#revision": "revision"},
                             ExpressionAttributeValues={":revision": {"N": str(previous["revision"])}})
        return {"Put": operation}

    def _transaction(self, operations):
        try:
            self.client.transact_write_items(TransactItems=operations, ClientRequestToken=uuid.uuid4().hex)
            return True
        except ClientError as error:
            if error.response["Error"]["Code"] == "TransactionCanceledException":
                return False
            raise

    def _authorize(self, principal, space, scope="retention:manage"):
        principal.authorize(space, scope)
        configuration = self.registry._read(space)
        self.registry._authorize_generation(principal, configuration)
        if self.keys.authenticate_id(principal.key_id or "") != principal:
            raise ControlError(403, "FORBIDDEN")
        return configuration

    def _settings(self, configuration):
        return self._get(self._key(configuration["space_id"], "RETENTION#" + configuration["generation"])) or {
            "revision": 0, "state": "DISABLED", "policy": {}, "run_as_key_id": None, "run_as_principal_id": None}

    def _query(self, configuration, prefix, limit=2000):
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
            if not start or len(items) >= limit:
                return items

    def holds(self, configuration):
        now = self.clock()
        result = []
        for item in self._query(configuration, "HOLD#" + configuration["generation"] + "#", 256):
            value = json.loads(item["record"]["S"])
            if value["status"] == "ACTIVE" and value["expires_unix"] <= now:
                value["status"] = "EXPIRED"
            result.append(value)
        return result

    # -- API --------------------------------------------------------------------------------------

    def status(self, principal, space):
        scope = "retention:manage" if "retention:manage" in principal.scopes else "monitor:read"
        configuration = self._authorize(principal, space, scope)
        settings = self._settings(configuration)
        collections = []
        items = self._query(configuration, "COLLECTION#" + configuration["generation"] + "#", 200)
        for item in sorted(items, key=lambda value: json.loads(value["record"]["S"])["requested_unix"],
                           reverse=True)[:20]:
            value = json.loads(item["record"]["S"])
            collections.append({"collection_id": value["collection_id"], "status": value["status"],
                                "dry_run": value["dry_run"], "requested_at": stamp(value["requested_unix"]),
                                "finished_at": stamp(value.get("finished_unix")),
                                "collected_objects": value.get("collected_objects", 0),
                                "collected_bytes": value.get("collected_bytes", 0),
                                "error_code": value.get("error_code")})
        result = {"space_id": space, "generation": configuration["generation"], "observed_at": stamp(self.clock()),
                  "revision": "ret_" + str(settings["revision"]), "state": settings["state"],
                  "policy": effective(space, settings["policy"]), "probe_minimum": probe(space),
                  "categories": [{"category": name, "rule": rule, "collector": collector}
                                 for name, rule, collector in CATEGORIES],
                  "holds": [self._public_hold(value) for value in self.holds(configuration)],
                  "collections": collections}
        validate("retention_status", result)
        return result

    def update(self, principal, space, request, expected):
        configuration = self._authorize(principal, space)
        validate("retention_request", request)
        settings = self._settings(configuration)
        if expected != '"ret_' + str(settings["revision"]) + '"':
            raise ControlError(412, "RETENTION_REVISION_CHANGED")
        effective(space, request["policy"])
        updated = {"revision": settings["revision"] + 1, "state": request["state"],
                   "policy": copy.deepcopy(request["policy"]), "updated_unix": self.clock(),
                   "run_as_key_id": principal.key_id if request["state"] == "ENABLED" else None,
                   "run_as_principal_id": principal.principal_id if request["state"] == "ENABLED" else None}
        index = {"PK": {"S": SPACE_INDEX}, "SK": {"S": space + "#" + configuration["generation"]}}
        operations = [
            self._put(self._key(space, "RETENTION#" + configuration["generation"]), updated,
                      settings if settings["revision"] else None),
            {"Put": {"TableName": self.table, "Item": {**index, "run_as_key_id": {"S": principal.key_id}}}}
            if request["state"] == "ENABLED" else {"Delete": {"TableName": self.table, "Key": index}},
            *self.keys.operation_conditions(principal, space, configuration["generation"], "retention:manage"),
            self.audit.put(space, configuration["generation"], "RETENTION_POLICY_CHANGED", key_actor(principal),
                           {"kind": "space", "id": space}, identity="retention:" + str(updated["revision"]),
                           details={"state": request["state"], **{name: value for name, value in
                                                                  sorted(request["policy"].items())}})]
        if not self._transaction(operations):
            raise ControlError(412, "RETENTION_REVISION_CHANGED")
        return self.status(principal, space)

    @staticmethod
    def _public_hold(value):
        return {"hold_id": value["hold_id"], "target": value["target"], "reason": value["reason"],
                "created_at": stamp(value["created_unix"]), "expires_at": stamp(value["expires_unix"]),
                "status": value["status"], "created_by": value["created_by"]}

    def place_hold(self, principal, space, request):
        configuration = self._authorize(principal, space)
        validate("hold_request", request)
        expires = epoch(request["expires_at"])
        if not self.clock() < expires <= self.clock() + 366 * 86400:
            raise ControlError(422, "RETENTION_HOLD_EXPIRY")
        if len([value for value in self.holds(configuration) if value["status"] == "ACTIVE"]) >= 256:
            raise ControlError(429, "RETENTION_HOLD_QUOTA")
        hold = {"revision": 1, "hold_id": "hold_" + uuid.uuid4().hex, "target": request["target"],
                "reason": request["reason"], "created_unix": self.clock(), "expires_unix": expires, "status": "ACTIVE",
                "created_by": principal.principal_id}
        if not self._transaction([
                self._put(self._key(space, "HOLD#" + configuration["generation"] + "#" + hold["hold_id"]), hold),
                *self.keys.operation_conditions(principal, space, configuration["generation"], "retention:manage"),
                self.audit.put(space, configuration["generation"], "RETENTION_HOLD_PLACED", key_actor(principal),
                               {"kind": "retention_hold", "id": hold["hold_id"]}, identity=hold["hold_id"],
                               details={"target_kind": request["target"]["kind"], "target_id": request["target"]["id"],
                                        "expires_at": request["expires_at"], "reason": request["reason"]})]):
            raise ControlError(409, "RETENTION_HOLD_NOT_ADMITTED")
        return self._public_hold(hold)

    def release_hold(self, principal, space, hold_id):
        configuration = self._authorize(principal, space)
        validate("identifier", hold_id)
        key = self._key(space, "HOLD#" + configuration["generation"] + "#" + hold_id)
        hold = self._get(key)
        if hold is None:
            raise ControlError(404, "RETENTION_HOLD_NOT_FOUND")
        if hold["status"] != "ACTIVE":
            return self._public_hold(hold)
        updated = {**hold, "revision": hold["revision"] + 1, "status": "RELEASED", "released_unix": self.clock(),
                   "released_by": principal.principal_id}
        if not self._transaction([
                self._put(key, updated, hold),
                *self.keys.operation_conditions(principal, space, configuration["generation"], "retention:manage"),
                self.audit.put(space, configuration["generation"], "RETENTION_HOLD_RELEASED", key_actor(principal),
                               {"kind": "retention_hold", "id": hold_id}, identity=hold_id + ":release")]):
            raise ControlError(409, "RETENTION_HOLD_CHANGED")
        return self._public_hold(updated)

    def request(self, principal, space, request, nonce):
        configuration = self._authorize(principal, space)
        validate("collection_request", request)
        if not isinstance(nonce, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", nonce):
            raise ControlError(428, "IDEMPOTENCY_KEY_REQUIRED")
        identity = "col_" + digest([principal.principal_id, nonce, space, configuration["generation"]])[:40]
        key = self._key(space, "COLLECTION#" + configuration["generation"] + "#" + identity)
        existing = self._get(key)
        if existing is None:
            record = {"revision": 1, "collection_id": identity, "space_id": space,
                      "generation": configuration["generation"], "status": "QUEUED", "dry_run": request["dry_run"],
                      "requested_unix": self.clock(), "trigger": "api",
                      "principal": {"principal_id": principal.principal_id, "key_id": principal.key_id},
                      "request_sha256": digest(request)}
            operations = [self._put(key, record),
                          {"Put": {"TableName": self.table, "Item": {
                              "PK": {"S": QUEUE_INDEX}, "SK": {"S": "#".join([space, configuration["generation"],
                                                                                 identity])}}}},
                          *self.keys.operation_conditions(principal, space, configuration["generation"],
                                                          "retention:manage"),
                          self.audit.put(space, configuration["generation"], "RETENTION_COLLECTION_REQUESTED",
                                         key_actor(principal), {"kind": "retention_collection", "id": identity},
                                         identity=identity, details={"dry_run": request["dry_run"]})]
            if not self._transaction(operations):
                existing = self._get(key)
            else:
                self.invoke()
        if existing is not None and existing["request_sha256"] != digest(request):
            raise ControlError(409, "IDEMPOTENCY_KEY_CONFLICT")
        return {"collection_id": identity, "status": "QUEUED",
                "status_url": f"/v1/admin/spaces/{space}/retention/collections/{identity}"}

    def collection(self, principal, space, identity):
        scope = "retention:manage" if "retention:manage" in principal.scopes else "monitor:read"
        configuration = self._authorize(principal, space, scope)
        validate("identifier", identity)
        record = self._get(self._key(space, "COLLECTION#" + configuration["generation"] + "#" + identity))
        if record is None:
            raise ControlError(404, "RETENTION_COLLECTION_NOT_FOUND")
        result = {"collection_id": identity, "space_id": space, "generation": configuration["generation"],
                  "status": record["status"], "dry_run": record["dry_run"],
                  "requested_at": stamp(record["requested_unix"]), "started_at": stamp(record.get("started_unix")),
                  "finished_at": stamp(record.get("finished_unix")), "error_code": record.get("error_code"),
                  "summary": record.get("summary", {}), "receipt": record.get("receipt"),
                  "objects": record.get("objects", []), "truncated": record.get("truncated", False),
                  "failure": record.get("failure")}
        validate("collection_report", result)
        return result


class Collector:
    """Executes queued and scheduled collections; one space lease at a time."""

    def __init__(self, retention, writer, s3, bucket, admission, ledger, quotas=None, *, clock=time.time):
        self.retention, self.writer, self.s3, self.bucket = retention, writer, s3, bucket
        self.admission, self.ledger, self.quotas, self.clock = admission, ledger, quotas, clock
        self.client, self.table = retention.client, retention.table
        self.runs, self.keys, self.registry = retention.runs, retention.keys, retention.registry

    # -- queue ------------------------------------------------------------------------------------

    def tick(self, deadline):
        summary = {"collections": [], "scheduled": []}
        for item in self.client.query(TableName=self.table, ConsistentRead=True,
                                      KeyConditionExpression="PK = :pk",
                                      ExpressionAttributeValues={":pk": {"S": QUEUE_INDEX}}).get("Items", []):
            if time.monotonic() >= deadline:
                break
            space, generation, identity = item["SK"]["S"].split("#")
            summary["collections"].append(self.run(space, generation, identity, deadline))
        return summary

    def schedule(self):
        """Enabled spaces get one scheduled collection per hour, as the enabling principal."""
        admitted = []
        hour = int(self.clock() // 3600)
        for item in self.client.query(TableName=self.table, ConsistentRead=True, KeyConditionExpression="PK = :pk",
                                      ExpressionAttributeValues={":pk": {"S": SPACE_INDEX}}).get("Items", []):
            space, generation = item["SK"]["S"].split("#")
            identity = "col_" + digest(["schedule", space, generation, hour])[:40]
            try:
                configuration = self.registry._read(space)
                if configuration["generation"] != generation:
                    self.client.delete_item(TableName=self.table, Key={"PK": {"S": SPACE_INDEX}, "SK": item["SK"]})
                    continue
                principal = self.keys.authenticate_id(item["run_as_key_id"]["S"])
                principal.authorize(space, "retention:manage")
            except ControlError:
                continue
            record = {"revision": 1, "collection_id": identity, "space_id": space, "generation": generation,
                      "status": "QUEUED", "dry_run": False, "requested_unix": self.clock(), "trigger": "schedule",
                      "principal": {"principal_id": principal.principal_id, "key_id": principal.key_id},
                      "request_sha256": digest({"dry_run": False})}
            if self.retention._transaction([
                    self.retention._put(self.retention._key(space, "COLLECTION#" + generation + "#" + identity),
                                        record),
                    {"Put": {"TableName": self.table, "Item": {
                        "PK": {"S": QUEUE_INDEX}, "SK": {"S": "#".join([space, generation, identity])}}}}]):
                admitted.append(identity)
        return admitted

    def _lease(self, space, holder, release=False):
        key = {"PK": {"S": "RETENTIONLEASE#" + space}, "SK": {"S": "META"}}
        now = self.clock()
        try:
            if release:
                self.client.delete_item(TableName=self.table, Key=key, ConditionExpression="holder = :holder",
                                        ExpressionAttributeValues={":holder": {"S": holder}})
                return True
            self.client.put_item(TableName=self.table, Item={**key, "holder": {"S": holder},
                                                             "expires_unix": {"N": str(now + LEASE_SECONDS)}},
                                 ConditionExpression="attribute_not_exists(PK) OR expires_unix < :now",
                                 ExpressionAttributeValues={":now": {"N": str(now)}})
            return True
        except ClientError as error:
            if error.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise

    def run(self, space, generation, identity, deadline):
        key = self.retention._key(space, "COLLECTION#" + generation + "#" + identity)
        index = {"PK": {"S": QUEUE_INDEX}, "SK": {"S": "#".join([space, generation, identity])}}
        record = self.retention._get(key)
        if record is None or record["status"] in {"COMPLETED", "FAILED"}:
            self.client.delete_item(TableName=self.table, Key=index)
            return {"collection_id": identity, "status": record["status"] if record else "MISSING"}
        holder = identity + ":" + uuid.uuid4().hex
        if not self._lease(space, holder):
            # Another collector holds this space: the collection stays queued.
            return {"collection_id": identity, "status": "WAITING_FOR_LEASE"}
        try:
            running = {**record, "revision": record["revision"] + 1, "status": "RUNNING",
                       "started_unix": record.get("started_unix") or self.clock()}
            if not self.retention._transaction([self.retention._put(key, running, record)]):
                return {"collection_id": identity, "status": "CHANGED"}
            sweep, failure = None, None
            try:
                principal = self.keys.authenticate_id(record["principal"]["key_id"])
                if principal.principal_id != record["principal"]["principal_id"]:
                    raise ControlError(403, "RETENTION_PRINCIPAL_CHANGED")
                principal.authorize(space, "retention:manage")
                configuration = self.registry._read(space)
                if configuration["generation"] != generation:
                    raise ControlError(409, "SPACE_GENERATION_CHANGED")
                sweep = Sweep(self, principal, configuration, identity, record["dry_run"], deadline)
                report = sweep.execute()
                status, error = "COMPLETED", None
            except ControlError as caught:
                report, status, error = None, "FAILED", caught.code
                failure = {"operation": "control", "code": caught.code}
            except ClientError as caught:
                report, status, error = None, "FAILED", "RETENTION_DEPENDENCY_UNAVAILABLE"
                failure = {"operation": caught.operation_name, "code": caught.response.get("Error", {}).get("Code")}
            except BotoCoreError as caught:
                report, status, error = None, "FAILED", "RETENTION_DEPENDENCY_UNAVAILABLE"
                failure = {"operation": "transport", "code": type(caught).__name__}
            if failure is not None and sweep is not None and sweep.objects:
                # Objects already deleted before the failure keep their object-level evidence.
                try:
                    report = sweep.report(sweep.receipt(failure))
                except (ClientError, BotoCoreError):
                    report = {**sweep.report(None), "receipt": None}
            final = {**running, "revision": running["revision"] + 1, "status": status, "finished_unix": self.clock()}
            if failure:
                final["failure"] = failure
            if report:
                final.update(summary=report["summary"], receipt=report["receipt"], objects=report["objects"],
                             truncated=report["truncated"], collected_objects=report["collected_objects"],
                             collected_bytes=report["collected_bytes"])
            if error:
                final["error_code"] = error
            operations = [self.retention._put(key, final, running), {"Delete": {"TableName": self.table, "Key": index}},
                          self.retention.audit.put(space, generation, "RETENTION_COLLECTION_COMPLETED",
                                                   SYSTEM_ACTOR, {"kind": "retention_collection", "id": identity},
                                                   identity=identity + ":completed", outcome="COMMITTED", details={
                                                       "status": status, "error_code": error,
                                                       "failure_operation": (failure or {}).get("operation"),
                                                       "failure_code": (failure or {}).get("code"),
                                                       "dry_run": record["dry_run"],
                                                       "collected_objects": final.get("collected_objects", 0),
                                                       "collected_bytes": final.get("collected_bytes", 0),
                                                       "receipt_sha256": (final.get("receipt") or {}).get("sha256")})]
            self.retention._transaction(operations)
            return {"collection_id": identity, "status": status, "error_code": error, "failure": failure}
        finally:
            self._lease(space, holder, release=True)


class Sweep:
    """One collection pass over one space generation."""

    def __init__(self, collector, principal, configuration, identity, dry_run, deadline):
        self.c, self.principal, self.configuration = collector, principal, configuration
        self.identity, self.dry_run, self.deadline = identity, dry_run, deadline
        self.space, self.generation = configuration["space_id"], configuration["generation"]
        self.now = collector.clock()
        self.objects, self.summary = [], {}
        settings = collector.retention._settings(configuration)
        self.policy = effective(self.space, settings["policy"])

    # -- evidence ---------------------------------------------------------------------------------

    def note(self, category, key, version, size, action, reason):
        self.objects.append({"category": category, "key": key, "version_id": version, "bytes": size,
                             "action": action, "reason": reason[:256]})
        counts = self.summary.setdefault(category, {"candidates": 0, "collected": 0, "retained": 0, "bytes": 0})
        counts["candidates"] += action in {"COLLECTED", "WOULD_COLLECT"}
        counts["collected"] += action == "COLLECTED"
        counts["retained"] += action == "RETAINED"
        counts["bytes"] += (size or 0) if action == "COLLECTED" else 0

    def delete(self, category, key, version, size, reason):
        """Delete one exact version and confirm that version is gone."""
        if not key.startswith(f"spaces/{self.space}/") or "/warehouse/" in key:
            raise ControlError(403, "RETENTION_KEY_SCOPE")
        if self.dry_run:
            self.note(category, key, version, size, "WOULD_COLLECT", reason)
            return
        self.c.s3.delete_object(Bucket=self.c.bucket, Key=key, VersionId=version, ExpectedBucketOwner=account())
        # Confirm by listing the exact key's versions: the collector has no s3:ListBucket, so a HEAD
        # of the deleted version would be 403, not 404.
        if any(value["Key"] == key and value["VersionId"] == version for value in self.versions(key, 1000)):
            raise ControlError(503, "RETENTION_DELETE_UNCONFIRMED")
        self.note(category, key, version, size, "COLLECTED", reason)

    def versions(self, prefix, limit=MAX_LISTED):
        result, kwargs = [], {"Bucket": self.c.bucket, "Prefix": prefix, "ExpectedBucketOwner": account()}
        while len(result) < limit:
            page = self.c.s3.list_object_versions(**kwargs)
            result += [value for value in page.get("Versions", []) if value["VersionId"] not in {None, "null"}]
            if not page.get("IsTruncated"):
                break
            kwargs.update(KeyMarker=page["NextKeyMarker"], VersionIdMarker=page["NextVersionIdMarker"])
        return result[:limit]

    def receipt(self, failure=None):
        """The immutable object-level record; a failed pass still records every object it already deleted."""
        raw = canonical({"format_version": 1, "collection_id": self.identity, "space_id": self.space,
                         "generation": self.generation, "dry_run": self.dry_run, "started_unix": self.now,
                         "finished_unix": self.c.clock(), "policy": self.policy, "summary": self.summary,
                         "objects": self.objects, **({"failure": failure} if failure else {})}).encode()
        sha = hashlib.sha256(raw).hexdigest()
        key = f"spaces/{self.space}/retention/{self.generation}/{self.identity}.json"
        try:
            response = self.c.s3.put_object(Bucket=self.c.bucket, Key=key, Body=raw, IfNoneMatch="*",
                                            ExpectedBucketOwner=account(), ContentType="application/json",
                                            Metadata={"sha256": sha},
                                            ChecksumSHA256=base64.b64encode(bytes.fromhex(sha)).decode())
            version = response["VersionId"]
        except ClientError as error:
            if error.response["Error"]["Code"] not in {"PreconditionFailed", "412"}:
                raise
            version = self.versions(key, 2)[0]["VersionId"]
        return {"s3_uri": f"s3://{self.c.bucket}/{key}", "object_version": version, "sha256": sha, "bytes": len(raw)}

    # -- references -------------------------------------------------------------------------------

    def references(self):
        """Everything an active run, a hold, an active version or a draft still binds."""
        protected = {"exports": set(), "runs": set(), "versions": set(), "releases": set(), "ingestions": set(),
                     "environments": set(), "uploads": set()}
        holds = [value for value in self.c.retention.holds(self.configuration) if value["status"] == "ACTIVE"]
        for hold in holds:
            target = hold["target"]
            kind = {"release": "releases", "pipeline_version": "versions", "run": "runs", "ingestion": "ingestions",
                    "environment": "environments"}[target["kind"]]
            protected[kind].add(target["id"] if target["kind"] != "pipeline_version"
                                else (target.get("pipeline_id"), target["id"]))
        active = []
        for space, generation, pipeline, run_id in self.c.runs.active():
            if (space, generation) == (self.space, self.generation):
                record = self.c.runs.pipelines._read(self.c.runs._key(self.configuration, pipeline, run_id),
                                                     missing=None)
                if record is not None:
                    active.append(record)
        held_runs = []
        for run_id in protected["runs"]:
            for pipeline in self.pipelines():
                record = self.c.runs.pipelines._read(self.c.runs._key(self.configuration, pipeline, run_id),
                                                     missing=None)
                if record is not None:
                    held_runs.append(record)
        for record in active + held_runs:
            protected["runs"].add(record["run_id"])
            protected["versions"].add((record["pipeline_id"], record["pipeline_version"]))
            for value in (record.get("root_inputs") or {}).values():
                if value.get("release_id"):
                    protected["releases"].add(value["release_id"])
            for value in ((record.get("trial") or {}).get("upstream") or {}).values():
                protected["releases"].add(value)
            for job in record["tasks"].values():
                if job.get("export_id"):
                    protected["exports"].add(job["export_id"])
                if job.get("release_id"):
                    protected["releases"].add(job["release_id"])
        for pipeline in self.pipelines():
            current = self.c.runs.pipelines._read(self.c.runs.pipelines._key(self.configuration, "PIPELINE", pipeline),
                                                  missing=None)
            if current is None:
                continue
            if current.get("active_version"):
                protected["versions"].add((pipeline, current["active_version"]))
            for artifact in current["draft"]["artifacts"]:
                protected["uploads"].add(artifact["upload_id"])
        return protected

    def pipelines(self):
        if not hasattr(self, "_pipelines"):
            self._pipelines = [item["SK"]["S"].rsplit("#", 1)[1] for item in self.c.runs.pipelines._records(
                self.configuration, "PIPELINE#" + self.generation + "#", 512)]
        return self._pipelines

    # -- categories -------------------------------------------------------------------------------

    def execute(self):
        protected = self.references()
        for step in [self.inputs, self.releases, self.versions_and_uploads, self.staging, self.environments,
                     self.raw]:
            if time.monotonic() >= self.deadline:
                self.summary.setdefault("deadline", {"candidates": 0, "collected": 0, "retained": 0, "bytes": 0})
                break
            step(protected)
        self.summary.setdefault("query_results", {"candidates": 0, "collected": 0, "retained": 0, "bytes": 0})
        return self.report(self.receipt())

    def report(self, receipt):
        collected = [value for value in self.objects if value["action"] == "COLLECTED"]
        return {"summary": self.summary, "receipt": receipt, "objects": self.objects[:MAX_RECORD_OBJECTS],
                "truncated": len(self.objects) > MAX_RECORD_OBJECTS, "collected_objects": len(collected),
                "collected_bytes": sum(value["bytes"] or 0 for value in collected)}

    def writer(self, operation, **fields):
        return self.c.writer.call(self.principal, self.configuration, operation, collection_id=self.identity,
                                  **fields)

    def inputs(self, protected):
        candidates = self.c.writer.call(self.principal, self.configuration, "retention_candidates",
                                        protected_exports=sorted(protected["exports"]))
        eligible = [value for value in candidates["exports"] if value["eligible"]]
        for value in candidates["exports"]:
            if not value["eligible"] and value["state"] != "COLLECTED":
                self.note("fixed_inputs", value["export_id"], None, value["retained_bytes"], "RETAINED",
                          "PROTECTED_BY_ACTIVE_RUN" if value["export_id"] in protected["exports"]
                          else "PINNED_OR_UNEXPIRED")
        # An original left COLLECTING by an interrupted pass is resumed under the space lease and
        # finished under the collection that marked it, which the native record keeps.
        resumed = {value["export_id"] for value in eligible if value["reason"] == "RESUME_COLLECTION"}
        for start in range(0, len(eligible), 32):
            batch = [value["export_id"] for value in eligible[start:start + 32]]
            if self.dry_run:
                for value in eligible[start:start + 32]:
                    self.note("fixed_inputs", value["export_id"], None, value["retained_bytes"], "WOULD_COLLECT",
                              value["reason"])
                continue
            marked = self.writer("retention_mark_exports", export_ids=batch,
                                 protected_exports=sorted(protected["exports"]))["marked"]
            results = {}
            for value in marked:
                owner, reason = self.identity, value["reason"]
                if value["export_id"] in resumed:
                    export = self.c.writer.call(self.principal, self.configuration, "governance_read",
                                                kind="input_export", id=value["export_id"])
                    owner = (export.get("collection") or {}).get("collection_id") or self.identity
                    reason = "RESUMED_FROM_" + owner if owner != self.identity else reason
                evidence = []
                for version in self.versions(value["prefix"]):
                    self.delete("fixed_inputs", version["Key"], version["VersionId"], version["Size"], reason)
                    evidence.append({"key": version["Key"], "version_id": version["VersionId"],
                                     "bytes": version["Size"]})
                results.setdefault(owner, []).append({"export_id": value["export_id"],
                                                      "evidence": {"objects": evidence}})
            for owner, values in sorted(results.items()):
                self.c.writer.call(self.principal, self.configuration, "retention_finish_exports",
                                   collection_id=owner, results=values)

    def releases(self, protected):
        # Fresh native state: exports collected by this pass no longer bind their releases.
        native = self.c.writer.call(self.principal, self.configuration, "retention_candidates",
                                    protected_exports=sorted(protected["exports"]))
        pinned = set(native["protected_releases"]) | protected["releases"]
        collect = []
        by_dataset = {}
        for value in native["releases"]:
            by_dataset.setdefault(value["dataset"], []).append(value)
        for dataset, values in sorted(by_dataset.items()):
            ordered = sorted([value for value in values if value["status"] == "PUBLISHED"],
                             key=lambda value: value["commit_sequence"], reverse=True)
            for rank, value in enumerate(ordered):
                published = value.get("published_unix")
                reason = ("CURRENT_RELEASE" if value["current"] else "PROTECTED_REFERENCE" if value["release_id"] in pinned
                          else "WITHIN_RELEASE_KEEP" if rank < self.policy["release_keep"]
                          else "PUBLISHED_TIME_UNKNOWN" if published is None
                          else "WITHIN_RELEASE_SECONDS" if self.now - published < self.policy["release_seconds"]
                          else None)
                if reason:
                    self.note("releases", value["release_id"], None, None, "RETAINED", reason)
                else:
                    collect.append(value["release_id"])
        generations = [value["generation_id"] for value in native["generations"] if value["leftover"]]
        if self.dry_run:
            for identity in collect + generations:
                self.note("releases", identity, None, None, "WOULD_COLLECT", "RETENTION_EXPIRED")
            return
        for start in range(0, max(len(collect), len(generations)), 32):
            releases, leftovers = collect[start:start + 32], generations[start:start + 32]
            if not releases and not leftovers:
                continue
            result = self.writer("retention_collect_native", release_ids=releases, generation_ids=leftovers,
                                 protected_releases=sorted(pinned))
            for entry in result["collected"]:
                self.note("releases", entry["kind"] + ":" + entry["identity"], None, None, "COLLECTED",
                          "NATIVE_TABLES_DROPPED:" + ",".join(entry["tables"]))

    def versions_and_uploads(self, protected):
        artifacts = self.c.runs.pipelines.artifacts
        retained_keys = set()
        for pipeline in self.pipelines():
            for item in self.c.runs.pipelines._records(self.configuration,
                                                       f"PIPELINE_VERSION#{self.generation}#{pipeline}#"):
                version = item["SK"]["S"].rsplit("#", 1)[1]
                key = self.c.runs.pipelines._key(self.configuration, "PIPELINE_VERSION", pipeline, version)
                published = self.c.runs.pipelines._read(key)
                package = published["package"]
                if published.get("collected_unix"):
                    continue
                used = (pipeline, version) in protected["versions"]
                recent = self.now - (published.get("created_unix") or self.now) < self.policy["pipeline_version_seconds"]
                if used or recent:
                    retained_keys.update(entry["key"] for entry in published["snapshot"]["artifacts"])
                    self.note("code_versions", package["package_uri"], package["package_object_version"],
                              package["package_bytes"], "RETAINED",
                              "ACTIVE_OR_RUNNING_OR_HELD" if used else "WITHIN_PIPELINE_VERSION_SECONDS")
                    continue
                prefix = f"s3://{self.c.bucket}/"
                self.delete("code_versions", package["package_uri"][len(prefix):], package["package_object_version"],
                            package["package_bytes"], "PIPELINE_VERSION_UNUSED")
                if not self.dry_run:
                    updated = {**published, "collected_unix": self.now, "collection_id": self.identity}
                    self.c.runs.pipelines._transaction([self.c.runs.pipelines._put(key, updated, published)])
            for item in self.c.runs.pipelines._records(self.configuration, f"UPLOAD#{self.generation}#{pipeline}#"):
                upload_id = item["SK"]["S"].rsplit("#", 1)[1]
                key = self.c.runs.pipelines._key(self.configuration, "UPLOAD", pipeline, upload_id)
                upload = self.c.runs.pipelines._read(key)
                if upload.get("collected_unix"):
                    continue
                created = upload["expires_unix"] - 300
                if upload_id in protected["uploads"] or upload["key"] in retained_keys:
                    self.note("code_uploads", upload["key"], None, upload["declaration"]["bytes"], "RETAINED",
                              "REFERENCED_BY_DRAFT_OR_VERSION")
                    continue
                if self.now - created < self.policy["upload_seconds"]:
                    self.note("code_uploads", upload["key"], None, upload["declaration"]["bytes"], "RETAINED",
                              "WITHIN_UPLOAD_SECONDS")
                    continue
                for version in self.versions(upload["key"], 8):
                    if version["Key"] == upload["key"]:
                        self.delete("code_uploads", version["Key"], version["VersionId"], version["Size"],
                                    "UPLOAD_UNREFERENCED")
                if not self.dry_run:
                    updated = {**upload, "collected_unix": self.now, "collection_id": self.identity}
                    self.c.runs.pipelines._transaction([self.c.runs.pipelines._put(key, updated, upload)])
                    if self.c.quotas is not None:
                        self._release_bytes(upload["declaration"]["bytes"])

    def _release_bytes(self, size):
        try:
            self.c.quotas.release_code(self.space, self.generation, size)
        except ClientError:
            pass  # The counter is conservative: an unreleased reservation only over-counts.

    def staging(self, protected):
        for pipeline in self.pipelines():
            page = self.c.client.query(
                TableName=self.c.table, ConsistentRead=True, Limit=200,
                KeyConditionExpression="PK = :pk AND begins_with(SK, :prefix)",
                ExpressionAttributeValues={":pk": {"S": "SPACE#" + self.space},
                                           ":prefix": {"S": f"RUNHISTORY#{self.generation}#{pipeline}#"}})
            for entry in page.get("Items", []):
                run_id = entry["run_id"]["S"]
                record = self.c.runs.pipelines._read(self.c.runs._key(self.configuration, pipeline, run_id),
                                                     missing=None)
                if record is None:
                    continue
                prefix = f"spaces/{self.space}/staging/{self.generation}/{pipeline}/{run_id}/"
                if run_id in protected["runs"] or record["status"] not in {"PUBLISHED", "FAILED", "CANCELLED",
                                                                             "VALIDATED"}:
                    self.note("staging", prefix, None, None, "RETAINED", "ACTIVE_OR_HELD_RUN")
                    continue
                finished = record.get("finished_unix") or record["updated_unix"]
                if self.now - finished < self.policy["staging_seconds"]:
                    self.note("staging", prefix, None, None, "RETAINED", "WITHIN_STAGING_SECONDS")
                    continue
                for version in self.versions(prefix):
                    relative = version["Key"][len(prefix):].split("/")
                    # Data files only; completion, manifests and descriptors remain as audit lineage.
                    # <job>/<attempt>/<execution>/(result|state)/<file>
                    if len(relative) > 4 and relative[3] in {"result", "state"}:
                        self.delete("staging", version["Key"], version["VersionId"], version["Size"],
                                    "TERMINAL_RUN_" + record["status"])

    def environments(self, protected):
        partition = f"ENVIRONMENT#1#{self.space}#{self.generation}"
        item = self.c.client.get_item(TableName=self.c.table, ConsistentRead=True,
                                      Key={"PK": {"S": partition}, "SK": {"S": "META"}}).get("Item")
        if not item:
            return
        for key, record in sorted(json.loads(item["record"]["S"])["versions"].items()):
            build = record.get("build") or {}
            if not build:
                continue
            prefix = f"spaces/{self.space}/environment-builds/{self.generation}/{build['build_id']}/"
            if record["status"] != "FAILED" or key in protected["environments"]:
                self.note("environments", prefix, None, None, "RETAINED",
                          "READY_FOR_GENERATION" if record["status"] == "READY" else "BUILD_NOT_FAILED_OR_HELD")
                continue
            if self.now - build.get("admitted_unix", self.now) < self.policy["failed_build_seconds"]:
                self.note("environments", prefix, None, None, "RETAINED", "WITHIN_FAILED_BUILD_SECONDS")
                continue
            for version in self.versions(prefix):
                self.delete("environments", version["Key"], version["VersionId"], version["Size"], "FAILED_BUILD")

    def raw(self, protected):
        try:
            queue = self.c.admission.observe(self.configuration)
            quiet = queue["pending_messages"] == 0 and queue["dlq_messages"] == 0
        except (ClientError, BotoCoreError, ControlError):
            quiet = False
        prefix = f"spaces/{self.space}/ingest/{self.generation}/"
        for version in self.versions(prefix):
            key = version["Key"]
            identity = key.rsplit("/", 1)[1].removesuffix(".json")
            created = version["LastModified"].timestamp()
            if identity in protected["ingestions"]:
                self.note("raw_archive", key, version["VersionId"], version["Size"], "RETAINED", "HELD")
                continue
            if self.now - created < self.policy["raw_seconds"]:
                self.note("raw_archive", key, version["VersionId"], version["Size"], "RETAINED", "WITHIN_RAW_SECONDS")
                continue
            replays = self.versions(f"spaces/{self.space}/ingestion-replay/{self.generation}/{identity}/", 64)
            last = max([value["LastModified"].timestamp() for value in replays], default=created)
            if self.now - last < self.policy["replay_grace_seconds"]:
                self.note("raw_archive", key, version["VersionId"], version["Size"], "RETAINED", "REPLAY_GRACE")
                continue
            if not quiet:
                self.note("raw_archive", key, version["VersionId"], version["Size"], "RETAINED",
                          "QUEUE_OR_DLQ_NOT_EMPTY_OR_UNOBSERVED")
                continue
            try:
                status = self.c.ledger.read(self.configuration, identity)
            except (ControlError, ClientError, BotoCoreError):
                status = {"status": "UNKNOWN"}
            if status.get("status") != "COMMITTED":
                self.note("raw_archive", key, version["VersionId"], version["Size"], "RETAINED",
                          "NATIVE_STATUS_" + str(status.get("status")))
                continue
            self.delete("raw_archive", key, version["VersionId"], version["Size"], "COMMITTED_AND_EXPIRED")
