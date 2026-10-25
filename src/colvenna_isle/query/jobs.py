"""Shared bounded query admission and CAS lifecycle; no automatic execution retry.

SQL/configuration live in one immutable, versioned S3 intent. DynamoDB holds
identity, queue custody and status, never SQL or result rows. These internal
operations must be called by the independently authenticated query service.
"""

import base64
import hashlib
import json
import math
import re
import time
import uuid

from botocore.exceptions import BotoCoreError, ClientError

from colvenna_isle.contracts.validation import validate
from colvenna_isle.control.http import decode_json
from colvenna_isle.control.registry import ControlError, canonical
from colvenna_isle.query.objects import version_absent

TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED"}
STATES = TERMINAL | {"QUEUED", "RUNNING", "CANCELLING", "UNKNOWN"}
MAX_PENDING, MAX_RUNNING, SPACE_PENDING = 32, 4, 8
MAX_SOURCE_READS, SPACE_SOURCE_READS = 8, 4
FORMAT = 3
RETAINED_BYTES, SPACE_RETAINED_BYTES, RETAINED_JOBS = 67108864, 33554432, 64
INTENT_BYTES = 1048576
HISTORY_SECONDS = 86400


def sha(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


class Jobs:
    def __init__(self, ddb, table, s3, bucket, account, source_commit, image_digest,
                 clock=time.time):
        if (not re.fullmatch("[0-9a-f]{40}", source_commit)
                or not re.fullmatch("sha256:[0-9a-f]{64}", image_digest)):
            raise ValueError("Require immutable source/image identity")
        self.ddb, self.table, self.s3, self.bucket, self.account = ddb, table, s3, bucket, account
        self.source, self.image, self.clock = source_commit, image_digest, clock

    def key(self, space, generation, identity):
        return {"PK": {"S": f"QUERY#{space}#{generation}"}, "SK": {"S": identity}}

    def _get(self, key):
        try:
            item = self.ddb.get_item(TableName=self.table, Key=key, ConsistentRead=True).get("Item")
        except (ClientError, BotoCoreError):
            raise ControlError(503, "QUERY_STATE_UNKNOWN") from None
        if not item:
            return None
        try:
            record = decode_json(item["record"]["S"].encode())
            if (type(record["revision"]) is not int or record["revision"] < 1
                    or item["revision"] != {"N": str(record["revision"])}
                    or "format_version" in record and record["format_version"] != FORMAT):
                raise ValueError("Corrupt revision")
            return record
        except (AttributeError, KeyError, TypeError, ValueError):
            raise ControlError(503, "QUERY_STATE_UNKNOWN") from None

    @staticmethod
    def _queue_key():
        return {"PK": {"S": "QUERY#QUEUE"}, "SK": {"S": "META"}}

    @staticmethod
    def _marker_key():
        return {"PK": {"S": "QUERY#INITIALIZED"}, "SK": {"S": "META"}}

    def _queue(self):
        record, marker = self._get(self._queue_key()), self._get(self._marker_key())
        if record is None and marker is not None:
            # Initialization may commit between the two consistent reads.
            record = self._get(self._queue_key())
        if record is None:
            if marker is not None:
                raise ControlError(503, "QUERY_QUEUE_UNKNOWN")
            return {"format_version": FORMAT, "revision": 0, "pending": [], "running": [],
                    "retained": [], "sources": []}
        if marker is None or record.get("format_version") != FORMAT or marker.get("format_version") != FORMAT:
            raise ControlError(503, "QUERY_QUEUE_UNKNOWN")
        refs = record["pending"] + record["running"]
        if (len(record["pending"]) > MAX_PENDING or len(record["running"]) > MAX_RUNNING
                or len({entry["query_id"] for entry in refs}) != len(refs)
                or len(record.get("sources", [])) > MAX_SOURCE_READS
                or len({entry["capture_id"] for entry in record.get("sources", [])})
                != len(record.get("sources", []))):
            raise ControlError(503, "QUERY_QUEUE_UNKNOWN")
        return record

    def _put(self, key, record, previous):
        write = {"TableName": self.table, "Item": {**key, "record": {"S": canonical(record)},
            "revision": {"N": str(record["revision"])}},
            "ConditionExpression": "attribute_not_exists(PK)"}
        if "principal_id" in record:
            write["Item"]["owner"] = {"S": record["principal_id"]}
        if previous and previous["revision"]:
            write.update(ConditionExpression="#revision=:revision",
                ExpressionAttributeNames={"#revision": "revision"},
                ExpressionAttributeValues={":revision": {"N": str(previous["revision"])}})
        return {"Put": write}

    def _history_key(self, job):
        return self.key(job["space_id"], job["generation"],
            f"HISTORY#{job['principal_id']}#{int(job['created_unix'] * 1000000):020d}#{job['query_id']}")

    @staticmethod
    def _gc_key(job):
        return {"PK": {"S": f"QUERY#GC#{FORMAT}"},
                "SK": {"S": f"GC#{int(job['history_expires_unix'] * 1000000):020d}#{job['query_id']}"}}

    def _gc_write(self, job):
        return self._put(self._gc_key(job), {"revision": 1, **{name: job[name] for name in [
            "query_id", "space_id", "generation", "history_expires_unix"]}}, None)

    def _transaction(self, writes):
        try:
            self.ddb.transact_write_items(TransactItems=writes, ClientRequestToken=sha(writes)[:36])
            return True
        except ClientError as error:
            if error.response["Error"]["Code"] == "TransactionCanceledException":
                return False
            raise ControlError(503, "QUERY_STATE_UNKNOWN") from None
        except BotoCoreError:
            raise ControlError(503, "QUERY_STATE_UNKNOWN") from None

    def _authorize(self, actor, job, data=False):
        actor.authorize(job["space_id"], "query:read")
        if actor.generation != job["generation"]:
            raise ControlError(403, "SPACE_GENERATION_CHANGED")
        if actor.principal_id != job["principal_id"]:
            if data or "query:manage" not in actor.scopes:
                raise ControlError(403, "QUERY_OWNER_REQUIRED")

    def get(self, actor, space, generation, identity, *, data=False):
        for value in [space, generation, identity]:
            validate("identifier", value)
        actor.authorize(space, "query:read")
        if actor.generation != generation:
            raise ControlError(403, "SPACE_GENERATION_CHANGED")
        job = self._get(self.key(space, generation, identity))
        if job is None:
            raise ControlError(404, "QUERY_NOT_FOUND")
        if (job["query_id"] != identity or job["space_id"] != space
                or job["generation"] != generation or job["status"] not in STATES
                or job.get("format_version") != FORMAT):
            raise ControlError(503, "QUERY_STATE_UNKNOWN")
        self._authorize(actor, job, data)
        return job

    def _intent(self, space, generation, identity, version=None):
        key = f"spaces/{space}/query-intents/{generation}/{identity}.json"
        try:
            value = self.s3.get_object(Bucket=self.bucket, Key=key, ExpectedBucketOwner=self.account,
                                      **({"VersionId": version} if version else {}))
            with value["Body"] as stream:
                raw = stream.read(INTENT_BYTES + 1)
            original = decode_json(raw)
            reference = {"bucket": self.bucket, "key": key, "version_id": value["VersionId"],
                         "sha256": value["Metadata"]["sha256"], "bytes": len(raw)}
            if (len(raw) > INTENT_BYTES or hashlib.sha256(raw).hexdigest() != reference["sha256"]
                    or reference["version_id"] in {None, "", "null"}
                    or version is not None and version != reference["version_id"]
                    or original["query_id"] != identity or original["space_id"] != space
                    or original["generation"] != generation):
                raise ValueError("Immutable intent identity differs")
            return original, reference
        except (ClientError, BotoCoreError, KeyError, ValueError, TypeError):
            raise ControlError(503, "QUERY_INTENT_UNKNOWN") from None

    def admit(self, actor, configuration, request, client_id, guard=lambda: None):
        """Reconcile the same original after a lost reply; never create a replacement ID."""
        guard()
        space, generation = configuration["space_id"], configuration["generation"]
        for value in [space, generation]:
            validate("identifier", value)
        actor.authorize(space, "query:read")
        if actor.generation != generation:
            raise ControlError(403, "SPACE_GENERATION_CHANGED")
        validate("identifier", client_id)
        validate("query_request", request)
        if request["client_request_id"] != client_id:
            raise ControlError(400, "QUERY_CLIENT_ID_MISMATCH")
        identity = "qry_" + sha([space, generation, actor.principal_id, client_id])
        binding = sha(request)
        key = self.key(space, generation, identity)
        prior = self._get(key)
        if prior:
            self._authorize(actor, prior)
            if prior["binding_sha256"] != binding:
                raise ControlError(409, "QUERY_BINDING_CONFLICT")
            return prior
        queue = self._queue()
        if (len(queue["pending"]) >= MAX_PENDING or sum(
                item["space_id"] == space for item in queue["pending"] + queue["running"]) >= SPACE_PENDING):
            raise ControlError(429, "QUERY_QUEUE_QUOTA")
        original = {"query_id": identity, "space_id": space, "generation": generation,
            "principal_id": actor.principal_id, "key_id": actor.key_id, "binding_sha256": binding,
            "source_commit": self.source, "image_digest": self.image,
            "configuration": configuration, "request": request, "created_unix": self.clock()}
        raw = canonical(original).encode()
        if len(raw) > INTENT_BYTES:
            raise ControlError(413, "QUERY_INPUT_TOO_LARGE")
        digest = hashlib.sha256(raw).hexdigest()
        version = None
        try:
            value = self.s3.put_object(Bucket=self.bucket,
                Key=f"spaces/{space}/query-intents/{generation}/{identity}.json", Body=raw,
                IfNoneMatch="*", ExpectedBucketOwner=self.account, ContentType="application/json",
                Metadata={"sha256": digest},
                ChecksumSHA256=base64.b64encode(hashlib.sha256(raw).digest()).decode())
            version = value.get("VersionId")
        except (ClientError, BotoCoreError):
            pass  # Read this same original; do not replace an ambiguous put.
        stored, reference = self._intent(space, generation, identity, version)
        if stored["principal_id"] != actor.principal_id or stored["binding_sha256"] != binding:
            raise ControlError(409, "QUERY_BINDING_CONFLICT")
        if (stored["source_commit"] != self.source or stored["image_digest"] != self.image
                or stored["configuration"] != configuration):
            raise ControlError(409, "QUERY_INPUT_IDENTITY_CHANGED")
        job = {name: stored[name] for name in ["query_id", "space_id", "generation", "principal_id",
            "key_id", "binding_sha256", "source_commit", "image_digest", "created_unix"]}
        job.update(format_version=FORMAT, revision=1, status="QUEUED", input=reference)
        entry = {name: job[name] for name in ["query_id", "space_id", "generation",
                                            "principal_id", "source_commit", "image_digest"]}
        for _ in range(8):
            guard()
            prior = self._get(key)
            if prior:
                if prior["binding_sha256"] != binding:
                    raise ControlError(409, "QUERY_BINDING_CONFLICT")
                return prior
            queue = self._queue()
            refs = queue["pending"] + queue["running"]
            if (len(queue["pending"]) >= MAX_PENDING or sum(
                    item["space_id"] == space for item in refs) >= SPACE_PENDING):
                raise ControlError(429, "QUERY_QUEUE_QUOTA")
            changed = {**queue, "revision": queue["revision"] + 1,
                       "pending": queue["pending"] + [entry]}
            writes = [self._put(key, job, None), self._put(self._queue_key(), changed, queue)]
            writes += [self._put(self._history_key(job), {"revision": 1, "principal_id": actor.principal_id,
                                              "query_id": identity}, None)]
            if not queue["revision"]:
                writes += [self._put(self._marker_key(), {"format_version": FORMAT, "revision": 1}, None)]
            writes += [{"ConditionCheck": {"TableName": self.table,
                "Key": {"PK": {"S": "SPACE#" + space}, "SK": {"S": "META"}},
                "ConditionExpression": "generation=:generation AND #revision=:revision",
                "ExpressionAttributeNames": {"#revision": "revision"},
                "ExpressionAttributeValues": {":generation": {"S": generation},
                    ":revision": {"N": str(configuration["revision"])}}}}]
            try:
                if self._transaction(writes):
                    return job
            except ControlError:
                confirmed = self._get(key)
                if confirmed and confirmed["input"] == reference:
                    return confirmed
                raise
        raise ControlError(409, "QUERY_STATE_BUSY")

    def claim(self, executor):
        """Reserve one eligible space. Heartbeat expiry never releases execution custody."""
        if executor["source_commit"] != self.source or executor["image_digest"] != self.image:
            raise ControlError(503, "QUERY_EXECUTOR_IDENTITY_CHANGED")
        for _ in range(8):
            queue = self._queue()
            if len(queue["running"]) >= MAX_RUNNING:
                return None
            occupied = {item["space_id"] for item in queue["running"]}
            entry = next((item for item in queue["pending"] if item["space_id"] not in occupied
                and item["source_commit"] == self.source and item["image_digest"] == self.image), None)
            if entry is None:
                return None
            key = self.key(entry["space_id"], entry["generation"], entry["query_id"])
            prior = self._get(key)
            if not prior or prior["status"] != "QUEUED":
                raise ControlError(503, "QUERY_QUEUE_UNKNOWN")
            job = {**prior, "revision": prior["revision"] + 1, "status": "RUNNING",
                   "started_unix": self.clock(), "execution_id": "qexec_" + uuid.uuid4().hex,
                   "executor": executor, "process": None}
            changed = {**queue, "revision": queue["revision"] + 1,
                "pending": [item for item in queue["pending"] if item != entry],
                "running": queue["running"] + [entry]}
            try:
                if self._transaction([self._put(key, job, prior),
                                      self._put(self._queue_key(), changed, queue)]):
                    return job
            except ControlError:
                confirmed = self._get(key)
                if confirmed and confirmed.get("execution_id") == job["execution_id"]:
                    self._execution(confirmed, job)
                    return confirmed
                raise
        raise ControlError(409, "QUERY_STATE_BUSY")

    def cancel(self, actor, space, generation, identity):
        for _ in range(8):
            prior = self.get(actor, space, generation, identity)
            if prior["status"] in TERMINAL | {"CANCELLING", "UNKNOWN"}:
                return prior
            job = {**prior, "revision": prior["revision"] + 1, "cancelled_by": actor.principal_id}
            key = self.key(space, generation, identity)
            if prior["status"] == "RUNNING":
                job["status"] = "CANCELLING"
                writes = [self._put(key, job, prior)]
            else:
                queue = self._queue()
                entry = next((item for item in queue["pending"] if item["query_id"] == identity), None)
                if entry is None:
                    raise ControlError(503, "QUERY_QUEUE_UNKNOWN")
                changed = {**queue, "revision": queue["revision"] + 1,
                           "pending": [item for item in queue["pending"] if item != entry]}
                job.update(status="CANCELLED", finished_unix=self.clock(),
                           history_expires_unix=self.clock() + HISTORY_SECONDS,
                           terminal={"state": "NOT_STARTED", "outcome": "CANCELLED", "pid": None})
                writes = [self._put(key, job, prior), self._put(self._queue_key(), changed, queue),
                          self._gc_write(job)]
            if self._transaction(writes):
                return job
        raise ControlError(409, "QUERY_STATE_BUSY")

    def attach_process(self, job, process):
        identity = {name: process[name] for name in
                    ["pid", "start_ticks", "boot_id", "command_sha256"]}
        for _ in range(8):
            key = self.key(job["space_id"], job["generation"], job["query_id"])
            prior = self._get(key)
            self._execution(prior, job)
            if prior["process"] is not None:
                if prior["process"] != identity:
                    raise ControlError(503, "QUERY_PROCESS_CUSTODY_UNKNOWN")
                return prior
            changed = {**prior, "revision": prior["revision"] + 1, "process": identity}
            try:
                if self._transaction([self._put(key, changed, prior)]):
                    return changed
            except ControlError:
                confirmed = self._get(key)
                self._execution(confirmed, job)
                if confirmed.get("process") == identity:
                    return confirmed
                raise
        raise ControlError(409, "QUERY_STATE_BUSY")

    @staticmethod
    def _execution(prior, job):
        if (not prior or prior.get("execution_id") != job.get("execution_id")
                or prior.get("executor") != job.get("executor")
                or prior["source_commit"] != job["source_commit"]
                or prior["image_digest"] != job["image_digest"]):
            raise ControlError(503, "QUERY_EXECUTION_CUSTODY_UNKNOWN")

    def reserve_result(self, job, terminal, intent):
        """Persist bounded artifact custody before any S3 result upload."""
        expected = (f"spaces/{job['space_id']}/query-results/{job['generation']}/"
                    f"{job['query_id']}/{job['execution_id']}/manifest.json")
        if (not isinstance(intent, dict)
                or set(intent) != {"bucket", "key", "sha256", "bytes", "expires_unix"}
                or intent["bucket"] != self.bucket or intent["key"] != expected
                or not isinstance(intent["sha256"], str)
                or not re.fullmatch("[0-9a-f]{64}", intent["sha256"])
                or type(intent["bytes"]) is not int or not 1 <= intent["bytes"] <= 16777216
                or type(intent["expires_unix"]) not in {int, float}
                or not math.isfinite(intent["expires_unix"])):
            raise ControlError(503, "QUERY_RESULT_UNCONFIRMED")
        for _ in range(8):
            key = self.key(job["space_id"], job["generation"], job["query_id"])
            prior = self._get(key)
            self._execution(prior, job)
            if (terminal["state"] != "REAPED" or terminal["outcome"] != "SUCCEEDED"
                    or type(terminal["exit_code"]) is not int or terminal["exit_code"] != 0
                    or prior["process"] is None
                    or any(terminal.get(name) != value for name, value in prior["process"].items())):
                raise ControlError(503, "QUERY_PROCESS_CUSTODY_UNKNOWN")
            if prior.get("result_intent"):
                if prior["result_intent"] != intent or prior["result_terminal"] != terminal:
                    raise ControlError(409, "QUERY_RESULT_BINDING_CONFLICT")
                return prior
            if prior["status"] != "RUNNING":
                raise ControlError(409, "QUERY_CANCELLED")
            if intent["expires_unix"] <= self.clock():
                raise ControlError(410, "QUERY_RESULT_EXPIRED")
            queue = self._queue()
            entry = next((item for item in queue["running"] if item["query_id"] == job["query_id"]), None)
            if entry is None or any(item["query_id"] == job["query_id"] for item in queue["retained"]):
                raise ControlError(503, "QUERY_QUEUE_UNKNOWN")
            # Expiry alone does not release bytes. Unconfirmed deletions and
            # uploads retain their original charge until positive collection.
            retained = queue["retained"]
            if (len(retained) >= RETAINED_JOBS
                    or sum(item["bytes"] for item in retained) + intent["bytes"] > RETAINED_BYTES
                    or sum(item["bytes"] for item in retained if item["space_id"] == job["space_id"])
                    + intent["bytes"] > SPACE_RETAINED_BYTES):
                raise ControlError(429, "QUERY_RESULT_RETENTION_QUOTA")
            changed = {**prior, "revision": prior["revision"] + 1,
                       "result_intent": intent, "result_terminal": terminal}
            changed_queue = {**queue, "revision": queue["revision"] + 1,
                "retained": retained + [{**entry, "bytes": intent["bytes"],
                                        "expires_unix": intent["expires_unix"]}]}
            try:
                if self._transaction([self._put(key, changed, prior),
                                      self._put(self._queue_key(), changed_queue, queue)]):
                    return changed
            except ControlError:
                observed = self._get(key)
                if observed and observed.get("result_intent") == intent \
                        and observed.get("result_terminal") == terminal:
                    return observed
                raise
        raise ControlError(409, "QUERY_STATE_BUSY")

    def finish(self, job, terminal, result=None, *, failure_code=None, expires_unix=None):
        """Release capacity only with the original process's positive terminal."""
        for _ in range(8):
            key = self.key(job["space_id"], job["generation"], job["query_id"])
            prior = self._get(key)
            self._execution(prior, job)
            if prior["status"] in TERMINAL:
                return prior
            not_started = (terminal["state"] == "NOT_STARTED" and terminal.get("pid") is None
                and terminal.get("exit_code") is None and prior["process"] is None
                and terminal["outcome"] in {"CANCELLED", "LAUNCH_FAILED"})
            reaped = (terminal["state"] == "REAPED" and type(terminal["exit_code"]) is int
                and prior["process"] is not None and all(
                    terminal.get(name) == value for name, value in prior["process"].items()))
            if not (not_started or reaped):
                raise ControlError(503, "QUERY_PROCESS_CUSTODY_UNKNOWN")
            queue = self._queue()
            entry = next((item for item in queue["running"] if item["query_id"] == job["query_id"]), None)
            if entry is None:
                raise ControlError(503, "QUERY_QUEUE_UNKNOWN")
            cancelled = prior["status"] == "CANCELLING" or terminal["outcome"] == "CANCELLED"
            success = terminal["outcome"] == "SUCCEEDED" and terminal["exit_code"] == 0 and failure_code is None
            if success and not cancelled and result is None:
                raise ControlError(503, "QUERY_RESULT_UNCONFIRMED")
            changed = {**prior, "revision": prior["revision"] + 1,
                "status": "CANCELLED" if cancelled else ("SUCCEEDED" if success else "FAILED"),
                "finished_unix": self.clock(), "terminal": terminal,
                "history_expires_unix": self.clock() + HISTORY_SECONDS}
            if success and not cancelled:
                self._result_reference(job, result)
                intent = prior.get("result_intent")
                if (not intent or any(result[name] != intent[name] for name in
                        ["bucket", "key", "sha256", "bytes"])
                        or terminal != prior["result_terminal"] or expires_unix != intent["expires_unix"]):
                    raise ControlError(503, "QUERY_RESULT_UNCONFIRMED")
                held = next((item for item in queue["retained"] if item["query_id"] == job["query_id"]), None)
                if held is None or held["bytes"] != intent["bytes"] or held["expires_unix"] != intent["expires_unix"]:
                    raise ControlError(503, "QUERY_RESULT_CUSTODY_UNKNOWN")
                changed["result"] = result
                changed["result_expires_unix"] = expires_unix
            if failure_code:
                changed["error_code"] = failure_code
            changed_queue = {**queue, "revision": queue["revision"] + 1,
                             "running": [item for item in queue["running"] if item != entry]}
            if self._transaction([self._put(key, changed, prior),
                                  self._put(self._queue_key(), changed_queue, queue), self._gc_write(changed)]):
                return changed
        raise ControlError(409, "QUERY_STATE_BUSY")

    def mark_unknown(self, job, code):
        for _ in range(8):
            key = self.key(job["space_id"], job["generation"], job["query_id"])
            prior = self._get(key)
            self._execution(prior, job)
            if prior["status"] in TERMINAL:
                return prior
            changed = {**prior, "revision": prior["revision"] + 1, "status": "UNKNOWN", "error_code": code}
            if self._transaction([self._put(key, changed, prior)]):
                return changed
        raise ControlError(409, "QUERY_STATE_BUSY")

    def running_jobs(self):
        values = []
        for entry in self._queue()["running"]:
            job = self._get(self.key(entry["space_id"], entry["generation"], entry["query_id"]))
            if not job or job["status"] not in {"RUNNING", "CANCELLING", "UNKNOWN"}:
                raise ControlError(503, "QUERY_QUEUE_UNKNOWN")
            values.append(job)
        return values

    def reconcile_stopped(self, job, task):
        """Positive ECS death releases capacity, never proves SQL outcome or retries."""
        executor = job["executor"]
        application = [container for container in task.get("containers", []) if container["name"] == "baseline"]
        if (task.get("taskArn") != executor["task_arn"]
                or task.get("taskDefinitionArn") != executor.get("task_definition")
                or task.get("lastStatus") != "STOPPED" or task.get("stoppedAt") is None
                or len(application) != 1 or application[0].get("lastStatus") != "STOPPED"
                or application[0].get("imageDigest") != executor["image_digest"]):
            raise ControlError(503, "QUERY_EXECUTION_CUSTODY_UNKNOWN")
        proof = {"task_arn": task["taskArn"], "task_definition": task["taskDefinitionArn"],
            "image_digest": application[0]["imageDigest"], "last_status": "STOPPED",
            "stopped_at": task["stoppedAt"].isoformat()}
        for _ in range(8):
            key = self.key(job["space_id"], job["generation"], job["query_id"])
            prior = self._get(key)
            self._execution(prior, job)
            if prior["status"] in TERMINAL or prior.get("task_stop") == proof:
                return prior
            queue = self._queue()
            entry = next((item for item in queue["running"] if item["query_id"] == job["query_id"]), None)
            if entry is None:
                raise ControlError(503, "QUERY_QUEUE_UNKNOWN")
            changed = {**prior, "revision": prior["revision"] + 1, "status": "UNKNOWN",
                "error_code": "QUERY_EXECUTOR_STOPPED_UNCONFIRMED", "task_stop": proof,
                "capacity_released_unix": self.clock(), "history_expires_unix": self.clock() + HISTORY_SECONDS}
            changed_queue = {**queue, "revision": queue["revision"] + 1,
                             "running": [item for item in queue["running"] if item != entry]}
            if self._transaction([self._put(key, changed, prior),
                                  self._put(self._queue_key(), changed_queue, queue), self._gc_write(changed)]):
                return changed
        raise ControlError(409, "QUERY_STATE_BUSY")

    def expired_results(self):
        return [entry for entry in self._queue()["retained"] if entry["expires_unix"] <= self.clock()]

    def release_result(self, job):
        for _ in range(8):
            queue = self._queue()
            entry = next((item for item in queue["retained"] if item["query_id"] == job["query_id"]), None)
            if entry is None:
                return
            retired = job["status"] in TERMINAL or job["status"] == "UNKNOWN" and job.get("task_stop")
            unpublished = job["status"] in {"FAILED", "CANCELLED"} and "result" not in job
            if not retired or entry["expires_unix"] > self.clock() and not unpublished:
                raise ControlError(503, "QUERY_RESULT_CUSTODY_UNKNOWN")
            changed = {**queue, "revision": queue["revision"] + 1,
                       "retained": [item for item in queue["retained"] if item != entry]}
            if self._transaction([self._put(self._queue_key(), changed, queue)]):
                return
        raise ControlError(409, "QUERY_STATE_BUSY")

    def history(self, actor, space, generation, limit=50):
        actor.authorize(space, "query:read")
        if actor.generation != generation or type(limit) is not int or not 1 <= limit <= 100:
            raise ControlError(403, "FORBIDDEN")
        response = self.ddb.query(TableName=self.table, ConsistentRead=True,
            KeyConditionExpression="PK=:pk AND begins_with(SK,:prefix)", ExpressionAttributeValues={
                ":pk": {"S": f"QUERY#{space}#{generation}"},
                ":prefix": {"S": f"HISTORY#{actor.principal_id}#"}},
            ScanIndexForward=False, Limit=limit)
        jobs = []
        for item in response.get("Items", []):
            reference = decode_json(item["record"]["S"].encode())
            job = self.get(actor, space, generation, reference["query_id"], data=True)
            if self.clock() < job.get("history_expires_unix", float("inf")):
                jobs.append(job)
        return sorted(jobs, key=lambda job: job["created_unix"], reverse=True)[:limit]

    def expired_history(self, limit=4):
        try:
            response = self.ddb.query(TableName=self.table, ConsistentRead=True,
                KeyConditionExpression="PK=:pk AND SK<=:until", ExpressionAttributeValues={
                    ":pk": {"S": f"QUERY#GC#{FORMAT}"}, ":until": {"S": f"GC#{int(self.clock() * 1000000):020d}~"}},
                ScanIndexForward=True, Limit=limit)
            return [decode_json(item["record"]["S"].encode()) for item in response.get("Items", [])]
        except (ClientError, BotoCoreError, ValueError, KeyError):
            raise ControlError(503, "QUERY_HISTORY_CLEANUP_UNKNOWN") from None

    def collect_history(self, job):
        """Delete only a terminal original intent after history and result references expire."""
        if (not (job["status"] in TERMINAL or job["status"] == "UNKNOWN" and job.get("task_stop"))
                or self.clock() < job.get("history_expires_unix", float("inf"))
                or self.clock() < job.get("result_expires_unix", 0)
                or job.get("source_read", {}).get("state", "CLOSED") not in {"CLOSED", "STOPPED"}):
            raise ControlError(503, "QUERY_HISTORY_CUSTODY_UNKNOWN")
        if not job.get("input_collected_unix"):
            # Reconcile an interrupted exact-version deletion before re-reading.
            try:
                original, reference = self._intent(job["space_id"], job["generation"], job["query_id"],
                                                   job["input"]["version_id"])
            except ControlError:
                if not self._intent_absent(job["input"]):
                    raise
            else:
                if reference != job["input"] or original["binding_sha256"] != job["binding_sha256"]:
                    raise ControlError(503, "QUERY_HISTORY_CUSTODY_UNKNOWN")
                try:
                    self.s3.delete_object(Bucket=self.bucket, Key=reference["key"],
                        VersionId=reference["version_id"], ExpectedBucketOwner=self.account)
                except (ClientError, BotoCoreError):
                    pass
                if not self._intent_absent(reference):
                    raise ControlError(503, "QUERY_HISTORY_CLEANUP_UNKNOWN")
        for _ in range(8):
            key = self.key(job["space_id"], job["generation"], job["query_id"])
            prior = self._get(key)
            if prior.get("input_collected_unix"):
                return
            if prior["status"] != job["status"] or prior["input"] != job["input"]:
                raise ControlError(503, "QUERY_HISTORY_CUSTODY_UNKNOWN")
            changed = {**prior, "revision": prior["revision"] + 1, "input_collected_unix": self.clock()}
            deletes = [{"Delete": {"TableName": self.table, "Key": reference_key,
                        "ConditionExpression": "#revision=:revision",
                        "ExpressionAttributeNames": {"#revision": "revision"},
                        "ExpressionAttributeValues": {":revision": {"N": "1"}}}}
                       for reference_key in [self._history_key(job), self._gc_key(job)]]
            if self._transaction([self._put(key, changed, prior), *deletes]):
                return
        raise ControlError(409, "QUERY_STATE_BUSY")

    def _intent_absent(self, reference):
        return version_absent(self.s3, self.bucket, self.account, reference,
                              "QUERY_HISTORY_CLEANUP_UNKNOWN")

    def _result_reference(self, job, reference):
        expected = (f"spaces/{job['space_id']}/query-results/{job['generation']}/"
                    f"{job['query_id']}/{job['execution_id']}/manifest.json")
        if (not isinstance(reference, dict)
                or set(reference) != {"bucket", "key", "version_id", "sha256", "bytes"}
                or reference["bucket"] != self.bucket or reference["key"] != expected
                or not isinstance(reference["version_id"], str)
                or reference["version_id"] in {"", "null"}
                or not isinstance(reference["sha256"], str)
                or not re.fullmatch("[0-9a-f]{64}", reference["sha256"])
                or type(reference["bytes"]) is not int or not 1 <= reference["bytes"] <= 16777216):
            raise ControlError(503, "QUERY_RESULT_UNCONFIRMED")

    def input(self, job):
        if self.clock() >= job.get("history_expires_unix", float("inf")):
            raise ControlError(410, "QUERY_HISTORY_EXPIRED")
        original, reference = self._intent(job["space_id"], job["generation"], job["query_id"],
                                          job["input"]["version_id"])
        if reference != job["input"] or any(original[name] != job[name] for name in [
                "binding_sha256", "principal_id", "source_commit", "image_digest", "key_id"]):
            raise ControlError(503, "QUERY_INTENT_UNKNOWN")
        if sha(original["request"]) != original["binding_sha256"]:
            raise ControlError(503, "QUERY_INTENT_UNKNOWN")
        return original

    def source_admit(self, job, producer):
        """One bounded controlled read per original execution, even after reply loss."""
        capture_id = "qsource_" + uuid.uuid4().hex
        source = {"capture_id": capture_id, "state": "CAPTURING", "producer": producer,
                  "started_unix": self.clock()}
        for _ in range(8):
            key = self.key(job["space_id"], job["generation"], job["query_id"])
            prior = self._get(key)
            self._execution(prior, job)
            if (prior["source_commit"] != self.source or prior["image_digest"] != self.image
                    or prior["input"] != job["input"]):
                raise ControlError(503, "QUERY_EXECUTION_CUSTODY_UNKNOWN")
            if prior.get("source_read"):
                raise ControlError(503, "QUERY_SOURCE_ALREADY_CAPTURED")
            if prior["status"] != "RUNNING":
                raise ControlError(409, "QUERY_CANCELLED")
            queue = self._queue()
            sources = queue.get("sources", [])
            if (len(sources) >= MAX_SOURCE_READS or sum(
                    item["space_id"] == job["space_id"] for item in sources) >= SPACE_SOURCE_READS):
                raise ControlError(429, "QUERY_SOURCE_RETENTION_QUOTA")
            reference = {name: job[name] for name in [
                "query_id", "execution_id", "space_id", "generation"]}
            reference.update(capture_id=capture_id, producer=producer)
            changed = {**prior, "revision": prior["revision"] + 1, "source_read": source}
            changed_queue = {**queue, "revision": queue["revision"] + 1, "sources": sources + [reference]}
            try:
                if self._transaction([self._put(key, changed, prior),
                                      self._put(self._queue_key(), changed_queue, queue)]):
                    return changed
            except ControlError:
                observed = self._get(key)
                self._execution(observed, job)
                if observed.get("source_read") == source:
                    return observed
                raise
        raise ControlError(409, "QUERY_STATE_BUSY")

    def source_closed(self, job, proof):
        """Record positive read closure independently of cancellation/key expiry."""
        for _ in range(8):
            key = self.key(job["space_id"], job["generation"], job["query_id"])
            prior = self._get(key)
            self._execution(prior, job)
            expected, current = job.get("source_read", {}), prior.get("source_read", {})
            if any(current.get(name) != expected.get(name) for name in ["capture_id", "producer"]):
                raise ControlError(503, "QUERY_SOURCE_UNCONFIRMED")
            if current["state"] == "CLOSED":
                if current["proof"] != proof:
                    raise ControlError(503, "QUERY_SOURCE_UNCONFIRMED")
                return prior
            if current["state"] != "CAPTURING":
                raise ControlError(503, "QUERY_SOURCE_UNCONFIRMED")
            queue = self._queue()
            sources = queue.get("sources", [])
            if not any(item["capture_id"] == current["capture_id"] for item in sources):
                raise ControlError(503, "QUERY_SOURCE_UNCONFIRMED")
            changed = {**prior, "revision": prior["revision"] + 1, "source_read": {
                **current, "state": "CLOSED", "finished_unix": self.clock(), "proof": proof}}
            changed_queue = {**queue, "revision": queue["revision"] + 1, "sources": [
                item for item in sources if item["capture_id"] != current["capture_id"]]}
            try:
                if self._transaction([self._put(key, changed, prior),
                                      self._put(self._queue_key(), changed_queue, queue)]):
                    return changed
            except ControlError:
                observed = self._get(key)
                self._execution(observed, job)
                if (observed.get("source_read", {}).get("state") == "CLOSED"
                        and observed["source_read"].get("proof") == proof):
                    return observed
                raise
        raise ControlError(409, "QUERY_STATE_BUSY")

    def source_reads(self):
        records = []
        for reference in self._queue().get("sources", []):
            job = self._get(self.key(reference["space_id"], reference["generation"], reference["query_id"]))
            source = job.get("source_read", {}) if job else {}
            if (source.get("capture_id") != reference["capture_id"]
                    or source.get("producer") != reference["producer"]
                    or job["execution_id"] != reference["execution_id"]):
                raise ControlError(503, "QUERY_SOURCE_UNCONFIRMED")
            if source.get("state") in {"CLOSED", "STOPPED"}:
                continue  # Closure can commit after the preceding queue read.
            if source.get("state") != "CAPTURING":
                raise ControlError(503, "QUERY_SOURCE_UNCONFIRMED")
            records.append(job)
        return records

    def source_stopped(self, job, task):
        source = job.get("source_read", {})
        producer = source.get("producer", {})
        if (source.get("state") != "CAPTURING" or task.get("lastStatus") != "STOPPED"
                or task.get("taskArn") != producer.get("task_arn")
                or task.get("taskDefinitionArn") != producer.get("task_definition")):
            raise ControlError(503, "QUERY_SOURCE_UNCONFIRMED")
        for _ in range(8):
            key = self.key(job["space_id"], job["generation"], job["query_id"])
            prior = self._get(key)
            self._execution(prior, job)
            if prior["source_read"] != source:
                return prior
            queue = self._queue()
            changed = {**prior, "revision": prior["revision"] + 1, "source_read": {
                **source, "state": "STOPPED", "task_stop": {name: task.get(name) for name in [
                    "taskArn", "taskDefinitionArn", "lastStatus", "stopCode", "stoppedReason"]}}}
            changed_queue = {**queue, "revision": queue["revision"] + 1, "sources": [
                item for item in queue.get("sources", []) if item["capture_id"] != source["capture_id"]]}
            if self._transaction([self._put(key, changed, prior),
                                  self._put(self._queue_key(), changed_queue, queue)]):
                return changed
        raise ControlError(409, "QUERY_STATE_BUSY")
