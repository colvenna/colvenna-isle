"""Space overview, metrics and quota usage from bounded authoritative observations.

Every section carries its own observation time and source. A missing, stale or
unavailable source is UNKNOWN with a reason; a container that merely exists is
never reported as a healthy writer, and the overview never scans whole tables.
"""

import json
import math
import time
from datetime import datetime, timedelta, timezone

from botocore.exceptions import BotoCoreError, ClientError

from colvenna_isle.contracts.validation import validate
from .quotas import usage
from .registry import ControlError

HEARTBEAT_SECONDS = 15
LAYERS = ["ods", "dwd", "dws", "ads"]
QUERY_GC_INDEX = "QUERY#GC#3"
SPACE_QUERY_PENDING, SPACE_QUERY_RETAINED_BYTES = 8, 33554432


def stamp(value):
    if value is None:
        return None
    return datetime.fromtimestamp(value, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def observation(status, observed, source, metrics=None, reason=None, now=None):
    value = {"status": status, "observed_at": stamp(observed), "source": source,
             "age_seconds": round(max(0.0, (now or time.time()) - observed), 3) if observed is not None else None,
             "metrics": metrics or {}}
    if reason:
        value["reason"] = reason
    return value


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, math.ceil(fraction * len(ordered)) - 1))]


class Overview:
    def __init__(self, registry, keys, ownership, writer, runs, admission, quotas, *, environments=None,
                 clock=time.time):
        self.registry, self.keys, self.ownership, self.writer = registry, keys, ownership, writer
        self.runs, self.admission, self.quotas, self.environments = runs, admission, quotas, environments
        self.client, self.table, self.clock = registry.client, registry.table, clock

    def _configuration(self, principal, space, scope="monitor:read"):
        principal.authorize(space, scope)
        configuration = self.registry._read(space)
        self.registry._authorize_generation(principal, configuration)
        if self.keys.authenticate_id(principal.key_id or "") != principal:
            raise ControlError(403, "FORBIDDEN")
        return configuration

    # -- sources ---------------------------------------------------------------------------------

    def _writer(self, configuration, now):
        try:
            permit = self.ownership.read()
        except (ClientError, BotoCoreError, ControlError):
            return observation("UNKNOWN", None, "WRITER_PERMIT", reason="WRITER_PERMIT_UNAVAILABLE", now=now), None
        identity = permit.get("identity") or {}
        metrics = {"permit_state": permit["state"], "epoch": permit.get("epoch"),
                   "task_arn": identity.get("task_arn"), "image_digest": identity.get("image_digest"),
                   "source_commit": identity.get("source_commit"), "permit_observed_at": stamp(permit.get("observed_at"))}
        if permit["state"] == "STOPPED":
            return observation("STOPPED", permit.get("observed_at"), "WRITER_PERMIT", metrics,
                               "WRITER_STOPPED_CONFIRMED", now), None
        if permit["state"] != "ACTIVE" or not identity:
            return observation("DEGRADED", permit.get("observed_at"), "WRITER_PERMIT", metrics,
                               "WRITER_PERMIT_" + permit["state"], now), None
        try:
            registration = self.ownership.registration(identity["task_arn"])
        except (ClientError, BotoCoreError, ControlError):
            registration = None
        if not registration:
            return observation("UNKNOWN", None, "WRITER_HEARTBEAT", metrics, "HEARTBEAT_MISSING", now), None
        if registration["identity"] != identity:
            return observation("UNKNOWN", registration["observed_at"], "WRITER_HEARTBEAT", metrics,
                               "HEARTBEAT_IDENTITY_MISMATCH", now), None
        last = registration.get("last_commit") or {}
        own = last.get("space_id") == configuration["space_id"] and last.get("generation") == configuration["generation"]
        metrics.update(heartbeat_state=registration["state"], opened_databases=len(registration.get("opened_databases", [])),
                       last_commit_sequence=last.get("commit_sequence") if own else None,
                       last_commit_ingestion_id=last.get("ingestion_id") if own else None)
        age = now - registration["observed_at"]
        metrics["heartbeat_age_seconds"] = round(age, 3)
        if age > HEARTBEAT_SECONDS:
            return observation("UNKNOWN", registration["observed_at"], "WRITER_HEARTBEAT", metrics,
                               "HEARTBEAT_STALE", now), None
        if registration["state"] != "ACTIVE":
            return observation("DEGRADED", registration["observed_at"], "WRITER_HEARTBEAT", metrics,
                               "WRITER_PROCESS_" + str(registration["state"]), now), None
        return observation("OK", registration["observed_at"], "WRITER_HEARTBEAT", metrics, now=now), permit

    def _native(self, principal, configuration, permit):
        if permit is None:
            return None, "WRITER_AUTHORITY_UNAVAILABLE"
        try:
            return self.writer.call(principal, configuration, "space_overview"), None
        except ControlError as error:
            return None, error.code

    def _queue(self, configuration, now):
        try:
            value = self.admission.observe(configuration)
        except (ClientError, BotoCoreError, ControlError, KeyError, ValueError):
            unknown = observation("UNKNOWN", None, "SQS_ATTRIBUTES", reason="QUEUE_OBSERVATION_UNAVAILABLE", now=now)
            return unknown, dict(unknown, source="SQS_ATTRIBUTES_DLQ")
        observed = datetime.fromisoformat(value["observed_at"]).timestamp()
        capacity = value.get("capacity") or {}
        metrics = {"pending_messages": value["pending_messages"], "oldest_seconds": value["oldest_seconds"],
                   "oldest_observed_at": value["oldest_observed_at"], "queue_scope": value["queue_scope"],
                   "writer_capacity_state": capacity.get("state"),
                   "writer_batches_per_second": capacity.get("batches_per_second")}
        policy = configuration.get("ingestion_policy") or {}
        if value["oldest_seconds"] is None:
            queue = observation("UNKNOWN", observed, "SQS_ATTRIBUTES+CLOUDWATCH", metrics, "QUEUE_AGE_UNKNOWN", now)
        elif (value["pending_messages"] >= policy.get("max_pending_messages", math.inf)
              or value["oldest_seconds"] >= policy.get("max_oldest_seconds", math.inf)):
            queue = observation("DEGRADED", observed, "SQS_ATTRIBUTES+CLOUDWATCH", metrics, "BACKPRESSURE_ACTIVE", now)
        else:
            queue = observation("OK", observed, "SQS_ATTRIBUTES+CLOUDWATCH", metrics, now=now)
        dlq = observation("DEGRADED" if value["dlq_messages"] else "OK", observed, "SQS_ATTRIBUTES_DLQ",
                          {"messages": value["dlq_messages"], "queue_scope": value["queue_scope"]},
                          "DEAD_LETTERS_PRESENT" if value["dlq_messages"] else None, now)
        return queue, dlq

    def _active_runs(self, configuration):
        records = []
        for space, generation, pipeline, run_id in self.runs.active():
            if (space, generation) != (configuration["space_id"], configuration["generation"]):
                continue
            record = self.runs.pipelines._read(self.runs._key(configuration, pipeline, run_id), missing=None)
            if record is not None:
                records.append(record)
        return records

    def _jobs(self, configuration, now):
        try:
            records = self._active_runs(configuration)
        except (ClientError, BotoCoreError, ControlError):
            return observation("UNKNOWN", None, "RUN_RECORDS", reason="RUN_RECORDS_UNAVAILABLE", now=now), None
        counts = {name: 0 for name in ["QUEUED", "RUNNING", "CANCELLING", "UNKNOWN"]}
        dispatched, waiting, transient = 0, 0, {}
        for record in records:
            counts[record["status"]] = counts.get(record["status"], 0) + 1
            waiting += record.get("waiting") == "SPACE_RUN_QUOTA"
            for job in record["tasks"].values():
                dispatched += job["status"] in {"RUNNING", "STAGED", "IMPORTING"}
                if job.get("transient"):
                    transient[job["transient"]["code"]] = transient.get(job["transient"]["code"], 0) + 1
        oldest = min((record["created_unix"] for record in records), default=None)
        metrics = {"active_runs": len(records), **{"runs_" + name.lower(): value for name, value in counts.items()},
                   "dispatched_jobs": dispatched, "quota_waiting_runs": waiting,
                   "oldest_active_age_seconds": round(now - oldest, 3) if oldest else None,
                   **{"transient." + code.lower(): value for code, value in sorted(transient.items())[:16]}}
        status = "DEGRADED" if counts["UNKNOWN"] else "OK"
        return observation(status, now, "RUN_RECORDS", metrics,
                           "RUN_RECONCILIATION_PENDING" if counts["UNKNOWN"] else None, now), records

    def _query_queue(self, configuration, now):
        try:
            item = self.client.get_item(TableName=self.table, ConsistentRead=True, Key={
                "PK": {"S": "QUERY#QUEUE"}, "SK": {"S": "META"}}).get("Item")
            queue = json.loads(item["record"]["S"]) if item else {"pending": [], "running": [], "retained": []}
        except (ClientError, BotoCoreError, KeyError, ValueError):
            return None
        space = configuration["space_id"]
        mine = [entry for entry in queue.get("retained", []) if entry.get("space_id") == space]
        return {"pending": sum(entry["space_id"] == space for entry in queue["pending"]),
                "running": sum(entry["space_id"] == space for entry in queue["running"]),
                "retained_results": len(mine), "retained_bytes": sum(entry.get("bytes", 0) for entry in mine),
                "global_pending": len(queue["pending"]), "global_running": len(queue["running"])}

    def _queries(self, configuration, now):
        value = self._query_queue(configuration, now)
        if value is None:
            return observation("UNKNOWN", None, "QUERY_QUEUE", reason="QUERY_QUEUE_UNAVAILABLE", now=now)
        return observation("OK", now, "QUERY_QUEUE", value, now=now)

    # -- public views ----------------------------------------------------------------------------

    def status(self, principal, space):
        configuration = self._configuration(principal, space)
        now = self.clock()
        writer, permit = self._writer(configuration, now)
        native, reason = self._native(principal, configuration, permit)
        queue, dlq = self._queue(configuration, now)
        jobs, _ = self._jobs(configuration, now)
        datasets, layers = self._datasets(configuration, native, reason, now)
        scheduler = (observation("OK", native["observed_unix"], "WRITER_SCHEDULER", self._scheduler(native), now=now)
                     if native else observation("UNKNOWN", None, "WRITER_SCHEDULER", reason=reason, now=now))
        if native:
            writer["metrics"].update(consumer_paused=native["consumer"]["paused"])
            if native["consumer"]["paused"]:
                writer.update(status="DEGRADED", reason="CONSUMER_PAUSED")
        result = {"space_id": space, "generation": configuration["generation"], "observed_at": stamp(now),
                  "writer": writer, "queue": queue, "dlq": dlq, "layers": layers, "datasets": datasets,
                  "jobs": jobs, "queries": self._queries(configuration, now), "scheduler": scheduler}
        validate("admin_status", result)
        return result

    @staticmethod
    def _scheduler(native):
        scheduler = native["scheduler"]
        metrics = {"pending": scheduler["pending"], "max_pending": scheduler["max_pending"]}
        for kind, count in scheduler["queued"].items():
            metrics["queued." + kind] = count
        for kind, value in scheduler.get("wait_ms", {}).items():
            metrics["wait_ms_p95." + kind] = value.get("p95")
            metrics["wait_ms_max." + kind] = value.get("max")
        for kind, count in scheduler.get("rejected", {}).items():
            metrics["rejected." + kind] = count
        return metrics

    def _datasets(self, configuration, native, reason, now):
        observed = native["observed_unix"] if native else None
        versions = {value["dataset"]: value for value in (native or {}).get("datasets", [])}
        datasets, layers = [], {}
        for name, registered in sorted(configuration["datasets"].items()):
            definition = registered["definition"]
            owner = definition.get("write_owner", "ingestion" if name.startswith("ods.") else "pipeline")
            version = versions.get(name)
            if version is None:
                status = "UNKNOWN"
            elif owner == "pipeline" and version["release_id"] is None:
                status = "NOT_PUBLISHED"
            else:
                status = "OK"
            datasets.append({"dataset": name, "layer": name.split(".", 1)[0], "write_owner": owner, "status": status,
                             "commit_sequence": version["commit_sequence"] if version else None,
                             "release_id": version["release_id"] if version else None,
                             "published_at": stamp(version.get("published_unix")) if version else None,
                             "snapshot_sequence": version.get("snapshot_sequence") if version else None,
                             "observed_at": stamp(observed) if version else None})
        for layer in LAYERS:
            members = [value for value in datasets if value["layer"] == layer]
            if not members:
                continue
            if native is None:
                layers[layer] = observation("UNKNOWN", None, "NATIVE_LEDGER", {"datasets": len(members)}, reason, now)
                continue
            known = [value for value in members if value["status"] != "UNKNOWN"]
            published = [value["published_at"] for value in members if value["published_at"]]
            metrics = {"datasets": len(members), "max_commit_sequence": max(
                (value["commit_sequence"] or 0 for value in known), default=None),
                "not_published": sum(value["status"] == "NOT_PUBLISHED" for value in members),
                "latest_published_at": max(published) if published else None}
            status = "OK" if len(known) == len(members) else "UNKNOWN"
            layers[layer] = observation(status, observed, "NATIVE_LEDGER", metrics,
                                        None if status == "OK" else "DATASET_VERSION_MISSING", now)
        return datasets, layers

    def metrics(self, principal, space, window=3600):
        configuration = self._configuration(principal, space)
        if type(window) is not int or not 60 <= window <= 86400:
            raise ControlError(400, "INVALID_METRIC_WINDOW")
        now = self.clock()
        series = []

        def add(name, value, unit, observed, source, samples=1, reason=None):
            entry = {"name": name, "value": value, "unit": unit,
                     "status": "OK" if value is not None and observed is not None else "UNKNOWN",
                     "observed_at": stamp(observed), "window_seconds": window, "samples": samples, "source": source}
            if entry["status"] == "UNKNOWN":
                entry["reason"] = reason or "OBSERVATION_UNAVAILABLE"
            series.append(entry)

        writer, permit = self._writer(configuration, now)
        add("writer.heartbeat_age_seconds", writer["metrics"].get("heartbeat_age_seconds")
            if writer["status"] in {"OK", "DEGRADED"} else None, "s", writer["observed_at"] and now,
            "WRITER_HEARTBEAT", reason=writer.get("reason"))
        queue, dlq = self._queue(configuration, now)
        observed = now if queue["observed_at"] else None
        add("queue.pending_messages", queue["metrics"].get("pending_messages"), "count", observed, "SQS_ATTRIBUTES",
            reason=queue.get("reason"))
        add("queue.oldest_seconds", queue["metrics"].get("oldest_seconds"), "s", observed, "CLOUDWATCH_SQS",
            reason=queue.get("reason"))
        add("queue.dlq_messages", dlq["metrics"].get("messages"), "count", observed, "SQS_ATTRIBUTES_DLQ",
            reason=dlq.get("reason"))
        native, reason = self._native(principal, configuration, permit)
        latency = (native or {}).get("ingestion_latency") or {}
        for name in ["p50_ms", "p95_ms", "max_ms"]:
            add("ingestion.commit_latency_" + name.removesuffix("_ms"), latency.get(name), "ms",
                native and native["observed_unix"], "WRITER_CONSUMER", latency.get("samples", 0),
                reason if native is None else "NO_RECENT_COMMITS")
        add("ingestion.writer_batches_per_second", queue["metrics"].get("writer_batches_per_second"), "1/s",
            observed if queue["metrics"].get("writer_capacity_state") == "MEASURED" else None, "WRITER_CONSUMER",
            reason="WRITER_CAPACITY_UNMEASURED")
        if native:
            for kind, value in native["scheduler"].get("wait_ms", {}).items():
                add("writer.wait_ms_p95." + kind, value.get("p95"), "ms", native["observed_unix"], "WRITER_SCHEDULER",
                    value.get("samples", 0), "NO_RECENT_WORK")
        self._run_metrics(configuration, window, now, add)
        self._query_metrics(configuration, window, now, add)
        result = {"space_id": space, "observed_at": stamp(now), "window_seconds": window, "series": series[:128]}
        validate("metrics", result)
        return result

    def _history(self, configuration, since):
        """Runs created inside the window, newest first, bounded per pipeline."""
        records = []
        prefix = "PIPELINE#" + configuration["generation"] + "#"
        for item in self.runs.pipelines._records(configuration, prefix, 512):
            pipeline = item["SK"]["S"].rsplit("#", 1)[1]
            page = self.client.query(
                TableName=self.table, ConsistentRead=True, ScanIndexForward=False, Limit=100,
                KeyConditionExpression="PK = :pk AND SK BETWEEN :low AND :high",
                ExpressionAttributeValues={
                    ":pk": {"S": "SPACE#" + configuration["space_id"]},
                    ":low": {"S": f"RUNHISTORY#{configuration['generation']}#{pipeline}#{int(since * 1000000):020d}"},
                    ":high": {"S": f"RUNHISTORY#{configuration['generation']}#{pipeline}#~"}})
            for entry in page.get("Items", []):
                record = self.runs.pipelines._read(self.runs._key(configuration, pipeline, entry["run_id"]["S"]),
                                                   missing=None)
                if record is not None:
                    records.append(record)
        return records

    def _run_metrics(self, configuration, window, now, add):
        try:
            records = self._history(configuration, now - window)
        except (ClientError, BotoCoreError, ControlError):
            for name in ["runs.published", "runs.failed", "runs.duration_p95", "tasks.attempt_failures"]:
                add(name, None, "count", None, "RUN_RECORDS", 0, "RUN_RECORDS_UNAVAILABLE")
            return
        finished = [record for record in records if record.get("finished_unix")]
        durations = [record["finished_unix"] - record["created_unix"] for record in finished]
        add("runs.created", len(records), "count", now, "RUN_RECORDS", len(records))
        for status in ["PUBLISHED", "FAILED", "CANCELLED", "VALIDATED"]:
            add("runs." + status.lower(), sum(record["status"] == status for record in finished), "count", now,
                "RUN_RECORDS", len(finished))
        add("runs.duration_p50", percentile(durations, 0.5), "s", now if durations else None, "RUN_RECORDS",
            len(durations), "NO_FINISHED_RUNS")
        add("runs.duration_p95", percentile(durations, 0.95), "s", now if durations else None, "RUN_RECORDS",
            len(durations), "NO_FINISHED_RUNS")
        attempts = [attempt for record in records for job in record["tasks"].values() for attempt in job["attempts"]]
        add("tasks.attempts", len(attempts), "count", now, "RUN_RECORDS", len(attempts))
        add("tasks.attempt_failures", sum(attempt["status"] in {"FAILED", "UNKNOWN"} for attempt in attempts),
            "count", now, "RUN_RECORDS", len(attempts))
        windows = [attempt["usage"]["window_seconds"] for attempt in attempts if (attempt.get("usage") or {}).get(
            "window_seconds") is not None]
        add("tasks.execution_seconds_p95", percentile(windows, 0.95), "s", now if windows else None, "RUN_RECORDS",
            len(windows), "NO_STAGED_ATTEMPTS")
        imports = [job["import_metrics"]["import_elapsed_ms"] for record in records for job in record["tasks"].values()
                   if (job.get("import_metrics") or {}).get("import_elapsed_ms") is not None]
        add("imports.elapsed_ms_p95", percentile(imports, 0.95), "ms", now if imports else None, "RUN_RECORDS",
            len(imports), "NO_IMPORTS")

    def _query_metrics(self, configuration, window, now, add):
        """Recently finished queries from the query service's own 24-hour history index."""
        try:
            page = self.client.query(
                TableName=self.table, ConsistentRead=True, Limit=1000,
                KeyConditionExpression="PK = :pk AND SK BETWEEN :low AND :high",
                ExpressionAttributeValues={":pk": {"S": QUERY_GC_INDEX},
                                           ":low": {"S": f"GC#{int((now - window + 86400) * 1000000):020d}"},
                                           ":high": {"S": f"GC#{int((now + 86400) * 1000000):020d}~"}})
            elapsed, failed = [], 0
            for item in page.get("Items", []):
                entry = json.loads(item["record"]["S"])
                if (entry["space_id"], entry["generation"]) != (configuration["space_id"], configuration["generation"]):
                    continue
                job = self.client.get_item(TableName=self.table, ConsistentRead=True, Key={
                    "PK": {"S": f"QUERY#{entry['space_id']}#{entry['generation']}"},
                    "SK": {"S": entry["query_id"]}}).get("Item")
                if not job:
                    continue
                record = json.loads(job["record"]["S"])
                if record.get("finished_unix") and record.get("started_unix"):
                    elapsed.append((record["finished_unix"] - record["started_unix"]) * 1000)
                failed += record.get("status") in {"FAILED", "UNKNOWN"}
        except (ClientError, BotoCoreError, KeyError, ValueError):
            for name in ["queries.latency_p50", "queries.latency_p95", "queries.failed"]:
                add(name, None, "ms", None, "QUERY_HISTORY", 0, "QUERY_HISTORY_UNAVAILABLE")
            return
        add("queries.finished", len(elapsed), "count", now, "QUERY_HISTORY", len(elapsed))
        add("queries.latency_p50", percentile(elapsed, 0.5), "ms", now if elapsed else None, "QUERY_HISTORY",
            len(elapsed), "NO_FINISHED_QUERIES")
        add("queries.latency_p95", percentile(elapsed, 0.95), "ms", now if elapsed else None, "QUERY_HISTORY",
            len(elapsed), "NO_FINISHED_QUERIES")
        add("queries.failed", failed, "count", now, "QUERY_HISTORY", len(elapsed))

    def quota_status(self, principal, space):
        configuration = self._configuration(principal, space)
        now = self.clock()
        generation = configuration["generation"]
        policy = self.quotas.policy(space, generation)
        values = []
        _, records = self._jobs(configuration, now)
        active = None if records is None else sum(record["status"] in {"RUNNING", "CANCELLING", "UNKNOWN"}
                                                  for record in records)
        dispatched = None if records is None else sum(
            job["status"] in {"RUNNING", "STAGED", "IMPORTING"} for record in records for job in record["tasks"].values())
        values.append(usage("runs.active", policy["max_active_runs"], active, "runs", "WAIT", "RUN_RECORDS",
                            now if records is not None else None, "SPACE_RUN_QUOTA"))
        values.append(usage("tasks.dispatched", policy["max_dispatched_jobs"], dispatched, "jobs", "WAIT",
                            "RUN_RECORDS", now if records is not None else None, "SPACE_TASK_QUOTA"))
        try:
            pipelines = self.runs.pipelines._records(configuration, "PIPELINE#" + generation + "#", 512)
            values.append(usage("pipelines.defined", policy["max_pipelines"], len(pipelines), "pipelines", "REJECT",
                                "PIPELINE_RECORDS", now, "SPACE_PIPELINE_QUOTA"))
        except (ClientError, BotoCoreError, ControlError):
            values.append(usage("pipelines.defined", policy["max_pipelines"], None, "pipelines", "REJECT",
                                "PIPELINE_RECORDS", None, "SPACE_PIPELINE_QUOTA"))
        values.append(usage("artifacts.stored_code_bytes", policy["max_stored_code_bytes"],
                            self.quotas.code_bytes(space, generation), "bytes", "REJECT", "CODE_USAGE_COUNTER", now,
                            "SPACE_ARTIFACT_QUOTA"))
        builds = None
        if self.environments is not None:
            try:
                catalogue = self.environments._read(self.environments._partition(space, generation), "versions")
                builds = sum(bool(value["build"]) and value["status"] not in {"READY", "FAILED"}
                             for value in catalogue["versions"].values())
            except (ClientError, BotoCoreError, ControlError):
                builds = None
        values.append(usage("builds.concurrent", policy["max_concurrent_builds"], builds, "builds", "REJECT",
                            "ENVIRONMENT_CATALOGUE", now if builds is not None else None, "SPACE_BUILD_QUOTA"))
        values.append(usage("trials.sample_rows", policy["max_trial_sample_rows"], 0, "rows/request", "REJECT",
                            "QUOTA_POLICY", now, "SPACE_TRIAL_SAMPLE_QUOTA"))
        queries = self._query_queue(configuration, now)
        values.append(usage("queries.pending_running", SPACE_QUERY_PENDING,
                            None if queries is None else queries["pending"] + queries["running"], "queries", "REJECT",
                            "QUERY_QUEUE", now if queries is not None else None, "QUERY_QUEUE_QUOTA"))
        values.append(usage("queries.retained_result_bytes", SPACE_QUERY_RETAINED_BYTES,
                            None if queries is None else queries["retained_bytes"], "bytes", "REJECT", "QUERY_QUEUE",
                            now if queries is not None else None, "QUERY_RESULT_RETENTION_QUOTA"))
        ingestion = configuration.get("ingestion_policy") or {}
        queue, _ = self._queue(configuration, now)
        values.append(usage("ingestion.pending_messages", ingestion.get("max_pending_messages"),
                            queue["metrics"].get("pending_messages"), "messages", "BACKPRESSURE",
                            "SQS_ATTRIBUTES_SHARED_QUEUE", now if queue["observed_at"] else None, "QUEUE_BACKLOG"))
        writer, permit = self._writer(configuration, now)
        native, _ = self._native(principal, configuration, permit)
        observed = native["observed_unix"] if native else None
        exports, publications = (native or {}).get("exports") or {}, (native or {}).get("publications") or {}
        values.append(usage("exports.live", exports.get("max_exports", 128), exports.get("live"), "exports", "REJECT",
                            "NATIVE_LEDGER", observed, "INPUT_EXPORT_QUOTA"))
        values.append(usage("exports.retained_bytes", exports.get("max_retained_bytes", 536870912),
                            exports.get("retained_bytes"), "bytes", "REJECT", "NATIVE_LEDGER", observed,
                            "INPUT_RETAINED_BYTES_QUOTA"))
        values.append(usage("publications.in_flight", publications.get("max_jobs", 128), publications.get("in_flight"),
                            "jobs", "REJECT", "NATIVE_LEDGER", observed, "PUBLICATION_GENERATION_QUOTA"))
        values.append(usage("publications.reserved_bytes", publications.get("max_generation_bytes", 536870912),
                            publications.get("reserved_bytes"), "bytes", "REJECT", "NATIVE_LEDGER", observed,
                            "PUBLICATION_GENERATION_QUOTA"))
        scheduler = (native or {}).get("scheduler") or {}
        values.append(usage("writer.pending_work", scheduler.get("max_pending", 32), scheduler.get("pending"),
                            "units", "BACKPRESSURE", "WRITER_SCHEDULER", observed, "WRITER_QUEUE_FULL"))
        result = {"space_id": space, "generation": generation, "observed_at": stamp(now), "policy": policy,
                  "usage": values}
        validate("quota_status", result)
        return result
