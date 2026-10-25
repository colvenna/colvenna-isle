"""Cost observation: measured usage of recorded workloads multiplied by regional list prices.

Each line names its workload identity, the measured quantity and its source.
This is an estimate, not an invoice: shared baseline infrastructure, idle time,
data transfer and discounts are listed as excluded rather than apportioned.
"""

import math
import time
from datetime import datetime, timezone

from botocore.exceptions import BotoCoreError, ClientError

from colvenna_isle.contracts.validation import validate
from colvenna_isle.region import region
from . import prices
from .registry import ControlError

MAX_WINDOW_SECONDS = 7 * 86400
EXCLUDED = [
    "Shared baseline: writer and query task idle time, ALB, VPC endpoints, NAT, API Gateway, CloudFront",
    "DynamoDB, SQS, CloudWatch Logs/metrics, KMS and Lambda control/scheduler invocations",
    "Data transfer, free tier, savings plans, credits and taxes",
    "SlateDB native storage (compaction and its own retention protocol), raw ingestion archive storage",
    "Staging object storage between execution and retention collection",
]


def stamp(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse(value):
    try:
        moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        raise ControlError(400, "INVALID_COST_WINDOW") from None
    if moment.tzinfo is None:
        raise ControlError(400, "INVALID_COST_WINDOW")
    return moment.timestamp()


def gap(kind, identity, reason):
    return {"kind": kind, "id": str(identity)[:1024], "reason": reason}


class Costs:
    def __init__(self, registry, keys, runs, environments, ecs, *, clock=time.time):
        self.registry, self.keys, self.runs, self.environments, self.ecs = registry, keys, runs, environments, ecs
        self.client, self.table, self.clock = registry.client, registry.table, clock
        self.definitions = {}
        self.region = region()
        self.prices = prices.for_region(self.region)

    def _line(self, category, kind, identity, usage, unit, unit_price, observed, source, **attributes):
        return {"category": category, "workload": {"kind": kind, "id": str(identity)[:1024]},
                "usage": round(max(0.0, usage), 6), "unit": unit, "unit_price_usd": unit_price,
                "estimated_usd": round(max(0.0, usage) * unit_price, 8), "observed_at": stamp(observed) if observed else None,
                "source": source, "attributes": {name: value for name, value in attributes.items()
                                                 if value is None or isinstance(value, (bool, int, float, str))}}

    def _task_size(self, arn):
        """vCPU and GiB of an approved ECS task definition revision (cached)."""
        if arn not in self.definitions:
            try:
                value = self.ecs.describe_task_definition(taskDefinition=arn)["taskDefinition"]
                self.definitions[arn] = (int(value["cpu"]) / 1024, int(value["memory"]) / 1024)
            except (ClientError, BotoCoreError, KeyError, ValueError, AttributeError):
                self.definitions[arn] = None
        return self.definitions[arn]

    def report(self, principal, space, since=None, until=None):
        principal.authorize(space, "monitor:read")
        configuration = self.registry._read(space)
        self.registry._authorize_generation(principal, configuration)
        if self.keys.authenticate_id(principal.key_id or "") != principal:
            raise ControlError(403, "FORBIDDEN")
        now = self.clock()
        until = min(now, parse(until)) if until else now
        since = parse(since) if since else until - 86400
        if not 0 < until - since <= MAX_WINDOW_SECONDS:
            raise ControlError(400, "INVALID_COST_WINDOW")
        if self.prices is None:
            result = {"space_id": space, "generation": configuration["generation"], "since": stamp(since),
                "until": stamp(until), "observed_at": stamp(now), "basis": "MEASURED_USAGE_X_LIST_PRICE",
                "prices": {"region": self.region, "currency": "USD", "retrieved": None, "sources": []},
                "lines": [], "truncated": False, "totals": {}, "excluded": EXCLUDED, "status": "UNKNOWN",
                "unmeasured": [gap("prices", self.region, "REGIONAL_PRICES_UNAVAILABLE")]}
            validate("cost_report", result)
            return result
        # Every workload whose usage could not be measured is named, never silently priced at zero.
        lines, gaps = [], []
        try:
            records = [record for record in self._runs(configuration, since)
                       if record["created_unix"] <= until]
        except (ClientError, BotoCoreError, ControlError):
            records = []
            gaps.append(gap("runs", space, "RUN_RECORDS_UNAVAILABLE"))
        for record in records:
            self._run_lines(record, lines, until, gaps)
        self._build_lines(configuration, since, until, lines, gaps)
        self._query_lines(configuration, since, until, lines, gaps)
        totals = {}
        for line in lines:
            totals[line["category"]] = round(totals.get(line["category"], 0) + line["estimated_usd"], 8)
        result = {"space_id": space, "generation": configuration["generation"], "since": stamp(since),
                  "until": stamp(until), "observed_at": stamp(now), "basis": "MEASURED_USAGE_X_LIST_PRICE",
                  "prices": {"region": self.region, "currency": "USD", "retrieved": self.prices.RETRIEVED,
                             "sources": self.prices.SOURCES},
                  "lines": lines[:2000], "truncated": len(lines) > 2000, "totals": totals, "excluded": EXCLUDED,
                  "unmeasured": gaps[:200], "status": "PARTIAL" if gaps else "COMPLETE"}
        validate("cost_report", result)
        return result

    def _runs(self, configuration, since):
        records = []
        for item in self.runs.pipelines._records(configuration, "PIPELINE#" + configuration["generation"] + "#", 512):
            pipeline = item["SK"]["S"].rsplit("#", 1)[1]
            start = None
            while True:
                request = {"TableName": self.table, "ConsistentRead": True,
                           "KeyConditionExpression": "PK = :pk AND SK BETWEEN :low AND :high",
                           "ExpressionAttributeValues": {
                               ":pk": {"S": "SPACE#" + configuration["space_id"]},
                               ":low": {"S": f"RUNHISTORY#{configuration['generation']}#{pipeline}#"
                                             f"{int(since * 1000000):020d}"},
                               ":high": {"S": f"RUNHISTORY#{configuration['generation']}#{pipeline}#~"}}}
                if start:
                    request["ExclusiveStartKey"] = start
                page = self.client.query(**request)
                for entry in page.get("Items", []):
                    record = self.runs.pipelines._read(self.runs._key(configuration, pipeline, entry["run_id"]["S"]),
                                                       missing=None)
                    if record is not None:
                        records.append(record)
                start = page.get("LastEvaluatedKey")
                if not start or len(records) > 1000:
                    break
        return records

    def _run_lines(self, record, lines, until, gaps):
        for task_id, job in record["tasks"].items():
            for attempt in job["attempts"]:
                dispatch = attempt.get("dispatch") or {}
                usage = attempt.get("usage") or {}
                identity = f"{record['run_id']}/{job['job_id']}/{attempt['attempt_id']}"
                start = attempt.get("dispatched_unix")
                end = attempt.get("staged_unix") or attempt.get("ended_unix")
                window = usage.get("window_seconds", (end - start) if start and end else None)
                observed = end
                if dispatch.get("executor") == "lambda":
                    measured = usage.get("elapsed_ms")
                    seconds = math.ceil(measured) / 1000 if measured is not None else window
                    memory = usage.get("memory_mb") or 2048
                    if seconds is None:
                        gaps.append(gap("attempt", identity, "LAMBDA_DURATION_UNRECORDED"))
                        continue
                    lines.append(self._line("lambda_compute", "attempt", identity, seconds * memory / 1024, "GB-s",
                                            self.prices.LAMBDA_ARM_GB_SECOND, observed,
                                            "ADAPTER_ELAPSED" if measured is not None else "CONTROLLER_WINDOW",
                                            run_id=record["run_id"], task_id=task_id, memory_mb=memory,
                                            function_arn=dispatch.get("function_arn")))
                elif dispatch.get("executor") == "ecs_task":
                    size = self._task_size(dispatch.get("task_definition_arn"))
                    if window is None or size is None:
                        gaps.append(gap("attempt", identity, "ECS_WINDOW_UNRECORDED" if window is None
                                        else "TASK_DEFINITION_UNAVAILABLE"))
                        continue
                    seconds = max(self.prices.FARGATE_MINIMUM_SECONDS, window)
                    lines.append(self._line("ecs_compute", "attempt", identity, seconds, "task-s",
                                            self.prices.fargate_second(*size), observed, "CONTROLLER_WINDOW",
                                            run_id=record["run_id"], task_id=task_id, vcpu=size[0], memory_gib=size[1],
                                            task_arn=dispatch.get("task_arn")))
                elif dispatch.get("executor") == "writer" and window is not None:
                    lines.append(self._line("writer_compute", "attempt", identity, window, "task-s",
                                            self.prices.fargate_second(self.prices.SHARED_TASK_VCPU, self.prices.SHARED_TASK_GB),
                                            observed, "CONTROLLER_WINDOW", run_id=record["run_id"], task_id=task_id,
                                            allocation="SOLE_WRITER_TASK_TIME"))
            metrics = job.get("input_metrics") or {}
            if metrics:
                identity = job.get("export_id") or job["job_id"]
                elapsed = (metrics.get("elapsed_ms") or 0) + (metrics.get("upload_elapsed_ms") or 0)
                lines.append(self._line("writer_compute", "snapshot_export", identity, elapsed / 1000, "task-s",
                                        self.prices.fargate_second(self.prices.SHARED_TASK_VCPU, self.prices.SHARED_TASK_GB),
                                        job.get("observed_unix"), "NATIVE_EXPORT_METRICS", run_id=record["run_id"],
                                        task_id=task_id, copied_rows=metrics.get("copied_rows"),
                                        copied_bytes=metrics.get("copied_bytes")))
                uploaded = metrics.get("uploaded_bytes") or metrics.get("copied_bytes") or 0
                held = max(0.0, until - (record.get("started_unix") or record["created_unix"]))
                lines.append(self._line("s3_storage", "snapshot_export", identity,
                                        uploaded / self.prices.GIB * held / self.prices.MONTH_SECONDS, "GB-month",
                                        self.prices.S3_STANDARD_GB_MONTH, job.get("observed_unix"), "NATIVE_EXPORT_METRICS",
                                        uploaded_bytes=uploaded, held_seconds=round(held)))
                lines.append(self._line("s3_requests", "snapshot_export", identity,
                                        len(job.get("input_versions") or {}) + 1, "PUT", self.prices.S3_PUT_REQUEST,
                                        job.get("observed_unix"), "NATIVE_EXPORT_METRICS"))
            imports = job.get("import_metrics") or {}
            if imports.get("import_elapsed_ms") is not None:
                lines.append(self._line("writer_compute", "generation_import", job.get("release_id") or job["job_id"],
                                        imports["import_elapsed_ms"] / 1000, "task-s",
                                        self.prices.fargate_second(self.prices.SHARED_TASK_VCPU, self.prices.SHARED_TASK_GB),
                                        job.get("observed_unix"), "WRITER_IMPORT_WORKER", run_id=record["run_id"],
                                        task_id=task_id, copied_rows=imports.get("copied_rows"),
                                        copy_elapsed_ms=imports.get("copy_elapsed_ms"),
                                        row_count=imports.get("row_count")))

    def _build_lines(self, configuration, since, until, lines, gaps):
        if self.environments is None:
            gaps.append(gap("environment_builds", configuration["space_id"], "ENVIRONMENT_CATALOGUE_UNAVAILABLE"))
            return
        try:
            catalogue = self.environments._read(self.environments._partition(
                configuration["space_id"], configuration["generation"]), "versions")["versions"]
        except (ClientError, BotoCoreError, ControlError):
            gaps.append(gap("environment_builds", configuration["space_id"], "ENVIRONMENT_CATALOGUE_UNAVAILABLE"))
            return
        for key, record in sorted(catalogue.items()):
            build = record.get("build") or {}
            if not build or not since <= build.get("admitted_unix", 0) <= until:
                continue
            seconds = build.get("build_seconds")
            if seconds is None:
                if record["status"] in {"READY", "FAILED"}:
                    gaps.append(gap("environment_build", build["build_id"], "BUILD_TIME_UNRECORDED"))
                continue
            lines.append(self._line("codebuild", "environment_build", build["build_id"], math.ceil(seconds / 60),
                                    "build-minute", self.prices.CODEBUILD_ARM_SMALL_MINUTE, build.get("admitted_unix"),
                                    "CODEBUILD_BUILD_TIMES", environment=key, status=record["status"]))
            image_bytes = build.get("image_bytes")
            if image_bytes:
                held = until - max(since, build["admitted_unix"])
                lines.append(self._line("ecr_storage", "environment", key, image_bytes / self.prices.GIB * held
                                        / self.prices.MONTH_SECONDS, "GB-month", self.prices.ECR_GB_MONTH,
                                        build.get("admitted_unix"), "ECR_IMAGE_SIZE", image_bytes=image_bytes))

    def _query_lines(self, configuration, since, until, lines, gaps):
        import json

        try:
            page = self.client.query(
                TableName=self.table, ConsistentRead=True, Limit=1000,
                KeyConditionExpression="PK = :pk AND SK BETWEEN :low AND :high",
                ExpressionAttributeValues={":pk": {"S": "QUERY#GC#3"},
                                           ":low": {"S": f"GC#{int((since + 86400) * 1000000):020d}"},
                                           ":high": {"S": f"GC#{int((until + 86400) * 1000000):020d}~"}})
        except (ClientError, BotoCoreError):
            gaps.append(gap("queries", configuration["space_id"], "QUERY_HISTORY_UNAVAILABLE"))
            return
        rate = self.prices.fargate_second(self.prices.SHARED_TASK_VCPU, self.prices.SHARED_TASK_GB) / 2
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
            if record.get("started_unix") and record.get("finished_unix"):
                lines.append(self._line("query_compute", "query", record["query_id"],
                                        record["finished_unix"] - record["started_unix"], "slot-s", rate,
                                        record["finished_unix"], "QUERY_RECORD",
                                        allocation="QUERY_TASK_TIME_PER_SLOT_OF_2", status=record["status"]))
