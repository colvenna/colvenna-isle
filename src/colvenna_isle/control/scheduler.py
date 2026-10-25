"""Minute-level pipeline scheduler: colvenna-owned triggers, notifications and run progression.

EventBridge Scheduler invokes this function every minute (reserved concurrency
one); API admissions and publication progress may also wake it asynchronously.
Trigger decisions compare the writer's authoritative watermarks with each
pipeline's published task watermarks, so a lost notification, restart or
concurrent trigger cannot drop or duplicate a pending interval: the stable
admission key is the version, source watermark vector and trigger window.
"""

import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from zoneinfo import ZoneInfo

import boto3

from colvenna_isle.region import region
from colvenna_isle.contracts.validation import validate_schedule
from colvenna_isle.pipeline.task_descriptor import digest
from .registry import ControlError

SCHEDULE_INDEX = "PIPELINESCHEDULES#1"
LOOP_SECONDS = 50


def _cron_field(field, value, low, high):
    if field in {"*", "?"}:
        return True
    for item in field.split(","):
        base, _, step = item.partition("/")
        start, end = (low, high) if base == "*" else (
            (int(base.split("-")[0]), int(base.split("-")[1])) if "-" in base else (int(base), int(base) if not step else high))
        if start <= value <= end and (value - start) % (int(step) if step else 1) == 0:
            return True
    return False


def window(expression, zone, now):
    """The latest fire time <= now, as epoch minutes; one run per window."""
    minutes = int(now // 60)
    rate = re.fullmatch(r"rate\(([1-9]\d*) (minute|minutes|hour|hours|day|days)\)", expression)
    if rate:
        size = int(rate[1]) * {"minute": 1, "hour": 60, "day": 1440}[rate[2].rstrip("s")]
        return minutes - minutes % size
    fields = re.fullmatch(r"cron\(([0-9*?,/ -]+)\)", expression)[1].split()
    local = datetime.fromtimestamp(minutes * 60, timezone.utc).astimezone(ZoneInfo(zone))
    for _ in range(1441 * 7):
        weekday = local.isoweekday() % 7 + 1  # AWS cron: 1=Sunday
        if (_cron_field(fields[0], local.minute, 0, 59) and _cron_field(fields[1], local.hour, 0, 23)
                and _cron_field(fields[2], local.day, 1, 31) and _cron_field(fields[3], local.month, 1, 12)
                and _cron_field(fields[4], weekday, 1, 7) and _cron_field(fields[5], local.year, 1970, 2199)):
            return int(local.timestamp() // 60)
        local -= timedelta(minutes=1)
    return None


class Scheduler:
    def __init__(self, runs, keys, *, sqs=None, queue=None, clock=time.time):
        self.runs, self.keys, self.sqs, self.queue, self.clock = runs, keys, sqs, queue, clock
        self.pipelines, self.registry = runs.pipelines, runs.registry
        self.client, self.table = runs.client, runs.table

    def _drop(self, item):
        self.client.delete_item(TableName=self.table, Key={"PK": item["PK"], "SK": item["SK"]})

    def triggers(self, deadline=None):
        decisions = []
        items = self.client.query(TableName=self.table, KeyConditionExpression="PK = :pk",
                                  ExpressionAttributeValues={":pk": {"S": SCHEDULE_INDEX}},
                                  ConsistentRead=True).get("Items", [])
        for item in items:
            if deadline is not None and time.monotonic() >= deadline:
                break  # Remaining schedules are evaluated on the next minute tick.
            space, generation, pipeline = item["SK"]["S"].split("#")
            decision = {"space_id": space, "pipeline_id": pipeline}
            try:
                principal = self.keys.authenticate_id(item["run_as_key_id"]["S"])
                configuration = self.registry._read(space)
                if configuration["generation"] != generation:
                    self._drop(item)
                    decision["decision"] = "GENERATION_CHANGED"
                    decisions.append(decision)
                    continue
                record = self.pipelines._current(configuration, pipeline)
                schedule = record["schedule"]
                if (record["status"] != "ENABLED" or schedule.get("owner") != "colvenna"
                        or record["active_version"] is None):
                    # Paused/external/none: no new automatic runs; existing runs continue.
                    self._drop(item)
                    decision["decision"] = "NOT_COLVENNA_ENABLED"
                    decisions.append(decision)
                    continue
                validate_schedule(schedule)
                current = window(schedule["expression"], schedule["timezone"], self.clock())
                last = int(item.get("last_window", {"N": "-1"})["N"])
                if current is None or current <= last:
                    decision["decision"] = "NOT_DUE"
                    decisions.append(decision)
                    continue
                decision.update(self._trigger(principal, configuration, record, pipeline, current))
                self.client.update_item(TableName=self.table, Key={"PK": item["PK"], "SK": item["SK"]},
                    UpdateExpression="SET last_window = :window",
                    ConditionExpression="attribute_not_exists(last_window) OR last_window < :window",
                    ExpressionAttributeValues={":window": {"N": str(current)}})
            except ControlError as error:
                decision["decision"] = "ERROR"
                decision["code"] = error.code
            except self.client.exceptions.ConditionalCheckFailedException:
                decision["decision"] = "WINDOW_ALREADY_TRIGGERED"
            decisions.append(decision)
        return decisions

    def _trigger(self, principal, configuration, record, pipeline, current):
        version = record["active_version"]
        bound = self.pipelines.bind_run(principal, configuration["space_id"], pipeline, version)
        definition = bound["snapshot"]["definition"]
        for space, generation, name, _ in self.runs.active(limit=256):
            if (space, generation, name) == (configuration["space_id"], configuration["generation"], pipeline):
                # max_active_runs=1 with coalescing: the next window after this
                # run finishes binds every interval that arrived meanwhile.
                return {"decision": "ACTIVE_RUN_COALESCES"}
        roots = sorted({binding["dataset"] for task in definition["tasks"]
                        for binding in task["inputs"].values() if "dataset" in binding})
        marks = self.runs.writer.call(principal, configuration, "pipeline_watermarks",
                                      pipeline_id=pipeline, datasets=roots)
        vector = {name: marks["datasets"][name] for name in roots}
        pending = False
        for task in definition["tasks"]:
            published = marks["tasks"].get(task["task_id"])
            if published is None:
                pending = True
                continue
            for alias, binding in task["inputs"].items():
                if "dataset" not in binding:
                    continue
                consumed = (published.get("input_versions") or {}).get(alias) or {}
                observed = vector[binding["dataset"]]
                if (consumed.get("to_sequence", consumed.get("commit_sequence")) != observed["commit_sequence"]
                        or consumed.get("release_id") != observed["release_id"]):
                    pending = True
        if not pending:
            return {"decision": "NO_NEW_DATA", "vector": vector}
        nonce = "schedule." + digest([version, current, {name: [value["commit_sequence"], value["release_id"]]
                                                          for name, value in vector.items()}])[:48]
        receipt = self.runs.create(principal, configuration["space_id"], pipeline,
                                   {"pipeline_version": version, "mode": "snapshot"}, nonce,
                                   trigger="schedule", window=current)
        return {"decision": "RUN_ADMITTED", "run_id": receipt["run_id"], "vector": vector}

    def notifications(self, limit=20):
        """Lambda destinations and ECS STOPPED events are hints for prompt reconciliation."""
        if not self.sqs or not self.queue:
            return []
        received = []
        response = self.sqs.receive_message(QueueUrl=self.queue, MaxNumberOfMessages=10, WaitTimeSeconds=0,
                                            VisibilityTimeout=30)
        for message in response.get("Messages", [])[:limit]:
            observation = parse_notification(message["Body"])
            if observation:
                try:
                    self.runs.notify(observation)
                except ControlError:
                    pass
                received.append(observation)
            self.sqs.delete_message(QueueUrl=self.queue, ReceiptHandle=message["ReceiptHandle"])
        return received

    def loop(self, seconds=LOOP_SECONDS):
        deadline = time.monotonic() + seconds
        result = {"triggers": self.triggers(deadline - seconds / 2), "notifications": [], "rounds": []}
        while time.monotonic() < deadline:
            result["notifications"] += self.notifications()
            summary = self.runs.tick(deadline)
            result["rounds"].append(summary)
            if not summary["advanced"]:
                break
            if all(value["status"] in {"PUBLISHED", "FAILED", "CANCELLED"} for value in summary["advanced"]):
                break
            time.sleep(2)
        return result


def parse_notification(body):
    """Map a destination/ECS event to its original descriptor attempt path."""
    try:
        value = json.loads(body)
    except ValueError:
        return None
    if "requestContext" in value and "requestPayload" in value:
        descriptor = (value.get("requestPayload") or {}).get("descriptor", {}).get("s3_uri", "")
        response = value.get("responsePayload") if isinstance(value.get("responsePayload"), dict) else {}
        observation = {"source": "lambda", "request_id": value["requestContext"].get("requestId"),
                       "condition": value["requestContext"].get("condition"),
                       "status": response.get("status") or ("FAILED" if value["requestContext"].get("condition")
                                                            != "Success" else None),
                       "code": response.get("code") or response.get("errorType"),
                       "execution_id": response.get("execution_id")}
    elif value.get("detail-type") == "ECS Task State Change":
        detail = value.get("detail", {})
        command = next((entry.get("command") for entry in detail.get("overrides", {}).get("containerOverrides", [])
                        if entry.get("name") == "runtime"), None) or []
        try:
            descriptor = json.loads(command[1])["descriptor"]["s3_uri"] if len(command) == 2 else ""
        except (ValueError, KeyError, TypeError):
            descriptor = ""
        container = next((entry for entry in detail.get("containers", []) if entry.get("name") == "runtime"), {})
        observation = {"source": "ecs", "task_arn": detail.get("taskArn"), "status": detail.get("lastStatus"),
                       "exit_code": container.get("exitCode"), "code": detail.get("stopCode")}
    else:
        return None
    match = re.fullmatch(r"s3://[^/]+/spaces/([^/]+)/task-contexts/([^/]+)/([^/]+)/([^/]+)/([^/]+)/descriptor\.json",
                         descriptor or "")
    if not match:
        return None
    observation.update(zip(["space_id", "generation", "run_id", "job_id", "attempt_id"], match.groups()))
    return observation


@lru_cache(maxsize=1)
def services():
    from botocore.config import Config
    from colvenna_isle.writer.ownership import Ownership
    from .dispatch import Dispatcher
    from .environment_artifacts import EnvironmentArtifacts
    from .environments import Environments
    from .handler import decode_environment_policy, environment_clients
    from .keys import KeyStore
    from .pipeline_artifacts import PipelineArtifacts
    from .pipelines import Pipelines
    from .quotas import Quotas
    from .registry import Registry
    from .runs import Runs
    from .writer_client import WriterClient
    from colvenna_isle.pipeline.task_objects import TaskObjects

    options = Config(connect_timeout=3, read_timeout=10, retries={"total_max_attempts": 2})
    client = boto3.client("dynamodb", region_name=region(), config=options)
    s3 = boto3.client("s3", region_name=region(), config=options.merge(Config(signature_version="s3v4")))
    lam = boto3.client("lambda", region_name=region(), config=Config(connect_timeout=3, read_timeout=28, retries={"total_max_attempts": 1}))
    ecs = boto3.client("ecs", region_name=region(), config=options)
    table, bucket = os.environ["COLVENNA_CONTROL_TABLE"], os.environ["COLVENNA_DATA_BUCKET"]
    registry = Registry(client, table, bucket)
    keys = KeyStore(client, table)
    quotas = Quotas(client, table)
    policy = decode_environment_policy(os.environ.get("COLVENNA_ENVIRONMENT_POLICY", "{}"))
    environments = None
    if policy:
        codebuild, ecr, ecs_read = environment_clients()
        environments = Environments(registry, None, os.environ.get("COLVENNA_ENVIRONMENT_PROJECT", ""), policy,
                                    EnvironmentArtifacts(s3, ecr, lam, ecs_read, bucket), keys, quotas)
    pipelines = Pipelines(registry, PipelineArtifacts(s3, bucket), environments, keys,
                          writer_source=os.environ["COLVENNA_WRITER_SOURCE_COMMIT"], quotas=quotas)
    writer = WriterClient(Ownership(client, table), lam, os.environ["COLVENNA_WRITER_CONTROL_FUNCTION"])
    network = json.loads(os.environ["COLVENNA_COMPUTE_NETWORK"])
    dispatcher = Dispatcher(lam, ecs, s3, bucket, cluster=network["cluster"], subnets=network["subnets"],
                            security_groups=network["security_groups"], prefix=policy.get("runtime_prefix", ""))
    from .faults import Faults, FaultyDispatcher, FaultyWriter

    # Lost-reply injection can only match synthetic probe_* qualification spaces.
    faults = Faults(client, table)
    runs = Runs(pipelines, keys, FaultyWriter(writer, faults), FaultyDispatcher(dispatcher, faults),
                lambda space, generation: TaskObjects(s3, bucket, space, generation), quotas=quotas)
    sqs = boto3.client("sqs", region_name=region(), config=options) if os.environ.get("COLVENNA_PIPELINE_EVENTS_QUEUE") else None
    return Scheduler(runs, keys, sqs=sqs, queue=os.environ.get("COLVENNA_PIPELINE_EVENTS_QUEUE"))


def handler(event, context):
    scheduler = services()
    seconds = min(LOOP_SECONDS, max(5, context.get_remaining_time_in_millis() / 1000 - 20))
    result = scheduler.loop(seconds)
    print(json.dumps({"event": "SCHEDULER_TICK", "source_commit": os.environ.get("COLVENNA_SOURCE_COMMIT"),
                      "triggers": result["triggers"], "notifications": len(result["notifications"]),
                      "rounds": result["rounds"][-3:]}, default=str), flush=True)
    return {"triggers": result["triggers"], "rounds": len(result["rounds"]),
            "last": result["rounds"][-1] if result["rounds"] else None}
