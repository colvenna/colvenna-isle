"""Approved compute dispatch and its observable identities.

Lambda dispatch is an asynchronous Invoke of the environment's numeric function
version, carrying only the immutable descriptor reference. ECS dispatch uses the
approved task definition revision with a deterministic client token and
startedBy identity, so an uncertain RunTask can be reconciled to the original
task instead of creating a second business run. Completion custody is always the
immutable staging completion object, never the Invoke or RunTask reply.
"""

import hashlib
import json
import re

from botocore.exceptions import BotoCoreError, ClientError

from colvenna_isle.region import region
from colvenna_isle.account import account
from colvenna_isle.contracts.validation import validate
from colvenna_isle.pipeline.task_descriptor import MAX_DESCRIPTOR_BYTES, staging_prefix
from .registry import ControlError, canonical



class DispatchUnknown(ControlError):
    def __init__(self, code):
        super().__init__(503, code)


def token(descriptor_reference):
    """A stable ECS clientToken/startedBy for one attempt's descriptor (<= 36 chars)."""
    return "colvenna-" + hashlib.sha256(canonical(descriptor_reference).encode()).hexdigest()[:31]


class Dispatcher:
    def __init__(self, lam, ecs, s3, bucket, *, cluster, subnets, security_groups, prefix):
        # prefix: this environment's runtime resource prefix (environment policy).
        self.lam, self.ecs, self.s3, self.bucket, self.prefix = lam, ecs, s3, bucket, prefix
        self.cluster, self.subnets, self.security_groups = cluster, list(subnets), list(security_groups)

    def lambda_invoke(self, binding, descriptor_reference):
        event = {"operation": "task_execute", "descriptor": descriptor_reference}
        validate("task_event", event)
        if not self.prefix or not re.fullmatch(
                r"arn:aws:lambda:" + re.escape(region()) + ":" + account() + r":function:" + re.escape(self.prefix) + r"[a-z0-9_-]+:[1-9][0-9]*",
                binding["function_arn"]):
            raise ControlError(403, "DISPATCH_FUNCTION_NOT_APPROVED")
        try:
            response = self.lam.invoke(FunctionName=binding["function_arn"], InvocationType="Event",
                                       Payload=canonical(event).encode())
        except (ClientError, BotoCoreError):
            # Lambda async Invoke has no idempotency token; never re-invoke blindly.
            raise DispatchUnknown("LAMBDA_INVOKE_UNKNOWN") from None
        if response.get("StatusCode") != 202:
            raise DispatchUnknown("LAMBDA_INVOKE_UNKNOWN")
        return {"executor": "lambda", "function_arn": binding["function_arn"],
                "request_id": response["ResponseMetadata"]["RequestId"]}

    def ecs_run(self, binding, descriptor_reference, tags):
        event = {"operation": "task_execute", "descriptor": descriptor_reference}
        validate("task_event", event)
        if not self.prefix or not re.fullmatch(
                r"arn:aws:ecs:" + re.escape(region()) + ":" + account() + r":task-definition/" + re.escape(self.prefix)
                + r"[a-z0-9_-]+:[1-9][0-9]*", binding["task_definition_arn"]):
            raise ControlError(403, "DISPATCH_TASK_DEFINITION_NOT_APPROVED")
        identity = token(descriptor_reference)
        try:
            response = self.ecs.run_task(
                cluster=self.cluster, taskDefinition=binding["task_definition_arn"], launchType="FARGATE",
                count=1, startedBy=identity, clientToken=identity, enableExecuteCommand=False,
                networkConfiguration={"awsvpcConfiguration": {
                    "subnets": self.subnets, "securityGroups": self.security_groups, "assignPublicIp": "DISABLED"}},
                overrides={"containerOverrides": [{"name": "runtime",
                                                   "command": ["--task-event", canonical(event)]}]},
                tags=[{"key": name, "value": value} for name, value in sorted(tags.items())])
        except (ClientError, BotoCoreError):
            raise DispatchUnknown("ECS_RUN_TASK_UNKNOWN") from None
        if response.get("failures") or len(response.get("tasks", [])) != 1:
            # A reported failure created no task; it is safe to retry the same token.
            raise ControlError(503, "ECS_RUN_TASK_FAILED")
        task = response["tasks"][0]
        return {"executor": "ecs_task", "task_arn": task["taskArn"], "token": identity,
                "task_definition_arn": binding["task_definition_arn"]}

    def ecs_find(self, identity):
        """Reconcile an uncertain RunTask by its original startedBy identity."""
        arns = []
        for status in ["RUNNING", "STOPPED"]:
            arns += self.ecs.list_tasks(cluster=self.cluster, startedBy=identity, desiredStatus=status)["taskArns"]
        return sorted(set(arns))

    def ecs_observe(self, task_arn):
        result = self.ecs.describe_tasks(cluster=self.cluster, tasks=[task_arn])
        if result.get("failures") or len(result.get("tasks", [])) != 1:
            return {"last_status": "UNKNOWN"}
        task = result["tasks"][0]
        container = next((value for value in task.get("containers", []) if value["name"] == "runtime"), {})
        return {"last_status": task["lastStatus"], "stop_code": task.get("stopCode"),
                "stopped_reason": task.get("stoppedReason"), "exit_code": container.get("exitCode"),
                "image_digest": container.get("imageDigest"), "started_by": task.get("startedBy")}

    def ecs_stop(self, task_arn, reason):
        try:
            self.ecs.stop_task(cluster=self.cluster, task=task_arn, reason=reason[:255])
            return True
        except (ClientError, BotoCoreError):
            return False

    def _read(self, key, version, maximum):
        response = self.s3.get_object(Bucket=self.bucket, Key=key, VersionId=version, ExpectedBucketOwner=account())
        with response["Body"] as stream:
            raw = stream.read(maximum + 1)
        if response["VersionId"] != version or len(raw) > maximum:
            raise ValueError("Original object differs")
        return raw

    def completions(self, descriptor):
        """Every immutable STAGED completion of this attempt, oldest first.

        Duplicate invocations each own a separate execution directory. Only the
        writer-selected one can be imported; the others never publish.
        """
        prefix = staging_prefix(descriptor, "x")[:-2]
        found = []
        try:
            pages = self.s3.get_paginator("list_object_versions").paginate(
                Bucket=self.bucket, Prefix=prefix, ExpectedBucketOwner=account())
            versions = [value for page in pages for value in page.get("Versions", [])]
        except (ClientError, BotoCoreError):
            raise DispatchUnknown("STAGING_LIST_UNKNOWN") from None
        for value in versions:
            key = value["Key"]
            execution = key[len(prefix):].split("/", 1)[0]
            if not key.endswith("/completion.json") or key != prefix + execution + "/completion.json":
                continue
            try:
                raw = self._read(key, value["VersionId"], MAX_DESCRIPTOR_BYTES)
                completion = json.loads(raw)
            except (ClientError, BotoCoreError, ValueError):
                continue
            if (completion.get("execution_id") != execution or completion.get("status") != "STAGED"
                    or any(completion.get(name) != descriptor[name] for name in ["run_id", "job_id", "attempt_id"])):
                continue
            found.append({"execution_id": execution, "modified": value["LastModified"].timestamp(),
                          "invocation": completion.get("invocation", {}),
                          "reference": {"s3_uri": f"s3://{self.bucket}/{key}", "object_version": value["VersionId"],
                                        "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}})
        return sorted(found, key=lambda value: (value["modified"], value["execution_id"]))
