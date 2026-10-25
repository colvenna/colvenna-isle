"""Operator-only lifecycle. Replenished ECS tasks remain unapproved until admitted."""

import json
import time

from botocore.exceptions import BotoCoreError, ClientError

from colvenna_isle.control.registry import ControlError
from colvenna_isle.writer.ownership import Ownership, terminal_task


def serializable(value):
    return json.loads(json.dumps(value, default=lambda item: item.isoformat()))


class Controller:
    def __init__(self, current, values, save):
        self.ecs, self.iam = current.client("ecs"), current.client("iam")
        self.values, self.save = values, save
        self.ownership = Ownership(current.client("dynamodb"), values["ControlTable"])
        self.boundary, self.prefix = None, None

    def task(self, arn):
        result = self.ecs.describe_tasks(cluster=self.values["Cluster"], tasks=[arn], include=["TAGS"])
        if result.get("failures") or len(result["tasks"]) != 1 or result["tasks"][0]["taskArn"] != arn:
            raise ControlError(503, "WRITER_TASK_UNKNOWN")
        return serializable(result["tasks"][0])

    def service(self):
        result = self.ecs.describe_services(
            cluster=self.values["Cluster"], services=[self.values["WriterService"]])
        if result.get("failures") or len(result["services"]) != 1:
            raise ControlError(503, "WRITER_SERVICE_UNKNOWN")
        return result["services"][0]

    def all_tasks(self):
        # Include desired STOPPED tasks that may still be STOPPING. Running-only
        # list_tasks can hide the very predecessor that needs a terminal receipt.
        arns = {"RUNNING": set(), "STOPPED": set()}
        for desired in arns:
            for page in self.ecs.get_paginator("list_tasks").paginate(
                    cluster=self.values["Cluster"], desiredStatus=desired):
                arns[desired].update(page["taskArns"])
        tasks = []
        for arn in sorted(arns["RUNNING"] | arns["STOPPED"]):
            if arn not in arns["RUNNING"]:
                # A listed STOPPED task can expire before it is described; only
                # ECS's own MISSING answer for it is a positive absence.
                result = self.ecs.describe_tasks(cluster=self.values["Cluster"], tasks=[arn], include=["TAGS"])
                if (not result.get("tasks") and [(failure.get("arn"), failure.get("reason"))
                                                 for failure in result.get("failures", [])] == [(arn, "MISSING")]):
                    continue
            tasks.append(self.task(arn))
        # The query service shares the cluster but never participates in native
        # writer ownership. Its managed tasks must survive writer stop/takeover.
        # Keep unclassified standalone tasks in the custody audit: excluding
        # every task except the writer's current definition could hide an old
        # native writer or an unapproved contender.
        query = self.values.get("QueryService", "").rsplit("/", 1)[-1]
        definitions = {self.values.get("QueryTask")}
        if query and any(task.get("group") == "service:" + query for task in tasks):
            observed = self.ecs.describe_services(
                cluster=self.values["Cluster"], services=[self.values["QueryService"]])
            if observed.get("failures") or len(observed["services"]) != 1:
                raise ControlError(503, "QUERY_SERVICE_IDENTITY_UNKNOWN")
            service = observed["services"][0]
            if service["serviceName"] != query or service["serviceArn"] != self.values["QueryService"]:
                raise ControlError(503, "QUERY_SERVICE_IDENTITY_UNKNOWN")
            definitions.update(deployment["taskDefinition"] for deployment in service.get("deployments", []))
        return [task for task in tasks
                if not (query and task.get("group") == "service:" + query
                        and task.get("taskDefinitionArn") in definitions) and not self.compute(task)]

    def runtime_boundary(self):
        """The runtime permission boundary and resource prefix of the writer's own approved environment policy."""
        if self.boundary is None:
            self.boundary, self.prefix = "", ""
            if self.values.get("WriterTask"):
                definition = self.ecs.describe_task_definition(taskDefinition=self.values["WriterTask"])
                for container in definition["taskDefinition"]["containerDefinitions"]:
                    for item in container.get("environment", []):
                        if item["name"] == "COLVENNA_ENVIRONMENT_POLICY":
                            policy = json.loads(item["value"] or "{}")
                            self.boundary = policy.get("runtime_boundary_arn") or ""
                            self.prefix = policy.get("runtime_prefix") or ""
        return self.boundary

    def compute(self, task):
        """A pipeline compute task of an approved runtime environment is no writer contender.

        Its definition is an environment build whose task role (not overridden)
        is confined by the runtime boundary, which denies the native warehouse.
        Anything not positively classified stays in writer custody.
        """
        definition_arn = task.get("taskDefinitionArn") or ""
        family = definition_arn.rsplit("/", 1)[-1].rsplit(":", 1)[0]
        boundary = self.runtime_boundary()
        if not boundary or not self.prefix or not family.startswith(self.prefix):
            return False
        try:
            definition = self.ecs.describe_task_definition(taskDefinition=definition_arn)["taskDefinition"]
            role = definition.get("taskRoleArn") or ""
            if (task.get("overrides") or {}).get("taskRoleArn") not in {None, role} or ":role/" not in role:
                return False
            attached = self.iam.get_role(RoleName=role.rsplit("/", 1)[-1])["Role"].get("PermissionsBoundary") or {}
        except (ClientError, BotoCoreError, KeyError):
            return False
        return attached.get("PermissionsBoundaryArn") == boundary

    def idle(self):
        service = self.service()
        tasks = self.all_tasks()
        self.save("idle", {"service": service, "tasks": tasks, "owner": self.ownership.read()})
        if (any(service[field] for field in ["desiredCount", "pendingCount", "runningCount"])
                or any(task["lastStatus"] != "STOPPED" for task in tasks)
                or self.ownership.read()["state"] != "STOPPED"):
            raise ControlError(503, "OLD_WRITER_TERMINATION_UNKNOWN")
        return tasks

    def start(self):
        self.idle()
        self.save("start-admission", {"cluster": self.values["Cluster"],
                                    "service": self.values["WriterService"], "desiredCount": 1})
        result = self.ecs.update_service(cluster=self.values["Cluster"],
                                        service=self.values["WriterService"], desiredCount=1)
        self.save("start-dispatch", result)

    def candidate(self, source_commit, image_digest, *, timeout=300):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            service = self.service()
            tasks = [task for task in self.all_tasks()
                     if task.get("group") == "service:" + service["serviceName"]
                     and task["lastStatus"] == "RUNNING"
                     and task["taskDefinitionArn"] == service["taskDefinition"]]
            if len(tasks) == 1:
                task = tasks[0]
                registration = self.ownership.registration(task["taskArn"])
                if registration and time.time() - registration["observed_at"] < 15:
                    identity = registration["identity"]
                    addresses = [entry["value"] for attachment in task["attachments"]
                                 for entry in attachment.get("details", [])
                                 if entry["name"] == "privateIPv4Address"]
                    containers = [entry for entry in task["containers"] if entry["name"] == "baseline"]
                    if (identity["source_commit"] != source_commit
                            or identity["task_definition"] != task["taskDefinitionArn"]
                            or identity["image_digest"] != image_digest
                            or len(containers) != 1 or containers[0].get("imageDigest") != image_digest
                            or addresses != [identity["address"]]):
                        raise ControlError(503, "WRITER_SOURCE_IDENTITY_UNKNOWN")
                    self.save("candidate", {"task": task, "registration": registration})
                    return identity
            time.sleep(3)
        raise ControlError(503, "WRITER_CANDIDATE_UNKNOWN")

    def grant(self, identity):
        previous = self.ownership.read()
        if previous["revision"] == 0:
            # First bootstrap must also exclude every other actual process.
            # Unapproved contenders can be stopped and admitted afterwards.
            if any(task["lastStatus"] != "STOPPED" and task["taskArn"] != identity["task_arn"]
                   for task in self.all_tasks()):
                raise ControlError(503, "OLD_WRITER_TERMINATION_UNKNOWN")
        self.save("grant-admission", {"previous": previous, "identity": identity})
        result = self.ownership.grant(identity, previous)
        self.save("grant-terminal", result)
        return result

    def revoke(self):
        previous = self.ownership.read()
        self.save("revoke-admission", previous)
        result = self.ownership.revoke(previous)
        self.save("revoke-terminal", result)
        return result

    def confirm_stopped(self, *, timeout=300):
        previous = self.ownership.read()
        if previous["state"] != "REVOKED":
            raise ControlError(409, "WRITER_NOT_REVOKED")
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            task = self.task(previous["identity"]["task_arn"])
            self.save("stop-observation-" + str(time.time_ns()), task)
            if task["lastStatus"] == "STOPPED":
                terminal_task(task, previous["identity"])
                result = self.ownership.stopped(previous, task)
                self.save("stop-terminal", result)
                return result
            time.sleep(3)
        raise ControlError(503, "OLD_WRITER_TERMINATION_UNKNOWN")

    def stop(self, *, timeout=300):
        previous = self.ownership.read()
        if previous["state"] == "ACTIVE":
            previous = self.revoke()
        self.save("stop-admission", {"previous": previous, "desiredCount": 0})
        result = self.ecs.update_service(cluster=self.values["Cluster"],
                                        service=self.values["WriterService"], desiredCount=0)
        self.save("stop-dispatch", result)
        # Prevent ECS auto-replenishment before stopping every known active task.
        for task in self.all_tasks():
            if task["lastStatus"] != "STOPPED":
                self.save("task-stop-admission-" + task["taskArn"].rsplit("/", 1)[-1], task)
                result = self.ecs.stop_task(cluster=self.values["Cluster"],
                    task=task["taskArn"], reason="Colvenna Isle explicit writer takeover: permit revoked")
                self.save("task-stop-dispatch-" + task["taskArn"].rsplit("/", 1)[-1], result)
        if previous["identity"] and previous["state"] == "REVOKED":
            self.confirm_stopped(timeout=timeout)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            service, tasks = self.service(), self.all_tasks()
            if (not any(service[field] for field in ["desiredCount", "pendingCount", "runningCount"])
                    and all(task["lastStatus"] == "STOPPED" for task in tasks)):
                self.save("all-stopped", {"service": service, "tasks": tasks})
                return
            time.sleep(3)
        raise ControlError(503, "OLD_WRITER_TERMINATION_UNKNOWN")
