"""Writer custody excludes managed query replicas, retaining unknown contenders."""

import unittest
from unittest.mock import Mock

from colvenna_isle.writer.controller import Controller


class QueryIndependence(unittest.TestCase):
    def controller(self):
        controller = object.__new__(Controller)
        controller.values = {"Cluster": "cluster", "QueryService": "arn:service/cluster/query", "QueryTask": "query-def"}
        controller.ecs = Mock()
        controller.ecs.get_paginator.return_value.paginate.side_effect = [
            [{"taskArns": ["query", "writer", "unknown"]}], [{"taskArns": ["old"]}]]
        tasks = {
            "query": {"taskArn": "query", "group": "service:query", "lastStatus": "RUNNING",
                      "taskDefinitionArn": "query-def"},
            "writer": {"taskArn": "writer", "group": "service:writer", "lastStatus": "RUNNING"},
            "unknown": {"taskArn": "unknown", "lastStatus": "RUNNING"},
            "old": {"taskArn": "old", "group": "service:writer", "lastStatus": "STOPPED"},
        }
        controller.task = tasks.__getitem__
        controller.ecs.describe_services.return_value = {"services": [{
            "serviceName": "query", "serviceArn": "arn:service/cluster/query",
            "deployments": [{"taskDefinition": "query-def"}]}]}
        controller.boundary = ""
        controller.iam = Mock()
        controller.ecs.describe_tasks.side_effect = lambda cluster, tasks, include: {"tasks": [tasks[0]], "failures": []}
        return controller

    def compute_controller(self, *, boundary="arn:aws:iam::1:policy/RuntimeBoundary", override=None):
        controller = self.controller()
        controller.values["WriterTask"] = "writer-def"
        controller.boundary = None
        controller.ecs.get_paginator.return_value.paginate.side_effect = [
            [{"taskArns": ["writer", "compute"]}], [{"taskArns": []}]]
        compute = {"taskArn": "compute", "lastStatus": "RUNNING", "startedBy": "colvenna-0123",
                   "taskDefinitionArn": "arn:aws:ecs:us-west-2:1:task-definition/colvenna-isle-env-abc:1"}
        if override:
            compute["overrides"] = {"taskRoleArn": override}
        tasks = {"writer": {"taskArn": "writer", "group": "service:writer", "lastStatus": "RUNNING",
                            "taskDefinitionArn": "writer-def"}, "compute": compute}
        controller.task = tasks.__getitem__
        policy = '{"runtime_boundary_arn": "arn:aws:iam::1:policy/RuntimeBoundary", "runtime_prefix": "colvenna-isle-env-"}'
        definitions = {
            "writer-def": {"taskDefinition": {"containerDefinitions": [{"environment": [
                {"name": "COLVENNA_ENVIRONMENT_POLICY", "value": policy}]}]}},
            compute["taskDefinitionArn"]: {"taskDefinition": {"taskRoleArn": "arn:aws:iam::1:role/colvenna-isle-env-1"}}}
        controller.ecs.describe_task_definition.side_effect = lambda taskDefinition: definitions[taskDefinition]
        controller.iam.get_role.return_value = {"Role": {"PermissionsBoundary": {"PermissionsBoundaryArn": boundary}}}
        return controller

    def test_bounded_environment_compute_task_survives_writer_custody(self):
        self.assertEqual([task["taskArn"] for task in self.compute_controller().all_tasks()], ["writer"])

    def test_compute_family_without_the_runtime_boundary_or_with_a_role_override_stays_in_custody(self):
        for controller in [self.compute_controller(boundary="arn:aws:iam::1:policy/Other"),
                           self.compute_controller(override="arn:aws:iam::1:role/WriterRole")]:
            self.assertEqual([task["taskArn"] for task in controller.all_tasks()], ["compute", "writer"])

    def test_an_expired_stopped_task_is_skipped_but_a_running_one_must_be_described(self):
        controller = self.controller()
        controller.ecs.describe_tasks.side_effect = lambda **_: {"tasks": [], "failures": [{"arn": "old", "reason": "MISSING"}]}
        self.assertEqual([task["taskArn"] for task in controller.all_tasks()], ["unknown", "writer"])
        controller = self.controller()
        controller.ecs.get_paginator.return_value.paginate.side_effect = [
            [{"taskArns": ["old"]}], [{"taskArns": ["old"]}]]
        controller.ecs.describe_tasks.side_effect = lambda **_: {"tasks": [], "failures": [{"arn": "old", "reason": "MISSING"}]}
        self.assertEqual([task["taskArn"] for task in controller.all_tasks()], ["old"])

    def test_live_query_replica_does_not_enter_writer_custody(self):
        self.assertEqual([task["taskArn"] for task in self.controller().all_tasks()],
                         ["old", "unknown", "writer"])

    def test_without_positive_query_service_identity_nothing_is_excluded(self):
        controller = self.controller()
        del controller.values["QueryService"]
        self.assertEqual([task["taskArn"] for task in controller.all_tasks()],
                         ["old", "query", "unknown", "writer"])

    def test_forged_query_group_does_not_hide_a_writer_definition(self):
        controller = self.controller()
        original = controller.task
        controller.task = lambda arn: ({**original(arn), "taskDefinitionArn": "writer-def"}
                                       if arn == "query" else original(arn))
        self.assertEqual([task["taskArn"] for task in controller.all_tasks()],
                         ["old", "query", "unknown", "writer"])


if __name__ == "__main__":
    unittest.main()
