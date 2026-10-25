"""Uninstall order: builder-created roles hold the builder stack's boundary policy, so they go first."""

import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

# The operator tools name their stacks from an explicit environment.
os.environ.setdefault("COLVENNA_ENVIRONMENT", "qa")

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/aws"))
import uninstall  # noqa: E402

PREFIX = "colvenna-isle-qa-env-"


class Paginator:
    def __init__(self, pages):
        self.pages = pages

    def paginate(self, **_):
        return self.pages


class DefinitionPaginator:
    """ListTaskDefinitions: familyPrefix is an exact family name (a bare prefix lists nothing)."""

    def paginate(self, familyPrefix, status):
        exact = familyPrefix == PREFIX + "task" and status == "ACTIVE"
        return [{"taskDefinitionArns": ["arn:td/" + familyPrefix + ":1"] if exact else []}]


class Clients:
    def __init__(self):
        self.calls = []
        self.definitions = DefinitionPaginator()
        self.roles = [{"RoleName": PREFIX + "task", "Arn": "arn:role/" + PREFIX + "task"},
                      {"RoleName": "colvenna-isle-other-env-task", "Arn": "arn:role/other"}]

    def client(self, name):
        return self

    def get_paginator(self, operation):
        if operation == "list_functions":
            return Paginator([{"Functions": [{"FunctionName": PREFIX + "fn", "FunctionArn": "arn:fn"}]}])
        if operation == "list_task_definition_families":
            return Paginator([{"families": [PREFIX + "task"]}])
        if operation == "list_task_definitions":
            return self.definitions
        return Paginator([{"Roles": self.roles}])

    def list_tags(self, Resource):
        return {"Tags": {"Project": "colvenna-isle"}}

    def delete_function(self, FunctionName):
        self.calls.append(("delete_function", FunctionName))

    def describe_task_definition(self, taskDefinition, include):
        return {"tags": [{"key": "Project", "value": "colvenna-isle"}]}

    def list_role_tags(self, RoleName):
        return {"Tags": [{"Key": "Project", "Value": "colvenna-isle"}]}

    def list_role_policies(self, RoleName):
        return {"PolicyNames": ["inline"]}

    def list_attached_role_policies(self, RoleName):
        return {"AttachedPolicies": [{"PolicyArn": "arn:policy/boundary"}]}

    def get_role(self, RoleName):
        return {"Role": {"PermissionsBoundary": {"PermissionsBoundaryArn": "arn:policy/boundary"}}}

    def __getattr__(self, name):
        if name.startswith(("delete_", "detach_", "deregister_")):
            return lambda **arguments: self.calls.append((name, arguments.get("RoleName")))
        raise AttributeError(name)


class Uninstall(unittest.TestCase):
    def test_role_policies_and_boundary_are_released_before_the_role(self):
        clients = Clients()
        with tempfile.TemporaryDirectory() as directory:
            removed = uninstall.runtime_resources(clients, PREFIX, Path(directory))
        role = PREFIX + "task"
        self.assertEqual([call for call in clients.calls if call[1] == role], [
            ("delete_role_policy", role), ("detach_role_policy", role),
            ("delete_role_permissions_boundary", role), ("delete_role", role)])
        self.assertEqual(removed["roles"], ["arn:role/" + role])
        self.assertEqual(removed["task_definitions"], ["arn:td/" + PREFIX + "task:1"])
        self.assertNotIn("colvenna-isle-other-env-task", [call[1] for call in clients.calls])

    def test_runtime_resources_go_before_the_builder_stack(self):
        order = []
        with tempfile.TemporaryDirectory() as parent, \
                patch.object(uninstall, "STACKS", {"environment": "qa", "baseline": "b", "control": "c",
                                                   "environments": "e", "runtime_prefix": PREFIX}), \
                patch.object(uninstall, "source"), \
                patch.object(uninstall, "session", return_value=(SimpleNamespace(client=lambda name: SimpleNamespace(
                    describe_stacks=lambda **_: (_ for _ in ()).throw(uninstall.ClientError(
                        {"Error": {"Code": "ValidationError", "Message": "does not exist"}}, "DescribeStacks")))),
                    {})), \
                patch.object(uninstall, "delete_stack", side_effect=lambda cf, name, directory: order.append(name)), \
                patch.object(uninstall, "runtime_resources",
                             side_effect=lambda *arguments: order.append("runtime") or {}), \
                patch.object(uninstall, "_exists", return_value=False), \
                patch.object(sys, "argv", ["uninstall.py", "--output", parent + "/receipts"]):
            uninstall.main()
        self.assertEqual(order, ["c", "runtime", "e"])

    def test_child_tools_receive_the_explicit_account(self):
        calls = []
        with tempfile.TemporaryDirectory() as parent, \
                patch.object(uninstall, "STACKS", {"environment": "qa", "baseline": "b", "control": "c",
                                                   "environments": "e", "runtime_prefix": PREFIX}), \
                patch.object(uninstall, "ACCOUNT", "123456789012"), \
                patch.object(uninstall, "source"), \
                patch.object(uninstall, "session", return_value=(SimpleNamespace(client=lambda name: SimpleNamespace(
                    describe_stacks=lambda **_: {"Stacks": [{}]})), {})), \
                patch.object(uninstall, "delete_stack"), patch.object(uninstall, "runtime_resources", return_value={}), \
                patch.object(uninstall, "_exists", return_value=False), \
                patch.object(uninstall.subprocess, "run", side_effect=lambda command, **options: calls.append(
                    (command[1].rsplit("/", 1)[-1], options["env"])) or SimpleNamespace(returncode=0)), \
                patch.object(sys, "argv", ["uninstall.py", "--output", parent + "/receipts"]):
            uninstall.main()
        self.assertEqual([tool for tool, _ in calls], ["admission.py", "manage.py", "manage.py"])
        self.assertTrue(all(env["COLVENNA_ACCOUNT"] == "123456789012" and env["COLVENNA_ENVIRONMENT"] == "qa"
                            for _, env in calls))


if __name__ == "__main__":
    unittest.main()
