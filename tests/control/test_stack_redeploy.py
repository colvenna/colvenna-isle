"""Redeploying the same release: an identical stack update is a confirmed no-op, not a failed step."""

import os
import re
import sys
import unittest
from pathlib import Path

from botocore.exceptions import ClientError

# The operator tools name their stacks from an explicit environment.
os.environ.setdefault("COLVENNA_ENVIRONMENT", "qa")

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/aws"))
import manage  # noqa: E402
import install  # noqa: E402


def refusal(message):
    return ClientError({"Error": {"Code": "ValidationError", "Message": message}}, "UpdateStack")


class CloudFormation:
    def __init__(self, error=None):
        self.error, self.calls = error, []

    def update_stack(self, **arguments):
        self.calls.append(arguments)
        if self.error:
            raise self.error
        return {"StackId": "arn:stack/updated"}

    def describe_stacks(self, StackName):
        return {"Stacks": [{"StackId": "arn:stack/" + StackName, "StackStatus": "UPDATE_COMPLETE"}]}


class StackRedeploy(unittest.TestCase):
    def test_update_is_dispatched_unchanged(self):
        cf = CloudFormation()
        self.assertEqual(manage.update_stack(cf, StackName="one", TemplateBody="{}"), {"StackId": "arn:stack/updated"})
        self.assertEqual(cf.calls, [{"StackName": "one", "TemplateBody": "{}"}])

    def test_identical_template_and_parameters_is_a_confirmed_no_op(self):
        cf = CloudFormation(refusal("No updates are to be performed."))
        self.assertEqual(manage.update_stack(cf, StackName="one"), {"StackId": "arn:stack/one", "NoUpdates": True})

    def test_other_refusals_are_not_hidden(self):
        for message in ["Stack:arn is in UPDATE_IN_PROGRESS state and can not be updated.", "Template format error"]:
            with self.assertRaises(ClientError):
                manage.update_stack(CloudFormation(refusal(message)), StackName="one")

    def test_resume_reruns_the_builder_step_whose_configuration_later_steps_read(self):
        self.assertEqual(install.plan_steps("upgrade"), install.PLANS["upgrade"])
        self.assertEqual(install.plan_steps("upgrade", "open,close,writer"), ["close", "environments", "writer", "open"])
        self.assertEqual(install.plan_steps("upgrade", "intake,query"), ["intake", "query"])
        self.assertEqual(install.plan_steps("fresh", "control-registry,open"), ["environments", "control-registry", "open"])
        with self.assertRaises(ValueError):
            install.plan_steps("upgrade", "baseline")

    def test_every_deployment_tool_updates_through_the_helper(self):
        for path in sorted((ROOT / "scripts/aws").glob("*.py")):
            if path.name != "manage.py":
                self.assertIsNone(re.search(r"cf\.update_stack\b", path.read_text()), path.name)


if __name__ == "__main__":
    unittest.main()
