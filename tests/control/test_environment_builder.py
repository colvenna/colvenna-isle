"""Runtime-role reuse accepts IAM principal ordering without broadening trust; failure receipts are legible."""

import copy
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("environment_builder", ROOT / "scripts/build_environment.py")
builder = importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)
spec = importlib.util.spec_from_file_location("environment_resolve", ROOT / "runtime/environment_resolve.py")
resolver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(resolver)


class RuntimeRoleTests(unittest.TestCase):
    def setUp(self):
        self.specification = {"space_id": "owned", "generation": "gen_owned", "policy": {
            "bucket": "owned-bucket", "runtime_boundary_arn": "owned-boundary",
            "runtime_log_arn": "owned-logs", "runtime_prefix": "colvenna-isle-env-"}}
        self.trust = {"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Principal": {"Service": [
                "lambda.amazonaws.com", "ecs-tasks.amazonaws.com"]}, "Action": "sts:AssumeRole"}]}
        self.iam = Mock()
        self.iam.get_role.return_value = {"Role": {
            "Arn": "owned-role", "AssumeRolePolicyDocument": self.trust,
            "Tags": [{"Key": "Project", "Value": "colvenna-isle"}, {"Key": "SpaceId", "Value": "owned"},
                     {"Key": "Generation", "Value": "gen_owned"}],
            "PermissionsBoundary": {"PermissionsBoundaryArn": "owned-boundary"}}}
        self.iam.list_attached_role_policies.return_value = {"AttachedPolicies": []}
        self.iam.list_role_policies.return_value = {"PolicyNames": []}
        self.session = Mock()
        self.session.client.return_value = self.iam

    def test_existing_role_accepts_both_service_principal_orders(self):
        for services in [["lambda.amazonaws.com", "ecs-tasks.amazonaws.com"],
                         ["ecs-tasks.amazonaws.com", "lambda.amazonaws.com"]]:
            with self.subTest(services=services), patch.object(builder.time, "sleep"):
                self.trust["Statement"][0]["Principal"]["Service"] = services
                self.assertEqual(builder.runtime_role(self.session, self.specification), "owned-role")

    def test_additional_service_or_statement_cannot_broaden_runtime_trust(self):
        additional_service = copy.deepcopy(self.trust)
        additional_service["Statement"][0]["Principal"]["Service"].append("ec2.amazonaws.com")
        additional_statement = copy.deepcopy(self.trust)
        additional_statement["Statement"].append({
            "Effect": "Allow", "Principal": {"AWS": "*"}, "Action": "sts:AssumeRole"})
        for trust in [additional_service, additional_statement]:
            with self.subTest(trust=trust):
                self.iam.get_role.return_value["Role"]["AssumeRolePolicyDocument"] = trust
                with self.assertRaisesRegex(RuntimeError, "ENVIRONMENT_ROLE_OWNERSHIP_MISMATCH"):
                    builder.runtime_role(self.session, self.specification)
        self.iam.put_role_policy.assert_not_called()


class FunctionCreationTests(unittest.TestCase):
    """A role created moments earlier, possibly by a concurrent build of the same space, is not yet assumable."""

    class Refused(Exception):
        pass

    def client(self, failures, message="The role defined for the function cannot be assumed by Lambda."):
        lam = Mock()
        lam.exceptions.InvalidParameterValueException = self.Refused
        lam.create_function.side_effect = [self.Refused(message)] * failures + [{"Version": "1"}]
        return lam

    def test_role_propagation_refusal_is_retried_until_lambda_assumes_it(self):
        lam = self.client(2)
        with patch.object(builder.time, "sleep") as sleep:
            self.assertEqual(builder.create_function(lam, FunctionName="f"), {"Version": "1"})
        self.assertEqual(lam.create_function.call_count, 3)
        self.assertEqual(sleep.call_count, 2)

    def test_other_refusals_are_not_retried(self):
        lam = self.client(1, "Unzipped size must be smaller than 262144000 bytes")
        with patch.object(builder.time, "sleep"), self.assertRaises(self.Refused):
            builder.create_function(lam, FunctionName="f")
        self.assertEqual(lam.create_function.call_count, 1)


class FailureReceiptTests(unittest.TestCase):
    def receipt(self, stage, log, error=None):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "resolve.log"
            path.write_text(log)
            with patch.dict(builder.PROGRESS, {"stage": stage, "log": path}):
                return builder.failure_receipt(error or RuntimeError("exit 1"), "build_x")

    def test_resolver_terminal_replaces_image_pull_noise(self):
        log = "".join(f"layer{i}: Pull complete\n" for i in range(20)) + json.dumps({
            "status": "FAILED", "code": "DEPENDENCY_NOT_FOUND", "requirements": ["no-such==9.9.9"]}) + "\n"
        value = self.receipt("RESOLUTION", log)
        self.assertEqual(value["code"], "DEPENDENCY_NOT_FOUND")
        self.assertIn("no-such==9.9.9", value["message"])
        self.assertNotIn("Pull complete", value["message"])
        value = self.receipt("RESOLUTION", log.replace("DEPENDENCY_NOT_FOUND", "DEPENDENCY_LICENSE_NOT_APPROVED"))
        self.assertEqual(value["code"], "DEPENDENCY_LICENSE_NOT_APPROVED")
        self.assertIn("license", value["message"])

    def test_other_stages_and_unparsed_logs_keep_the_redacted_tail(self):
        value = self.receipt("IMAGE", "step 1\nhttps://user:secret@example.com/x AKIAABCDEFGHIJKLMNOP\n")
        self.assertEqual(value["code"], "ENVIRONMENT_IMAGE_BUILD_FAILED")
        self.assertNotIn("secret", value["message"])
        self.assertNotIn("AKIAABCDEFGHIJKLMNOP", value["message"])
        self.assertEqual(self.receipt("RESOLUTION", "killed\n")["code"], "DEPENDENCY_RESOLUTION_FAILED")


class ResolverDiagnosticsTests(unittest.TestCase):
    def test_only_submitted_requirements_are_named(self):
        output = ("ERROR: Could not find a version that satisfies the requirement Colvenna_Missing>=1 (from versions: none)\n"
                  "ERROR: No matching distribution found for transitive-dep==2 https://evil.example/\n")
        self.assertEqual(resolver.unresolved(output, ["six==1.16.0", "colvenna-missing>=1"]), ["colvenna-missing>=1"])
        self.assertEqual(resolver.unresolved("ERROR: ResolutionImpossible\n", ["six==1.16.0"]), [])


if __name__ == "__main__":
    unittest.main()
