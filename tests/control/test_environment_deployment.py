"""Independent source identities and the trusted-builder/user-runtime permission seam."""

import importlib.util
import json
import os
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]
ACCOUNT = "123456789012"


def module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "infra" / (name + ".py"))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


class DeploymentTests(unittest.TestCase):
    def test_independent_environments_share_no_stack_name_or_runtime_wildcard(self):
        names = module("names")
        first, isolated = names.names("prod"), names.names("uatb")
        self.assertEqual({key: first[key] for key in ["environment", "baseline", "control", "environments"]},
                         {"environment": "prod", "baseline": "colvenna-isle-prod-core",
                          "control": "colvenna-isle-prod-control", "environments": "colvenna-isle-prod-builder"})
        self.assertFalse({first[key] for key in ["baseline", "control", "environments"]}
                         & {isolated[key] for key in ["baseline", "control", "environments"]})
        # Neither environment's IAM wildcard (<prefix>*) matches the other's runtime resources.
        self.assertFalse(isolated["runtime_prefix"].startswith(first["runtime_prefix"]))
        self.assertFalse(first["runtime_prefix"].startswith(isolated["runtime_prefix"]))
        for invalid in ["env", "core", "control", "builder", "UATB", "a", "uat-2", "x" * 13]:
            with self.assertRaises(RuntimeError):
                names.names(invalid)
        with patch.dict(os.environ, {"COLVENNA_ENVIRONMENT": "uatb"}):
            self.assertEqual(names.names(), isolated)
        with patch.dict(os.environ, {"COLVENNA_ENVIRONMENT": ""}), self.assertRaises(RuntimeError):
            names.names()
        policy = json.loads((ROOT / "runtime/environment-lock.json").read_text())
        policy.update(source_commit="2" * 40, repository="owned-repository", runtime_prefix=isolated["runtime_prefix"])
        rendered = json.dumps(module("environments").template(
            self.baseline(), {"key": "artifacts/builder.zip", "version_id": "source-original"}, policy,
            isolated["environments"], account=ACCOUNT))
        self.assertNotIn("colvenna-isle-prod-env-", rendered)
        self.assertIn("project/colvenna-isle-uatb-builder", rendered)

    def test_backup_vault_is_retained_tls_only_and_the_writer_can_never_delete_backups(self):
        vault = module("backups").template("2" * 40)["Resources"]
        self.assertEqual((vault["Vault"]["DeletionPolicy"], vault["Vault"]["UpdateReplacePolicy"]), ("Retain", "Retain"))
        self.assertEqual(vault["Vault"]["Properties"]["VersioningConfiguration"], {"Status": "Enabled"})
        deny = vault["VaultPolicy"]["Properties"]["PolicyDocument"]["Statement"][0]
        self.assertEqual((deny["Effect"], deny["Condition"]), ("Deny", {"Bool": {"aws:SecureTransport": "false"}}))
        baseline = module("template").template()
        statements = baseline["Resources"]["WriterRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
        backups = [statement["Fn::If"][1] for statement in statements if "Fn::If" in statement]
        self.assertEqual(backups, [{"Effect": "Allow", "Action": ["s3:PutObject", "s3:GetObject", "s3:GetObjectVersion"],
                                    "Resource": {"Fn::Sub": "arn:aws:s3:::${BackupBucket}/backups/*"}}])
        self.assertEqual(baseline["Parameters"]["BackupBucket"]["Default"], "")
        rendered = json.dumps(statements)
        self.assertNotIn("${BackupBucket}/*\"", rendered)

    def test_packaged_buildspec_commands_are_yaml_strings_for_codebuild(self):
        import yaml

        spec = importlib.util.spec_from_file_location(
            "environment_source", ROOT / "scripts/build_environment_source.py")
        source = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(source)
        definition = yaml.safe_load(source.BUILDSPEC)
        self.assertEqual(definition["phases"]["install"]["runtime-versions"]["python"], "3.12")
        for phase in ["install", "build"]:
            self.assertTrue(all(isinstance(command, str)
                                for command in definition["phases"][phase]["commands"]))

    def baseline(self):
        return {"DataBucket": "owned-bucket", "ControlTable": "owned-table", "Api": "owned-api",
                "ApiUrl": "https://owned-api", "WriterControlFunction": "original-writer",
                "WriterSourceCommit": "1" * 40, "StackId": "owned-stack",
                "ExecutionRoleArn": "arn:aws:iam::123456789012:role/owned-execution", "Cluster": "owned-cluster"}

    def test_control_update_binds_the_existing_writer_source_independently(self):
        definition = module("control").template(
            self.baseline(), "auth", {"key": "code.zip", "version_id": "original"}, "2" * 40, account=ACCOUNT)
        variables = definition["Resources"]["Control"]["Properties"]["Environment"]["Variables"]
        self.assertEqual(variables["COLVENNA_SOURCE_COMMIT"], "2" * 40)
        self.assertEqual(variables["COLVENNA_WRITER_SOURCE_COMMIT"], "1" * 40)

        from colvenna_isle.control import handler
        environment = {**variables, "COLVENNA_REGION": "us-west-2",
                       "COLVENNA_ENVIRONMENT_PROJECT": "", "COLVENNA_ENVIRONMENT_POLICY": "{}"}
        with patch.dict(os.environ, environment), patch.object(handler, "clients", return_value=(Mock(), Mock(), Mock())):
            with patch("colvenna_isle.control.status.Ledger") as ledger, patch.object(handler, "handle"):
                handler.handler({}, None)
            self.assertEqual(ledger.call_args.args[-1], "1" * 40)

    def test_control_can_observe_ecs_definition_with_service_authorization_scope(self):
        definition = module("control").template(
            self.baseline(), "auth", {"key": "code.zip", "version_id": "original"}, "2" * 40,
            environment={"project": "owned-builder", "policy": {"repository": "owned-repository",
                                                                 "runtime_prefix": "colvenna-isle-env-"}},
            account=ACCOUNT)
        statements = definition["Resources"]["Role"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
        ecs = [statement for statement in statements
               if any(action.startswith("ecs:") for action in statement["Action"])]
        self.assertEqual(ecs, [{"Effect": "Allow", "Action": ["ecs:DescribeTaskDefinition"],
                               "Resource": "*"}])

    def test_user_boundary_has_no_builder_private_package_or_warehouse_permission(self):
        policy = json.loads((ROOT / "runtime/environment-lock.json").read_text())
        policy.update(source_commit="2" * 40, repository="owned-repository", runtime_prefix="colvenna-isle-env-", private_wheels={
            "approved": {"s3_uri": "s3://owned-bucket/artifacts/private-wheels/approved.whl",
                         "object_version": "private-original"}})
        resources = module("environments").template(
            self.baseline(), {"key": "artifacts/builder.zip", "version_id": "source-original"}, policy,
            "colvenna-isle-prod-builder", account=ACCOUNT)["Resources"]
        boundary = resources["RuntimeBoundary"]["Properties"]["PolicyDocument"]["Statement"]
        allowed = {action for statement in boundary for action in statement["Action"]}
        # sqs:SendMessage is limited to the pipeline notification queue (Lambda destinations).
        self.assertEqual(allowed, {"s3:GetObjectVersion", "s3:PutObject", "logs:CreateLogStream", "logs:PutLogEvents",
                                   "sqs:SendMessage"})
        queue = next(statement for statement in boundary if statement["Action"] == ["sqs:SendMessage"])
        self.assertEqual(queue["Resource"], {"Fn::GetAtt": ["PipelineEvents", "Arn"]})
        rendered = json.dumps(boundary)
        for forbidden in ["private-wheels", "warehouse", "codebuild", "ecr:", "iam:"]:
            self.assertNotIn(forbidden, rendered)
        builder = resources["BuildRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
        private = next(statement for statement in builder
                       if statement.get("Resource") == "arn:aws:s3:::owned-bucket/artifacts/private-wheels/approved.whl")
        self.assertEqual(private["Condition"], {"StringEquals": {"s3:VersionId": "private-original"}})
        source_reads = [statement for statement in builder
                        if statement.get("Resource") == "arn:aws:s3:::owned-bucket/artifacts/builder.zip"]
        self.assertEqual({action for statement in source_reads for action in statement["Action"]},
                         {"s3:GetObject", "s3:GetObjectVersion"})
        versioned = next(statement for statement in source_reads if "s3:GetObjectVersion" in statement["Action"])
        self.assertEqual(versioned["Condition"], {"StringEquals": {"s3:VersionId": "source-original"}})
        execution = resources["ExecutionRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
        log_statement = next(statement for statement in execution if "logs:PutLogEvents" in statement["Action"])
        self.assertIn("${RuntimeLogs}", log_statement["Resource"]["Fn::Sub"])
        self.assertFalse(any(action.startswith("s3:") for statement in execution for action in statement["Action"]))

    def test_only_control_can_publish_descriptors_and_user_boundary_cannot_write_them(self):
        control = module("control").template(
            self.baseline(), "auth", {"key": "code.zip", "version_id": "original"}, "2" * 40, account=ACCOUNT)
        statements = control["Resources"]["Role"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
        descriptor = [statement for statement in statements if "/task-contexts/" in str(statement["Resource"])]
        self.assertEqual(descriptor, [{"Effect": "Allow", "Action": ["s3:PutObject", "s3:GetObjectVersion"],
            "Resource": "arn:aws:s3:::owned-bucket/spaces/*/task-contexts/gen_*/*"}])
        policy = json.loads((ROOT / "runtime/environment-lock.json").read_text())
        policy.update(source_commit="2" * 40, repository="owned-repository", runtime_prefix="colvenna-isle-env-")
        runtime = module("environments").template(
            self.baseline(), {"key": "artifacts/builder.zip", "version_id": "source-original"}, policy,
            "colvenna-isle-prod-builder", account=ACCOUNT)
        boundary = runtime["Resources"]["RuntimeBoundary"]["Properties"]["PolicyDocument"]["Statement"]
        self.assertFalse(any("s3:PutObject" in statement["Action"]
                             and "task-contexts" in str(statement["Resource"]) for statement in boundary))

    def governed(self):
        baseline = {**self.baseline(), "PrivateSubnet1": "subnet-1", "PrivateSubnet2": "subnet-2", "Vpc": "vpc-1",
                    "IngressQueue": "https://sqs.us-west-2.amazonaws.com/123456789012/owned-ingress.fifo",
                    "Dlq": "https://sqs.us-west-2.amazonaws.com/123456789012/owned-dlq.fifo"}
        policy = {"repository": "owned-repository", "runtime_prefix": "colvenna-isle-env-", "notification_queue_arn":
                  "arn:aws:sqs:us-west-2:123456789012:owned-events", "notification_queue_url": "https://events",
                  "execution_role_arn": "arn:aws:iam::123456789012:role/owned-execution"}
        return module("control").template(baseline, "auth", {"key": "code.zip", "version_id": "original"}, "2" * 40,
                                          environment={"project": "owned-builder", "policy": policy},
                                          pipelines={"state": "ENABLED"}, account=ACCOUNT)

    def test_every_contract_route_has_a_gateway_route_and_admin_routes_use_the_authorizer(self):
        from colvenna_isle.contracts.model import ROUTES

        definition = self.governed()
        routes = {value["Properties"]["RouteKey"]: value["Properties"] for value in definition["Resources"].values()
                  if value["Type"] == "AWS::ApiGatewayV2::Route"}
        baseline = module("template").template()["Resources"]
        routes.update({value["Properties"]["RouteKey"]: value["Properties"] for value in baseline.values()
                       if value["Type"] == "AWS::ApiGatewayV2::Route"})
        # Intake-owned ingestion routes are served by the baseline $default integration. The
        # dataset metadata route has no service and also falls through to $default (404).
        intake = {"POST /v1/spaces/{space_id}/ingestions",
                  "GET /v1/spaces/{space_id}/ingestions/{ingestion_id}/diagnostics",
                  "POST /v1/spaces/{space_id}/ingestions/{ingestion_id}/replay",
                  "GET /v1/spaces/{space_id}/datasets/{dataset_id}"}
        declared = {method + " /v1" + path for method, path, *_ in ROUTES}
        self.assertEqual(declared - intake - set(routes), set())
        for key in declared - intake:
            self.assertEqual(routes[key]["AuthorizationType"], "CUSTOM", key)

    def test_collector_can_delete_only_collected_prefixes_and_never_native_database_objects(self):
        resources = self.governed()["Resources"]
        statements = resources["CollectorRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
        deletes = [statement for statement in statements if statement["Effect"] == "Allow"
                   and "s3:DeleteObjectVersion" in statement["Action"]]
        self.assertEqual(len(deletes), 1)
        self.assertTrue(all("/warehouse" not in resource and "/gen_*/" in resource
                            for resource in deletes[0]["Resource"]))
        deny = next(statement for statement in statements if statement["Effect"] == "Deny")
        self.assertEqual(deny["Resource"], "arn:aws:s3:::owned-bucket/spaces/*/gen_*/warehouse/*")
        self.assertEqual(resources["Collector"]["Properties"]["ReservedConcurrentExecutions"], 1)
        control = resources["Control"]["Properties"]["Environment"]["Variables"]
        self.assertEqual(control["COLVENNA_COLLECTOR_FUNCTION"], {"Ref": "Collector"})
        governance = resources["GovernancePolicy"]["Properties"]["PolicyDocument"]["Statement"]
        self.assertFalse(any(action.startswith("s3:Delete") for statement in governance
                             for action in statement["Action"]))

    def test_scheduler_can_write_the_audit_record_of_the_runs_it_admits(self):
        from fnmatch import fnmatch
        from colvenna_isle.control.audit import partition

        statements = self.governed()["Resources"]["SchedulerRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
        writable = [key for statement in statements if "dynamodb:PutItem" in statement.get("Action", [])
                    for key in statement["Condition"]["ForAllValues:StringLike"]["dynamodb:LeadingKeys"]]
        # Run admission puts the run record and its audit record in one transaction.
        for key in ["PIPELINERUNS#1", partition("space_a", "gen_" + "a" * 32)]:
            self.assertTrue(any(fnmatch(key, pattern) for pattern in writable), key)

    def test_intake_may_write_only_replay_audit_records(self):
        resources = module("template").template()["Resources"]
        statements = resources["IntakeRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
        writes = [statement for statement in statements if "dynamodb:PutItem" in statement["Action"]]
        self.assertEqual(len(writes), 1)
        self.assertEqual(writes[0]["Condition"]["ForAllValues:StringLike"]["dynamodb:LeadingKeys"],
                         ["AUDIT#*", "AUDITMARK#*"])


if __name__ == "__main__":
    unittest.main()
