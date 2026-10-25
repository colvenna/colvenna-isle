"""The public query path and narrow IAM/network boundary are enforceable."""

import importlib.util
import unittest
from pathlib import Path

from colvenna_isle.contracts.model import ROUTES

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("query_infrastructure", ROOT / "infra/template.py")


class QueryDeployment(unittest.TestCase):
    def setUp(self):
        module = importlib.util.module_from_spec(SPEC)
        SPEC.loader.exec_module(module)
        self.template = module.template()
        self.resources = self.template["Resources"]

    def test_all_query_routes_use_uncached_authorized_private_integration(self):
        for name in ["CatalogRoute", "QueryRoute", "QueryStatusRoute", "QueryCancelRoute",
                     "QueryHistoryRoute", "QueryExportRoute"]:
            route = self.resources[name]["Properties"]
            self.assertEqual(route["AuthorizationType"], "CUSTOM")
            self.assertEqual(route["AuthorizerId"], {"Ref": "ApiAuthorizer"})
            self.assertEqual(route["Target"], {"Fn::Sub": "integrations/${QueryIntegration}"})
            declared = {method + " /v1" + path for method, path, *_ in ROUTES}
            self.assertIn(route["RouteKey"], declared)
        integration = self.resources["QueryIntegration"]["Properties"]
        self.assertEqual(integration["ConnectionType"], "VPC_LINK")
        self.assertEqual(integration["IntegrationUri"], {"Ref": "Listener"})
        mapping = integration["RequestParameters"]
        for field, header in [("principal_id", "principal-id"), ("key_id", "key-id"),
                              ("spaces", "spaces"), ("scopes", "scopes"),
                              ("generation", "generation"), ("expires_at", "expires-at")]:
            self.assertEqual(mapping["overwrite:header.x-colvenna-" + header], "$context.authorizer." + field)
        self.assertEqual(self.resources["ApiAuthorizer"]["Properties"]["AuthorizerResultTtlInSeconds"], 0)

    def test_query_role_cannot_write_database_or_control_authority(self):
        statements = self.resources["QueryRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
        for statement in statements:
            actions = statement["Action"]
            if any(action in actions for action in ["dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem"]):
                self.assertEqual(statement["Condition"]["ForAllValues:StringLike"]["dynamodb:LeadingKeys"],
                                 ["QUERY#*"])
            if "s3:PutObject" in actions:
                resources = statement["Resource"]
                resources = resources if isinstance(resources, list) else [resources]
                for resource in resources:
                    path = resource["Fn::Sub"]
                    self.assertTrue("/manifest/*" in path or "/query-intents/gen_*/*" in path
                                    or "/query-results/gen_*/*" in path)
            self.assertNotIn("s3:DeleteObject", actions)
            self.assertNotIn("ecs:StopTask", actions)
        get = next(statement for statement in statements if statement["Action"] == ["dynamodb:GetItem"])
        self.assertEqual(set(get["Condition"]["ForAllValues:StringLike"]["dynamodb:LeadingKeys"]),
                          {"SPACE#*", "KEYHASH#*", "KEYID#*", "QUERY#*", "WRITER#PRIMARY"})

    def test_cleanup_can_list_versions_only_under_query_intent_and_result_prefixes(self):
        statements = self.resources["QueryRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
        listing = next(statement for statement in statements if "s3:ListBucketVersions" in statement["Action"])
        self.assertEqual(listing["Resource"], {"Fn::GetAtt": ["DataBucket", "Arn"]})
        self.assertEqual(listing["Condition"]["StringLike"]["s3:prefix"], [
            "spaces/*/query-intents/gen_*/*", "spaces/*/query-results/gen_*/*"])

    def test_only_internal_alb_reaches_nonpublic_query_tasks(self):
        service = self.resources["QueryService"]["Properties"]
        self.assertEqual(service["NetworkConfiguration"]["AwsvpcConfiguration"]["AssignPublicIp"], "DISABLED")
        self.assertEqual(self.resources["Alb"]["Properties"]["Scheme"], "internal")
        ingress = [value["Properties"] for value in self.resources.values()
                   if value["Type"] == "AWS::EC2::SecurityGroupIngress"
                   and value["Properties"]["GroupId"] == {"Ref": "QuerySecurityGroup"}]
        self.assertEqual(len(ingress), 1)
        self.assertEqual(ingress[0]["SourceSecurityGroupId"], {"Ref": "AlbSecurityGroup"})
        self.assertEqual(self.template["Parameters"]["QueryDesired"]["AllowedValues"], [0, 1, 2, 3, 4])
        self.assertEqual(self.resources["WriterService"]["Properties"]["DesiredCount"], 0)


if __name__ == "__main__":
    unittest.main()
