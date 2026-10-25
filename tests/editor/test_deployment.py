"""The browser can reach authenticated API routes without sharing cached responses."""
import importlib.util
import copy
import os
import sys
import unittest
from pathlib import Path

# The operator tools name their stacks from an explicit environment.
os.environ.setdefault("COLVENNA_ENVIRONMENT", "qa")

ROOT = Path(__file__).resolve().parents[2]


class EditorDeployment(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location("editor_infrastructure", ROOT / "infra/template.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.resources = module.template()["Resources"]

    def test_same_origin_api_preserves_authorization_and_has_zero_cache_ttl(self):
        distribution = self.resources["Distribution"]["Properties"]["DistributionConfig"]
        api = next(origin for origin in distribution["Origins"] if origin["Id"] == "browser-api")
        self.assertEqual(api["CustomOriginConfig"]["OriginProtocolPolicy"], "https-only")
        behavior = next(item for item in distribution["CacheBehaviors"] if item["PathPattern"] == "v1/*")
        self.assertEqual(behavior["TargetOriginId"], "browser-api")
        self.assertEqual(behavior["ViewerProtocolPolicy"], "https-only")
        cache = self.resources[behavior["CachePolicyId"]["Ref"]]["Properties"]["CachePolicyConfig"]
        self.assertEqual((cache["MinTTL"], cache["DefaultTTL"], cache["MaxTTL"]), (0, 0, 0))
        forwarding = self.resources[behavior["OriginRequestPolicyId"]["Ref"]]["Properties"]["OriginRequestPolicyConfig"]
        self.assertEqual(forwarding["HeadersConfig"], {"HeaderBehavior": "allExcept", "Headers": ["host"]})
        self.assertEqual(forwarding["CookiesConfig"], {"CookieBehavior": "none"})
        self.assertEqual(forwarding["QueryStringsConfig"], {"QueryStringBehavior": "all"})
        self.assertTrue(all(item["ErrorCachingMinTTL"] == 0 for item in distribution["CustomErrorResponses"]))
        # The API route's independent authorizer remains mandatory behind the CDN.
        for name in ["CatalogRoute", "QueryRoute", "QueryStatusRoute", "QueryCancelRoute",
                     "QueryHistoryRoute", "QueryExportRoute"]:
            self.assertEqual(self.resources[name]["Properties"]["AuthorizationType"], "CUSTOM")

    def test_static_access_is_limited_to_named_files_and_scripts_cannot_connect_off_origin(self):
        statement = self.resources["WebPolicy"]["Properties"]["PolicyDocument"]["Statement"][-1]
        self.assertEqual(statement["Principal"], {"Service": "cloudfront.amazonaws.com"})
        self.assertEqual(statement["Resource"], [
            {"Fn::Sub": "${WebBucket.Arn}/" + name}
            for name in ["index.html", "query-editor/app.js", "query-editor/app.css",
                         "admin.html", "admin/app.js", "admin/app.css"]])
        self.assertIn("AWS:SourceArn", statement["Condition"]["StringEquals"])
        policy = self.resources["Headers"]["Properties"]["ResponseHeadersPolicyConfig"]
        csp = policy["SecurityHeadersConfig"]["ContentSecurityPolicy"]["ContentSecurityPolicy"]
        self.assertIn("script-src 'self'", csp)
        self.assertIn("connect-src 'self'", csp)
        self.assertNotIn("'unsafe-inline'", csp)
        self.assertNotIn("'unsafe-eval'", csp)

    def test_browser_deployment_rejects_unrelated_query_service_and_iam_changes(self):
        sys.path.insert(0, str(ROOT / "scripts/aws"))
        spec = importlib.util.spec_from_file_location("editor_deployment", ROOT / "scripts/aws/editor.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        original = {"Resources": self.resources}
        for resource in ["QueryService", "QueryRole", "WriterService", "WriterRole"]:
            candidate = copy.deepcopy(original)
            candidate["Resources"][resource]["Properties"]["UnexpectedChange"] = True
            with self.assertRaisesRegex(RuntimeError, "Unrelated template changes"):
                module.check_template(original, candidate)
        candidate = copy.deepcopy(original)
        candidate["Resources"]["Headers"]["Properties"]["Comment"] = "browser-only change"
        module.check_template(original, candidate)


if __name__ == "__main__":
    unittest.main()
