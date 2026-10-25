"""Region selection, regional contracts, and unavailable-price behavior."""

import importlib.util
import json
import os
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from colvenna_isle.contracts.validation import ContractError, validate
from colvenna_isle.control.costs import Costs
from colvenna_isle.control.prices import for_region
from colvenna_isle.control.registry import Principal
from colvenna_isle.region import region


ROOT = Path(__file__).resolve().parents[2]


class Regions(unittest.TestCase):
    def test_explicit_configuration_and_runtime_injection_select_a_commercial_region(self):
        for name in ["COLVENNA_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"]:
            for value in ["us-west-2", "eu-central-1", "ap-southeast-2"]:
                with self.subTest(name=name, value=value), patch.dict(os.environ, {name: value}, clear=True):
                    self.assertEqual(region(), value)

    def test_missing_conflicting_and_other_partition_regions_are_rejected(self):
        for environment in [{}, {"COLVENNA_REGION": "eu-central-1", "AWS_REGION": "us-west-2"},
                {"AWS_REGION": "eu-central-1", "AWS_DEFAULT_REGION": "us-west-2"},
                {"AWS_REGION": "cn-north-1"}, {"AWS_REGION": "us-gov-west-1"},
                {"AWS_REGION": "us-iso-east-1"}, {"AWS_REGION": "invalid"}]:
            with self.subTest(environment=environment), patch.dict(os.environ, environment, clear=True):
                with self.assertRaises(RuntimeError):
                    region()

    def test_lambda_ecs_and_ecr_contracts_accept_another_commercial_region(self):
        validate("executor_binding", {"executor": "lambda", "profile": "python_lambda_standard",
            "lambda_mode": "standard",
            "function_arn": "arn:aws:lambda:eu-central-1:123456789012:function:fixture:7"})
        validate("executor_binding", {"executor": "ecs_task", "profile": "python_ecs_standard",
            "task_definition_arn": "arn:aws:ecs:eu-central-1:123456789012:task-definition/fixture:3",
            "image": "123456789012.dkr.ecr.eu-central-1.amazonaws.com/fixture@sha256:" + "f" * 64})
        with self.assertRaises(ContractError):
            validate("executor_binding", {"executor": "lambda", "profile": "python_lambda_standard",
                "lambda_mode": "standard",
                "function_arn": "arn:aws-cn:lambda:cn-north-1:123456789012:function:fixture:7"})

    def test_baseline_uses_stack_region_for_zones_endpoints_and_runtime(self):
        spec = importlib.util.spec_from_file_location("regional_template", ROOT / "infra/template.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        template = module.template()
        self.assertNotIn("us-west-2", json.dumps(template))
        resources = template["Resources"]
        self.assertEqual(resources["PublicSubnet1"]["Properties"]["AvailabilityZone"],
                         {"Fn::Select": [0, {"Fn::GetAZs": {"Ref": "AWS::Region"}}]})
        self.assertEqual(resources["S3Endpoint"]["Properties"]["ServiceName"],
                         {"Fn::Sub": "com.amazonaws.${AWS::Region}.s3"})
        for resource in resources.values():
            if resource["Type"] == "AWS::Lambda::Function":
                self.assertEqual(resource["Properties"]["Environment"]["Variables"]["COLVENNA_REGION"],
                                 {"Ref": "AWS::Region"})
            if resource["Type"] == "AWS::ECS::TaskDefinition":
                for container in resource["Properties"]["ContainerDefinitions"]:
                    if container.get("Image", {}).get("Ref") in {"WriterImage", "QueryImage"}:
                        self.assertIn({"Name": "COLVENNA_REGION", "Value": {"Ref": "AWS::Region"}},
                                      container["Environment"])

    def test_same_environment_in_two_regions_has_disjoint_account_global_runtime_names(self):
        spec = importlib.util.spec_from_file_location("regional_names", ROOT / "infra/names.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        prefixes = []
        for value in ["us-west-2", "eu-central-1"]:
            with patch.dict(os.environ, {"COLVENNA_REGION": value}, clear=True):
                prefixes.append(module.names("prod")["runtime_prefix"])
        self.assertFalse(prefixes[0].startswith(prefixes[1]))
        self.assertFalse(prefixes[1].startswith(prefixes[0]))
        self.assertTrue(all(len(prefix + "f" * 32) <= 64 for prefix in prefixes))

    def test_same_stack_name_in_two_regions_has_disjoint_cloudfront_names(self):
        spec = importlib.util.spec_from_file_location("regional_template", ROOT / "infra/template.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        resources = module.template()["Resources"]
        names = []
        for value in ["us-west-2", "us-east-2"]:
            regional_names = set()
            for resource in resources.values():
                if resource["Type"] == "AWS::CloudFront::Distribution":
                    continue
                if resource["Type"].startswith("AWS::CloudFront::"):
                    configuration, = resource["Properties"].values()
                    name = configuration["Name"]["Fn::Sub"].replace(
                        "${AWS::StackName}", "colvenna-isle-" + "a" * 12 + "-core").replace(
                        "${AWS::Region}", value)
                    self.assertNotIn("${", name)
                    self.assertLessEqual(len(name), 64)
                    regional_names.add(name)
            self.assertEqual(len(regional_names), 4)
            names.append(regional_names)
        self.assertTrue(names[0].isdisjoint(names[1]))

    def test_an_unpriced_region_returns_unknown_without_borrowing_prices_or_measuring_workloads(self):
        principal = Principal("owner", frozenset(["fixture"]), frozenset(["monitor:read"]), "key",
                              "gen_fixture")
        registry = Mock(client=Mock(), table="fixture")
        registry._read.return_value = {"space_id": "fixture", "generation": "gen_fixture"}
        keys = Mock()
        keys.authenticate_id.return_value = principal
        with patch.dict(os.environ, {"COLVENNA_REGION": "eu-central-1"}, clear=True):
            costs = Costs(registry, keys, Mock(), Mock(), Mock(), clock=lambda: 1792551000)
            costs._runs = Mock(side_effect=AssertionError("unpriced workload must not be estimated"))
            value = costs.report(principal, "fixture")
        self.assertEqual(value["status"], "UNKNOWN")
        self.assertEqual(value["prices"], {"region": "eu-central-1", "currency": "USD",
                                          "retrieved": None, "sources": []})
        self.assertEqual((value["lines"], value["totals"]), ([], {}))
        self.assertEqual(value["unmeasured"][0]["reason"], "REGIONAL_PRICES_UNAVAILABLE")
        self.assertIsNone(for_region("eu-central-1"))
        self.assertAlmostEqual(for_region("us-west-2").fargate_second(2, 4),
                               .0000219444444444)
        self.assertEqual(for_region("us-east-2").RETRIEVED, "2026-10-21")
        self.assertEqual(for_region("us-east-2").S3_PUT_REQUEST, .000005)
