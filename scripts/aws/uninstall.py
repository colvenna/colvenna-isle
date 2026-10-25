#!/usr/bin/env python3
"""Remove one Colvenna Isle environment completely and prove what is gone.

Admission is closed first (writer stopped with a terminal receipt, query at zero). Then the control stack is
deleted, then every runtime resource the builder created under this environment's prefix (Lambda functions, task
definition families and execution roles, each positively identified by prefix and Project tag; the roles carry
the builder stack's boundary policy, which cannot be deleted while attached), then the builder stack, and finally
the baseline with its own buckets and GuardDuty endpoint. The account backup vault and the shared
image repository are never touched.
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT), str(Path(__file__).parent)]
from manage import ACCOUNT, REGION, STACKS, encode, session, source, wait


def delete_stack(cf, name, directory):
    try:
        stack = cf.describe_stacks(StackName=name)["Stacks"][0]
    except ClientError as error:
        if "does not exist" in error.response["Error"].get("Message", ""):
            return "ABSENT"
        raise
    tags = {tag["Key"]: tag["Value"] for tag in stack.get("Tags", [])}
    if tags.get("Project") != "colvenna-isle" or f":{REGION}:{ACCOUNT}:stack/{name}/" not in stack["StackId"]:
        raise RuntimeError("Refuse to delete a stack that is not positively owned: " + name)
    cf.delete_stack(StackName=stack["StackId"])
    status = wait(cf, stack["StackId"], directory / (name + "-terminal.json"))
    if status != "DELETE_COMPLETE":
        raise RuntimeError(name + " deletion " + status + "; reconcile before retrying")
    return status


def runtime_resources(current, prefix, directory):
    """Delete builder-created runtime resources: exact environment prefix and Project tag, nothing else."""
    lam, ecs, iam = current.client("lambda"), current.client("ecs"), current.client("iam")
    removed = {"functions": [], "task_definitions": [], "roles": []}
    for page in lam.get_paginator("list_functions").paginate():
        for function in page["Functions"]:
            if not function["FunctionName"].startswith(prefix):
                continue
            tags = lam.list_tags(Resource=function["FunctionArn"])["Tags"]
            if tags.get("Project") != "colvenna-isle":
                raise RuntimeError("Runtime function without the project tag: " + function["FunctionName"])
            lam.delete_function(FunctionName=function["FunctionName"])
            removed["functions"].append(function["FunctionArn"])
    # ListTaskDefinitions takes an exact family name; only ListTaskDefinitionFamilies matches a prefix.
    families = [family for page in ecs.get_paginator("list_task_definition_families").paginate(
        familyPrefix=prefix, status="ALL") for family in page["families"] if family.startswith(prefix)]
    for family in families:
        for status in ["ACTIVE", "INACTIVE"]:
            for page in ecs.get_paginator("list_task_definitions").paginate(familyPrefix=family, status=status):
                for arn in page["taskDefinitionArns"]:
                    described = ecs.describe_task_definition(taskDefinition=arn, include=["TAGS"])
                    if {tag["key"]: tag["value"] for tag in described.get("tags", [])}.get("Project") != "colvenna-isle":
                        raise RuntimeError("Runtime task definition without the project tag: " + arn)
                    if status == "ACTIVE":
                        ecs.deregister_task_definition(taskDefinition=arn)
                    ecs.delete_task_definitions(taskDefinitions=[arn])
                    removed["task_definitions"].append(arn)
    for page in iam.get_paginator("list_roles").paginate():
        for role in page["Roles"]:
            if not role["RoleName"].startswith(prefix):
                continue
            tags = {tag["Key"]: tag["Value"] for tag in iam.list_role_tags(RoleName=role["RoleName"])["Tags"]}
            if tags.get("Project") != "colvenna-isle":
                raise RuntimeError("Runtime role without the project tag: " + role["RoleName"])
            for name in iam.list_role_policies(RoleName=role["RoleName"])["PolicyNames"]:
                iam.delete_role_policy(RoleName=role["RoleName"], PolicyName=name)
            for policy in iam.list_attached_role_policies(RoleName=role["RoleName"])["AttachedPolicies"]:
                iam.detach_role_policy(RoleName=role["RoleName"], PolicyArn=policy["PolicyArn"])
            if iam.get_role(RoleName=role["RoleName"])["Role"].get("PermissionsBoundary"):
                iam.delete_role_permissions_boundary(RoleName=role["RoleName"])
            iam.delete_role(RoleName=role["RoleName"])
            removed["roles"].append(role["Arn"])
    (directory / "runtime-resources.json").write_text(encode(removed))
    return removed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    directory = args.output.resolve()
    if directory.exists() or directory == ROOT or ROOT in directory.parents:
        parser.error("use a new external receipt directory")
    source()
    current, identity = session()
    cf = current.client("cloudformation")
    directory.mkdir(parents=True)
    (directory / "identity.json").write_text(encode({**identity, "environment": STACKS}))
    record = {"environment": STACKS, "steps": []}

    def step(name, value):
        record["steps"].append({"step": name, "result": value, "at": time.time()})
        (directory / "uninstall.json").write_text(encode(record))
        print(json.dumps({"step": name, "result": value}, default=str), flush=True)

    # The child tools re-check the caller against the same explicit account.
    environment = {"PATH": "/usr/bin:/bin", "AWS_REGION": REGION, "COLVENNA_ENVIRONMENT": STACKS["environment"],
                   "COLVENNA_ACCOUNT": ACCOUNT, "HOME": str(Path.home())}
    try:
        cf.describe_stacks(StackName=STACKS["baseline"])
        closed = subprocess.run([sys.executable, str(ROOT / "scripts/aws/admission.py"), "close",
                                 "--output", str(directory / "close")], cwd=ROOT, env=environment)
        if closed.returncode:
            raise RuntimeError("Close admission and reconcile the writer before uninstalling")
        step("close", "CLOSED")
    except ClientError:
        step("close", "BASELINE_ABSENT")
    step("control", delete_stack(cf, STACKS["control"], directory))
    step("runtime", {name: len(values) for name, values in runtime_resources(
        current, STACKS["runtime_prefix"], directory).items()})
    step("environments", delete_stack(cf, STACKS["environments"], directory))
    for operation in ["stop", "delete"]:
        try:
            cf.describe_stacks(StackName=STACKS["baseline"])
        except ClientError:
            step(operation, "ABSENT")
            continue
        result = subprocess.run([sys.executable, str(ROOT / "scripts/aws/manage.py"), operation, "--stack",
                                 STACKS["baseline"], "--output", str(directory / operation)], cwd=ROOT, env=environment)
        if result.returncode:
            raise RuntimeError("baseline " + operation + " failed; reconcile the original stack operation")
        step(operation, "DONE")
    remaining = {name: STACKS[name] for name in ["baseline", "control", "environments"] if _exists(cf, STACKS[name])}
    step("verify", {"remaining_stacks": remaining})
    if remaining:
        raise RuntimeError("Stacks remain after uninstall: " + json.dumps(remaining))
    print(encode({"status": "UNINSTALLED", "environment": STACKS["environment"]}))


def _exists(cf, name):
    try:
        status = cf.describe_stacks(StackName=name)["Stacks"][0]["StackStatus"]
    except ClientError:
        return False
    return status != "DELETE_COMPLETE"


if __name__ == "__main__":
    main()
