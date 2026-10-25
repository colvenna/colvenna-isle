#!/usr/bin/env python3
"""Deploy/provision only the control service on the positively owned baseline."""

import argparse
import hashlib
import json
import sys
import uuid
from pathlib import Path

from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT), str(ROOT / "src"), str(Path(__file__).parent)]
from infra.control import template
from infra.template import template as baseline_template
from manage import ACCOUNT, STACKS, encode, outputs, owned_stack, publish_artifact, session, source, template_arguments, update_stack, wait
from colvenna_isle.control.registry import Principal, Registry
from colvenna_isle.control.keys import KeyStore
from colvenna_isle.writer.controller import Controller


def stopped(current, values):
    ecs = current.client("ecs")
    result = ecs.describe_services(
        cluster=values["Cluster"], services=[values["WriterService"], values["QueryService"]])
    if result.get("failures") or len(result["services"]) != 2 or any(
            service[field] for service in result["services"]
            for field in ["desiredCount", "runningCount", "pendingCount"]):
        raise RuntimeError("Prove old services stopped before deployment/rebuild")
    controller = Controller(current, values, lambda name, value: None)
    result["colvenna_task_terminal_receipts"] = controller.idle()
    result["colvenna_writer_ownership"] = controller.ownership.read()
    return result


def deploy_authorizer(current, baseline, artifact, head, directory, *, enabled=False):
    """Keep original CloudFormation resource ownership and preserve storage identity."""
    directory.mkdir()
    values = outputs(baseline)
    (directory / "stopped.json").write_text(encode(stopped(current, values)))
    previous = {entry["ParameterKey"]: entry["ParameterValue"] for entry in baseline["Parameters"]}
    parameters = {
        "SourceCommit": head, "StorageImage": previous["StorageImage"], "QueryDesired": "0",
        "AuthCodeBucket": values["DataBucket"], "AuthCodeKey": artifact["key"],
        "AuthCodeVersion": artifact["version_id"], "AuthEnabled": "true" if enabled else "false",
        "AuthSourceCommit": head,
    }
    definition = baseline_template()
    body = json.dumps(definition, separators=(",", ":"))
    cf = current.client("cloudformation")
    (directory / "template.json").write_text(body + "\n")
    (directory / "validation.json").write_text(encode(cf.validate_template(**template_arguments(current, body))))
    token = "colvenna-auth-" + uuid.uuid4().hex
    (directory / "admission.json").write_text(encode({
        "stack_id": baseline["StackId"], "source_commit": head, "artifact": artifact,
        "auth_enabled": enabled, "client_request_token": token,
        "storage_image": previous["StorageImage"], "parameters": parameters}))
    updates = [{"ParameterKey": name, "ParameterValue": value} for name, value in parameters.items()]
    updates += [{"ParameterKey": name, "UsePreviousValue": True}
                for name in previous if name not in parameters and name in definition["Parameters"]]
    result = update_stack(
        cf, StackName=baseline["StackId"], **template_arguments(current, body), Capabilities=["CAPABILITY_IAM"],
        Parameters=updates,
        ClientRequestToken=token, Tags=[{"Key": "Project", "Value": "colvenna-isle"},
            {"Key": "Component", "Value": "core"}, {"Key": "SourceCommit", "Value": head}])
    (directory / "dispatch.json").write_text(encode(result))
    if wait(cf, result["StackId"], directory / "terminal.json") != "UPDATE_COMPLETE":
        raise RuntimeError("Authorizer update not complete; reconcile the original stack operation")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["deploy", "enable-auth", "provision", "rebuild", "inspect",
                                              "bootstrap-key", "quota", "purge-generation"])
    parser.add_argument("--baseline", default=STACKS["baseline"])
    parser.add_argument("--stack", default=STACKS["control"])
    parser.add_argument("--bundle", type=Path)
    parser.add_argument("--registry-only", action="store_true",
                        help="update control handlers while preserving the qualified baseline authorizer")
    parser.add_argument("--environment-configuration", type=Path,
                        help="public configuration receipt from the owned environment builder deployment")
    parser.add_argument("--scheduler-state", choices=["ENABLED", "DISABLED"], default="ENABLED",
                        help="minute pipeline scheduler when the environment policy has a notification queue")
    parser.add_argument("--space")
    parser.add_argument("--expires-at")
    parser.add_argument("--token-output", type=Path, help="new private file for a one-time bootstrap token")
    parser.add_argument("--policy-file", type=Path, help="quota policy JSON (operator-only)")
    parser.add_argument("--generation", help="an invalidated generation to purge")
    parser.add_argument("--execute", action="store_true", help="delete; without it purge only writes the plan")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.registry_only and args.operation != "deploy":
        parser.error("--registry-only is a deployment option")
    if args.environment_configuration and args.operation != "deploy":
        parser.error("--environment-configuration is a deployment option")
    if args.baseline != STACKS["baseline"] or args.stack != STACKS["control"]:
        parser.error("scope is the stacks of COLVENNA_ENVIRONMENT=" + STACKS["environment"])
    directory = args.output.resolve()
    if directory.exists() or directory == ROOT or ROOT in directory.parents:
        parser.error("use a new external receipt directory")
    if args.operation in {"provision", "rebuild", "bootstrap-key", "quota", "purge-generation"} and not args.space:
        parser.error("--space is required")
    head = source() if args.operation != "inspect" else None
    current, identity = session()
    cf = current.client("cloudformation")
    baseline = owned_stack(cf, args.baseline)
    if baseline["StackStatus"] not in {"CREATE_COMPLETE", "UPDATE_COMPLETE"}:
        raise RuntimeError("Reconcile baseline deployment first")
    values = outputs(baseline) | {"StackId": baseline["StackId"]}
    directory.mkdir(parents=True)
    (directory / "identity.json").write_text(encode(identity))
    (directory / "baseline.json").write_text(encode(baseline))
    if args.operation == "deploy":
        if not args.bundle:
            parser.error("--bundle is required")
        baseline_parameters = {item["ParameterKey"]: item["ParameterValue"] for item in baseline["Parameters"]}
        if args.registry_only and (baseline_parameters.get("AuthEnabled") != "true"
                                  or not baseline_parameters.get("AuthCodeVersion")
                                  or not baseline_parameters.get("AuthSourceCommit")):
            raise RuntimeError("Registry-only update requires previously qualified authentication")
        manifest = json.loads((args.bundle / "manifest.json").read_text())
        artifact = (args.bundle / manifest["zip"]).resolve()
        if (args.bundle.resolve() not in artifact.parents or manifest["status"] != "PASS"
                or manifest["source_commit"] != head
                or hashlib.sha256(artifact.read_bytes()).hexdigest() != manifest["zip_sha256"]):
            raise RuntimeError("Artifact/source identity mismatch")
        try:
            existing = cf.describe_stacks(StackName=args.stack)["Stacks"][0]
        except ClientError as error:
            if "does not exist" not in error.response["Error"].get("Message", ""):
                raise
            existing = None
        if existing:
            tags = {tag["Key"]: tag["Value"] for tag in existing["Tags"]}
            if (tags.get("Project") != "colvenna-isle" or tags.get("Component") != "control"
                    or outputs(existing).get("BaselineStackId") != baseline["StackId"]
                    or existing["StackStatus"] not in {"CREATE_COMPLETE", "UPDATE_COMPLETE"}):
                raise RuntimeError("Existing control stack not positively owned/terminal")
        environment = None
        if args.environment_configuration:
            environment = json.loads(args.environment_configuration.read_text())
        elif existing:
            configuration = current.client("lambda").get_function_configuration(
                FunctionName=outputs(existing)["ControlFunction"])["Environment"]["Variables"]
            if configuration.get("COLVENNA_ENVIRONMENT_PROJECT"):
                environment = {"project": configuration["COLVENNA_ENVIRONMENT_PROJECT"],
                               "policy": json.loads(configuration["COLVENNA_ENVIRONMENT_POLICY"])}
        if environment:
            builder_stack = cf.describe_stacks(StackName=STACKS["environments"])["Stacks"][0]
            builder_values = outputs(builder_stack)
            builder_tags = {x["Key"]: x["Value"] for x in builder_stack["Tags"]}
            policy = environment["policy"]
            actual_projects = current.client("codebuild").batch_get_projects(
                names=[environment["project"]])
            if (environment["project"] != STACKS["environments"]
                    or builder_tags.get("Project") != "colvenna-isle"
                    or builder_tags.get("Component") != "builder"
                    or builder_stack["StackStatus"] not in {"CREATE_COMPLETE", "UPDATE_COMPLETE"}
                    or builder_values["BaselineStackId"] != baseline["StackId"]
                    or builder_values["SourceCommit"] != policy["source_commit"]
                    or builder_values["RuntimeBoundaryArn"] != policy["runtime_boundary_arn"]
                    or builder_values["RuntimeLogGroup"] != policy["runtime_log_group"]
                    or builder_values["ExecutionRoleArn"] != policy["execution_role_arn"]
                    or policy["bucket"] != values["DataBucket"]
                    or actual_projects.get("projectsNotFound")
                    or len(actual_projects.get("projects", [])) != 1):
                raise RuntimeError("Environment deployment configuration unconfirmed")
            actual = actual_projects["projects"][0]
            if (actual["source"]["type"] != "S3"
                    or actual["source"]["location"] != policy["source_location"]
                    or actual["sourceVersion"] != policy["source_version"]
                    or actual["environment"]["type"] != "ARM_CONTAINER"
                    or actual["environment"]["image"] != policy["builder_image"]
                    or actual["serviceRole"] != builder_values["BuildRoleArn"]):
                raise RuntimeError("Environment builder source/role changed")
            (directory / "environment-configuration.json").write_text(encode(environment))
        (directory / "stopped.json").write_text(encode(stopped(current, values)))
        gateway = current.client("apigatewayv2")
        authorizers = gateway.get_authorizers(ApiId=values["Api"])["Items"]
        if len(authorizers) != 1 or authorizers[0].get("AuthorizerResultTtlInSeconds") != 0:
            raise RuntimeError("Expected the existing uncached authorizer")
        key = f"artifacts/control/{head}/{manifest['zip_sha256']}/control.zip"
        binding = {"bucket": values["DataBucket"], "key": key, "version_id": publish_artifact(
            current.client("s3"), values["DataBucket"], key, artifact.read_bytes(), manifest["zip_sha256"]),
            "sha256": manifest["zip_sha256"]}
        (directory / "artifact.json").write_text(encode(binding))
        # The run controller, scheduler and collector call the writer: a fresh install adds them with
        # the registry-only redeploy that follows the writer's own deployment.
        pipelines = ({"state": args.scheduler_state}
                     if environment and environment["policy"].get("notification_queue_arn")
                     and values.get("WriterControlFunction") else None)
        value = template(values, authorizers[0]["AuthorizerId"], binding, head, environment, pipelines,
                         account=ACCOUNT)
        variables = value["Resources"]["Control"]["Properties"]["Environment"]["Variables"]
        # Intrinsic values (Fn::Sub/Ref) are bounded by their rendered JSON size.
        if sum(len(name.encode()) + len((content if isinstance(content, str) else json.dumps(content)).encode())
               for name, content in variables.items()) > 4096:
            raise RuntimeError("Control environment configuration exceeds Lambda quota")
        body = json.dumps(value, separators=(",", ":"))
        (directory / "template.json").write_text(body + "\n")
        (directory / "template-validation.json").write_text(encode(cf.validate_template(**template_arguments(current, body))))
        token = "colvenna-control-" + uuid.uuid4().hex
        admission = {"source_commit": head, "baseline_stack_id": baseline["StackId"],
                     "stack": args.stack, "client_request_token": token, "artifact": binding,
                     "template_sha256": hashlib.sha256(body.encode()).hexdigest()}
        (directory / "admission.json").write_text(encode(admission))
        operation = (lambda **arguments: update_stack(cf, **arguments)) if existing else cf.create_stack
        result = operation(
            StackName=args.stack, **template_arguments(current, body), Capabilities=["CAPABILITY_IAM"],
            ClientRequestToken=token, Tags=[{"Key": "Project", "Value": "colvenna-isle"},
                {"Key": "Component", "Value": "control"}, {"Key": "SourceCommit", "Value": head}])
        (directory / "dispatch.json").write_text(encode(result))
        terminal = wait(cf, result["StackId"], directory / "terminal.json")
        if terminal not in {"CREATE_COMPLETE", "UPDATE_COMPLETE"}:
            raise RuntimeError("Control deployment not complete; reconcile original operation")
        print(terminal)
        if args.registry_only:
            final_baseline = owned_stack(cf, args.baseline)
            final_parameters = {item["ParameterKey"]: item["ParameterValue"]
                                for item in final_baseline["Parameters"]}
            if final_parameters != baseline_parameters or final_baseline["StackStatus"] != "UPDATE_COMPLETE":
                raise RuntimeError("Independent baseline identities changed during registry update")
            (directory / "baseline-preserved.json").write_text(encode(final_baseline))
        else:
            deploy_authorizer(current, baseline, binding, head, directory / "authorizer", enabled=False)
    elif args.operation == "enable-auth":
        if not args.bundle:
            parser.error("--bundle is required")
        enable_auth(current, cf, baseline, args.bundle, head, directory)
    elif args.operation == "bootstrap-key":
        if not args.expires_at or not args.token_output:
            parser.error("--expires-at and --token-output are required")
        destination = args.token_output.resolve()
        if destination.exists() or destination == ROOT or ROOT in destination.parents:
            parser.error("use a new private external token file")
        # Reserve a private file before issuance so a local path failure cannot lose the token.
        import os
        descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            receipt = KeyStore(current.client("dynamodb"), values["ControlTable"]).bootstrap(
                args.space, args.expires_at, identity["Arn"])
            # The token goes into the reserved file first: a later local failure cannot lose it.
            stream = os.fdopen(descriptor, "w")
            descriptor = None
            with stream:
                stream.write(receipt["token"] + "\n")
            (directory / "key.json").write_text(encode(receipt["key"]))
            print(encode({"key_id": receipt["key"]["key_id"], "token_output": str(destination)}))
        finally:
            if descriptor is not None:
                # Nothing was issued into the reserved file: remove it rather than leave an empty token file.
                os.close(descriptor)
                destination.unlink()
    elif args.operation == "quota":
        from colvenna_isle.control.quotas import Quotas
        if not args.policy_file:
            parser.error("--policy-file is required")
        registry = Registry(current.client("dynamodb"), values["ControlTable"], values["DataBucket"])
        configuration = registry._read(args.space)
        policy = json.loads(args.policy_file.read_text())
        result = Quotas(current.client("dynamodb"), values["ControlTable"]).set(configuration, policy, identity["Arn"])
        (directory / "quota.json").write_text(encode({"space_id": args.space, "generation": configuration["generation"],
                                                      "requested": policy, "effective": result}))
        print(encode({"space_id": args.space, "effective": result}))
    elif args.operation == "purge-generation":
        purge_generation(current, values, args.space, args.generation, args.execute, identity, directory)
    elif args.operation in {"provision", "rebuild"}:
        registry = Registry(current.client("dynamodb"), values["ControlTable"], values["DataBucket"])
        principal = Principal("deployment-operator", frozenset([args.space]), frozenset(["datasets:manage"]))
        if args.operation == "rebuild":
            (directory / "stopped.json").write_text(encode(stopped(current, values)))
            previous = registry.configuration(principal, args.space)
            (directory / "previous.json").write_text(encode(previous))
            result = registry.rebuild(args.space, previous)
        else:
            result = registry.provision(args.space)
        (directory / "configuration.json").write_text(encode(result))
        print(encode({"space_id": args.space, "generation": result["generation"], "revision": result["revision"]}))
    else:
        control = cf.describe_stacks(StackName=args.stack)["Stacks"][0]
        (directory / "control.json").write_text(encode(control))
        print(encode({"stack": control["StackId"], "status": control["StackStatus"], "outputs": outputs(control)}))


def enable_auth(current, cf, baseline, bundle, head, directory):
    """Install admission for Bearer authentication of one exact control deployment.

    The authorizer, control and private probe functions must run the bundle's
    exact ZIP; the private probe and the still-disabled business authorizer deny
    missing/invalid credentials; then the business authorizer is enabled and the
    public API is checked to deny missing (401) and invalid (403) credentials.
    """
    import io
    import urllib.error
    import urllib.request
    import zipfile

    manifest = json.loads((bundle / "manifest.json").read_text())
    control = outputs(cf.describe_stacks(StackName=STACKS["control"])["Stacks"][0])
    values = outputs(baseline)
    parameters = {entry["ParameterKey"]: entry["ParameterValue"] for entry in baseline["Parameters"]}
    if (manifest["status"] != "PASS" or manifest["source_commit"] != head or control["SourceCommit"] != head
            or control["BaselineStackId"] != baseline["StackId"] or parameters.get("AuthSourceCommit") != head):
        raise RuntimeError("Deploy the exact-source control stack before enabling authentication")
    lam = current.client("lambda")
    checks = []
    for function in [values["AuthorizerFunction"], control["ControlFunction"], control["AuthProbeFunction"]]:
        configuration = lam.get_function(FunctionName=function)
        with urllib.request.urlopen(configuration["Code"]["Location"], timeout=30) as stream:
            code = stream.read()
        with zipfile.ZipFile(io.BytesIO(code)) as archive:
            provenance = json.loads(archive.read("colvenna-source.json"))
        checks.append({"function": function, "zip_sha256": hashlib.sha256(code).hexdigest(),
                       "source_commit": provenance["source_commit"],
                       "environment_source": configuration["Configuration"]["Environment"]["Variables"].get(
                           "COLVENNA_SOURCE_COMMIT")})
    for name, token in [("missing", None), ("invalid", "x" * 48)]:
        event = json.dumps({"headers": {"authorization": "Bearer " + token} if token else {}}).encode()
        for function in [control["AuthProbeFunction"], values["AuthorizerFunction"]]:
            result = lam.invoke(FunctionName=function, Payload=event)
            checks.append({"function": function, "credential": name, "function_error": result.get("FunctionError"),
                           "response": json.loads(result["Payload"].read())})
    (directory / "private-checks.json").write_text(encode(checks))
    if any(check.get("zip_sha256", manifest["zip_sha256"]) != manifest["zip_sha256"]
           or check.get("source_commit", head) != head or check.get("environment_source", head) != head
           or "credential" in check and (check["function_error"] or check["response"] != {"isAuthorized": False})
           for check in checks):
        raise RuntimeError("Authentication admission checks failed; business authorizer stays disabled")
    artifact = {"key": parameters["AuthCodeKey"], "version_id": parameters["AuthCodeVersion"]}
    deploy_authorizer(current, baseline, artifact, head, directory / "enable", enabled=True)
    live = outputs(owned_stack(cf, STACKS["baseline"]))
    public = []
    for name, token, expected in [("missing", None, 401), ("invalid", "x" * 48, 403)]:
        request = urllib.request.Request(live["ApiUrl"] + "/v1/admin/spaces/install_probe/configuration",
                                         headers={"Authorization": "Bearer " + token} if token else {})
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                status = response.status
        except urllib.error.HTTPError as error:
            status = error.code
        public.append({"credential": name, "status": status, "expected": expected})
    (directory / "public-checks.json").write_text(encode(public))
    if any(check["status"] != check["expected"] for check in public):
        raise RuntimeError("Public authentication denial unconfirmed; reconcile before opening admission")
    print(encode({"status": "AUTH_ENABLED", "source_commit": head}))


# Every object namespace keyed by generation, including the SlateDB database of that generation.
GENERATION_PREFIXES = ["ingest", "ingestion-replay", "inputs", "pipeline-code", "environment-builds", "staging",
                       "task-contexts", "query-intents", "query-results", "write-intents", "retention",
                       "qualification-import"]


def purge_generation(current, values, space, generation, execute, identity, directory):
    """Delete a generation a space rebuild replaced: only after the rebuild invalidated every old reference."""
    import re
    from colvenna_isle.control.audit import Audit, operator_actor
    from colvenna_isle.writer.ownership import Ownership

    if not generation or not re.fullmatch(r"gen_[0-9a-f]{32}", generation):
        raise RuntimeError("Name one exact generation")
    ddb, s3 = current.client("dynamodb"), current.client("s3")
    registry = Registry(ddb, values["ControlTable"], values["DataBucket"])
    configuration = registry._read(space)
    if configuration["generation"] == generation:
        raise RuntimeError("The current generation is never purged; rebuild the space first")
    ownership = Ownership(ddb, values["ControlTable"])
    permit = ownership.read()
    opened = []
    if permit.get("identity") and permit["state"] != "STOPPED":
        # A proven-STOPPED writer holds nothing open; its last registration is only history.
        registration = ownership.registration(permit["identity"]["task_arn"]) or {}
        opened = registration.get("opened_databases", [])
    active = [item["SK"]["S"] for item in ddb.query(
        TableName=values["ControlTable"], ConsistentRead=True, KeyConditionExpression="PK = :pk",
        ExpressionAttributeValues={":pk": {"S": "PIPELINERUNS#1"}})["Items"]
        if item["SK"]["S"].startswith(space + "#" + generation + "#")]
    checks = {"current_generation": configuration["generation"], "writer_permit": permit["state"],
              "opened_databases": opened, "active_runs": active}
    (directory / "preconditions.json").write_text(encode(checks))
    if space + "/" + generation in opened or active:
        raise RuntimeError("The old generation is still referenced; stop and reconcile first")
    prefixes = [f"spaces/{space}/{generation}/"] + [f"spaces/{space}/{name}/{generation}/"
                                                    for name in GENERATION_PREFIXES]
    plan = []
    for prefix in prefixes:
        kwargs = {"Bucket": values["DataBucket"], "Prefix": prefix, "ExpectedBucketOwner": ACCOUNT}
        while True:
            page = s3.list_object_versions(**kwargs)
            plan += [{"key": value["Key"], "version_id": value["VersionId"], "bytes": value.get("Size", 0),
                      "marker": marker} for marker, values_ in [(False, page.get("Versions", [])),
                                                               (True, page.get("DeleteMarkers", []))]
                     for value in values_]
            if not page.get("IsTruncated"):
                break
            kwargs.update(KeyMarker=page["NextKeyMarker"], VersionIdMarker=page["NextVersionIdMarker"])
    (directory / "plan.json").write_text(encode({"space_id": space, "generation": generation,
                                                 "objects": len(plan), "bytes": sum(v["bytes"] for v in plan),
                                                 "prefixes": prefixes, "versions": plan}))
    if not execute:
        print(encode({"status": "PLANNED_NOT_EXECUTED", "objects": len(plan)}))
        return
    deleted = []
    for value in plan:
        s3.delete_object(Bucket=values["DataBucket"], Key=value["key"], VersionId=value["version_id"],
                         ExpectedBucketOwner=ACCOUNT)
        deleted.append(value)
    remaining = sum(len(s3.list_object_versions(Bucket=values["DataBucket"], Prefix=prefix,
                                                ExpectedBucketOwner=ACCOUNT).get("Versions", [])) for prefix in prefixes)
    receipt = {"status": "PURGED" if not remaining else "INCOMPLETE", "space_id": space, "generation": generation,
               "deleted": len(deleted), "bytes": sum(v["bytes"] for v in deleted), "remaining_versions": remaining}
    (directory / "receipt.json").write_text(encode({**receipt, "versions": deleted}))
    Audit(ddb, values["ControlTable"]).write(
        space, configuration["generation"], "GENERATION_PURGED", operator_actor(identity["Arn"]),
        {"kind": "generation", "id": generation}, identity=generation + ":purge",
        details={name: receipt[name] for name in ["status", "deleted", "bytes", "remaining_versions"]})
    print(encode(receipt))
    if remaining:
        raise RuntimeError("Purge incomplete; reconcile the remaining exact versions")


if __name__ == "__main__":
    main()
