#!/usr/bin/env python3
"""Open or close business admission of one installed Colvenna Isle environment.

open: every component runs one exact source; intake and synchronous writes are
enabled while all services are stopped, then the writer is approved and the
query Service reaches its healthy count. close reverses it (writer stopped with
a terminal receipt, query at zero, admission disabled), which every deployment,
upgrade and rebuild requires first.
"""

import argparse
import hashlib
import json
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT), str(ROOT / "src"), str(Path(__file__).parent)]
from infra.template import template
from manage import STACKS, encode, outputs, owned_stack, session, source, template_arguments, update_stack, wait
from control import stopped
from colvenna_isle.writer.controller import Controller

SOURCES = ["SourceCommit", "AuthSourceCommit", "IntakeSourceCommit", "WriterSourceCommit", "SyncSourceCommit",
           "QuerySourceCommit", "BrowserSourceCommit"]


def flags(current, baseline, head, directory, enabled=None, timing=None):
    """One baseline update of IntakeEnabled/SyncEnabled or Timing; every other parameter keeps its value."""
    directory.mkdir()
    values = outputs(baseline)
    (directory / "stopped.json").write_text(encode(stopped(current, values)))
    previous = {entry["ParameterKey"]: entry["ParameterValue"] for entry in baseline["Parameters"]}
    definition = template()
    supplied = {"QueryDesired": "0"}
    if enabled is not None:
        supplied.update(IntakeEnabled="true" if enabled else "false", SyncEnabled="true" if enabled else "false")
    if timing is not None:
        supplied["Timing"] = timing
    parameters = [{"ParameterKey": name, "ParameterValue": content} for name, content in supplied.items()]
    parameters += [{"ParameterKey": name, "UsePreviousValue": True}
                   for name in previous if name not in supplied and name in definition["Parameters"]]
    body = json.dumps(definition, separators=(",", ":"))
    cf = current.client("cloudformation")
    (directory / "template.json").write_text(body + "\n")
    token = "colvenna-admission-" + uuid.uuid4().hex
    (directory / "admission.json").write_text(encode({
        "stack_id": baseline["StackId"], "source_commit": head, "enabled": enabled, "timing": timing,
        "client_request_token": token,
        "template_sha256": hashlib.sha256(body.encode()).hexdigest(), "parameters": parameters}))
    result = update_stack(
        cf, StackName=baseline["StackId"], **template_arguments(current, body), Capabilities=["CAPABILITY_IAM"],
        Parameters=parameters, ClientRequestToken=token,
        Tags=[{"Key": "Project", "Value": "colvenna-isle"}, {"Key": "Component", "Value": "core"},
              {"Key": "SourceCommit", "Value": previous["SourceCommit"]}])
    (directory / "dispatch.json").write_text(encode(result))
    if wait(cf, result["StackId"], directory / "terminal.json") != "UPDATE_COMPLETE":
        raise RuntimeError("Admission update incomplete; reconcile the original stack operation")
    final = owned_stack(cf, STACKS["baseline"])
    retained = {entry["ParameterKey"]: entry["ParameterValue"] for entry in final["Parameters"]}
    if any(retained.get(name) != previous.get(name) for name in previous if name not in supplied):
        raise RuntimeError("An independent component identity changed during the admission update")
    return final


def query_count(current, values, count, directory, timeout=600):
    ecs, elb = current.client("ecs"), current.client("elbv2")
    (directory / ("query-" + str(count) + "-admission.json")).write_text(encode({"desired": count}))
    ecs.update_service(cluster=values["Cluster"], service=values["QueryService"], desiredCount=count)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        service = ecs.describe_services(cluster=values["Cluster"], services=[values["QueryService"]])["services"][0]
        healthy = [target for target in elb.describe_target_health(
            TargetGroupArn=values["QueryTargetArn"])["TargetHealthDescriptions"]
            if target["TargetHealth"]["State"] == "healthy"]
        if service["runningCount"] == count and service["pendingCount"] == 0 and len(healthy) == count:
            (directory / ("query-" + str(count) + ".json")).write_text(encode({
                "running": service["runningCount"], "desired": service["desiredCount"], "healthy": len(healthy)}))
            return
        time.sleep(10)
    raise RuntimeError("Query Service count unobserved; reconcile its actual tasks")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["open", "close", "timing"])
    parser.add_argument("--query-count", type=int, choices=[1, 2], default=1)
    parser.add_argument("--timing", choices=["on", "off"], help="timing: per-request segment lines (closed only)")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    directory = args.output.resolve()
    if directory.exists() or directory == ROOT or ROOT in directory.parents:
        parser.error("use a new external receipt directory")
    head = source()
    current, identity = session()
    cf = current.client("cloudformation")
    baseline = owned_stack(cf, STACKS["baseline"])
    if baseline["StackStatus"] != "UPDATE_COMPLETE":
        raise RuntimeError("Reconcile the original baseline operation first")
    directory.mkdir(parents=True)
    (directory / "identity.json").write_text(encode({**identity, "environment": STACKS["environment"]}))
    values = outputs(baseline)
    parameters = {entry["ParameterKey"]: entry["ParameterValue"] for entry in baseline["Parameters"]}
    counter = iter(range(1, 10000))

    def save(name, value):
        with (directory / ("%04d-%s.json" % (next(counter), name))).open("x") as stream:
            stream.write(encode(value))

    controller = Controller(current, values, save)
    if args.operation == "timing":
        if not args.timing:
            parser.error("--timing is required")
        flags(current, baseline, head, directory / "timing", timing=args.timing)
        print(encode({"status": "TIMING_" + args.timing.upper(), "environment": STACKS["environment"]}))
        return
    if args.operation == "close":
        if controller.ownership.read()["state"] != "STOPPED":
            controller.stop()
        query_count(current, values, 0, directory)
        if parameters["IntakeEnabled"] == "true" or parameters["SyncEnabled"] == "true":
            flags(current, baseline, head, directory / "disable", enabled=False)
        print(encode({"status": "CLOSED", "environment": STACKS["environment"]}))
        return
    control = outputs(cf.describe_stacks(StackName=STACKS["control"])["Stacks"][0])
    observed = {name: parameters.get(name) for name in SOURCES} | {"ControlStack": control["SourceCommit"]}
    (directory / "sources.json").write_text(encode(observed))
    if set(observed.values()) != {head} or parameters["AuthEnabled"] != "true":
        raise RuntimeError("Every component must run the exact source with authentication enabled")
    if parameters["IntakeEnabled"] != "true" or parameters["SyncEnabled"] != "true":
        baseline = flags(current, baseline, head, directory / "enable", enabled=True)
        values = outputs(baseline)
    controller.start()
    candidate = controller.candidate(head, values["WriterImage"].split("@", 1)[1])
    controller.grant(candidate)
    query_count(current, values, args.query_count, directory)
    print(encode({"status": "OPEN", "environment": STACKS["environment"], "source_commit": head}))


if __name__ == "__main__":
    main()
