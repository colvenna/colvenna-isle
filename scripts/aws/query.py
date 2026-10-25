#!/usr/bin/env python3
"""Deploy only the query image/routes on the positively owned, idle baseline stack."""

import argparse
import hashlib
import json
import subprocess
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT), str(ROOT / "src"), str(Path(__file__).parent)]
from infra.template import template
from manage import ACCOUNT, STACKS, encode, outputs, owned_stack, push_image, session, source, storage_image, template_arguments, update_stack, wait
from control import stopped


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image-build", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    directory = args.output.resolve()
    if directory.exists() or directory == ROOT or ROOT in directory.parents:
        parser.error("use a fresh external receipt directory; reconcile uncertain originals")
    head = source()
    image = json.loads((args.image_build / "manifest.json").read_text())
    if image.get("status") != "PASS" or image.get("service") != "query" or image.get("source_commit") != head:
        raise RuntimeError("Query build/source identity mismatch")
    actual = json.loads(subprocess.check_output(["docker", "image", "inspect", image["tag"]], text=True))[0]
    if actual["Id"] != image["image_id"] or actual["Architecture"] != "arm64":
        raise RuntimeError("Local query image identity mismatch")
    current, identity = session()
    cf = current.client("cloudformation")
    baseline = owned_stack(cf, STACKS["baseline"])
    if baseline["StackStatus"] != "UPDATE_COMPLETE":
        raise RuntimeError("Reconcile the original stack operation")
    values = outputs(baseline)
    previous = {item["ParameterKey"]: item["ParameterValue"] for item in baseline["Parameters"]}
    if (previous["StorageImage"] != storage_image(image["storage_image"]) or previous["AuthEnabled"] != "true"
            or previous["IntakeEnabled"] != "false" or previous["SyncEnabled"] != "false"):
        raise RuntimeError("Require qualified storage/Bearer authentication and paused write admissions")
    directory.mkdir(parents=True)

    def save(name, value):
        with (directory / (name + ".json")).open("x") as stream:
            stream.write(encode(value))

    save("identity", identity)
    save("baseline", baseline)
    save("stopped", stopped(current, values))
    queues = {}
    for name in ["IngressQueue", "Dlq"]:
        queues[name] = current.client("sqs").get_queue_attributes(QueueUrl=values[name], AttributeNames=[
            "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed"])["Attributes"]
        if any(int(count) for count in queues[name].values()):
            raise RuntimeError("Reconcile original queue work before changing the environment")
    save("queues", queues)
    ecr = current.client("ecr")
    repository = "colvenna-isle-images"
    record = ecr.describe_repositories(repositoryNames=[repository])["repositories"][0]
    if (record["registryId"] != ACCOUNT or record["imageTagMutability"] != "IMMUTABLE"
            or {item["Key"]: item["Value"] for item in ecr.list_tags_for_resource(
                resourceArn=record["repositoryArn"])["tags"]}.get("Project") != "colvenna-isle"):
        raise RuntimeError("Artifact repository ownership/immutability mismatch")
    tag = "query-" + head
    save("image-admission", {"source_commit": head, "local_image_id": image["image_id"],
                             "target": record["repositoryUri"] + ":" + tag})
    query_image, published = push_image(current, image["tag"], image["image_id"], tag, directory / "image-push.log")
    save("image", {"query_image": query_image, "published": published, "local_image_id": image["image_id"]})
    definition = template()
    supplied = {"SourceCommit": head, "QueryDesired": "0", "QueryImage": query_image, "QuerySourceCommit": head}
    parameters = [{"ParameterKey": name, "ParameterValue": value} for name, value in supplied.items()]
    parameters += [{"ParameterKey": name, "UsePreviousValue": True}
                   for name in previous if name not in supplied and name in definition["Parameters"]]
    body = json.dumps(definition, separators=(",", ":"))
    (directory / "template.json").write_text(body + "\n")
    save("validation", cf.validate_template(**template_arguments(current, body)))
    token = "colvenna-query-" + uuid.uuid4().hex
    save("deployment-admission", {"stack_id": baseline["StackId"], "source_commit": head,
        "client_request_token": token, "parameters": parameters, "query_image": query_image,
        "template_sha256": hashlib.sha256(body.encode()).hexdigest()})
    dispatched = update_stack(
        cf, StackName=baseline["StackId"], **template_arguments(current, body), Capabilities=["CAPABILITY_IAM"],
        Parameters=parameters, ClientRequestToken=token,
        Tags=[{"Key": "Project", "Value": "colvenna-isle"}, {"Key": "Component", "Value": "core"},
              {"Key": "SourceCommit", "Value": head}])
    save("deployment-dispatch", dispatched)
    if wait(cf, dispatched["StackId"], directory / "deployment-terminal.json") != "UPDATE_COMPLETE":
        raise RuntimeError("Reconcile the original query deployment")
    final = owned_stack(cf, STACKS["baseline"])
    retained = {item["ParameterKey"]: item["ParameterValue"] for item in final["Parameters"]}
    for name in previous:
        if name not in supplied and retained.get(name) != previous[name]:
            raise RuntimeError("Independent source/parameter changed: " + name)
    save("final", final)
    save("stopped-after", stopped(current, outputs(final)))
    print(json.dumps({"status": "PASS", "source_commit": head, "query_image": query_image,
                      "scope": "QUERY_TRANSPORT_INSTALLED_WITH_DESIRED_ZERO"}))


if __name__ == "__main__":
    main()
