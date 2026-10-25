#!/usr/bin/env python3
"""Deploy/start/stop the owned writer, preserving exact receipts and source."""

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
from manage import REGION, ACCOUNT, REPOSITORY, STACKS, encode, outputs, owned_stack, publish_artifact, push_image, session, source, storage_image, template_arguments, update_stack, vault_bucket, wait
from colvenna_isle.writer.controller import Controller


def save_in(directory):
    def save(name, value):
        path = directory / (name + ".json")
        with path.open("x") as stream:
            stream.write(encode(value))
    return save


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["deploy", "start", "stop", "inspect"])
    parser.add_argument("--bundle", type=Path)
    parser.add_argument("--image-build", type=Path)
    parser.add_argument("--environment", type=Path, help="Current owned builder configuration for publication binding")
    parser.add_argument("--probe-space", default="")
    parser.add_argument("--probe-generation", default="")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    directory = args.output.resolve()
    if directory.exists() or directory == ROOT or ROOT in directory.parents:
        parser.error("use a new external receipt directory, preserve uncertain originals")
    current, identity = session()
    cf = current.client("cloudformation")
    baseline = owned_stack(cf, STACKS["baseline"])
    if baseline["StackStatus"] != "UPDATE_COMPLETE":
        raise RuntimeError("Reconcile the original deployment before proceeding")
    values = outputs(baseline)
    directory.mkdir(parents=True)
    save = save_in(directory)
    save("identity", identity)
    save("baseline", baseline)
    controller = Controller(current, values, save)
    if args.operation == "inspect":
        save("owner", controller.ownership.read())
        save("tasks", controller.all_tasks())
        save("service", controller.service())
        return
    if args.operation == "stop":
        controller.stop()
        return
    if args.operation == "start":
        head = source()
        if values["WriterSourceCommit"] != head:
            raise RuntimeError("Writer source mismatch")
        controller.start()
        candidate = controller.candidate(head, values["WriterImage"].split("@", 1)[1])
        controller.grant(candidate)
        return
    if not args.bundle or not args.image_build:
        parser.error("deploy requires --bundle and --image-build")
    head = source()
    environment = json.loads(args.environment.read_text()) if args.environment else None
    if environment is not None and (
            set(environment) != {"project", "policy"}
            or environment["policy"].get("source_commit") != head
            or environment["policy"].get("bucket") != values["DataBucket"]):
        raise RuntimeError("Writer publication requires the exact-source owned environment policy")
    manifest = json.loads((args.bundle / "manifest.json").read_text())
    image = json.loads((args.image_build / "manifest.json").read_text())
    artifact = (args.bundle / manifest["zip"]).resolve()
    if (args.bundle.resolve() not in artifact.parents or manifest["status"] != "PASS"
            or image["status"] != "PASS" or manifest["source_commit"] != head
            or image["source_commit"] != head or image["application_zip_sha256"] != manifest["zip_sha256"]
            or hashlib.sha256(artifact.read_bytes()).hexdigest() != manifest["zip_sha256"]):
        raise RuntimeError("Writer image/application/source mismatch")
    actual = json.loads(subprocess.check_output(["docker", "image", "inspect", image["tag"]], text=True))[0]
    if actual["Id"] != image["image_id"] or actual["Architecture"] != "arm64":
        raise RuntimeError("Local writer image identity mismatch")
    controller.idle()
    previous = {entry["ParameterKey"]: entry["ParameterValue"] for entry in baseline["Parameters"]}
    if previous["StorageImage"] != storage_image(image["storage_image"]) or previous["AuthEnabled"] != "true":
        raise RuntimeError("Require qualified native storage and Bearer source")
    queues = {}
    for name in ["IngressQueue", "Dlq"]:
        queues[name] = current.client("sqs").get_queue_attributes(QueueUrl=values[name],
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible",
                            "ApproximateNumberOfMessagesDelayed"])["Attributes"]
        if any(int(value) for value in queues[name].values()):
            raise RuntimeError("Reconcile original queue work before deploying a non-materializing writer")
    save("queue-before", queues)
    # Push only to the existing private project repository (an identical earlier push is reused).
    tag = "writer-" + head
    save("image-admission", {"source_commit": head, "local_image_id": image["image_id"],
                             "target": f"{ACCOUNT}.dkr.ecr.{REGION}.amazonaws.com/{REPOSITORY}:{tag}"})
    writer_image, published = push_image(current, image["tag"], image["image_id"], tag, directory / "image-push.log")
    save("image", {"source_commit": head, "writer_image": writer_image, "published": published,
                   "local_image_id": image["image_id"]})
    key = f"artifacts/writer/{head}/{manifest['zip_sha256']}/control.zip"
    save("artifact-admission", {"bucket": values["DataBucket"], "key": key, "sha256": manifest["zip_sha256"]})
    result = {"VersionId": publish_artifact(current.client("s3"), values["DataBucket"], key,
                                            artifact.read_bytes(), manifest["zip_sha256"])}
    binding = {"bucket": values["DataBucket"], "key": key, "version_id": result["VersionId"],
               "sha256": manifest["zip_sha256"]}
    save("artifact", binding)
    definition = template()
    supplied = {
        "SourceCommit": head, "QueryDesired": "0", "WriterImage": writer_image,
        "WriterSourceCommit": head, "WriterCodeBucket": values["DataBucket"], "WriterCodeKey": key,
        "WriterCodeVersion": result["VersionId"], "WriterProbeSpace": args.probe_space,
        "WriterProbeGeneration": args.probe_generation,
        # A writer deployment leaves intake admission closed; admission.py opens it.
        "IntakeEnabled": "false",
        "WriterEnvironmentPolicy": json.dumps(environment["policy"] if environment else {},
                                             separators=(",", ":")),
        "BackupBucket": vault_bucket(cf),
    }
    parameters = [{"ParameterKey": name, "ParameterValue": value} for name, value in supplied.items()]
    parameters += [{"ParameterKey": name, "UsePreviousValue": True}
                   for name in previous if name not in supplied and name in definition["Parameters"]]
    body = json.dumps(definition, separators=(",", ":"))
    (directory / "template.json").write_text(body + "\n")
    save("validation", cf.validate_template(**template_arguments(current, body)))
    token = "colvenna-writer-" + uuid.uuid4().hex
    save("deployment-admission", {"source_commit": head, "stack_id": baseline["StackId"],
         "client_request_token": token, "parameters": parameters, "artifact": binding,
         "writer_image": writer_image, "template_sha256": hashlib.sha256(body.encode()).hexdigest()})
    result = update_stack(cf, StackName=baseline["StackId"], **template_arguments(current, body),
        Capabilities=["CAPABILITY_IAM"], Parameters=parameters, ClientRequestToken=token,
        Tags=[{"Key": "Project", "Value": "colvenna-isle"}, {"Key": "Component", "Value": "core"},
              {"Key": "SourceCommit", "Value": head}])
    save("deployment-dispatch", result)
    if wait(cf, result["StackId"], directory / "deployment-terminal.json") != "UPDATE_COMPLETE":
        raise RuntimeError("Deployment not complete; reconcile the original operation")
    final = owned_stack(cf, STACKS["baseline"])
    retained = {entry["ParameterKey"]: entry["ParameterValue"] for entry in final["Parameters"]}
    for name in ["StorageImage", "AuthCodeVersion", "AuthSourceCommit", "IntakeCodeVersion", "IntakeSourceCommit"]:
        if retained[name] != previous[name]:
            raise RuntimeError("Independent qualified source changed: " + name)
    save("final", final)
    # All code/roles are installed with desiredCount0; no native handle is opened.
    final_directory = directory / "idle-after"
    final_directory.mkdir()
    Controller(current, outputs(final), save_in(final_directory)).idle()
    print(json.dumps({"status": "PASS", "source_commit": head, "writer_image": writer_image}))


if __name__ == "__main__":
    main()
