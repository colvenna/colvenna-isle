#!/usr/bin/env python3
"""Consistent space backups and isolated, same-version restores (IAM operator).

vault    deploy the account-level backup vault, retained outside every environment.
create   the approved writer takes the native snapshot at one commit sequence; this tool
         adds the space's control records (never credentials), copies of its pipeline
         code and audit history, and publishes one backup manifest. The source is read only.
inspect  re-read one backup by exact version and verify every referenced object.
restore  into COLVENNA_ENVIRONMENT's own stacks: provision the same space and generation,
         bootstrap a new administrator, load the native snapshot through that
         environment's approved writer, then re-establish quota, retention policy and
         holds, runtime environments and pipelines through the public API.

Every operation writes a new external receipt directory. A timeout is UNKNOWN for its
original identity; reconcile it (inspect/status) and never replay under a new identity.
"""

import argparse
import hashlib
import json
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT), str(ROOT / "src"), str(Path(__file__).parent)]
from infra.backups import STACK, template
from manage import ACCOUNT, STACKS, encode, outputs, owned_stack, session, source, update_stack, vault_bucket, wait
from colvenna_isle.control.audit import Audit, operator_actor
from colvenna_isle.control.keys import KeyStore
from colvenna_isle.control.quotas import Quotas
from colvenna_isle.control.registry import Registry, canonical
from colvenna_isle.control.writer_client import WriterClient
from colvenna_isle.writer.ownership import Ownership

# Control records restored or re-established; credentials, run history, status
# projections (rebuilt from the native ledger) and query records are not.
CONTROL_PREFIXES = ["META", "PIPELINE#", "PIPELINE_VERSION#", "PIPELINE_PUBLICATION#", "UPLOAD#", "QUOTA#",
                    "RETENTION#", "HOLD#"]


def stamp(seconds=0):
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat(timespec="seconds").replace(
        "+00:00", "Z")


class Operation:
    def __init__(self, directory):
        self.directory, self.counter = directory, 0
        self.current, self.identity = session()
        self.cf = self.current.client("cloudformation")
        self.ddb, self.s3 = self.current.client("dynamodb"), self.current.client("s3")
        self.lam = self.current.client("lambda", config=Config(retries={"total_max_attempts": 1}, read_timeout=60))

    def save(self, name, value):
        self.counter += 1
        path = self.directory / ("%04d-%s.json" % (self.counter, name))
        with path.open("x") as stream:
            stream.write(encode(value))
        return path.name

    def environment(self):
        baseline = owned_stack(self.cf, STACKS["baseline"])
        values = outputs(baseline)
        self.values, self.vault = values, vault_bucket(self.cf)
        if not self.vault:
            raise RuntimeError("Deploy the backup vault first")
        if values.get("BackupBucket") != self.vault:
            raise RuntimeError("This environment's writer is not bound to the backup vault; redeploy the writer")
        self.registry = Registry(self.ddb, values["ControlTable"], values["DataBucket"])
        self.keys = KeyStore(self.ddb, values["ControlTable"])
        self.writer = WriterClient(Ownership(self.ddb, values["ControlTable"]), self.lam,
                                   values["WriterControlFunction"])
        return baseline

    # -- vault objects ----------------------------------------------------------------------------
    def put(self, key, raw):
        import base64

        digest = hashlib.sha256(raw).hexdigest()
        response = self.s3.put_object(Bucket=self.vault, Key=key, Body=raw, IfNoneMatch="*",
                                      ExpectedBucketOwner=ACCOUNT, Metadata={"sha256": digest},
                                      ChecksumSHA256=base64.b64encode(bytes.fromhex(digest)).decode())
        reference = {"bucket": self.vault, "key": key, "version_id": response["VersionId"], "sha256": digest,
                     "bytes": len(raw)}
        if self.get(reference) != raw:
            raise RuntimeError("Vault object unconfirmed")
        return reference

    def get(self, reference):
        response = self.s3.get_object(Bucket=reference["bucket"], Key=reference["key"],
                                      VersionId=reference["version_id"], ExpectedBucketOwner=ACCOUNT)
        with response["Body"] as stream:
            raw = stream.read()
        if (response["VersionId"] != reference["version_id"] or hashlib.sha256(raw).hexdigest() != reference["sha256"]
                or "bytes" in reference and len(raw) != reference["bytes"]):
            raise RuntimeError("Backup object differs from its manifest: " + reference["key"])
        return raw

    # -- writer -----------------------------------------------------------------------------------
    def wait_writer(self, principal, configuration, operation, field, identity, timeout):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = self.writer.call(principal, configuration, operation, **{field: identity})
            if value["state"] != "RUNNING":
                self.save(operation, value)
                return value
            time.sleep(3)
        raise RuntimeError(operation + " observation timed out; the original job stays UNKNOWN")


def control_items(ddb, table, space, generation):
    def query(partition):
        items, start = [], None
        while True:
            arguments = {"TableName": table, "ConsistentRead": True, "KeyConditionExpression": "PK = :pk",
                         "ExpressionAttributeValues": {":pk": {"S": partition}}}
            if start:
                arguments["ExclusiveStartKey"] = start
            response = ddb.query(**arguments)
            items += response["Items"]
            start = response.get("LastEvaluatedKey")
            if not start:
                return items
    space_items = [item for item in query("SPACE#" + space)
                   if any(item["SK"]["S"] == prefix or item["SK"]["S"].startswith(prefix + generation)
                          for prefix in CONTROL_PREFIXES) or item["SK"]["S"] == "META"]
    return {"space": space_items, "environments": query("ENVIRONMENT#1#" + space + "#" + generation),
            "audit": query("AUDIT#" + space + "#" + generation)}


def create(arguments, directory):
    operation = Operation(directory)
    head = source()
    baseline = operation.environment()
    token = arguments.token_file.read_text().strip()
    principal = operation.keys.authenticate("Bearer " + token)
    if "backup:manage" not in principal.scopes:
        raise RuntimeError("The key needs backup:manage")
    configuration = operation.registry._read(arguments.space)
    space, generation = configuration["space_id"], configuration["generation"]
    backup_id = "backup_" + uuid.uuid4().hex
    prefix = f"backups/{space}/{generation}/{backup_id}/"
    started = time.monotonic()
    operation.save("admission", {"backup_id": backup_id, "space_id": space, "generation": generation,
                                 "environment": STACKS["environment"], "baseline_stack_id": baseline["StackId"],
                                 "operator": operation.identity, "key_id": principal.key_id, "source_commit": head})
    operation.save("backup_native", operation.writer.call(principal, configuration, "backup_native",
                                                          backup_id=backup_id))
    native = operation.wait_writer(principal, configuration, "backup_status", "backup_id", backup_id,
                                   arguments.timeout)
    if native["state"] != "COMPLETED":
        raise RuntimeError("Native snapshot " + native["state"] + " " + str(native.get("code")))
    native_seconds = time.monotonic() - started
    records = control_items(operation.ddb, operation.values["ControlTable"], space, generation)
    artifacts = {}
    for item in records["space"]:
        if not item["SK"]["S"].startswith("PIPELINE_VERSION#"):
            continue
        for artifact in json.loads(item["record"]["S"])["snapshot"]["artifacts"]:
            if artifact["sha256"] in artifacts:
                continue
            original = {"bucket": operation.values["DataBucket"], "key": artifact["key"],
                        "version_id": artifact["object_version"], "sha256": artifact["sha256"],
                        "bytes": artifact["bytes"]}
            artifacts[artifact["sha256"]] = operation.put(prefix + "artifacts/" + artifact["sha256"],
                                                          operation.get(original))
    control = operation.put(prefix + "control/records.json", (canonical(
        {"space": records["space"], "environments": records["environments"]}) + "\n").encode())
    audit = operation.put(prefix + "control/audit.json", (canonical(records["audit"]) + "\n").encode())
    manifest = {
        "format_version": 1, "kind": "colvenna_space_backup", "backup_id": backup_id, "space_id": space,
        "generation": generation, "created_at": stamp(), "point": native["point"],
        "native": {name: native["manifest"][name] for name in ["bucket", "key", "version_id", "sha256", "bytes"]},
        "control": control, "audit": audit, "artifacts": artifacts,
        "source": {"environment": STACKS["environment"], "baseline_stack_id": baseline["StackId"],
                   "data_bucket": operation.values["DataBucket"],
                   "writer_source_commit": operation.values["WriterSourceCommit"]},
        "requested_by": {"operator": operator_actor(operation.identity["Arn"])["principal_id"],
                         "key_id": principal.key_id},
        "timing": {"native_seconds": round(native_seconds, 3), "total_seconds": round(time.monotonic() - started, 3),
                   "native": native.get("timing")},
        "rows": native["rows"], "native_bytes": native["uploaded_bytes"],
    }
    reference = operation.put(prefix + "backup.json", (canonical(manifest) + "\n").encode())
    Audit(operation.ddb, operation.values["ControlTable"]).write(
        space, generation, "BACKUP_CREATED", operator_actor(operation.identity["Arn"]),
        {"kind": "backup", "id": backup_id}, identity=backup_id,
        details={"commit_sequence": native["point"]["commit_sequence"], "manifest_version": reference["version_id"],
                 "rows": native["rows"], "key_id": principal.key_id})
    operation.save("backup", {"reference": reference, "manifest": manifest})
    print(encode({"status": "COMPLETED", "backup": reference, "point": native["point"]}))


def inspect(operation, uri, version):
    bucket, key = uri.removeprefix("s3://").split("/", 1)
    response = operation.s3.get_object(Bucket=bucket, Key=key, VersionId=version, ExpectedBucketOwner=ACCOUNT)
    with response["Body"] as stream:
        raw = stream.read()
    if response["VersionId"] != version:
        raise RuntimeError("Backup manifest version differs")
    manifest = json.loads(raw)
    if manifest.get("kind") != "colvenna_space_backup" or key != (
            f"backups/{manifest['space_id']}/{manifest['generation']}/{manifest['backup_id']}/backup.json"):
        raise RuntimeError("Not a Colvenna Isle space backup manifest")
    native = json.loads(operation.get(manifest["native"]))
    for table in native["tables"]:
        for file in table["files"]:
            head = operation.s3.head_object(Bucket=file["bucket"], Key=file["key"], VersionId=file["version_id"],
                                            ExpectedBucketOwner=ACCOUNT, ChecksumMode="ENABLED")
            if head["ContentLength"] != file["bytes"] or head["Metadata"].get("sha256") != file["sha256"]:
                raise RuntimeError("Native part differs: " + file["key"])
    control = json.loads(operation.get(manifest["control"]))
    for reference in manifest["artifacts"].values():
        operation.get(reference)
    operation.get(manifest["audit"])
    return manifest, {"bucket": bucket, "key": key, "version_id": version,
                      "sha256": hashlib.sha256(raw).hexdigest()}, native, control


class Api:
    def __init__(self, operation, url, token):
        self.operation, self.url, self.token = operation, url.rstrip("/"), token

    def __call__(self, method, path, body=None, headers=None, expected=(200,)):
        request = Request(self.url + path, method=method, data=None if body is None else canonical(body).encode(),
                          headers={"Content-Type": "application/json", "Authorization": "Bearer " + self.token,
                                   **(headers or {})})
        try:
            with urlopen(request, timeout=60) as response:
                status, raw = response.status, response.read()
        except HTTPError as error:
            status, raw = error.code, error.read()
        value = json.loads(raw) if raw else None
        self.operation.save("api-" + method + path.split("?")[0].replace("/", "_")[-60:],
                            {"method": method, "path": path, "status": status, "response": value})
        if status not in expected:
            raise RuntimeError(method + " " + path + " -> " + str(status) + " " + json.dumps(value)[:300])
        return value


def decode(item):
    return json.loads(item["record"]["S"])


def reestablish(operation, api, configuration, control, artifacts, schedules):
    """Policies, holds, runtime environments and pipelines through this environment's public API."""
    space, generation = configuration["space_id"], configuration["generation"]
    result = {"quota": None, "retention": None, "holds": [], "environments": [], "pipelines": []}
    items = {item["SK"]["S"]: item for item in control["space"]}
    quota = items.get("QUOTA#" + generation)
    if quota and decode(quota):
        result["quota"] = Quotas(operation.ddb, operation.values["ControlTable"]).set(
            configuration, decode(quota), operation.identity["Arn"])
    retention = items.get("RETENTION#" + generation)
    if retention:
        settings = decode(retention)
        current = api("GET", f"/v1/admin/spaces/{space}/retention")
        result["retention"] = api("PUT", f"/v1/admin/spaces/{space}/retention",
                                  {"state": settings["state"], "policy": settings["policy"]},
                                  {"If-Match": '"' + current["revision"] + '"'})["revision"]
    for name, item in sorted(items.items()):
        if name.startswith("HOLD#") and decode(item)["status"] == "ACTIVE" and decode(item)["expires_unix"] > time.time():
            hold = decode(item)
            result["holds"].append(api("POST", f"/v1/admin/spaces/{space}/retention/holds", {
                "target": hold["target"], "reason": hold["reason"],
                "expires_at": datetime.fromtimestamp(hold["expires_unix"], timezone.utc).isoformat(
                    timespec="seconds").replace("+00:00", "Z")}, expected=(201,))["hold_id"])
    catalog = decode(control["environments"][0]) if control["environments"] else {"versions": {}}
    for key, version in sorted(catalog["versions"].items()):
        specification = version["specification"]
        body = {name: specification[name] for name in ["environment_id", "version", "python_version", "platform",
                                                       "build_profile", "dependency_source"] if name in specification}
        api("POST", f"/v1/spaces/{space}/runtime-environments", body, expected=(201, 409))
        path = (f"/v1/spaces/{space}/runtime-environments/{body['environment_id']}/versions/{body['version']}")
        api("POST", path + "/build", None, {"Idempotency-Key": "restore-" + digest_text(key)}, expected=(202, 200))
        deadline = time.monotonic() + 1800
        while time.monotonic() < deadline:
            observed = api("GET", path)
            if observed["status"] in {"READY", "FAILED"}:
                break
            time.sleep(20)
        source_lock = (version.get("ready") or {}).get("dependency_lock_sha256")
        target_lock = (observed.get("artifact") or observed.get("ready") or {}).get("dependency_lock_sha256")
        result["environments"].append({"environment": key, "status": observed["status"],
                                       "source_lock_sha256": source_lock, "target_lock_sha256": target_lock})
        if observed["status"] != "READY":
            raise RuntimeError("Runtime environment rebuild " + key + " " + observed["status"])
    for name, item in sorted(items.items()):
        if not name.startswith("PIPELINE#" + generation + "#"):
            continue
        pipeline = decode(item)
        version = items.get("PIPELINE_VERSION#" + generation + "#" + name.rsplit("#", 1)[1] + "#"
                            + str(pipeline.get("active_version")))
        snapshot = decode(version)["snapshot"] if version else None
        definition = snapshot["definition"] if snapshot else pipeline["draft"]["definition"]
        declared = snapshot["artifacts"] if snapshot else pipeline["draft"]["artifacts"]
        files = {artifact["path"]: operation.get(artifacts[artifact["sha256"]]) for artifact in declared}
        result["pipelines"].append(publish(api, space, definition, files, pipeline.get("active_version"),
                                           schedules))
    return result


def digest_text(value):
    return hashlib.sha256(value.encode()).hexdigest()[:32]


def publish(api, space, definition, files, active, schedules):
    pipeline = definition["pipeline_id"]
    artifacts = [{"path": path, "upload_id": "pending", "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
                 for path, raw in sorted(files.items())]
    restored = dict(definition, schedule={"owner": "none"}) if schedules == "disable" else definition
    api("POST", f"/v1/spaces/{space}/pipelines", {"definition": restored, "artifacts": artifacts},
        expected=(201, 409))
    for artifact in artifacts:
        receipt = api("POST", f"/v1/spaces/{space}/pipelines/{pipeline}/artifact-uploads",
                      {name: artifact[name] for name in ["path", "sha256", "bytes"]}, expected=(201,))
        with urlopen(Request(receipt["upload_url"], method="PUT", data=files[artifact["path"]],
                             headers=receipt["upload_headers"]), timeout=60) as response:
            if response.status != 200:
                raise RuntimeError("Artifact upload failed")
        artifact["upload_id"] = receipt["upload_id"]
    metadata = api("GET", f"/v1/spaces/{space}/pipelines/{pipeline}")
    draft = api("PUT", f"/v1/spaces/{space}/pipelines/{pipeline}/draft",
                {"definition": restored, "artifacts": artifacts}, {"If-Match": '"' + metadata["revision"] + '"'})
    check = api("POST", f"/v1/spaces/{space}/pipelines/{pipeline}/validate", {"draft_revision": draft["revision"]})
    if not check["valid"]:
        raise RuntimeError("Restored pipeline " + pipeline + " does not validate: " + json.dumps(check)[:300])
    value = {"pipeline_id": pipeline, "source_active_version": active}
    if active:
        version = api("POST", f"/v1/spaces/{space}/pipelines/{pipeline}/versions",
                      {"draft_revision": draft["revision"]}, {"Idempotency-Key": "restore-" + pipeline},
                      expected=(201,))
        metadata = api("GET", f"/v1/spaces/{space}/pipelines/{pipeline}")
        api("POST", f"/v1/spaces/{space}/pipelines/{pipeline}/activate",
            {"pipeline_version": version["pipeline_version"]}, {"If-Match": '"' + metadata["revision"] + '"'})
        value["restored_version"] = version["pipeline_version"]
    return value


def restore(arguments, directory):
    operation = Operation(directory)
    head = source()
    baseline = operation.environment()
    started = time.monotonic()
    manifest, reference, native, control = inspect(operation, arguments.backup_uri, arguments.version_id)
    space, generation = manifest["space_id"], manifest["generation"]
    operation.save("admission", {"backup": reference, "environment": STACKS["environment"],
                                 "baseline_stack_id": baseline["StackId"], "operator": operation.identity,
                                 "source_commit": head})
    if manifest["source"]["data_bucket"] == operation.values["DataBucket"]:
        raise RuntimeError("RESTORE_TARGET_IS_SOURCE: restore into an isolated environment")
    if operation.values["WriterSourceCommit"] != native["runtime"]["writer_source_commit"]:
        raise RuntimeError("BACKUP_VERSION_MISMATCH: the target runs " + operation.values["WriterSourceCommit"])
    meta = next(item for item in control["space"] if item["SK"]["S"] == "META")
    configuration = operation.registry.restore(json.loads(meta["configuration"]["S"]),
                                               operator_actor(operation.identity["Arn"]),
                                               {"backup_id": manifest["backup_id"],
                                                "commit_sequence": manifest["point"]["commit_sequence"]})
    operation.save("configuration", configuration)
    descriptor = os.open(arguments.token_output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    bootstrap = operation.keys.bootstrap(space, stamp(arguments.key_hours * 3600), operation.identity["Arn"])
    with os.fdopen(descriptor, "w") as stream:
        stream.write(bootstrap["token"] + "\n")
    operation.save("administrator", bootstrap["key"])
    principal = operation.keys.authenticate("Bearer " + bootstrap["token"])
    restore_id = "restore_" + uuid.uuid4().hex
    operation.save("restore_native", operation.writer.call(principal, configuration, "restore_native",
                                                           restore_id=restore_id, manifest={
        name: manifest["native"][name] for name in ["bucket", "key", "version_id", "sha256"]}))
    loaded = operation.wait_writer(principal, configuration, "restore_status", "restore_id", restore_id,
                                   arguments.timeout)
    if loaded["state"] != "COMPLETED":
        raise RuntimeError("Native restore " + loaded["state"] + " " + str(loaded.get("code")))
    native_seconds = time.monotonic() - started
    api = Api(operation, operation.values["ApiUrl"], bootstrap["token"])
    established = reestablish(operation, api, configuration, control, manifest["artifacts"], arguments.schedules)
    receipt = {"format_version": 1, "kind": "colvenna_space_restore", "restore_id": restore_id, "backup": reference,
               "space_id": space, "generation": generation, "target": {
                   "environment": STACKS["environment"], "baseline_stack_id": baseline["StackId"],
                   "data_bucket": operation.values["DataBucket"]},
               "point": manifest["point"], "native": loaded, "administrator_key_id": bootstrap["key"]["key_id"],
               "reestablished": established, "timing": {
                   "native_seconds": round(native_seconds, 3),
                   "total_seconds": round(time.monotonic() - started, 3)}, "finished_at": stamp()}
    receipt["vault_receipt"] = operation.put(
        f"backups/{space}/{generation}/{manifest['backup_id']}/restores/{restore_id}.json",
        (canonical(receipt) + "\n").encode())
    operation.save("restore", receipt)
    print(encode({"status": "RESTORED", "restore_id": restore_id, "timing": receipt["timing"],
                  "token_output": str(arguments.token_output)}))


def vault(directory):
    operation = Operation(directory)
    head = source()
    body = json.dumps(template(head), separators=(",", ":"))
    try:
        existing = operation.cf.describe_stacks(StackName=STACK)["Stacks"][0]
    except ClientError as error:
        if "does not exist" not in error.response["Error"].get("Message", ""):
            raise
        existing = None
    if existing and existing["StackStatus"].endswith("_IN_PROGRESS"):
        # Environments share the vault: another installation's update finishes before this one decides.
        wait(operation.cf, existing["StackId"], directory / "concurrent-terminal.json")
        existing = operation.cf.describe_stacks(StackName=STACK)["Stacks"][0]
    if existing:
        tags = {tag["Key"]: tag["Value"] for tag in existing.get("Tags", [])}
        if tags.get("Project") != "colvenna-isle" or tags.get("Environment") != "backups":
            raise RuntimeError("Existing vault stack is not positively owned")
        if outputs(existing).get("SourceCommit") == head:
            print(encode({"status": "UNCHANGED", "bucket": vault_bucket(operation.cf)}))
            return
    token = "colvenna-backups-" + uuid.uuid4().hex
    operation.save("admission", {"stack": STACK, "source_commit": head, "client_request_token": token,
                                 "template_sha256": hashlib.sha256(body.encode()).hexdigest()})
    kwargs = {"StackName": STACK, "TemplateBody": body, "ClientRequestToken": token,
              "Tags": [{"Key": "Project", "Value": "colvenna-isle"}, {"Key": "Environment", "Value": "backups"},
                       {"Key": "SourceCommit", "Value": head}]}
    result = (update_stack(operation.cf, **kwargs) if existing else operation.cf.create_stack(**kwargs))
    operation.save("dispatch", result)
    status = wait(operation.cf, result["StackId"], directory / "terminal.json")
    if status not in {"CREATE_COMPLETE", "UPDATE_COMPLETE"}:
        raise RuntimeError("Vault deployment not complete; reconcile the original stack operation")
    print(encode({"status": status, "bucket": vault_bucket(operation.cf)}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["vault", "create", "inspect", "restore"])
    parser.add_argument("--space")
    parser.add_argument("--token-file", type=Path, help="create: a key with backup:manage in the space")
    parser.add_argument("--backup-uri", help="inspect/restore: s3://<vault>/backups/<space>/<gen>/<id>/backup.json")
    parser.add_argument("--version-id", help="inspect/restore: the exact backup manifest version")
    parser.add_argument("--token-output", type=Path, help="restore: new private file for the new administrator token")
    parser.add_argument("--key-hours", type=int, default=24)
    parser.add_argument("--schedules", choices=["restore", "disable"], default="disable",
                        help="restore: re-enable backed-up pipeline schedules, or leave them owner=none")
    parser.add_argument("--timeout", type=int, default=3600)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    directory = arguments.output.resolve()
    if directory.exists() or directory == ROOT or ROOT in directory.parents:
        parser.error("use a new external receipt directory")
    if arguments.operation == "create" and not (arguments.space and arguments.token_file):
        parser.error("create needs --space and --token-file")
    if arguments.operation in {"inspect", "restore"} and not (arguments.backup_uri and arguments.version_id):
        parser.error("inspect/restore need --backup-uri and --version-id")
    if arguments.operation == "restore":
        if not arguments.token_output:
            parser.error("restore needs --token-output")
        arguments.token_output = arguments.token_output.resolve()
        if arguments.token_output.exists() or ROOT in arguments.token_output.parents:
            parser.error("use a new private external token file")
    directory.mkdir(parents=True)
    if arguments.operation == "vault":
        vault(directory)
    elif arguments.operation == "create":
        create(arguments, directory)
    elif arguments.operation == "inspect":
        operation = Operation(directory)
        operation.vault = vault_bucket(operation.cf)
        manifest, reference, native, _ = inspect(operation, arguments.backup_uri, arguments.version_id)
        operation.save("inspect", {"reference": reference, "manifest": manifest})
        print(encode({"status": "VERIFIED", "backup": reference, "point": manifest["point"],
                      "tables": len(native["tables"]), "rows": manifest["rows"]}))
    else:
        restore(arguments, directory)


if __name__ == "__main__":
    main()
