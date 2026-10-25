"""Fixed Python execution, independent Parquet checks and immutable STAGED output."""

import copy
import hashlib
import os
import sys
import time
import uuid
import zipfile
from pathlib import Path

from colvenna_isle.region import region
from colvenna_isle.contracts.model import LIMITS, TASK_BUDGETS
from colvenna_isle.contracts.validation import ContractError, safe_relative, validate, validate_parameters
from colvenna_isle.control.http import decode_json
from colvenna_isle.control.registry import ControlError, canonical
from .output_validation import OutputValidator
from .parquet import describe
from .task_descriptor import (
    MAX_DESCRIPTOR_BYTES, check_descriptor, context_for, descriptor_key, digest, task_environment,
    staging_prefix,
)
from .task_objects import TaskObjects, UploadUnknown, input_reference
from .workspace import Workspace

MAX_INPUT_BYTES = 33554432


class ExecutionFailure(ControlError):
    def __init__(self, error, execution, prefix):
        super().__init__(error.status, error.code)
        self.receipt = {"execution_id": execution, "staging_prefix": prefix}
        if isinstance(error, UploadUnknown):
            self.receipt["original_upload"] = error.original_upload
            self.receipt["status"] = "UNKNOWN"
        else:
            self.receipt["status"] = "FAILED"


class TaskAdapter:
    def __init__(self, s3, environment, *, workspace_root="/tmp/colvenna",
                 python_path="/opt/colvenna/application:/opt/colvenna/site"):
        self.environment = copy.deepcopy(environment)
        self.objects = TaskObjects(s3, environment["bucket"],
                                   environment["space_id"], environment["generation"])
        self.workspace_root, self.python_path = workspace_root, python_path

    def _environment(self, descriptor):
        installed, bound = self.environment, descriptor["environment"]
        if any(descriptor[name] != installed[name] for name in ["space_id", "generation", "definition_epoch"]):
            raise ControlError(409, "TASK_INSTALLED_ENVIRONMENT_MISMATCH")
        if installed["executor"] == "writer":
            # Writer SQL runs only under the exact writer source the version names.
            if bound != {"executor_binding": {"executor": "writer", "profile": "writer_short",
                                              "source_commit": installed["source_commit"]}}:
                raise ControlError(409, "TASK_INSTALLED_ENVIRONMENT_MISMATCH")
            return
        profiles = installed.get("profiles", [installed.get("profile")])
        if ("reference" not in bound
                or bound["reference"] != {"environment_id": installed["environment_id"],
                                          "version": installed["version"]}
                or any(bound[name] != installed[name] for name in [
                    "specification_sha256", "source_commit", "adapter_sha256", "dependency_lock_sha256"])
                or bound["executor_binding"]["executor"] != installed["executor"]
                or bound["executor_binding"]["profile"] not in profiles):
            raise ControlError(409, "TASK_INSTALLED_ENVIRONMENT_MISMATCH")

    def execute(self, event, *, remaining_seconds=None, invocation=None):
        started = time.monotonic()
        # Actual compute identity (Lambda request, ECS task or writer process),
        # recorded in the completion so one dispatch's candidates are traceable.
        self.invocation = copy.deepcopy(invocation or {})
        validate("task_event", event)
        raw = self.objects.read(event["descriptor"], "task-contexts", maximum_bytes=MAX_DESCRIPTOR_BYTES)
        descriptor = decode_json(raw)
        check_descriptor(descriptor)
        if self.objects.key(event["descriptor"], "task-contexts") != descriptor_key(descriptor):
            raise ControlError(403, "TASK_DESCRIPTOR_SCOPE")
        self._environment(descriptor)
        task = descriptor["task"]
        budgets = TASK_BUDGETS[task["resource_profile"]]
        seconds = task["timeout_seconds"]
        if remaining_seconds is not None:
            # Leave room for the bounded SDK operation and process/FS cleanup.
            # Descriptor IO consumes this same deadline, rather than resetting it.
            seconds = min(seconds, int(remaining_seconds) - 20)
        deadline = started + seconds
        if deadline - time.monotonic() <= 0:
            raise ControlError(503, "TASK_DISPATCH_DEADLINE_EXHAUSTED")
        execution = "exec_" + uuid.uuid4().hex
        workspace = Workspace(self.workspace_root, descriptor["run_id"], descriptor["attempt_id"], execution,
                              maximum_bytes=budgets["workspace_bytes"], maximum_nodes=budgets["file_nodes"])
        result, failure = None, None

        def guard():
            if time.monotonic() >= deadline:
                raise ControlError(422, "TASK_EXECUTION_TIMEOUT")
            workspace.guard()

        try:
            # Known package + input + output reservations are checked before IO.
            # Spill/index growth is then monitored under the same disk budget.
            sizes = [file["bytes"] for binding in descriptor["inputs"].values() for file in binding["files"]]
            input_bytes = sum(sizes) if task.get("input_mode", "local_files") == "local_files" else max(sizes, default=0)
            if (2 * descriptor["package"]["bytes"] + input_bytes + budgets["output_bytes"] + MAX_DESCRIPTOR_BYTES
                    > budgets["workspace_bytes"] or any(size > MAX_INPUT_BYTES for size in sizes)):
                raise ControlError(413, "TASK_INPUT_WORKSPACE_BUDGET")
            self._package(descriptor, workspace, guard)
            directory = workspace.path / "output" / "result"
            directory.mkdir(parents=True)
            context = context_for(descriptor, execution, directory)
            self._inputs(context, workspace, guard)
            validate("context", context)
            context_path = workspace.path / "context.json"
            context_raw = canonical(context).encode()
            if len(context_raw) > MAX_DESCRIPTOR_BYTES:
                raise ControlError(413, "TASK_CONTEXT_TOO_LARGE")
            context_path.write_bytes(context_raw)
            temporary = workspace.path / "tmp"
            temporary.mkdir()
            sql = task["language"] == "sql"
            environment = self._process_environment(temporary, credentials=not sql)
            seconds_left = deadline - time.monotonic()
            if seconds_left <= 0:
                raise ControlError(422, "TASK_EXECUTION_TIMEOUT")
            entrypoint = str(workspace.path / "code" / task["entrypoint"])
            command = [sys.executable, "-s", entrypoint, "--context", str(context_path), "--output-dir", str(directory)]
            status = workspace.path / "sql-status.json"
            if sql:
                # The trusted runner executes the published SELECT; it holds no
                # credentials and returns only a bounded rejection code.
                # -P: the workspace (holding user code/) never prefixes sys.path.
                command = [sys.executable, "-s", "-P", "-m", "colvenna_isle.pipeline.sql_task", "--context", str(context_path),
                           "--output-dir", str(directory), "--sql", entrypoint, "--status", str(status)]
                environment.update(COLVENNA_SQL_MEMORY_BYTES=str(budgets["memory_bytes"]),
                                   COLVENNA_SQL_OUTPUT_BYTES=str(budgets["output_bytes"]),
                                   COLVENNA_SQL_OUTPUT_ROWS=str(budgets["output_rows"]))
            try:
                process = workspace.run(command, timeout_seconds=seconds_left, environment=environment,
                                        maximum_log_bytes=budgets["log_bytes"])
            except ControlError as error:
                if sql and error.code == "TASK_SCRIPT_FAILED":
                    raise ControlError(422, self._sql_code(status)) from None
                raise
            checked = OutputValidator(maximum_bytes=budgets["output_bytes"],
                                      maximum_rows=budgets["output_rows"]).verify(
                                          context, workspace=workspace.path, guard=guard)
            # Measured compute until staging, for cost observation; never a billing record.
            self.invocation["elapsed_ms"] = round((time.monotonic() - started) * 1000)
            completion = self._stage(event["descriptor"], descriptor, context, checked, process,
                                     workspace, guard)
            result = {"status": "STAGED", "publication_status": "NOT_PUBLISHED", "execution_id": execution,
                      "completion": completion, "row_count": checked["row_count"],
                      "peak_workspace_bytes": workspace.peak_bytes, "logs": process["logs"]}
        except ControlError as error:
            failure = ExecutionFailure(error, execution, staging_prefix(descriptor, execution))
            raise failure from None
        except (ContractError, ValueError, UnicodeError, OSError) as error:
            code = "TASK_IO_FAILED" if isinstance(error, OSError) else "TASK_CONTRACT_INVALID"
            failure = ExecutionFailure(ControlError(422, code), execution, staging_prefix(descriptor, execution))
            raise failure from None
        finally:
            # Warm Lambda reuse must not accumulate old invocation directories.
            # Audit custody is the original descriptor and exact staging objects.
            cleanup = "CONFIRMED"
            try:
                workspace.cleanup()
            except OSError:
                cleanup = "UNKNOWN"
            if failure is not None:
                failure.receipt["workspace_cleanup"] = cleanup
            elif result is not None:
                result["workspace_cleanup"] = cleanup
        return result

    def _package(self, descriptor, workspace, guard):
        package = descriptor["package"]
        expected = (f"spaces/{descriptor['space_id']}/pipeline-code/{descriptor['generation']}/"
                    f"{descriptor['pipeline_id']}/versions/{descriptor['pipeline_version']}/package.zip")
        if self.objects.key(package, "pipeline-code") != expected:
            raise ControlError(403, "TASK_CODE_SCOPE")
        path = workspace.path / "package.zip"
        self.objects.read(package, "pipeline-code", maximum_bytes=LIMITS["artifact_package_bytes"],
                          path=path, guard=guard)
        try:
            with zipfile.ZipFile(path) as archive:
                entries = archive.infolist()
                names = [entry.filename for entry in entries]
                if (len(names) != len(set(names)) or len(names) > LIMITS["pipeline_tasks"] + 2
                        or sum(entry.file_size for entry in entries) > LIMITS["artifact_package_bytes"]
                        or any(entry.compress_type != zipfile.ZIP_STORED
                               or entry.external_attr >> 16 != 0o100644 for entry in entries)):
                    raise ControlError(422, "TASK_CODE_PACKAGE_INVALID")
                for name in names:
                    safe_relative(name)
                snapshot = decode_json(archive.read(".colvenna/manifest.json"))
                definition = decode_json(archive.read(".colvenna/definition.json"))
                validate("pipeline", definition)
                validate_parameters(definition.get("parameters", {}), descriptor["parameters"])
                if (digest(snapshot) != descriptor["snapshot_sha256"] or definition != snapshot["definition"]
                        or any(snapshot[name] != descriptor[name] for name in [
                            "space_id", "generation", "pipeline_id", "pipeline_version", "definition_epoch"])
                        or [task for task in definition["tasks"] if task["task_id"] == descriptor["task"]["task_id"]]
                        != [descriptor["task"]]
                        or task_environment(snapshot, descriptor["task"]["task_id"]) != descriptor["environment"]):
                    raise ControlError(422, "TASK_CODE_BINDING")
                artifacts = {entry["path"]: entry for entry in snapshot["artifacts"]}
                if (len(artifacts) != len(snapshot["artifacts"])
                        or set(names) != set(artifacts) | {".colvenna/manifest.json", ".colvenna/definition.json"}
                        or any(name.startswith(".colvenna/") for name in artifacts)
                        or any(other.startswith(name + "/") for name in names for other in names)):
                    raise ControlError(422, "TASK_CODE_PACKAGE_INVALID")
                for name, entry in artifacts.items():
                    guard()
                    raw = archive.read(name)
                    if len(raw) != entry["bytes"] or hashlib.sha256(raw).hexdigest() != entry["sha256"]:
                        raise ControlError(422, "TASK_CODE_HASH")
                    destination = workspace.path / "code" / name
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    with destination.open("xb") as stream:
                        stream.write(raw)
                    guard()
        except (zipfile.BadZipFile, KeyError, ValueError):
            raise ControlError(422, "TASK_CODE_PACKAGE_INVALID") from None

    def _inputs(self, context, workspace, guard):
        for alias, binding in context["inputs"].items():
            for number, file in enumerate(binding["files"]):
                guard()
                path = workspace.path / "input" / alias / f"part-{number:05}.parquet"
                self.objects.read(input_reference(file), "inputs", maximum_bytes=MAX_INPUT_BYTES,
                                  path=path, guard=guard)
                describe(path, binding["schema"], maximum_bytes=MAX_INPUT_BYTES, expected=file, guard=guard)
                if context["input_mode"] == "local_files":
                    file["local_path"] = str(path)
                else:
                    path.unlink()  # Verify one bounded object; context remains S3-only.
                guard()

    @staticmethod
    def _sql_code(path):
        import re

        try:
            if path.is_symlink() or path.stat().st_size > 4096:
                raise ValueError()
            value = decode_json(path.read_bytes())
            if value["status"] == "FAILED" and re.fullmatch(r"(SQL|PARQUET)_[A-Z_]{1,64}", value["code"]):
                return value["code"]
        except (OSError, ValueError, KeyError, TypeError):
            pass
        return "TASK_SCRIPT_FAILED"

    def _process_environment(self, temporary, *, credentials=True):
        # Only runtime-role credentials reach trusted space-member code. Builder
        # and API identities never enter the runtime descriptor/container.
        # Controlled SQL needs none, and the writer never passes its own role.
        inherited = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
                     "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"] if credentials else []
        return {**{name: os.environ[name] for name in inherited if name in os.environ},
                "PATH": os.defpath, "PYTHONPATH": self.python_path,
                "PYTHONDONTWRITEBYTECODE": "1", "PYTHONUNBUFFERED": "1", "PYTHONNOUSERSITE": "1",
                "COLVENNA_REGION": region(), "AWS_REGION": region(), "AWS_DEFAULT_REGION": region(),
                "AWS_EC2_METADATA_DISABLED": "true", "TMPDIR": str(temporary)}

    def _stage(self, reference, descriptor, context, checked, process, workspace, guard):
        prefix = staging_prefix(descriptor, context["execution_id"])
        completion = {"format_version": 1, "status": "STAGED", "publication_status": "NOT_PUBLISHED",
            "descriptor": reference, "generation": descriptor["generation"],
            **{name: context[name] for name in ["space_id", "pipeline_id", "pipeline_version",
                "run_id", "task_id", "job_id", "attempt_id", "execution_id"]},
            "definition_epoch": descriptor["definition_epoch"], "package": descriptor["package"],
            "environment": descriptor["environment"],
            "input_manifest_hashes": {alias: value["manifest_sha256"]
                                      for alias, value in descriptor["inputs"].items()},
            "processing": descriptor["processing"],
            # Target/watermark/write policy come from the admitted in-memory context.
            "output": {name: value for name, value in context["output"].items() if name != "directory"},
            "manifest": checked["manifest"], "manifest_sha256": checked["manifest_sha256"],
            "files": [], "process": process, "peak_workspace_bytes": workspace.peak_bytes,
            "invocation": copy.deepcopy(getattr(self, "invocation", {}))}
        # Bound completion metadata before any output Put, including the maximum
        # permitted opaque VersionId. A large file list cannot leave a knowingly
        # unpublishable completion after thousands of otherwise valid uploads.
        planned = copy.deepcopy(completion)
        # Control characters require six JSON bytes each, the largest encoding
        # of any single permitted character in an opaque version string.
        maximum_version = "\0" * 1024
        planned["files"] = [{**file, "s3_uri": f"s3://{self.objects.bucket}/{prefix}result/{file['path']}",
                             "object_version": maximum_version} for file in checked["manifest"]["files"]]
        planned["output_manifest"] = {
            "s3_uri": f"s3://{self.objects.bucket}/{prefix}output-manifest.json", "object_version": maximum_version,
            "sha256": checked["manifest_sha256"], "bytes": checked["manifest_bytes"]}
        if len(canonical(planned).encode()) > MAX_DESCRIPTOR_BYTES:
            raise ControlError(413, "TASK_COMPLETION_TOO_LARGE")
        for file in checked["manifest"]["files"]:
            guard()
            path = Path(context["output"]["directory"]) / file["path"]
            raw = path.read_bytes()
            if (path.is_symlink() or len(raw) != file["bytes"]
                    or hashlib.sha256(raw).hexdigest() != file["sha256"]):
                raise ControlError(422, "OUTPUT_FILE_CHANGED")
            original = self.objects.put(prefix + "result/" + file["path"], raw, guard=guard,
                                        content_type="application/vnd.apache.parquet")
            completion["files"].append({**file, **original})
        manifest_path = Path(context["output"]["directory"]) / "output-manifest.json"
        guard()
        raw_manifest = manifest_path.read_bytes()
        if (manifest_path.is_symlink() or len(raw_manifest) != checked["manifest_bytes"]
                or hashlib.sha256(raw_manifest).hexdigest() != checked["manifest_sha256"]):
            raise ControlError(422, "OUTPUT_MANIFEST_CHANGED")
        completion["output_manifest"] = self.objects.put(
            prefix + "output-manifest.json", raw_manifest, guard=guard)
        raw = canonical(completion).encode()
        if len(raw) > MAX_DESCRIPTOR_BYTES:
            raise ControlError(413, "TASK_COMPLETION_TOO_LARGE")
        # Last, after every file's exact VersionId/hash was confirmed. This is
        # a staging receipt; it cannot grant native eligibility or publish data.
        return self.objects.put(prefix + "completion.json", raw, guard=guard)
