"""Trusted controller admission and checks for portable Python task descriptors."""

import copy
import hashlib

from colvenna_isle.contracts.model import PROFILES, TASK_BUDGETS
from colvenna_isle.contracts.validation import validate, validate_parameters, wire_schema
from colvenna_isle.control.registry import ControlError, canonical

MAX_DESCRIPTOR_BYTES = 1048576


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def environment_identity(binding):
    runtime = binding["ready"]["runtime"]
    return {"reference": binding["reference"],
            "specification_sha256": binding["specification_sha256"],
            "dependency_lock_sha256": binding["ready"]["dependency_lock"]["sha256"],
            **{name: runtime[name] for name in ["source_commit", "adapter_sha256"]},
            "executor_binding": binding["executor_binding"]}


def task_environment(snapshot, task_id):
    """The immutable version's executor identity for one task."""
    if task_id in snapshot["environments"]:
        return environment_identity(snapshot["environments"][task_id])
    return {"executor_binding": copy.deepcopy(snapshot["executors"][task_id])}


def descriptor_key(value):
    return (f"spaces/{value['space_id']}/task-contexts/{value['generation']}/"
            f"{value['run_id']}/{value['job_id']}/{value['attempt_id']}/descriptor.json")


def staging_prefix(descriptor, execution):
    return (f"spaces/{descriptor['space_id']}/staging/{descriptor['generation']}/"
            f"{descriptor['pipeline_id']}/{descriptor['run_id']}/{descriptor['job_id']}/"
            f"{descriptor['attempt_id']}/{execution}/")


def context_for(value, execution, directory):
    """Local paths are produced only by the adapter, after portable admission."""
    task, state = value["task"], value["output_state"]
    output = task["output"]
    wire = wire_schema(output) if output["write_mode"] == "merge_keys" else output["schema"]
    return {"contract_version": 2,
            **{name: value[name] for name in ["space_id", "pipeline_id", "pipeline_version",
                                            "run_id", "job_id", "attempt_id"]},
            "task_id": task["task_id"], "execution_id": execution,
            "input_mode": task.get("input_mode", "local_files"),
            "parameters": copy.deepcopy(value["parameters"]), "inputs": copy.deepcopy(value["inputs"]),
            "processing": copy.deepcopy(value["processing"]),
            "output": {"name": "result", "directory": str(directory),
                **{name: copy.deepcopy(output[name]) for name in [
                    "dataset", "write_mode", "key_columns", "allow_empty"]},
                **copy.deepcopy(state), "schema": copy.deepcopy(wire),
                **({"target_schema": copy.deepcopy(output["schema"])}
                   if output["write_mode"] == "merge_keys" else {}),
                **({"partition_columns": output["partition_columns"]}
                   if "partition_columns" in output else {})}}


def check_descriptor(value):
    validate("task_descriptor", value)
    task, environment = value["task"], value["environment"]
    binding = environment["executor_binding"]
    if (task["resource_profile"] not in TASK_BUDGETS or task["executor"] != binding["executor"]
            or task["resource_profile"] != binding["profile"]
            or task["timeout_seconds"] > PROFILES[task["resource_profile"]]["max_timeout_seconds"]
            or task["language"] not in PROFILES[task["resource_profile"]]["languages"]
            or (task["executor"] == "writer") != ("reference" not in environment)
            or "reference" in environment and task["runtime_environment"] != environment["reference"]):
        raise ControlError(422, "TASK_ENVIRONMENT_BINDING")
    if any("local_path" in file for binding in value["inputs"].values() for file in binding["files"]):
        raise ControlError(422, "TASK_PORTABLE_INPUT_REQUIRED")
    # Validate all context semantics using a synthetic path, never persist it.
    context = context_for(value, "validation_execution",
                          "/validation/validation_attempt/validation_execution/output/result")
    context["attempt_id"] = "validation_attempt"
    for alias, binding in context["inputs"].items():
        manifest = {"contract_version": 2,
            **{name: binding[name] for name in ["dataset", "release_id", "watermark", "schema", "files"]}}
        validate("input_manifest", manifest)
        if digest(manifest) != binding["manifest_sha256"]:
            raise ControlError(422, "TASK_INPUT_MANIFEST_HASH")
        if context["input_mode"] == "local_files":
            for number, file in enumerate(binding["files"]):
                file["local_path"] = f"/validation/validation_execution/input/{alias}/{number}.parquet"
    validate("context", context)
    if set(value["inputs"]) != set(task["inputs"]):
        raise ControlError(422, "TASK_INPUT_BINDING")
    for alias, declared in task["inputs"].items():
        binding = value["inputs"][alias]
        if (binding["selection"]["mode"] != declared["selection"]
                or "dataset" in declared and binding["dataset"] != declared["dataset"]
                or "state" in declared and binding["dataset"] != task["output"]["dataset"]):
            raise ControlError(422, "TASK_INPUT_BINDING")
    if value["processing"]["mode"] != task["processing"]["mode"]:
        raise ControlError(422, "TASK_PROCESSING_BINDING")
    if len(canonical(value).encode()) > MAX_DESCRIPTOR_BYTES:
        raise ControlError(413, "TASK_DESCRIPTOR_TOO_LARGE")


def check_processing(task, processing, versions, replace_partitions):
    """Interval/state/partition claims must equal the writer's exported native versions."""
    from colvenna_isle.contracts.validation import partition_set

    mode = task["processing"]["mode"]
    if processing.get("mode") != mode:
        raise ControlError(422, "TASK_PROCESSING_BINDING")
    for alias, declared in task["inputs"].items():
        version, selection = versions[alias].get("native_version") or {}, versions[alias]["selection"]
        if declared["selection"] == "changes" and (
                version.get("from_sequence") != processing["from_watermark"]["ods_sequence"]
                or version.get("to_sequence") != processing["to_watermark"]["ods_sequence"]):
            raise ControlError(409, "TASK_PROCESSING_INTERVAL_CHANGED")
        if "state" in declared and version.get("state_release_id") != processing["state_release_id"]:
            raise ControlError(409, "TASK_PROCESSING_STATE_CHANGED")
        if declared["selection"] == "affected_partitions" and (
                partition_set(selection["partitions"]) != partition_set(processing["affected_partitions"])):
            raise ControlError(409, "TASK_PROCESSING_PARTITIONS_CHANGED")
    if mode == "partition_recompute":
        if partition_set(replace_partitions) != partition_set(processing["affected_partitions"]):
            raise ControlError(409, "TASK_PROCESSING_PARTITIONS_CHANGED")
    elif replace_partitions:
        raise ControlError(422, "TASK_PROCESSING_BINDING")


class TaskAdmission:
    """Internal seam; no user-supplied context, object list, target or AWS resource.

    input_provider must call the active writer's authenticated input_manifest
    operation and return its READY original, including retained exact versions.
    This does not select a publish candidate or implement the Run controller.
    """

    def __init__(self, pipelines, input_provider, objects, output_provider):
        self.pipelines, self.input_provider, self.objects = pipelines, input_provider, objects
        self.output_provider = output_provider

    def prepare(self, principal, space, pipeline, version, task_id, *,
                run_id, job_id, attempt_id, export_id, parameters, processing=None):
        for identity in [task_id, run_id, job_id, attempt_id, export_id]:
            validate("identifier", identity)
        bound = self.pipelines.bind_run(principal, space, pipeline, version)
        snapshot = bound["snapshot"]
        tasks = [task for task in snapshot["definition"]["tasks"] if task["task_id"] == task_id]
        if len(tasks) != 1:
            raise ControlError(404, "TASK_NOT_FOUND")
        task = tasks[0]
        processing = copy.deepcopy(processing or {"mode": "full"})
        validate_parameters(snapshot["definition"].get("parameters", {}), parameters)
        fixed_parameters = {name: definition["default"]
                            for name, definition in snapshot["definition"].get("parameters", {}).items()
                            if "default" in definition}
        fixed_parameters.update(parameters)
        original = self.input_provider(principal, export_id)
        if (original["state"] != "READY" or original["space_id"] != space
                or original["generation"] != snapshot["generation"]):
            raise ControlError(409, "TASK_INPUT_AUTHORITY_CHANGED")
        portable = original["manifest"]
        if (portable["space_id"] != space or portable["generation"] != snapshot["generation"]
                or portable["export_id"] != export_id or set(portable["inputs"]) != set(task["inputs"])):
            raise ControlError(409, "TASK_INPUT_AUTHORITY_CHANGED")
        outputs = {value["task_id"]: value["output"]["dataset"] for value in snapshot["definition"]["tasks"]}
        inputs = {}
        for alias, value in portable["inputs"].items():
            manifest, declared = value["manifest"], task["inputs"][alias]
            validate("input_manifest", manifest)
            expected = (declared.get("dataset") or outputs.get(declared.get("task"))
                        or (task["output"]["dataset"] if "state" in declared else None))
            if manifest["dataset"] != expected or value["selection"]["mode"] != declared["selection"]:
                raise ControlError(409, "TASK_INPUT_AUTHORITY_CHANGED")
            inputs[alias] = {**copy.deepcopy({name: manifest[name] for name in [
                "dataset", "release_id", "watermark", "schema", "files"]}),
                "manifest_sha256": digest(manifest), "selection": copy.deepcopy(value["selection"])}
        package = bound["package"]
        output_state = self.output_provider(principal, space, pipeline, version, task_id)
        contract = output_state.get("state_contract") if isinstance(output_state, dict) else None
        if contract is not None and (contract["protocol"] != "key_state_v1"
                                     or task["output"]["write_mode"] != "merge_keys"):
            # Complete task-produced state images still lack a producer protocol.
            raise ControlError(422, "TASK_STATE_AUTHORITY_NOT_READY")
        if (not isinstance(output_state, dict)
                or set(output_state) - {"state_contract", "previous"} != {"dataset", "expected_previous_release"}
                or output_state["dataset"] != task["output"]["dataset"]):
            raise ControlError(409, "TASK_OUTPUT_AUTHORITY_CHANGED")
        replace = processing.get("affected_partitions", []) if task["processing"]["mode"] == "partition_recompute" else []
        check_processing(task, processing, portable["inputs"], replace)
        descriptor = {"contract_version": 2, **{name: snapshot[name] for name in [
            "space_id", "generation", "pipeline_id", "pipeline_version", "definition_epoch"]},
            "run_id": run_id, "job_id": job_id, "attempt_id": attempt_id,
            "task": copy.deepcopy(task), "environment": task_environment(snapshot, task_id),
            "snapshot_sha256": digest(snapshot),
            "package": {"s3_uri": package["package_uri"], "object_version": package["package_object_version"],
                        "sha256": package["package_sha256"], "bytes": package["package_bytes"]},
            "parameters": fixed_parameters, "inputs": inputs, "processing": processing,
            "output_state": {"expected_previous_release": output_state["expected_previous_release"],
                             "replace_partitions": copy.deepcopy(replace)}}
        check_descriptor(descriptor)
        if (self.objects.space != space or self.objects.generation != snapshot["generation"]):
            raise ControlError(403, "TASK_OBJECT_SCOPE")
        for binding in inputs.values():
            for file in binding["files"]:
                self.objects.key({name: file[name] for name in [
                    "s3_uri", "object_version", "sha256", "bytes"]}, "inputs")

        def guard():
            self.pipelines.confirm_run_binding(principal, space, pipeline, version, bound)

        reference = self.objects.put(descriptor_key(descriptor), canonical(descriptor).encode(), guard=guard)
        return {"operation": "task_execute", "descriptor": reference}
