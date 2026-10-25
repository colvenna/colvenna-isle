"""Writer-side verification of the selected original task's immutable output."""

import copy
import hashlib
from pathlib import Path

from colvenna_isle.contracts.model import TASK_BUDGETS
from colvenna_isle.contracts.validation import validate
from colvenna_isle.control.http import decode_json
from colvenna_isle.control.registry import ControlError, canonical
from .output_validation import OutputValidator
from .publications import CHUNK_ROWS
from .task_descriptor import MAX_DESCRIPTOR_BYTES, context_for, staging_prefix
from .task_objects import input_reference


class Candidates:
    def __init__(self, objects):
        self.objects = objects

    def prepare(self, grant, execution, reference, directory, guard):
        """Re-read exact versions and all Parquet; no producer validation is trusted.

        The caller exclusively owns and cleans directory. Downloads and decoding
        run outside the native lock; only verified chunks enter the fair scheduler.
        """
        validate("identifier", execution)
        descriptor = grant["descriptor"]
        if grant["status"] not in {"GRANTED", "SELECTED", "IMPORTING"}:
            raise ControlError(409, "PUBLICATION_ALREADY_TERMINAL")
        prefix = staging_prefix(descriptor, execution)
        if self.objects.key(reference, "staging") != prefix + "completion.json":
            raise ControlError(403, "PUBLICATION_EXECUTION_SCOPE")
        guard()
        raw = self.objects.read(reference, "staging", maximum_bytes=MAX_DESCRIPTOR_BYTES, guard=guard)
        try:
            completion = decode_json(raw)
            directory = Path(directory).resolve(strict=True)
            output = directory / descriptor["attempt_id"] / execution / "result"
            output.mkdir(parents=True, exist_ok=False, mode=0o700)
            context = context_for(descriptor, execution, output)
            if context["input_mode"] == "local_files":
                for alias, binding in context["inputs"].items():
                    for number, file in enumerate(binding["files"]):
                        file["local_path"] = str(directory / "inputs" / alias / (str(number) + ".parquet"))
            expected = {"format_version": 1, "status": "STAGED", "publication_status": "NOT_PUBLISHED",
                "descriptor": grant["reference"], "generation": descriptor["generation"],
                **{name: context[name] for name in ["space_id", "pipeline_id", "pipeline_version", "run_id",
                    "task_id", "job_id", "attempt_id", "execution_id"]},
                "definition_epoch": descriptor["definition_epoch"], "package": descriptor["package"],
                "environment": descriptor["environment"],
                "input_manifest_hashes": {alias: value["manifest_sha256"]
                    for alias, value in descriptor["inputs"].items()},
                "processing": descriptor["processing"],
                "output": {name: value for name, value in context["output"].items() if name != "directory"}}
            state_contract = grant.get("state_contract")
            # A merge target's key state is derived by the writer, never staged.
            if state_contract is not None and state_contract["protocol"] != "complete_image_v1":
                state_contract = None
            if (set(completion) != set(expected) | {"manifest", "manifest_sha256", "files",
                    "output_manifest", "process", "peak_workspace_bytes", "invocation"} | (
                    {"state"} if state_contract is not None else set())
                    or any(completion[name] != value for name, value in expected.items())):
                raise ControlError(422, "PUBLICATION_COMPLETION_BINDING")
            budgets = TASK_BUDGETS[descriptor["task"]["resource_profile"]]
            images = [completion] + ([completion["state"]] if state_contract is not None else [])
            if (sum(len(image["files"]) for image in images) > 10000
                    or sum(image["manifest"]["row_count"] for image in images) > budgets["output_rows"]
                    or sum(sum(file["bytes"] for file in image["manifest"]["files"])
                           + image["output_manifest"]["bytes"] for image in images) > budgets["output_bytes"]):
                raise ControlError(413, "PUBLICATION_OUTPUT_QUOTA")
            prepared = self._image(context, completion, prefix, "result", directory, budgets, guard)
            state = None
            if state_contract is not None:
                state = completion["state"]
                if (set(state) != {"contract_sha256", "manifest", "manifest_sha256", "files", "output_manifest"}
                        or state["contract_sha256"] != hashlib.sha256(
                            canonical(state_contract).encode()).hexdigest()):
                    raise ControlError(422, "PUBLICATION_STATE_BINDING")
                state_context = copy.deepcopy(context)
                state_output = output.parent / "state"
                state_output.mkdir(mode=0o700)
                state_context["output"].update(directory=str(state_output),
                    **{name: copy.deepcopy(state_contract[name]) for name in ["schema", "key_columns", "allow_empty"]})
                state = self._image(state_context, state, prefix, "state", directory, budgets, guard)
            guard()
            return {"completion": copy.deepcopy(reference), **prepared, "state": state}
        except ControlError:
            raise
        except (KeyError, TypeError, ValueError):
            raise ControlError(422, "PUBLICATION_COMPLETION_INVALID") from None

    def _image(self, context, image, prefix, namespace, directory, budgets, guard):
        """Verify each complete image against its independently approved schema."""
        manifest, files = image["manifest"], image["files"]
        validate("output_manifest", manifest, context=context)
        manifest_name = "output-manifest.json" if namespace == "result" else "state-manifest.json"
        if (len(files) != len(manifest["files"])
                or self.objects.key(image["output_manifest"], "staging") != prefix + manifest_name
                or image["output_manifest"]["sha256"] != image["manifest_sha256"]):
            raise ControlError(422, "PUBLICATION_MANIFEST_BINDING")
        manifest_raw = self.objects.read(image["output_manifest"], "staging",
            maximum_bytes=MAX_DESCRIPTOR_BYTES, guard=guard)
        if (decode_json(manifest_raw) != manifest
                or hashlib.sha256(manifest_raw).hexdigest() != image["manifest_sha256"]):
            raise ControlError(422, "PUBLICATION_MANIFEST_BINDING")
        output = Path(context["output"]["directory"])
        (output / "output-manifest.json").write_bytes(manifest_raw)
        portable = []
        for file, declared in zip(files, manifest["files"]):
            if (set(file) != set(declared) | {"s3_uri", "object_version"}
                    or any(file[name] != value for name, value in declared.items())
                    or self.objects.key(input_reference(file), "staging") != prefix + namespace + "/" + declared["path"]):
                raise ControlError(422, "PUBLICATION_FILE_BINDING")
            self.objects.read(input_reference(file), "staging", maximum_bytes=budgets["output_bytes"],
                guard=guard, path=output / declared["path"])
            portable.append({**input_reference(file), "rows": file["rows"]})
        checked = OutputValidator(maximum_bytes=budgets["output_bytes"],
            maximum_rows=budgets["output_rows"]).verify(context, workspace=directory, guard=guard)
        if checked["manifest_sha256"] != image["manifest_sha256"]:
            raise ControlError(422, "PUBLICATION_MANIFEST_BINDING")
        return {"files": portable, "directory": str(output), "manifest": copy.deepcopy(manifest)}

    @staticmethod
    def chunks(prepared, guard):
        """Yield bounded typed rows in the same declared file order after verification."""
        import pyarrow.parquet as pq

        for number, file in enumerate(prepared["manifest"]["files"]):
            guard()
            path = Path(prepared["directory"]) / file["path"]
            for chunk, batch in enumerate(pq.ParquetFile(path).iter_batches(batch_size=CHUNK_ROWS)):
                guard()
                values = batch.to_pydict()
                rows = list(zip(*(values[column["name"]] for column in prepared["manifest"]["schema"])))
                yield number, chunk, rows
                guard()

    @staticmethod
    def state_chunks(prepared, guard):
        if prepared["state"] is not None:
            yield from Candidates.chunks(prepared["state"], guard)
