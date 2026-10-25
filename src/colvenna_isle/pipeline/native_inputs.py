"""Stable native transaction export, called by the approved writer under its lock.

Every bound input is fixed in one native read transaction: an ODS commit
interval, an exact pipeline release, a declared partition set or the target's
key state. Immutable publication uses the input ledger/object boundary.
"""

import copy
import time
from pathlib import Path

from colvenna_isle.contracts.validation import validate
from colvenna_isle.control.registry import ControlError
from colvenna_isle.query.native import stage
from .parquet import write


class _Target:
    def __init__(self, directory, aliases, selections, maximum_bytes, maximum_rows, guard, sample_rows=None):
        self.directory, self.aliases, self.selections = directory, aliases, selections
        self.maximum_bytes, self.maximum_rows, self.guard = maximum_bytes, maximum_rows, guard
        self.sample_rows = sample_rows
        self.inputs, self.ready = {}, False

    def _sample(self, batches):
        """A trial reads at most sample_rows rows of each exact native version, in native order."""
        remaining = self.sample_rows
        for batch in batches:
            if remaining <= 0:
                return
            yield batch[:remaining]
            remaining -= min(len(batch), remaining)

    def load(self, definition, batches, version):
        dataset = definition["dataset"]
        alias = self.aliases[dataset]
        self.guard()
        if self.sample_rows is not None:
            batches = self._sample(batches)
        file = self.directory / (alias + ".parquet")
        used = sum(value["file"]["bytes"] for value in self.inputs.values())
        metadata = write(file, definition["schema"], batches,
            maximum_bytes=self.maximum_bytes - used, maximum_rows=self.maximum_rows, guard=self.guard)
        self.inputs[alias] = {"dataset": dataset, "schema": copy.deepcopy(definition["schema"]),
            "selection": copy.deepcopy(self.selections[alias]), "native_version": copy.deepcopy(version),
            "file": {"local_path": str(file), **metadata}}

    def seal(self):
        self.guard()
        if len(self.inputs) != len(self.aliases):
            raise ControlError(503, "INPUT_NATIVE_VERSION_UNAVAILABLE")
        versions = [value["native_version"] for value in self.inputs.values()]
        if len({value["snapshot_sequence"] for value in versions}) != 1:
            raise ControlError(503, "INPUT_NATIVE_VECTOR_UNCONFIRMED")
        self.ready = True


def capture(connection, configuration, approved_bindings, directory, *,
            maximum_bytes, maximum_rows, guard, sample_rows=None):
    """Export only the immutable definition's approved inputs in one read transaction.

The caller owns the writer lock, epoch and fresh principal/generation guard.
Callers cannot receive a completed local export after any transaction/guard
failure. A full snapshot cannot silently substitute for changes or state input.
"""
    from .input_exports import validate_bindings

    if (not approved_bindings or type(maximum_bytes) is not int or maximum_bytes < 1
            or type(maximum_rows) is not int or maximum_rows < 0):
        raise ControlError(400, "INPUT_EXPORT_BUDGET")
    guard()
    validate_bindings(approved_bindings, configuration)
    aliases, selections, specs, selected = {}, {}, {}, copy.deepcopy(configuration)
    selected["datasets"] = {}
    for alias, binding in approved_bindings.items():
        dataset = binding["dataset"]
        aliases[dataset] = alias
        selections[alias] = binding["selection"]
        spec = {"mode": binding["selection"]["mode"],
                **{name: binding[name] for name in ["release_id", "from_sequence", "to_sequence"] if name in binding}}
        if spec["mode"] == "affected_partitions":
            spec["partitions"] = binding["selection"]["partitions"]
        if spec["mode"] == "affected_keys":
            changes = approved_bindings[binding["keys_from"]]
            spec["keys_from"] = {"dataset": changes["dataset"], "from_sequence": changes["from_sequence"],
                                 "to_sequence": changes["to_sequence"]}
        specs[dataset] = spec
        selected["datasets"][dataset] = configuration["datasets"][dataset]
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=False, mode=0o700)
    if sample_rows is not None and (type(sample_rows) is not int or not 1 <= sample_rows <= 10000):
        raise ControlError(400, "INPUT_EXPORT_SAMPLE")
    target = _Target(directory, aliases, selections, maximum_bytes, maximum_rows, guard, sample_rows)
    started = time.monotonic()
    try:
        stage(connection, selected, target, guard, selections=specs)
        guard()
        if not target.ready:
            raise ControlError(503, "INPUT_EXPORT_INCOMPLETE")
    except BaseException:
        target.ready = False
        raise
    return {"status": "EXPORTED_LOCAL", "space_id": configuration["space_id"],
        "generation": configuration["generation"], "inputs": target.inputs,
        **({"sample_rows": sample_rows} if sample_rows is not None else {}),
        "metrics": {"copied_bytes": sum(value["file"]["bytes"] for value in target.inputs.values()),
            "copied_rows": sum(value["file"]["rows"] for value in target.inputs.values()),
            "elapsed_ms": round((time.monotonic() - started) * 1000)},
        "publication_status": "NOT_PUBLISHED"}
