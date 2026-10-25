"""Native authority for one original input export and its retention references."""

import copy
import json
import re
import time
import uuid

from colvenna_isle.contracts.validation import validate
from colvenna_isle.control.registry import ControlError, canonical
from colvenna_isle.writer.ingestion import digest
from .input_objects import MAX_FILE_BYTES, freeze

MAX_EXPORTS, MAX_RETAINED_BYTES = 128, 536870912
# Retention: a FAILED original is collected after this grace; an original that
# never became READY is abandoned once its retention and this grace both pass.
FAILED_GRACE_SECONDS, ABANDONED_GRACE_SECONDS = 3600, 86400
COLLECTION_STATES = {"COLLECTING", "COLLECTED"}


SELECTION_FIELDS = {
    "snapshot": {"release_id", "to_sequence"},
    "changes": {"from_sequence", "to_sequence"},
    "affected_keys": {"release_id", "keys_from"},
    "affected_partitions": {"release_id"},
}


def _owner(registered, dataset):
    return registered["definition"].get("write_owner", "ingestion" if dataset.startswith("ods.") else "pipeline")


def validate_bindings(bindings, configuration=None):
    """Each alias names one dataset and one exact, writer-resolvable version selection."""
    if not isinstance(bindings, dict) or not 1 <= len(bindings) <= 16:
        raise ControlError(400, "INVALID_INPUT_EXPORT")
    datasets = set()
    for alias, binding in bindings.items():
        validate("input_alias", alias)
        if not isinstance(binding, dict) or not {"dataset", "selection"} <= set(binding):
            raise ControlError(400, "INVALID_INPUT_EXPORT")
        validate("dataset", binding["dataset"])
        validate("selection", binding["selection"])
        mode, dataset = binding["selection"]["mode"], binding["dataset"]
        if (set(binding) - {"dataset", "selection"} - SELECTION_FIELDS[mode]
                or ("partitions" in binding["selection"]) != (mode == "affected_partitions")):
            raise ControlError(400, "INVALID_INPUT_EXPORT")
        if dataset in datasets:
            raise ControlError(400, "DUPLICATE_INPUT_DATASET")
        datasets.add(dataset)
        for name in ["from_sequence", "to_sequence"]:
            if name in binding and (type(binding[name]) is not int or binding[name] < 0):
                raise ControlError(400, "INVALID_INPUT_EXPORT")
        if ("from_sequence" in binding or "to_sequence" in binding) and not dataset.startswith("ods."):
            raise ControlError(400, "INVALID_INPUT_EXPORT")
        if mode == "changes" and ("from_sequence" not in binding or "to_sequence" not in binding
                                  or binding["from_sequence"] > binding["to_sequence"]):
            raise ControlError(400, "INVALID_INPUT_EXPORT")
        if "release_id" in binding and not (binding["release_id"] is None and mode == "affected_keys"):
            validate("identifier", binding["release_id"])
        if mode == "affected_partitions" and "release_id" not in binding:
            # Partition impact is computed against one exact upstream release.
            raise ControlError(400, "INVALID_INPUT_EXPORT")
        if mode == "affected_keys":
            source = bindings.get(binding.get("keys_from"))
            if ("release_id" not in binding or not isinstance(source, dict)
                    or source.get("selection", {}).get("mode") != "changes"):
                raise ControlError(400, "INVALID_INPUT_EXPORT")
        if configuration is not None:
            registered = configuration["datasets"].get(dataset)
            if not registered:
                raise ControlError(403, "INPUT_DATASET_FORBIDDEN")
            owner = _owner(registered, dataset)
            if (mode == "changes" and owner != "ingestion"
                    or mode in {"affected_keys", "affected_partitions"} and owner != "pipeline"
                    or "release_id" in binding and owner != "pipeline"):
                raise ControlError(422, "INPUT_SELECTION_UNSUPPORTED")
            if mode == "affected_keys":
                state = registered["definition"].get("publication_state")
                source = configuration["datasets"].get(bindings[binding["keys_from"]]["dataset"])
                columns = {column["name"]: column["type"] for column in source["definition"]["schema"]} if source else {}
                if (not state or state["protocol"] != "key_state_v1" or any(
                        columns.get(column["name"]) != column["type"]
                        for column in state["schema"] if column["name"] in state["key_columns"])):
                    raise ControlError(422, "INPUT_KEY_STATE_UNAVAILABLE")


class InputExports:
    def __init__(self, native, clock=time.time):
        self.native, self.clock, self.initialized = native, clock, False

    def initialize(self):
        self.native.initialize()
        if not self.initialized:
            self.native.transaction(lambda: self.native.connection.execute(
                "CREATE TABLE IF NOT EXISTS warehouse.ctl.input_exports "
                "(export_id VARCHAR PRIMARY KEY,binding_sha256 VARCHAR NOT NULL,record VARCHAR NOT NULL)"))
            self.initialized = True

    def _read(self, identity):
        self.initialize()
        value = self.native.connection.execute(
            "SELECT binding_sha256,record FROM warehouse.ctl.input_exports WHERE export_id=?", [identity]).fetchone()
        if value is None:
            return None
        record = json.loads(value[1])
        configuration = self.native.configuration
        if (record["format_version"] != 1 or record["export_id"] != identity
                or record["binding_sha256"] != value[0]
                or record["space_id"] != configuration["space_id"]
                or record["generation"] != configuration["generation"]):
            raise ControlError(503, "INPUT_EXPORT_CUSTODY_UNKNOWN")
        return record

    def _owner(self, principal, record):
        principal.authorize(record["space_id"], "pipeline:run")
        if principal.generation != record["generation"]:
            raise ControlError(403, "SPACE_GENERATION_CHANGED")
        if principal.principal_id != record["principal_id"] and "pipeline:manage" not in principal.scopes:
            raise ControlError(403, "INPUT_OWNER_REQUIRED")

    def _records(self):
        return [json.loads(row[0]) for row in
                self.native.connection.execute("SELECT record FROM warehouse.ctl.input_exports").fetchall()]

    def retained_bytes(self):
        # Reservations include unknown/failed originals and their partial local
        # files. Neither expiry nor failure proves those bytes were reclaimed.
        return sum(record["retained_bytes"] for record in self._records() if self.live(record))

    def get(self, principal, identity):
        validate("identifier", identity)
        record = self._read(identity)
        if record is None:
            raise ControlError(404, "INPUT_EXPORT_NOT_FOUND")
        self._owner(principal, record)
        return record

    def admit(self, principal, configuration, bindings, nonce, retention_seconds, producer, guard, sample_rows=None):
        guard()
        principal.authorize(configuration["space_id"], "pipeline:run")
        if principal.generation != configuration["generation"]:
            raise ControlError(403, "SPACE_GENERATION_CHANGED")
        if (not isinstance(nonce, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", nonce)
                or type(retention_seconds) is not int or not 600 <= retention_seconds <= 2592000
                or not re.fullmatch(r"[0-9a-f]{40}", producer["source_commit"])
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", producer["image_digest"])):
            raise ControlError(400, "INVALID_INPUT_EXPORT")
        validate_bindings(bindings, configuration)
        definitions = {}
        for alias, binding in bindings.items():
            registered = configuration["datasets"][binding["dataset"]]
            validate("registered_dataset", registered)
            definitions[alias] = registered["definition_sha256"]
        if sample_rows is not None and (type(sample_rows) is not int or not 1 <= sample_rows <= 10000):
            raise ControlError(400, "INVALID_INPUT_EXPORT")
        intent = {"space_id": configuration["space_id"], "generation": configuration["generation"],
            "principal_id": principal.principal_id, "bindings": bindings, "definitions": definitions,
            "nonce": nonce, "retention_seconds": retention_seconds,
            **({"sample_rows": sample_rows} if sample_rows is not None else {})}
        identity = "input_" + digest([configuration["space_id"], configuration["generation"],
                                      principal.principal_id, nonce])
        binding = digest(intent)
        self.initialize()

        def save():
            prior = self._read(identity)
            if prior is not None:
                self._owner(principal, prior)
                if prior["binding_sha256"] != binding:
                    raise ControlError(409, "INPUT_EXPORT_BINDING_CONFLICT")
                return prior, False
            if sum(self.live(record) for record in self._records()) >= MAX_EXPORTS:
                raise ControlError(429, "INPUT_EXPORT_QUOTA")
            if self.retained_bytes() + MAX_FILE_BYTES > MAX_RETAINED_BYTES:
                raise ControlError(429, "INPUT_RETAINED_BYTES_QUOTA")
            record = {"format_version": 1, **intent, "export_id": identity,
                "execution_id": "exec_" + uuid.uuid4().hex, "binding_sha256": binding,
                "state": "ADMITTED", "created_unix": self.clock(),
                "retain_until_unix": self.clock() + retention_seconds, "references": {},
                "retained_bytes": MAX_FILE_BYTES,
                "producer": producer, "source_commit": producer["source_commit"],
                "image_digest": producer["image_digest"]}
            self.native.connection.execute("INSERT INTO warehouse.ctl.input_exports VALUES (?,?,?)",
                [identity, binding, canonical(record)])
            return record, True
        return self.native.transaction(save, guard)

    def _change(self, original, action, guard):
        def save():
            current = self._read(original["export_id"])
            if current is None or any(current[name] != original[name] for name in
                    ["binding_sha256", "execution_id", "principal_id", "generation"]):
                raise ControlError(503, "INPUT_EXPORT_CUSTODY_UNKNOWN")
            changed = action(copy.deepcopy(current))
            self.native.connection.execute("UPDATE warehouse.ctl.input_exports SET record=? WHERE export_id=?",
                [canonical(changed), original["export_id"]])
            return changed
        return self.native.transaction(save, guard)

    def stage(self, original, exported, guard):
        frozen = freeze(exported)
        if (frozen["space_id"] != original["space_id"] or frozen["generation"] != original["generation"]
                or set(frozen["inputs"]) != set(original["bindings"])):
            raise ControlError(409, "INPUT_EXPORT_BINDING_CONFLICT")
        for alias, value in frozen["inputs"].items():
            version = value["native_version"]
            if (value["dataset"] != original["bindings"][alias]["dataset"]
                    or value["selection"] != original["bindings"][alias]["selection"]
                    or version["dataset"] != value["dataset"] or version["generation"] != original["generation"]
                    or any(type(version[name]) is not int for name in ["commit_sequence", "snapshot_sequence"])
                    or not 0 <= version["commit_sequence"] <= version["snapshot_sequence"]
                    or not 0 < value["file"]["bytes"] <= MAX_FILE_BYTES):
                raise ControlError(409, "INPUT_EXPORT_BINDING_CONFLICT")
        if len({value["native_version"]["snapshot_sequence"] for value in frozen["inputs"].values()}) != 1:
            raise ControlError(409, "INPUT_EXPORT_BINDING_CONFLICT")
        size = sum(value["file"]["bytes"] for value in frozen["inputs"].values())
        if size > MAX_FILE_BYTES or size != frozen["metrics"]["copied_bytes"]:
            raise ControlError(413, "INPUT_EXPORT_BYTES_QUOTA")

        def update(current):
            if "staged" in current:
                if current["staged"] != frozen:
                    raise ControlError(409, "INPUT_EXPORT_BINDING_CONFLICT")
                return current
            if current["state"] != "ADMITTED":
                raise ControlError(503, "INPUT_EXPORT_CUSTODY_UNKNOWN")
            if (size > current["retained_bytes"]
                    or self.retained_bytes() - current["retained_bytes"] + size > MAX_RETAINED_BYTES):
                raise ControlError(429, "INPUT_RETAINED_BYTES_QUOTA")
            current.update(state="STAGED", staged=frozen, retained_bytes=size)
            return current
        return self._change(original, update, guard)

    def manifest_intent(self, original, intent, guard):
        def update(current):
            if current["state"] not in {"STAGED", "READY"}:
                raise ControlError(503, "INPUT_EXPORT_CUSTODY_UNKNOWN")
            if "manifest_intent" in current and current["manifest_intent"] != intent:
                raise ControlError(409, "INPUT_MANIFEST_BINDING_CONFLICT")
            current["manifest_intent"] = intent
            return current
        return self._change(original, update, guard)

    def ready(self, original, reference, metrics, guard):
        def update(current):
            if current["state"] in COLLECTION_STATES:
                raise ControlError(410, "INPUT_EXPORT_COLLECTED")
            if current["state"] == "READY":
                if current["manifest"] != reference:
                    raise ControlError(409, "INPUT_MANIFEST_BINDING_CONFLICT")
                return current
            if not self.available(current):
                raise ControlError(410, "INPUT_RETENTION_EXPIRED")
            if (current["state"] != "STAGED" or "manifest_intent" not in current
                    or any(reference[name] != current["manifest_intent"][name]
                           for name in ["bucket", "key", "sha256", "bytes"])
                    or reference.get("version_id") in {"", "null", None}):
                raise ControlError(503, "INPUT_MANIFEST_UNCONFIRMED")
            current.update(state="READY", manifest=reference, publication_metrics=metrics)
            return current
        return self._change(original, update, guard)

    def failed(self, original, code, guard):
        def update(current):
            if current["state"] == "ADMITTED":
                current.update(state="FAILED", error_code=code)
            return current
        return self._change(original, update, guard)

    def retain(self, original, consumer, until_unix, guard):
        validate("identifier", consumer)
        if (type(until_unix) not in {int, float} or not self.clock() < until_unix <= self.clock() + 2592000):
            raise ControlError(400, "INVALID_INPUT_RETENTION")

        def update(current):
            if current["state"] in COLLECTION_STATES:
                raise ControlError(410, "INPUT_EXPORT_COLLECTED")
            if current["state"] != "READY":
                raise ControlError(409, "INPUT_EXPORT_NOT_READY")
            if not self.available(current):
                raise ControlError(410, "INPUT_RETENTION_EXPIRED")
            if consumer not in current["references"] and len(current["references"]) >= 128:
                raise ControlError(429, "INPUT_REFERENCE_QUOTA")
            prior = current["references"].get(consumer)
            current["references"][consumer] = {"state": "ACTIVE",
                "retain_until_unix": max(until_unix, prior["retain_until_unix"] if prior else 0)}
            return current
        return self._change(original, update, guard)

    def release(self, original, consumer, guard):
        """The execution authority positively ends one consumer's pin; files stay immutable."""
        validate("identifier", consumer)

        def update(current):
            prior = current["references"].get(consumer)
            if prior is None:
                raise ControlError(404, "INPUT_REFERENCE_NOT_FOUND")
            if prior["state"] == "ACTIVE":
                current["references"][consumer] = {**prior, "state": "RELEASED", "released_unix": self.clock()}
            return current
        return self._change(original, update, guard)

    def live(self, record):
        # Expired READY originals without an ACTIVE consumer, and READY originals
        # whose every consumer was positively released, no longer reserve export
        # capacity (quota counts in-flight work). Their immutable objects remain
        # for audit/retention, and a retry can still re-pin them until expiry.
        if record["state"] == "COLLECTED":
            return False
        if record["state"] != "READY":
            return True
        references = record["references"].values()
        if references and all(value["state"] == "RELEASED" for value in references):
            return False
        return self.available(record)

    def available(self, record):
        # Expiry does not prove a consumer stopped. ACTIVE references continue
        # to pin files until the execution authority positively releases them.
        if record["state"] in COLLECTION_STATES:
            return False
        return self.clock() < record["retain_until_unix"] or any(
            value["state"] == "ACTIVE" for value in record["references"].values())

    # -- retention -------------------------------------------------------------------------------

    def eligibility(self, record, protected=()):
        """Why one original may be collected now, or None; ACTIVE pins always win."""
        now = self.clock()
        if record["export_id"] in protected:
            return None
        if record["state"] == "COLLECTING":
            return "RESUME_COLLECTION"
        if record["state"] == "COLLECTED" or any(
                value["state"] == "ACTIVE" for value in record["references"].values()):
            return None
        if record["state"] == "READY":
            # Released originals stay until expiry: a retried job may re-pin them.
            return "RETENTION_EXPIRED" if now >= record["retain_until_unix"] else None
        if record["state"] == "FAILED" and now >= record["created_unix"] + FAILED_GRACE_SECONDS:
            return "FAILED_ORIGINAL"
        if now >= record["retain_until_unix"] + ABANDONED_GRACE_SECONDS:
            return "ABANDONED_ORIGINAL"
        return None

    def candidates(self, protected=()):
        result = []
        for record in self._records():
            reason = self.eligibility(record, protected)
            result.append({"export_id": record["export_id"], "state": record["state"],
                           "created_unix": record["created_unix"], "retain_until_unix": record["retain_until_unix"],
                           "retained_bytes": record["retained_bytes"], "eligible": reason is not None,
                           "reason": reason, "references": len(record["references"]),
                           "release_ids": sorted({binding["release_id"] for binding in record["bindings"].values()
                                                  if binding.get("release_id")})})
        return result

    def pinned_releases(self):
        """Releases bound by originals that may still be read; collecting/collected originals bind none."""
        return {release for value in self.candidates() if not value["eligible"]
                and value["state"] not in COLLECTION_STATES for release in value["release_ids"]}

    def mark_collecting(self, identity, collection, protected, guard):
        """Native authority first: no consumer can pin or read an original being deleted."""
        record = self._read(identity)
        if record is None:
            raise ControlError(404, "INPUT_EXPORT_NOT_FOUND")
        reason = self.eligibility(record, protected)
        if reason is None:
            raise ControlError(409, "INPUT_EXPORT_RETAINED")

        def update(current):
            if current["state"] == "COLLECTING":
                return current
            if self.eligibility(current, protected) is None:
                raise ControlError(409, "INPUT_EXPORT_RETAINED")
            current["collection"] = {"collection_id": collection, "previous_state": current["state"],
                                     "reason": reason, "started_unix": self.clock()}
            current["state"] = "COLLECTING"
            return current
        return self._change(record, update, guard)

    def mark_collected(self, identity, collection, evidence, guard):
        record = self._read(identity)
        if record is None or record["state"] not in COLLECTION_STATES:
            raise ControlError(409, "INPUT_EXPORT_NOT_COLLECTING")

        def update(current):
            if current["state"] == "COLLECTED":
                return current
            if current["collection"]["collection_id"] != collection:
                raise ControlError(409, "INPUT_EXPORT_COLLECTION_CONFLICT")
            current["collection"].update(finished_unix=self.clock(), evidence_sha256=digest(evidence),
                                         objects=len(evidence["objects"]),
                                         bytes=sum(value.get("bytes") or 0 for value in evidence["objects"]))
            current.update(state="COLLECTED", retained_bytes=0)
            return current
        return self._change(record, update, guard)
