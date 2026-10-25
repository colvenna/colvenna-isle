"""IAM-only native COMMIT faults for the one explicitly installed qualification (probe_*) generation."""

import os
import threading

from colvenna_isle.contracts.validation import validate
from colvenna_isle.control.registry import ControlError
from colvenna_isle.pipeline.publications import CHUNK_ROWS
from .ingestion import digest

FAULTS = {"commit_reply_lost", "before_commit_abort", "after_commit_abort"}
MAX_FAULTS = 16


class _CommitFault:
    """Delegate the real transaction; fail precisely at its original COMMIT."""

    def __init__(self, connection, fault, record, log):
        self.connection, self.fault, self.record, self.log = connection, fault, record, log
        self.armed = True

    def execute(self, sql, *args):
        if sql != "COMMIT" or not self.armed:
            return self.connection.execute(sql, *args)
        self.armed = False
        if self.fault == "before_commit_abort":
            self.log("PUBLICATION_QUALIFICATION_COMMIT", {**self.record, "effect": "BEFORE_COMMIT_EXIT137"})
            os._exit(137)
        result = self.connection.execute(sql, *args)
        self.log("PUBLICATION_QUALIFICATION_COMMIT", {**self.record, "effect": "REAL_COMMIT_RETURNED"})
        if self.fault == "after_commit_abort":
            os._exit(137)
        raise OSError("Original publication Native COMMIT reply lost")


class PublicationQualification:
    """No grant, arbitrary SQL, target change, or public fault interface."""

    def __init__(self, publisher):
        self.publisher = publisher
        self.runtime = publisher.runtime
        self.faults, self.lock = {}, threading.Lock()

    def require(self, configuration):
        space = getattr(self.runtime, "probe_space", "")
        if (not space.startswith("probe_")
                or configuration["space_id"] != space
                or configuration["generation"] != getattr(self.runtime, "probe_generation", "")):
            raise ControlError(403, "QUALIFICATION_DISABLED")

    @staticmethod
    def key(request, phase, image=None, file_number=None, chunk_number=None):
        return tuple(request[name] for name in [
            "space_id", "generation", "principal_id", "key_id", "job_id",
            "eligibility_epoch", "attempt_id", "execution_id"]) + (
            phase, image, file_number, chunk_number)

    def execute(self, request):
        actor, configuration = self.publisher.actor(request)
        self.require(configuration)
        for name in ["job_id", "attempt_id", "execution_id"]:
            validate("identifier", request.get(name, ""))
        epoch, phase, fault = request.get("eligibility_epoch"), request.get("phase"), request.get("fault")
        if type(epoch) is not int or epoch < 1 or phase not in {"chunk", "publish"} or fault not in FAULTS:
            raise ControlError(400, "INVALID_PUBLICATION_FAULT")
        image, file, chunk = request.get("image"), request.get("file_number"), request.get("chunk_number")
        if phase == "chunk":
            if (image not in {"data", "state"} or type(file) is not int or not 0 <= file < 10000
                    or type(chunk) is not int or not 0 <= chunk < 100000):
                raise ControlError(400, "INVALID_PUBLICATION_FAULT")
        elif any(value is not None for value in [image, file, chunk]):
            raise ControlError(400, "INVALID_PUBLICATION_FAULT")
        key = self.key(request, phase, image, file, chunk)

        def arm():
            current_actor, current = self.publisher.actor(request)
            self.require(current)
            if current_actor != actor or current["route_version"] != configuration["route_version"]:
                raise ControlError(403, "SPACE_CONFIGURATION_CHANGED")
            _, store = self.publisher.store(request, actor, configuration)
            original = store.status(request["job_id"])
            if (original["epoch"] != epoch or original["descriptor"]["attempt_id"] != request["attempt_id"]
                    or original["status"] in {"REVOKED", "PUBLISHED"}
                    or original.get("execution_id", request["execution_id"]) != request["execution_id"]):
                raise ControlError(409, "PUBLICATION_ELIGIBILITY_CHANGED")
            if image == "state" and original["state_contract"] is None:
                raise ControlError(422, "PUBLICATION_IMAGE_SCOPE")
            if phase == "chunk" and "selection" in original:
                files = original["selection"]["files" if image == "data" else "state_files"]
                if file >= len(files) or chunk * CHUNK_ROWS >= files[file]["rows"]:
                    raise ControlError(422, "PUBLICATION_CHUNK_SCOPE")
                committed = store.connection.execute(
                    "SELECT count(*) FROM warehouse.ctl.publication_chunks "
                    "WHERE generation_id=? AND image=? AND file_number=? AND chunk_number=?",
                    [original["generation_id"], image, file, chunk]).fetchone()[0]
                if committed:
                    raise ControlError(409, "QUALIFICATION_CHUNK_ALREADY_COMMITTED")
            with self.lock:
                if key in self.faults and self.faults[key] != fault:
                    raise ControlError(409, "QUALIFICATION_FAULT_CONFLICT")
                if key not in self.faults and len(self.faults) >= MAX_FAULTS:
                    raise ControlError(429, "QUALIFICATION_FAULT_LIMIT")
                self.faults[key] = fault
            return {"job_id": request["job_id"], "eligibility_epoch": epoch,
                    "attempt_id": request["attempt_id"], "execution_id": request["execution_id"],
                    "phase": phase, "fault": fault, "image": image,
                    "file_number": file, "chunk_number": chunk,
                    "scope": "INSTALLED_QUALIFICATION_NATIVE_FAULT_ONLY"}

        # Different payloads must independently reach the Native binding/conflict
        # check, even while an earlier arming request is still queued.
        return self.publisher.unit(digest(["qualification-arm", key, fault]), arm)

    def run(self, store, request, phase, action, *, image=None, file_number=None, chunk_number=None):
        """Called exclusively inside the scheduler's Native lock."""
        key = self.key(request, phase, image, file_number, chunk_number)
        with self.lock:
            fault = self.faults.pop(key, None)
        if fault is None:
            return action()
        _, configuration = self.publisher.actor(request)
        self.require(configuration)
        record = {name: request[name] for name in [
            "space_id", "generation", "job_id", "eligibility_epoch", "attempt_id", "execution_id"]}
        record.update(phase=phase, image=image, file_number=file_number, chunk_number=chunk_number, fault=fault)
        connection = store.native.connection
        store.native.connection = _CommitFault(connection, fault, record, self.runtime.log)
        try:
            return action()
        finally:
            # Native.transaction keeps the handle paused after an uncertain
            # COMMIT. Restoring the delegate does not clear that authoritative
            # result_unknown flag or authorize another mutation.
            store.native.connection = connection
