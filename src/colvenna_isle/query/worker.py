"""Trusted native worker. User SQL runs only after closing the native reader."""

import hashlib
import json
import os
import sys
from pathlib import Path


def main():
    path, identity, directory = Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3])

    def guard():
        if hashlib.sha256(path.read_bytes()).hexdigest() != identity:
            raise RuntimeError("QUERY_INPUT_CHANGED")

    guard()
    request = json.loads(path.read_text())
    from colvenna_isle.control.registry import ControlError
    if not request["catalog_only"] and request["consistency"] not in {
            "current", "published", "after_write", "after_ingestion"}:
        raise ControlError(503, "CONSISTENCY_NOT_READY")
    from colvenna_isle.query.native import Reader
    from colvenna_isle.query.sql import Session, references
    from colvenna_isle.timing import Timing, io_counters, io_delta
    # The child's stdout is discarded: its segments travel in result.json to the parent's line.
    timing = Timing("query_worker")
    if request["open_only"]:
        reader = Reader(request["configuration"], directory / "native", guard)
        reader.close()
        result = {"opened": True}
    elif request["catalog_only"]:
        reader = Reader(request["configuration"], directory / "native", guard)
        try:
            catalog, versions = reader.describe()
            result = {"catalog": catalog, "data_versions": versions}
        finally:
            reader.close()
    else:
        snapshot = Session(directory / "snapshot")
        try:
            # Stage only what the SQL names; parsing never runs it.
            with timing.segment("references"):
                datasets = references([statement["sql"] for statement in request["statements"]],
                                      request["configuration"]["datasets"])
            timing.count("datasets", len(datasets))
            if request["consistency"] in {"after_write", "after_ingestion"}:
                from colvenna_isle.query.controlled import Reader as ControlledReader
                if not request.get("source"):
                    raise ControlError(503, "QUERY_EXECUTION_CUSTODY_UNKNOWN")
                with timing.segment("writer_snapshot"):
                    ControlledReader.from_environment().stage(
                        request["source"], request["configuration"], snapshot, guard, datasets)
            else:
                with timing.segment("native_open"):
                    reader = Reader(request["configuration"], directory / "native", guard)
                try:
                    before = io_counters(reader.connection) if timing.active else None
                    with timing.segment("stage"):
                        reader.stage(snapshot, datasets=datasets, consistency=request["consistency"],
                                     release_id=(request.get("condition") or {}).get("release_id"))
                    if before is not None:
                        timing.count("stage_s3", io_delta(before, io_counters(reader.connection)))
                finally:
                    reader.close()
            if request["consistency"] == "published":
                snapshot.publications_only(request["configuration"]["datasets"])
            with timing.segment("execute"):
                results = [snapshot.execute(statement["sql"], statement["max_rows"])
                           for statement in request["statements"]]
            if timing.active:
                from colvenna_isle.timing import process_resources
                timing.count("worker_resources", process_resources(snapshot.connection))
            result = {"catalog": snapshot.catalog, "results": results,
                      **({"timing": timing.value("worker")} if timing.active else {})}
        finally:
            snapshot.close()
    guard()
    with (directory / "result.json").open("x") as stream:
        os.chmod(directory / "result.json", 0o600)
        stream.write(json.dumps(result, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        from colvenna_isle.control.registry import ControlError
        directory = Path(sys.argv[3])
        with (directory / "failure.json").open("x") as stream:
            os.chmod(directory / "failure.json", 0o600)
            stream.write(json.dumps({
                "code": error.code if isinstance(error, ControlError) else "NATIVE_EXECUTION_FAILED",
                "status": error.status if isinstance(error, ControlError) else 503}) + "\n")
        raise SystemExit(1)
