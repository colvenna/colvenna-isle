"""Authoritative native watermarks and release impact for the pipeline controller.

Scheduling decisions use these reads, inside one native MVCC transaction, rather
than DynamoDB projections. Impact is the full-row symmetric difference of two
immutable releases, so cross-partition updates and deletes affect both sides.
"""

import copy
import json

from colvenna_isle.contracts.validation import validate
from colvenna_isle.control.registry import ControlError
from colvenna_isle.pipeline.publications import impact
from colvenna_isle.query.native import describe
from .ingestion import digest
from .scheduler import NotSubmitted

OPERATIONS = {"pipeline_watermarks", "pipeline_impact"}


def _read(connection, action):
    connection.execute("BEGIN")
    try:
        result = action()
        connection.execute("COMMIT")
        return result
    except BaseException:
        try:
            connection.execute("ROLLBACK")
        except BaseException:
            raise ControlError(503, "NATIVE_READ_CLOSURE_UNKNOWN") from None
        raise


def watermarks(connection, configuration, pipeline, datasets, guard):
    """Dataset versions in one read transaction, then published task watermarks.

    Both only advance; a task release read later can be newer, never older,
    than the dataset vector it consumed.
    """
    selected = copy.deepcopy(configuration)
    selected["datasets"] = {name: configuration["datasets"][name] for name in datasets}
    _, versions = describe(connection, selected, guard)
    return _read(connection, lambda: _tasks(connection, pipeline, versions, datasets))


def _tasks(connection, pipeline, versions, datasets):
    result = {"datasets": {}, "tasks": {}}
    for version in versions:
        result["datasets"][version["dataset"]] = {name: version[name] for name in [
            "release_id", "commit_sequence", "snapshot_sequence"]}
    for name in datasets:
        result["datasets"].setdefault(name, {"release_id": None, "commit_sequence": 0,
                                             "snapshot_sequence": None})
    tables = {row[0] for row in connection.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name='warehouse' AND schema_name='ctl'").fetchall()}
    if "pipeline_task_watermarks" in tables:
        for task, record in connection.execute(
                "SELECT task_id,record FROM warehouse.ctl.pipeline_task_watermarks WHERE pipeline_id=?",
                [pipeline]).fetchall():
            value = json.loads(record)
            result["tasks"][task] = {name: copy.deepcopy(value.get(name)) for name in [
                "release_id", "dataset", "pipeline_version", "run_id", "job_id", "commit_sequence",
                "input_watermarks", "input_versions", "processing"]}
    result["snapshot_sequence"] = connection.execute(
        "SELECT sequence FROM warehouse.ctl.commit_sequence WHERE slot=1").fetchone()[0]
    return result


class PipelineReads:
    def __init__(self, runtime):
        self.runtime = runtime

    def execute(self, request):
        operation = request.get("operation")
        if operation not in OPERATIONS:
            raise ControlError(400, "UNKNOWN_PIPELINE_READ")
        actor, configuration = self.runtime.inputs.authorize(request)

        def guard():
            current_actor, current = self.runtime.inputs.authorize(request)
            if current_actor != actor or current["route_version"] != configuration["route_version"]:
                raise ControlError(403, "SPACE_CONFIGURATION_CHANGED")

        if operation == "pipeline_watermarks":
            validate("identifier", request.get("pipeline_id", ""))
            datasets = request.get("datasets")
            if (not isinstance(datasets, list) or not 1 <= len(datasets) <= 64 or len(set(datasets)) != len(datasets)
                    or any(name not in configuration["datasets"] for name in datasets)):
                raise ControlError(400, "INVALID_PIPELINE_READ")

            def action():
                database = self.runtime.database(configuration)
                database.ingestions.initialize()
                return watermarks(database.connection, configuration, request["pipeline_id"], datasets, guard)
        else:
            dataset, columns = request.get("dataset", ""), request.get("partition_columns")
            validate("dataset", dataset)
            for name in ["from_release", "to_release"]:
                if request.get(name) is not None:
                    validate("identifier", request[name])
            # An empty column list asks only whether the two releases' rows differ.
            if (request.get("to_release") is None or not isinstance(columns, list) or len(columns) > 16
                    or len(set(columns)) != len(columns) or not all(isinstance(name, str) for name in columns)):
                raise ControlError(400, "INVALID_PIPELINE_READ")

            def action():
                database = self.runtime.database(configuration)
                database.ingestions.initialize()
                return _read(database.connection, lambda: impact(
                    database.connection, configuration, dataset, request.get("from_release"),
                    request["to_release"], columns))
        try:
            return self.runtime.scheduler.submit("export", digest([operation, request]), action,
                                                 configuration["space_id"]).future.result()
        except NotSubmitted:
            raise ControlError(429, "PIPELINE_READ_QUEUE_FULL") from None
