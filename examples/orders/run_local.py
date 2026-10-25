#!/usr/bin/env python3
"""Check the example DAG and file contract locally; no AWS executor is invoked."""

import hashlib
import json
import subprocess
import sys
import tempfile
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parents[1] / "src"))
from colvenna_isle.contracts.validation import load_document, validate


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def arrow_type(value):
    types = {"BIGINT": pa.int64(), "BOOLEAN": pa.bool_(), "VARCHAR": pa.string(), "DATE": pa.date32()}
    if value.startswith("DECIMAL("):
        precision, scale = map(int, value.removeprefix("DECIMAL(").removesuffix(")").split(","))
        return pa.decimal128(precision, scale)
    return types[value]


def validate_table(table, output):
    expected = output["schema"]
    if table.column_names != [column["name"] for column in expected]:
        raise ValueError("Output column names/order differ from registered schema")
    for column in expected:
        actual = table[column["name"]]
        if actual.type != arrow_type(column["type"]):
            raise ValueError(f"Wrong type for {column['name']}")
        if not column["nullable"] and actual.null_count:
            raise ValueError(f"Null values in {column['name']}")
    keys = [tuple(row[key] for key in output["key_columns"]) for row in table.to_pylist()]
    if len(keys) != len(set(keys)):
        raise ValueError("Duplicate output business keys")
    if not table.num_rows and not output["allow_empty"]:
        raise ValueError("Empty output is forbidden")


def validate_result(context):
    output_dir = Path(context["output"]["directory"]).resolve()
    manifest = json.loads((output_dir / "output-manifest.json").read_text())
    validate("output_manifest", manifest, context=context)
    for key in ["contract_version", "job_id", "attempt_id", "execution_id"]:
        if manifest[key] != context[key]:
            raise ValueError(f"Wrong result identity: {key}")
    if manifest["format"] != "parquet" or manifest["output_name"] != "result":
        raise ValueError("Unexpected output format/name")
    if manifest["schema"] != context["output"]["schema"]:
        raise ValueError("Manifest schema differs from definition")
    tables, seen = [], set()
    for file in manifest["files"]:
        relative = Path(file["path"])
        path = (output_dir / relative).resolve()
        if relative.is_absolute() or ".." in relative.parts or not path.is_relative_to(output_dir):
            raise ValueError("Output file leaves its assigned directory")
        if path in seen:
            raise ValueError("Duplicate output file")
        seen.add(path)
        if file["bytes"] != path.stat().st_size or file["sha256"] != sha256(path):
            raise ValueError("Output hash/bytes mismatch")
        table = pq.ParquetFile(path).read()
        if table.num_rows != file["rows"]:
            raise ValueError("Output row count mismatch")
        tables.append(table)
    schema = pa.schema(
        [pa.field(c["name"], arrow_type(c["type"]), nullable=c["nullable"])
         for c in context["output"]["schema"]]
    )
    table = pa.concat_tables(tables) if tables else pa.Table.from_pylist([], schema=schema)
    if table.num_rows != manifest["row_count"]:
        raise ValueError("Total output row count mismatch")
    validate_table(table, context["output"])
    return table


def check_definition(definition):
    validate("pipeline", definition)
    tasks = {task["task_id"]: task for task in definition["tasks"]}
    if len(tasks) != len(definition["tasks"]):
        raise ValueError("Duplicate task IDs")
    visited = set()
    for task in definition["tasks"]:
        if task["processing"]["mode"] not in {"full", "incremental", "partition_recompute"}:
            raise ValueError("Unknown processing mode")
        if not set(task.get("depends_on", [])).issubset(visited):
            raise ValueError("Example tasks must be in valid dependency order")
        for binding in task["inputs"].values():
            if "task" in binding and binding["task"] not in task.get("depends_on", []):
                raise ValueError("Input task must be an explicit dependency")
        entrypoint = (ROOT / task["entrypoint"]).resolve()
        if not entrypoint.is_relative_to(ROOT) or not entrypoint.is_file():
            raise ValueError("Invalid entrypoint")
        validate_table(
            pa.Table.from_pylist([], schema=pa.schema(
                [pa.field(c["name"], arrow_type(c["type"])) for c in task["output"]["schema"]]
            )),
            task["output"],
        )
        visited.add(task["task_id"])
    return tasks


def fixture():
    schema = pa.schema([
        ("order_id", pa.int64()), ("source_version", pa.int64()),
        ("_colvenna_commit_sequence", pa.int64()), ("event_time", pa.timestamp("us")),
        ("amount", pa.decimal128(18, 2)), ("_colvenna_operation", pa.string()),
    ])
    values = [
        (1, 1, "2026-10-14", "10.00", "upsert"),
        (1, 2, "2026-10-14", "12.34", "upsert"),
        (2, 1, "2026-10-14", "600.00", "upsert"),
        (3, 1, "2026-10-14", "-2.34", "upsert"),
        (4, 1, "2026-10-14", "100.00", "upsert"),
        (4, 2, "2026-10-14", "100.00", "delete"),
        (5, 1, "2026-10-15", "50.00", "upsert"),
    ]
    rows = [
        dict(order_id=key, source_version=version, _colvenna_commit_sequence=sequence,
             event_time=datetime.fromisoformat(day), amount=Decimal(amount), _colvenna_operation=operation)
        for sequence, (key, version, day, amount, operation) in enumerate(values, start=1)
    ]
    return pa.Table.from_pylist(rows, schema=schema)


def run_dag(definition, directory, source):
    outputs = {}
    source_path = directory / "events.parquet"
    pq.write_table(source, source_path)
    for task in definition["tasks"]:
        task_id = task["task_id"]
        inputs = {}
        for alias, binding in task["inputs"].items():
            path = outputs[binding["task"]] if "task" in binding else source_path
            inputs[alias] = path
        result_path = directory / f"{task_id}.parquet"
        if task["language"] == "sql":
            with duckdb.connect() as connection:
                for alias, path in inputs.items():
                    connection.read_parquet(str(path)).create_view(alias)
                query = (ROOT / task["entrypoint"]).read_text()
                result = connection.sql(query).to_arrow_table()
                validate_table(result, task["output"])
                pq.write_table(result, result_path)
        else:
            output_dir = directory / task_id / "attempt_1" / "exec_1" / "output"
            input_bindings = {}
            declared_tasks = {value["task_id"]: value for value in definition["tasks"]}
            for alias, path in inputs.items():
                source_task = task["inputs"][alias]["task"]
                descriptor = {
                    "contract_version": 2,
                    "dataset": declared_tasks[source_task]["output"]["dataset"],
                    "release_id": f"local_{source_task}_v1",
                    "watermark": {"ods_sequence": source.num_rows},
                    "schema": declared_tasks[source_task]["output"]["schema"],
                    "files": [{
                        "s3_uri": f"s3://local-demo-fixtures/inputs/{source_task}/{path.name}",
                        "object_version": "local-fixture-version", "sha256": sha256(path),
                        "bytes": path.stat().st_size, "rows": pq.ParquetFile(path).metadata.num_rows,
                    }],
                }
                validate("input_manifest", descriptor)
                encoded = (json.dumps(descriptor, sort_keys=True, indent=2) + "\n").encode()
                (directory / f"{task_id}-{alias}-input-manifest.json").write_bytes(encoded)
                input_bindings[alias] = {
                    key: value for key, value in descriptor.items() if key != "contract_version"
                }
                input_bindings[alias].update(
                    manifest_sha256=hashlib.sha256(encoded).hexdigest(),
                    selection={"mode": "snapshot", "partitions": []},
                    files=[{**file, "local_path": str(path)} for file in descriptor["files"]],
                )
            context = {
                "contract_version": 2, "space_id": "local_demo", "pipeline_id": "orders",
                "pipeline_version": "local_v1", "run_id": directory.name, "task_id": task_id,
                "job_id": f"job_{task_id}", "attempt_id": "attempt_1", "execution_id": "exec_1",
                "input_mode": "local_files",
                "parameters": {"high_value_threshold": "500.00"},
                "inputs": input_bindings,
                "processing": {"mode": "full"},
                "output": dict(task["output"], name="result", directory=str(output_dir),
                               expected_previous_release=None, replace_partitions=[]),
            }
            # Same document fields as the adapter contract; these S3 references
            # identify local fixtures and are never fetched from AWS.
            validate("context", context)
            context_path = directory / f"{task_id}-context.json"
            context_path.write_text(json.dumps(context, indent=2) + "\n")
            for binding in context["inputs"].values():
                for file in binding["files"]:
                    if sha256(Path(file["local_path"])) != file["sha256"]:
                        raise ValueError("Input hash mismatch")
            subprocess.run([
                sys.executable, str(ROOT / task["entrypoint"]), "--context", str(context_path),
                "--output-dir", str(output_dir),
            ], check=True)
            result = validate_result(context)
            pq.write_table(result, result_path)
            # A declared hash cannot bless changed bytes; test the independent validator.
            manifest_path = output_dir / "output-manifest.json"
            original = manifest_path.read_text()
            changed = json.loads(original)
            changed["files"][0]["sha256"] = "0" * 64
            manifest_path.write_text(json.dumps(changed))
            try:
                validate_result(context)
            except ValueError as error:
                if str(error) != "Output hash/bytes mismatch":
                    raise
            else:
                raise AssertionError("Tampered output hash was accepted")
            finally:
                manifest_path.write_text(original)
        outputs[task_id] = result_path
    return {name: pq.ParquetFile(path).read().to_pylist() for name, path in outputs.items()}


def check_incremental_cleaning():
    """Exercise delta SQL and a local state model; not the cloud merge publisher."""
    state_schema = pa.schema([
        ("order_id", pa.int64()), ("_colvenna_version", pa.int64()),
        ("_colvenna_sequence", pa.int64()), ("_colvenna_deleted", pa.bool_()),
    ])
    batches = [
        [(1, 2, "12.34", "upsert"), (2, 1, "600.00", "upsert")],
        [(1, 1, "99.00", "upsert"), (2, 2, None, "delete"), (3, 1, "-2.34", "upsert")],
        [(2, 1, "1000.00", "upsert")],  # Older version must not resurrect a deleted key.
        [(2, 3, "700.00", "upsert")],
    ]
    expected_operations = [
        {1: "UPSERT", 2: "UPSERT"}, {2: "DELETE", 3: "UPSERT"}, {}, {2: "UPSERT"},
    ]
    key_state, current, history = {}, {}, []
    query = (ROOT / "sql/clean_orders_incremental.sql").read_text()
    with duckdb.connect() as connection:
        for batch, expected in zip(batches, expected_operations, strict=True):
            rows = []
            for key, version, amount, operation in batch:
                row = dict(order_id=key, source_version=version,
                           _colvenna_commit_sequence=len(history) + 1,
                           event_time=datetime(2026, 10, 14),
                           amount=Decimal(amount) if amount is not None else None,
                           _colvenna_operation=operation)
                rows.append(row)
                history.append(row)
            connection.register("changes", pa.Table.from_pylist(rows, schema=fixture().schema))
            connection.register("key_state", pa.Table.from_pylist(list(key_state.values()), schema=state_schema))
            mutations = connection.sql(query).to_arrow_table().to_pylist()
            actual = {row["order_id"]: row["_colvenna_op"] for row in mutations}
            if actual != expected:
                raise AssertionError(f"Incorrect incremental mutations: {actual}")
            for row in mutations:
                key = row["order_id"]
                key_state[key] = dict(order_id=key, _colvenna_version=row["_colvenna_version"],
                                      _colvenna_sequence=row["_colvenna_sequence"],
                                      _colvenna_deleted=row["_colvenna_op"] == "DELETE")
                if row["_colvenna_op"] == "DELETE":
                    if row["amount"] is not None or row["order_date"] is not None:
                        raise AssertionError("Delete rows should not require business payload")
                    current.pop(key, None)
                else:
                    current[key] = {name: row[name] for name in ["order_id", "order_date", "amount"]}
        connection.register("events", pa.Table.from_pylist(history, schema=fixture().schema))
        full = connection.sql((ROOT / "sql/clean_orders.sql").read_text()).to_arrow_table().to_pylist()
        if current != {row["order_id"]: row for row in full}:
            raise AssertionError("Incremental cleaning differs from full result at the same watermark")


def main():
    definition = load_document(ROOT / "pipeline.yaml")
    check_definition(definition)
    with tempfile.TemporaryDirectory(prefix="colvenna-orders-contract-") as temporary:
        work = Path(temporary)
        populated = work / "populated"
        populated.mkdir()
        outputs = run_dag(definition, populated, fixture())
        if sorted(row["order_id"] for row in outputs["clean_orders"]) != [1, 2, 3, 5]:
            raise AssertionError("Latest-version selection/delete failed")
        scores = {row["order_id"]: (row["high_value"], row["value_band"])
                  for row in outputs["score_orders"]}
        if scores != {1: (False, "REGULAR"), 2: (True, "HIGH"),
                      3: (False, "REFUND"), 5: (False, "REGULAR")}:
            raise AssertionError("Python scoring failed")
        actual = sorted(
            (row["order_date"], row["revenue"], row["order_count"], row["high_value_count"])
            for row in outputs["dashboard"]
        )
        expected = [(date(2026, 10, 14), Decimal("610.00"), 3, 1),
                    (date(2026, 10, 15), Decimal("50.00"), 1, 0)]
        if actual != expected:
            raise AssertionError(f"Wrong ADS totals: {actual}")
        empty = work / "empty"
        empty.mkdir()
        if any(run_dag(definition, empty, fixture().slice(0, 0)).values()):
            raise AssertionError("Empty inputs must produce schema-correct empty outputs")
    check_incremental_cleaning()
    print("PASS: four-task SQL/Python DAG, updates/delete/refund, exact Decimal totals, "
          "typed empty results, and tampered output rejection")
    print("PASS: incremental cleaning, stale events, delete tombstones, newer-version restoration, "
          "and comparison with full recomputation")
    print("NOT_RUN: Lambda/ECS, S3, IAM, environment builds, scheduler and database publication")


if __name__ == "__main__":
    main()
