"""Controlled SQL over fixed input relations, producing the Python task file contract.

The adapter runs this trusted module in its own bounded process group, for the
writer, Lambda and ECS executors alike. Each input alias becomes an in-memory
table loaded from the admitted local Parquet files using the declared types.
External access, extension loading and configuration changes are then locked;
the published SQL must be exactly one SELECT whose result names and native types
equal the declared output schema. The product, never the SQL, writes targets.
"""

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path

from colvenna_isle.contracts.model import LIMITS
from colvenna_isle.contracts.validation import validate
from colvenna_isle.control.http import decode_json
from colvenna_isle.control.registry import ControlError, canonical
from colvenna_isle.writer.ingestion import quoted
from .parquet import write

class SqlFailure(ControlError):
    pass


def _declaration(columns):
    return ",".join(quoted(column["name"]) + " " + column["type"]
                    + ("" if column["nullable"] else " NOT NULL") for column in columns)


def connect(directory, memory_bytes):
    import duckdb

    connection = duckdb.connect(":memory:", config={
        "autoload_known_extensions": False, "autoinstall_known_extensions": False,
        "allow_unsigned_extensions": False})
    spill = Path(directory) / "sql-spill"
    spill.mkdir(mode=0o700)
    for command in ["SET threads=2", "SET TimeZone='UTC'", "SET preserve_insertion_order=true",
                    "SET memory_limit='" + str(max(memory_bytes // 1048576, 64)) + "MB'",
                    "SET max_temp_directory_size='" + str(max(memory_bytes // 1048576, 64)) + "MB'"]:
        connection.execute(command)
    connection.execute("SET temp_directory=?", [str(spill)])
    return connection


def load_inputs(connection, context):
    """Bind each alias to an exact typed copy of its admitted fixed files."""
    for alias, binding in context["inputs"].items():
        columns = binding["schema"]
        connection.execute("CREATE TABLE " + quoted(alias) + " (" + _declaration(columns) + ")")
        for file in binding["files"]:
            path = Path(file["local_path"])
            # Declared CASTs restore JSON and exact Decimal/temporal types; no
            # implicit inference from Parquet metadata reaches the user SQL.
            connection.execute(
                "INSERT INTO " + quoted(alias) + " SELECT " + ",".join(
                    "CAST(" + quoted(column["name"]) + " AS " + column["type"] + ")" for column in columns)
                + " FROM read_parquet(?)", [str(path)])
            if connection.execute("SELECT count(*) FROM " + quoted(alias)).fetchone()[0] < file["rows"]:
                raise SqlFailure(422, "SQL_INPUT_UNCONFIRMED")
        actual = connection.execute("SELECT count(*) FROM " + quoted(alias)).fetchone()[0]
        if actual != sum(file["rows"] for file in binding["files"]):
            raise SqlFailure(422, "SQL_INPUT_UNCONFIRMED")


def seal(connection):
    connection.execute("SET enable_external_access=false")
    connection.execute("SET lock_configuration=true")


def statement(connection, sql):
    import duckdb

    if not isinstance(sql, str) or not sql.strip() or len(sql.encode()) > LIMITS["sql_bytes"]:
        raise SqlFailure(422, "SQL_INVALID")
    try:
        statements = connection.extract_statements(sql)
    except duckdb.Error:
        raise SqlFailure(422, "SQL_INVALID") from None
    if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
        # DML/DDL/ATTACH/COPY/PRAGMA/SET and multiple statements never run.
        raise SqlFailure(403, "SQL_READ_ONLY_REQUIRED")
    return statements[0].query


def check_schema(connection, query, columns):
    """Result names/types must equal the declared output; no silent coercion."""
    import duckdb

    relation = connection.sql(query)
    actual = [(name, str(kind)) for name, kind in zip(relation.columns, relation.types)]
    expected = [(column["name"], str(duckdb.sqltype(column["type"]))) for column in columns]
    if actual != expected:
        raise SqlFailure(422, "SQL_SCHEMA_MISMATCH")


def execute(context, sql, output, *, memory_bytes, maximum_bytes, maximum_rows):
    import duckdb
    from colvenna_isle.query.native import partition_text, partition_value

    started = time.monotonic()
    validate("context", context)
    target = context["output"]
    columns = target["schema"]
    connection = connect(Path(output).parent.parent, memory_bytes)
    try:
        load_inputs(connection, context)
        seal(connection)
        query = statement(connection, sql)
        check_schema(connection, query, columns)
        partitions = target.get("partition_columns", []) if target["write_mode"] == "replace_partitions" else []
        allowed = {partition_text(columns, part): part for part in target["replace_partitions"]}
        # The original statement runs intact, including a trailing semicolon or
        # comment: never textually wrapped, and ordered by passing it to query()
        # as a value. Results stream from an execute() result, which DuckDB 1.5
        # and 2.0 both read in batches; 2.0 relations have no fetchmany().
        if partitions:
            cursor = connection.execute("SELECT * FROM query(?) ORDER BY "
                                        + ",".join(quoted(name) for name in partitions), [query])
        else:
            cursor = connection.execute(query)
        names = [column["name"] for column in columns]
        files, rows = [], 0

        def flush(identity, batches):
            number = len(files)
            path = Path(output) / ("part-" + str(number).zfill(5) + ".parquet")
            metadata = write(path, columns, batches,
                             maximum_bytes=maximum_bytes - sum(file["bytes"] for file in files),
                             maximum_rows=maximum_rows - sum(file["rows"] for file in files))
            files.append({"path": path.name, **metadata,
                          "partition_values": allowed[identity] if identity is not None else {}})

        def stream():
            nonlocal rows
            while True:
                batch = cursor.fetchmany(512)
                if not batch:
                    return
                rows += len(batch)
                if rows > maximum_rows:
                    raise SqlFailure(413, "SQL_OUTPUT_ROWS_BUDGET")
                yield from batch

        def identity_of(row):
            return partition_text(columns, {name: partition_value(row[names.index(name)]) for name in partitions})

        values = stream()
        pending = next(values, None)
        if not partitions:
            if pending is not None:
                # An empty result is declared by row_count 0, with no file.
                flush(None, _chunks(_prepend(pending, values)))
        else:
            seen = set()
            while pending is not None:
                identity = identity_of(pending)
                if identity not in allowed or identity in seen:
                    raise SqlFailure(422, "SQL_PARTITION_SCOPE")
                seen.add(identity)

                def partition_rows():
                    nonlocal pending
                    while pending is not None and identity_of(pending) == identity:
                        yield pending
                        pending = next(values, None)
                # ORDER BY makes each partition contiguous; one file per partition.
                flush(identity, _chunks(partition_rows()))
        manifest = {"contract_version": 2, **{name: context[name] for name in ["job_id", "attempt_id", "execution_id"]},
                    "output_name": "result", "format": "parquet", "schema": columns,
                    "row_count": sum(file["rows"] for file in files), "files": files,
                    "metrics": {"sql_elapsed_ms": round((time.monotonic() - started) * 1000),
                                "sql_input_rows": sum(file["rows"] for binding in context["inputs"].values()
                                                      for file in binding["files"])}}
        if target["write_mode"] == "replace_partitions":
            manifest["replace_partitions"] = target["replace_partitions"]
        raw = canonical(manifest).encode()
        (Path(output) / "output-manifest.json").write_bytes(raw)
        return {"status": "STAGED_LOCAL", "row_count": manifest["row_count"],
                "manifest_sha256": hashlib.sha256(raw).hexdigest()}
    except ControlError:
        raise
    except duckdb.PermissionException:
        raise SqlFailure(403, "SQL_EXTERNAL_ACCESS_FORBIDDEN") from None
    except duckdb.OutOfMemoryException:
        raise SqlFailure(413, "SQL_MEMORY_BUDGET") from None
    except (duckdb.ParserException, duckdb.BinderException, duckdb.CatalogException):
        raise SqlFailure(422, "SQL_INVALID") from None
    except duckdb.Error:
        raise SqlFailure(422, "SQL_EXECUTION_FAILED") from None
    finally:
        connection.close()


def _prepend(first, rest):
    yield first
    yield from rest


def _chunks(rows):
    batch = []
    for row in rows:
        batch.append(row)
        if len(batch) == 512:
            yield batch
            batch = []
    if batch:
        yield batch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ["--context", "--output-dir", "--sql", "--status"]:
        parser.add_argument(name, type=Path, required=True)
    args = parser.parse_args()
    result = {"status": "FAILED", "code": "SQL_EXECUTION_FAILED"}
    try:
        context = decode_json(args.context.read_bytes())
        memory = int(os.environ["COLVENNA_SQL_MEMORY_BYTES"])
        result = execute(context, args.sql.read_text(encoding="utf-8"), args.output_dir,
                         memory_bytes=memory, maximum_bytes=int(os.environ["COLVENNA_SQL_OUTPUT_BYTES"]),
                         maximum_rows=int(os.environ["COLVENNA_SQL_OUTPUT_ROWS"]))
    except ControlError as error:
        result = {"status": "FAILED", "code": error.code}
    except (OSError, ValueError, UnicodeError, KeyError):
        result = {"status": "FAILED", "code": "SQL_CONTRACT_INVALID"}
    # The trusted adapter reads only this bounded code; logs keep no values.
    with args.status.open("x") as stream:
        json.dump(result, stream, sort_keys=True)
    if result["status"] != "STAGED_LOCAL":
        sys.exit(1)


if __name__ == "__main__":
    main()
