#!/usr/bin/env python3
"""Exercise the locked native database through SQL in an isolated process."""

import argparse
import hashlib
import json
import os
import time
from pathlib import Path

import duckdb

from colvenna_isle.region import region


def connect(args):
    artifacts = Path(args.artifacts).resolve()
    manifest = json.loads((artifacts / "manifest.json").read_text())
    for name, expected in manifest["files"].items():
        path = artifacts / name
        if path.parent != artifacts or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise RuntimeError(f"Artifact checksum mismatch: {name}")
    if duckdb.__version__ != manifest["duckdb_version"]:
        raise RuntimeError("DuckDB package differs from the artifact manifest")
    connection = duckdb.connect(
        config={
            "allow_unsigned_extensions": True,
            "autoload_known_extensions": False,
            "autoinstall_known_extensions": False,
        }
    )
    for name in ["httpfs", "aws", "duckdb_object_storage"]:
        # Only image/build-owned, hash-checked extension paths are loadable here.
        connection.execute("LOAD '" + str(artifacts / f"{name}.duckdb_extension").replace("'", "''") + "'")
    connection.execute("SET threads = 2")
    connection.execute("SET memory_limit = '256MB'")
    connection.execute("SET TimeZone = 'UTC'")
    connection.execute("SET duckdb_objfs_memory_cache_size = 67108864")
    connection.execute("SET duckdb_objfs_metadata_cache_size = 16777216")
    connection.execute("SET temp_directory = ?", [args.temp_directory])
    for setting, value in [
        ("duckdb_objfs_backend", "s3"),
        ("duckdb_objfs_bucket", args.bucket),
        ("duckdb_objfs_root", args.prefix),
    ]:
        connection.execute(f"SET {setting} = ?", [value])
    scope = f"s3://{args.bucket}/{args.prefix}"
    # DuckDB materializes role credentials internally; neither keys nor tokens
    # are exported, persisted, selected, or included in evidence.
    connection.execute(
        "CREATE SECRET ec2_role (TYPE S3, PROVIDER credential_chain, CHAIN 'instance', "
        "REGION '" + region() + "', SCOPE '" + scope.replace("'", "''") + "')"
    )
    suffix = " (READ_ONLY)" if args.phase == "read" else ""
    connection.execute("ATTACH 'duckdb_objfs://warehouse.db' AS warehouse" + suffix)
    return connection


def rows(connection):
    return connection.execute(
        "SELECT order_id, code, CAST(amount AS VARCHAR), "
        "strftime(occurred_at, '%Y-%m-%dT%H:%M:%SZ'), enabled, "
        "json_extract_string(metadata, '$.kind'), tags, hex(payload) "
        "FROM warehouse.ods.orders ORDER BY order_id"
    ).fetchall()


def initialize(connection):
    for schema in ["ods", "dwd", "dws", "ads", "ctl"]:
        connection.execute(f"CREATE SCHEMA warehouse.{schema}")
    connection.execute(
        "CREATE TABLE warehouse.ods.orders ("
        "order_id BIGINT PRIMARY KEY, code VARCHAR UNIQUE NOT NULL, "
        "amount DECIMAL(18,2) CHECK (amount >= 0), occurred_at TIMESTAMPTZ NOT NULL, "
        "enabled BOOLEAN, metadata JSON, tags VARCHAR[], payload BLOB)"
    )
    connection.execute(
        "INSERT INTO warehouse.ods.orders VALUES "
        "(1, 'A', 12.30, '2026-01-02 08:00:00+08', true, '{\"kind\":\"one\"}', ['a','b'], from_hex('0011')), "
        "(2, 'B', 21.00, '2026-01-03 00:00:00+00', false, '{\"kind\":\"two\"}', [], from_hex('FF')), "
        "(3, 'C', 9.50, '2026-01-04 00:00:00+00', NULL, NULL, NULL, NULL)"
    )
    connection.execute("CREATE INDEX amount_index ON warehouse.ods.orders(amount)")
    connection.execute("UPDATE warehouse.ods.orders SET amount = 40.25 WHERE order_id = 2")
    connection.execute("DELETE FROM warehouse.ods.orders WHERE order_id = 3")
    connection.execute("BEGIN")
    connection.execute(
        "INSERT INTO warehouse.ods.orders VALUES "
        "(99, 'ROLLBACK', 1.00, '2026-01-01 00:00:00+00', true, '{}', [], '')"
    )
    connection.execute("UPDATE warehouse.ods.orders SET amount = 999 WHERE order_id = 1")
    connection.execute("ROLLBACK")
    failures = {}
    for name, sql in {
        "primary_key": "INSERT INTO warehouse.ods.orders SELECT * FROM warehouse.ods.orders WHERE order_id = 1",
        "unique": "UPDATE warehouse.ods.orders SET code = 'A' WHERE order_id = 2",
        "check": "UPDATE warehouse.ods.orders SET amount = -1 WHERE order_id = 1",
        "not_null": "UPDATE warehouse.ods.orders SET occurred_at = NULL WHERE order_id = 1",
    }.items():
        try:
            connection.execute(sql)
        except duckdb.ConstraintException:
            failures[name] = "REJECTED"
        else:
            raise RuntimeError(f"Constraint was not enforced: {name}")
    connection.execute("CHECKPOINT warehouse")
    return {
        "rows": rows(connection),
        "constraints": failures,
        "indexes": connection.execute(
            "SELECT index_name FROM duckdb_indexes() WHERE database_name = 'warehouse' ORDER BY index_name"
        ).fetchall(),
    }


def crash_writer(connection, args):
    connection.execute(
        "INSERT INTO warehouse.ods.orders VALUES "
        "(4, 'D', 77.77, '2026-01-05 00:00:00+00', true, '{\"kind\":\"four\"}', ['d'], from_hex('DEAD'))"
    )
    # Leave both a committed WAL change and an open, uncommitted transaction.
    connection.execute("BEGIN")
    connection.execute(
        "INSERT INTO warehouse.ods.orders VALUES "
        "(999, 'UNCOMMITTED', 1.00, '2026-01-01 00:00:00+00', true, '{}', [], '')"
    )
    connection.execute("UPDATE warehouse.ods.orders SET amount = 888 WHERE order_id = 1")
    ready = Path(args.ready_file)
    temporary = ready.with_suffix(".tmp")
    temporary.write_text(json.dumps({"pid": os.getpid(), "phase": "committed_then_open_transaction"}) + "\n")
    temporary.replace(ready)
    while True:
        time.sleep(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=["initialize", "crash", "recover", "read"], required=True)
    parser.add_argument("--artifacts", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--temp-directory", required=True)
    parser.add_argument("--ready-file")
    parser.add_argument("--parquet")
    args = parser.parse_args()
    if args.phase == "crash" and not args.ready_file:
        parser.error("crash phase requires --ready-file")
    Path(args.temp_directory).mkdir(parents=True, exist_ok=True)
    connection = connect(args)
    if args.phase == "initialize":
        report = initialize(connection)
    elif args.phase == "crash":
        crash_writer(connection, args)
        return
    else:
        report = {"rows": rows(connection)}
        if args.phase == "recover":
            connection.execute("CHECKPOINT warehouse")
        if args.parquet:
            connection.execute(
                "COPY (SELECT * FROM warehouse.ods.orders ORDER BY order_id) TO '"
                + str(Path(args.parquet).resolve()).replace("'", "''")
                + "' (FORMAT PARQUET)"
            )
        report["io_stats"] = connection.execute("SELECT * FROM duckdb_objfs_io_stats()").fetchall()
        report["cache_stats"] = connection.execute("SELECT * FROM duckdb_objfs_cache_stats()").fetchall()
    report["runtime"] = {
        "duckdb": connection.execute("PRAGMA version").fetchall(),
        "platform": connection.execute("PRAGMA platform").fetchall(),
        "extensions": connection.execute(
            "SELECT extension_name,extension_version FROM duckdb_extensions() WHERE loaded ORDER BY extension_name"
        ).fetchall(),
        "pid": os.getpid(),
    }
    connection.close()
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
