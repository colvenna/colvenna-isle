"""Prepare business-data replay from verified historical native backup Parquet.

This offline tool never opens a database or sends a request. The source system
prefix is explicit: it is not an alias accepted by the current services.
"""

import argparse
import hashlib
import json
import os
import re
from pathlib import Path

from ._version import __version__
from .contracts.validation import ContractError, load_document, require, validate, validate_record
from .control.registry import canonical


def native_type(kind):
    return {"TIMESTAMPTZ": "TIMESTAMP WITH TIME ZONE"}.get(kind, kind)


def backup_page(definition, manifest_path, manifest_sha256, parts_directory, source_system_prefix):
    """Read exact verified backup parts without executing any backup DDL."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    require(re.fullmatch(r"[0-9a-f]{64}", manifest_sha256) is not None,
            "Native manifest SHA-256 required from the historical backup receipt")
    with open(manifest_path, "rb") as stream:
        require(hashlib.file_digest(stream, "sha256").hexdigest() == manifest_sha256,
                "Native manifest checksum mismatch")
    manifest = load_document(manifest_path)
    require(manifest.get("format_version") == 1 and
            manifest.get("kind") == source_system_prefix.strip("_") + "_native_backup",
            "Historical native backup manifest required")
    matches = [table for table in manifest["tables"]
               if table["schema"] + "." + table["table"] == definition["dataset"]]
    require(len(matches) == 1, "Exactly one registered ODS table required")
    table = matches[0]
    columns = [{"name": name, "type": kind, "encoding": "native_json"}
               for name, kind, text_cast in table["columns"]]
    require(all(not text_cast for _, _, text_cast in table["columns"]),
            "Text-cast backup columns are outside the ingestion type contract")
    parts = []
    directory = Path(parts_directory).resolve()
    for reference in table["files"]:
        sha = reference["sha256"]
        require(re.fullmatch(r"[0-9a-f]{64}", sha) is not None, "Backup part checksum required")
        path = (directory / (sha + ".parquet")).resolve()
        require(path.parent == directory, "Backup parts must stay inside the private directory")
        require(path.stat().st_size == reference["bytes"], "Backup part size mismatch")
        with path.open("rb") as stream:
            require(hashlib.file_digest(stream, "sha256").hexdigest() == sha, "Backup part checksum mismatch")
        part = pq.read_table(path)
        require(part.column_names == [column["name"] for column in columns], "Backup column order mismatch")
        parts.append(part)
    require(bool(parts) or table["digest"]["rows"] == 0, "Backup parts are missing")
    if not parts:
        return {"columns": columns, "rows": [], "row_count": 0, "truncated": False, "next_page_token": None}
    data = pa.concat_tables(parts)
    require(data.num_rows == table["digest"]["rows"], "Backup row count mismatch")
    # COPY parts have no guaranteed physical order; replay the durable commit order.
    order = pc.sort_indices(data, sort_keys=[(source_system_prefix + "commit_sequence", "ascending"),
                                            (definition["ingestion"]["event_id_column"], "ascending")])
    from .writer.ingestion import wire

    kinds = {column["name"]: column["type"] for column in definition["schema"]}
    rows = []
    for record in data.take(order).to_pylist():
        for name, kind in kinds.items():
            value = record[name]
            if kind == "JSON" and value is not None:
                try:
                    value = json.loads(value)
                except (TypeError, json.JSONDecodeError):
                    raise ContractError("Invalid backed-up JSON value") from None
                require(value is not None, "A JSON null literal cannot be replayed as SQL NULL")
            record[name] = wire(value)
        rows.append([wire(record[column["name"]]) for column in columns])
    return {"columns": columns, "rows": rows, "row_count": len(rows),
            "truncated": False, "next_page_token": None}


def prepare(definition, page, source_system_prefix):
    validate("dataset_registration", definition)
    require("ingestion" in definition, "Replay requires an ingestion dataset")
    require(re.fullmatch(r"_[a-z][a-z0-9]*_", source_system_prefix) is not None,
            "Supply the historical system-column prefix")
    require(page.get("truncated") is False and page.get("next_page_token") is None,
            "Export must contain every row of one complete query page")
    columns, rows = page.get("columns"), page.get("rows")
    require(isinstance(columns, list) and isinstance(rows, list), "Export columns and rows required")
    require(page.get("row_count") == len(rows), "Export row count mismatch")
    names = [column.get("name") for column in columns]
    require(all(isinstance(name, str) for name in names) and len(set(names)) == len(names),
            "Export column names must be unique")
    business = [column["name"] for column in definition["schema"]]
    metadata = [source_system_prefix + suffix for suffix in [
        "ingestion_id", "commit_sequence", "operation", "disposition", "received_at"]]
    require(set(names) == set(business + metadata), "Export must include exact business and system columns")
    kinds = {column["name"]: column["type"] for column in definition["schema"]}
    kinds.update(zip(metadata, ["VARCHAR", "BIGINT", "VARCHAR", "VARCHAR", "TIMESTAMPTZ"]))
    require(all(native_type(column.get("type")) == native_type(kinds[column["name"]])
                and column.get("encoding") == "native_json" for column in columns),
            "Export requires exact typed native JSON values")
    source_sha = hashlib.sha256(canonical(page).encode()).hexdigest()
    result = {"format_version": 1, "product_version": __version__, "dataset": definition["dataset"],
              "source_sha256": source_sha, "rows": len(rows), "batches": []}
    previous = 0
    policy = definition["ingestion"]
    required = definition["key_columns"] + [policy["event_id_column"]]
    if policy.get("source_version_column"):
        required.append(policy["source_version_column"])
    for values in rows:
        require(isinstance(values, list) and len(values) == len(names), "Export row width mismatch")
        row = dict(zip(names, values))
        sequence = row[metadata[1]]
        # Native JSON encodes BIGINT exactly as a decimal string.
        require(type(sequence) in {str, int} and re.fullmatch(r"[1-9][0-9]*", str(sequence)) is not None,
                "Export commit sequence required")
        sequence = int(sequence)
        require(sequence >= previous, "Export must be ordered by commit sequence")
        previous = sequence
        operation = row[metadata[2]]
        require(operation in policy["operations"], "Export operation is not registered")
        record = {name: row[name] for name in business}
        validate_record(definition["schema"], record, operation, required)
        body = {"dataset": definition["dataset"].split(".", 1)[1],
                "schema_version": definition["schema_version"], "operation": operation, "records": [record]}
        validate("ingestion_request", body)
        require(len(canonical(body).encode()) <= 1048576, "One event exceeds the API body limit")
        # A separate bounded batch per event preserves replay order and tombstones.
        # Stable keys let the operator reconcile a lost reply without a new write.
        key = "migration_" + hashlib.sha256(canonical([source_sha, len(result["batches"]), body]).encode()).hexdigest()
        result["batches"].append({"idempotency_key": key, "body": body})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--definition", type=Path, required=True)
    parser.add_argument("--native-manifest", type=Path, required=True)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--parts-directory", type=Path, required=True,
                        help="exact-version downloads named <sha256>.parquet")
    parser.add_argument("--source-system-prefix", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        definition = load_document(args.definition)
        validate("dataset_registration", definition)
        require("ingestion" in definition, "Replay requires an ingestion dataset")
        page = backup_page(definition, args.native_manifest, args.manifest_sha256,
                           args.parts_directory, args.source_system_prefix)
        result = prepare(definition, page, args.source_system_prefix)
        result["native_manifest_sha256"] = args.manifest_sha256
        # Customer data stays in a new private local file.
        with open(args.output, "x", opener=lambda path, flags: os.open(path, flags, 0o600)) as stream:
            stream.write(canonical(result) + "\n")
        print(json.dumps({"status": "PASS", "scope": "OFFLINE_REPLAY_PLAN",
                          "rows": result["rows"], "batches": len(result["batches"])}))
    except (ContractError, FileExistsError):
        parser.exit(2, "Migration input rejected or output already exists\n")


if __name__ == "__main__":
    main()
