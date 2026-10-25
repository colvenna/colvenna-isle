"""Independent checks of untrusted task manifests and complete Parquet outputs."""

import hashlib
import json
import sqlite3
import tempfile
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from colvenna_isle.contracts.validation import load_document, validate, validate_value
from colvenna_isle.control.registry import ControlError, canonical
from .parquet import describe

MAX_MANIFEST_BYTES = 1048576


def key_value(value):
    """Lossless native key encoding; Decimal never uses arithmetic precision."""
    if isinstance(value, Decimal):
        return {"decimal": format(value, "f")}
    if isinstance(value, (date, datetime)):
        return {"temporal": value.isoformat()}
    if isinstance(value, bytes):
        return {"binary": value.hex()}
    if isinstance(value, list):
        return [key_value(member) for member in value]
    return value


class OutputValidator:
    def __init__(self, *, maximum_bytes, maximum_rows, maximum_files=10000):
        if any(type(value) is not int or value < 1
               for value in [maximum_bytes, maximum_rows, maximum_files]):
            raise ValueError("Positive output budgets required")
        self.maximum_bytes, self.maximum_rows, self.maximum_files = maximum_bytes, maximum_rows, maximum_files

    def verify(self, context, *, workspace, guard=lambda: None):
        """Trust the in-memory admitted context, never a script-edited context file."""
        validate("context", context)
        guard()
        workspace = Path(workspace).resolve(strict=True)
        output = context["output"]
        directory = Path(output["directory"])
        if directory.is_symlink() or not directory.is_dir():
            raise ControlError(422, "OUTPUT_DIRECTORY_SCOPE")
        directory = directory.resolve(strict=True)
        if workspace not in directory.parents:
            raise ControlError(422, "OUTPUT_DIRECTORY_SCOPE")
        for parent in Path(output["directory"]).parents:
            if parent.is_symlink():
                raise ControlError(422, "OUTPUT_DIRECTORY_SCOPE")
        manifest_path = directory / "output-manifest.json"
        if (manifest_path.is_symlink() or not manifest_path.is_file()
                or manifest_path.stat().st_nlink != 1
                or not 0 < manifest_path.stat().st_size <= MAX_MANIFEST_BYTES):
            raise ControlError(422, "OUTPUT_MANIFEST_INCOMPLETE")
        before = manifest_path.stat()
        raw = manifest_path.read_bytes()
        manifest = load_document(manifest_path)
        validate("output_manifest", manifest, context=context)
        if (len(manifest["files"]) > self.maximum_files
                or manifest["row_count"] > self.maximum_rows
                or len(raw) + sum(file["bytes"] for file in manifest["files"]) > self.maximum_bytes):
            raise ControlError(413, "OUTPUT_BUDGET_EXCEEDED")
        declared = {file["path"] for file in manifest["files"]} | {"output-manifest.json"}
        self._complete(directory, declared)
        columns = {column["name"]: column for column in output["schema"]}
        # Global key uniqueness cannot be proved one Parquet part at a time.
        # The temporary index shares the execution's independently guarded disk.
        with tempfile.TemporaryDirectory(prefix="validation-", dir=workspace) as temporary:
            with sqlite3.connect(Path(temporary) / "keys.sqlite") as index:
                index.execute("PRAGMA temp_store=FILE")
                index.execute("PRAGMA cache_size=-256")
                index.execute("CREATE TABLE output_keys (identity BLOB PRIMARY KEY) WITHOUT ROWID")
                actual_rows = 0
                for file in manifest["files"]:
                    path = directory / file["path"]
                    self._file_scope(directory, path)
                    before_file = path.stat()
                    describe(path, output["schema"], maximum_bytes=self.maximum_bytes,
                             expected=file, guard=guard)
                    actual_rows += self._rows(path, file, output, columns, index, guard)
                    after_file = path.stat()
                    if (path.is_symlink() or any(
                            getattr(before_file, name) != getattr(after_file, name)
                            for name in ["st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns"])):
                        raise ControlError(422, "OUTPUT_FILE_CHANGED")
                    guard()
                if actual_rows != manifest["row_count"]:
                    raise ControlError(422, "OUTPUT_ROW_COUNT")
        self._complete(directory, declared)
        after = manifest_path.stat()
        if (manifest_path.is_symlink() or manifest_path.read_bytes() != raw
                or any(getattr(before, name) != getattr(after, name)
                       for name in ["st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns"])):
            raise ControlError(422, "OUTPUT_MANIFEST_CHANGED")
        guard()
        return {"manifest": manifest, "manifest_sha256": hashlib.sha256(raw).hexdigest(),
                "manifest_bytes": len(raw), "row_count": actual_rows,
                "output_bytes": sum(file["bytes"] for file in manifest["files"])}

    def _file_scope(self, directory, path):
        if (path.is_symlink() or not path.is_file() or path.stat().st_nlink != 1
                or directory not in path.resolve(strict=True).parents):
            raise ControlError(422, "OUTPUT_FILE_SCOPE")
        for parent in path.parents:
            if parent == directory:
                break
            if parent.is_symlink():
                raise ControlError(422, "OUTPUT_FILE_SCOPE")

    def _complete(self, directory, declared):
        actual = set()
        for path in directory.rglob("*"):
            if path.is_symlink():
                raise ControlError(422, "OUTPUT_FILE_SCOPE")
            if not path.is_file() and not path.is_dir():
                raise ControlError(422, "OUTPUT_FILE_SCOPE")
            if path.is_file():
                actual.add(str(path.relative_to(directory)))
                if len(actual) > self.maximum_files + 1:
                    raise ControlError(413, "OUTPUT_BUDGET_EXCEEDED")
        if actual != declared:
            raise ControlError(422, "OUTPUT_FILES_INCOMPLETE")

    def _rows(self, path, file, output, columns, index, guard):
        import pyarrow.parquet as pq

        before = path.stat()
        rows = 0
        try:
            parquet = pq.ParquetFile(path)
            expected_partition = {name: validate_value(columns[name]["type"], value, False)
                                  for name, value in file["partition_values"].items()}
            for batch in parquet.iter_batches(batch_size=512):
                guard()
                for row in batch.to_pylist():
                    rows += 1
                    if output["write_mode"] == "merge_keys":
                        if (row["_colvenna_op"] not in {"UPSERT", "DELETE"}
                                or row["_colvenna_version"] < 0 or row["_colvenna_sequence"] < 0):
                            raise ControlError(422, "OUTPUT_CHANGE_METADATA")
                        if row["_colvenna_op"] == "UPSERT" and any(
                                row[column["name"]] is None and not column["nullable"]
                                for column in output["target_schema"]):
                            raise ControlError(422, "OUTPUT_UPSERT_NULLABILITY")
                    if any(row[name] != value for name, value in expected_partition.items()):
                        raise ControlError(422, "OUTPUT_PARTITION_VALUE")
                    identity = canonical([key_value(row[name]) for name in output["key_columns"]]).encode()
                    try:
                        index.execute("INSERT INTO output_keys VALUES (?)", [identity])
                    except sqlite3.IntegrityError:
                        raise ControlError(422, "OUTPUT_DUPLICATE_KEY") from None
                index.commit()
                guard()
            after = path.stat()
            if (rows != file["rows"] or path.is_symlink()
                    or any(getattr(before, name) != getattr(after, name)
                           for name in ["st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns"])):
                raise ControlError(422, "OUTPUT_FILE_CHANGED")
            return rows
        except ControlError:
            raise
        except (OSError, ValueError, KeyError, TypeError):
            raise ControlError(422, "OUTPUT_PARQUET_INVALID") from None
