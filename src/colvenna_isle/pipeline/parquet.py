"""Typed Parquet files with explicit limits and independent content checks."""

import hashlib
import io
import re
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from colvenna_isle.contracts.validation import finite_json, validate_schema
from colvenna_isle.control.registry import ControlError

MAX_BATCH_BYTES = 8388608


class _QuotaFile(io.RawIOBase):
    """Reject every write before it can exceed the reserved partial-file budget."""

    def __init__(self, stream, maximum_bytes):
        self.stream, self.maximum_bytes = stream, maximum_bytes

    def writable(self):
        return True

    def tell(self):
        return self.stream.tell()

    def write(self, data):
        if self.tell() + len(data) > self.maximum_bytes:
            raise ControlError(413, "PARQUET_BYTES_QUOTA")
        return self.stream.write(data)

    def flush(self):
        self.stream.flush()


def _value_bytes(value):
    # Conservative UTF-8/list overhead bound before Arrow or JSON conversion.
    if isinstance(value, str):
        return 4 * len(value)
    if isinstance(value, bytes):
        return len(value)
    if isinstance(value, list):
        return 8 * len(value) + sum(_value_bytes(member) for member in value)
    return 64


def arrow_schema(columns):
    import pyarrow as pa

    validate_schema(columns)
    types = {"BIGINT": pa.int64(), "BOOLEAN": pa.bool_(), "VARCHAR": pa.string(),
        "DATE": pa.date32(), "TIMESTAMP": pa.timestamp("us"),
        "TIMESTAMPTZ": pa.timestamp("us", tz="UTC"), "JSON": pa.string(),
        "BLOB": pa.binary(), "VARCHAR[]": pa.list_(pa.string())}
    fields = []
    for column in columns:
        kind = column["type"]
        decimal = re.fullmatch(r"DECIMAL\(([0-9]+),([0-9]+)\)", kind)
        if decimal:
            datatype = pa.decimal128(int(decimal[1]), int(decimal[2]))
        else:
            datatype = types[kind]
        fields.append(pa.field(column["name"], datatype, nullable=column["nullable"]))
    return pa.schema(fields)


def _native_value(column, value):
    """Reject coercions that could change business values before Arrow conversion."""
    import json

    if value is None:
        if not column["nullable"]:
            raise ControlError(422, "PARQUET_NULLABILITY")
        return
    kind = column["type"]
    valid = {
        "BIGINT": lambda: type(value) is int and -(2 ** 63) <= value < 2 ** 63,
        "BOOLEAN": lambda: type(value) is bool,
        "VARCHAR": lambda: type(value) is str,
        "DATE": lambda: type(value) is date,
        "TIMESTAMP": lambda: type(value) is datetime and value.utcoffset() is None,
        "TIMESTAMPTZ": lambda: type(value) is datetime and value.utcoffset() is not None,
        "BLOB": lambda: type(value) is bytes,
        "VARCHAR[]": lambda: type(value) is list and all(type(member) is str for member in value),
        "JSON": lambda: type(value) is str,
    }
    if kind.startswith("DECIMAL("):
        precision, scale = map(int, kind[8:-1].split(","))
        if type(value) is not Decimal or not value.is_finite():
            raise ControlError(422, "PARQUET_VALUE_TYPE")
        # Decimal's default arithmetic context has precision 28. Inspect its
        # exact coefficient instead, including valid Decimal(38,s) values.
        _, digits, exponent = value.as_tuple()
        exponent += scale
        if exponent < 0:
            if any(digits[max(0, len(digits) + exponent):]):
                raise ControlError(422, "PARQUET_DECIMAL_RANGE")
            digits = digits[:max(0, len(digits) + exponent)]
            exponent = 0
        significant = "".join(map(str, digits)).lstrip("0")
        if significant and len(significant) + exponent > precision:
            raise ControlError(422, "PARQUET_DECIMAL_RANGE")
    elif kind not in valid or not valid[kind]():
        raise ControlError(422, "PARQUET_VALUE_TYPE")
    if kind == "JSON":
        try:
            decoded = json.loads(value)
            finite_json(decoded)
        except (ValueError, TypeError):
            raise ControlError(422, "PARQUET_JSON_VALUE") from None


def describe(path, columns, *, maximum_bytes, expected=None, guard=lambda: None):
    """Reopen the file; metadata or a producer manifest cannot prove its contents."""
    import pyarrow.parquet as pq

    guard()
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise ControlError(422, "PARQUET_FILE_SCOPE")
    before = path.stat()
    size = before.st_size
    if not 0 < size <= maximum_bytes:
        raise ControlError(413, "PARQUET_BYTES_QUOTA")
    with path.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    if expected and (expected["bytes"] != size or expected["sha256"] != digest):
        raise ControlError(422, "PARQUET_FILE_HASH")
    schema = arrow_schema(columns)
    try:
        parquet = pq.ParquetFile(path)
        actual = parquet.schema_arrow
        if actual.names != schema.names or any(
                left.type != right.type for left, right in zip(actual, schema)):
            raise ControlError(422, "PARQUET_COLUMN_TYPE")
        rows = 0
        for batch in parquet.iter_batches(batch_size=512):
            guard()
            table = batch.to_pydict()
            for column in columns:
                for value in table[column["name"]]:
                    _native_value(column, value)
            rows += batch.num_rows
        if rows != parquet.metadata.num_rows or expected and rows != expected["rows"]:
            raise ControlError(422, "PARQUET_ROW_COUNT")
    except ControlError:
        raise
    except Exception:
        raise ControlError(422, "PARQUET_INVALID") from None
    guard()
    after = path.stat()
    if path.is_symlink() or any(getattr(after, name) != getattr(before, name)
            for name in ["st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns"]):
        raise ControlError(422, "PARQUET_FILE_CHANGED")
    return {"rows": rows, "bytes": size, "sha256": digest}


def write(path, columns, batches, *, maximum_bytes, maximum_rows, guard=lambda: None):
    """Write one new file; failed or partial files never produce a descriptor."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = Path(path)
    if (path.exists() or path.is_symlink() or type(maximum_rows) is not int or maximum_rows < 0
            or type(maximum_bytes) is not int or maximum_bytes < 1):
        raise ControlError(409, "PARQUET_ORIGINAL_REQUIRED")
    guard()
    schema = arrow_schema(columns)
    rows = 0
    # Exclusive creation also rejects dangling symlinks and a competing writer.
    with path.open("xb") as stream, _QuotaFile(stream, maximum_bytes) as bounded:
        with pq.ParquetWriter(bounded, schema, compression="zstd", version="2.6") as writer:
            for batch in batches:
                guard()
                if len(batch) > 512:
                    raise ControlError(413, "PARQUET_BATCH_QUOTA")
                rows += len(batch)
                if rows > maximum_rows:
                    raise ControlError(413, "PARQUET_ROWS_QUOTA")
                values = {column["name"]: [] for column in columns}
                batch_bytes = 0
                for row in batch:
                    if len(row) != len(columns):
                        raise ControlError(422, "PARQUET_COLUMN_TYPE")
                    for column, value in zip(columns, row):
                        batch_bytes += _value_bytes(value)
                        if batch_bytes > MAX_BATCH_BYTES:
                            raise ControlError(413, "PARQUET_BATCH_BYTES_QUOTA")
                        _native_value(column, value)
                        values[column["name"]].append(value)
                writer.write_table(pa.Table.from_pydict(values, schema=schema))
                stream.flush()
                if stream.tell() > maximum_bytes:
                    raise ControlError(413, "PARQUET_BYTES_QUOTA")
    guard()
    result = describe(path, columns, maximum_bytes=maximum_bytes, guard=guard)
    if result["rows"] != rows:
        raise ControlError(422, "PARQUET_ROW_COUNT")
    return result
