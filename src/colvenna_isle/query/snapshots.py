"""Bounded typed wire snapshots. User SQL never reaches the Native source."""

import base64
import copy
import hashlib
import json
import re
from datetime import date, datetime
from decimal import Decimal

from colvenna_isle.control.registry import ControlError, canonical
from colvenna_isle.pipeline.parquet import _native_value, _value_bytes
from .native import stage
from .sql import json_value
from colvenna_isle.writer.ingestion import quoted

MAX_BYTES, MAX_ROWS = 2097152, 100000


def capture(connection, configuration, actor, condition, guard, datasets):
    """Copy only the datasets the original SQL names; the receipt's dataset is always versioned."""
    actor.authorize(configuration["space_id"], "query:read")
    if actor.generation != configuration["generation"]:
        raise ControlError(403, "SPACE_GENERATION_CHANGED")
    mode = condition.get("consistency")
    kind = {"after_write": "write_id", "after_ingestion": "ingestion_id"}.get(mode)
    if (kind is None or set(condition) != {"consistency", kind}
            or not re.fullmatch(("write_" if kind == "write_id" else "ing_") + "[0-9a-f]{64}",
                                condition.get(kind, ""))):
        raise ControlError(400, "INVALID_QUERY_CONSISTENCY")
    after = {"kind": kind, "identity": condition[kind], "principal_id": actor.principal_id,
             "admin": ("datasets:manage" if kind == "write_id" else "monitor:read") in actor.scopes}
    target = _Target(configuration, guard)
    stage(connection, configuration, target, guard, after=after, maximum_rows=MAX_ROWS, batch_rows=16,
          datasets=datasets)
    return target.result()


class _Target:
    def __init__(self, configuration, guard):
        self.value = {"format_version": 2, "space_id": configuration["space_id"],
                      "generation": configuration["generation"], "tables": []}
        self.guard, self.rows, self.bytes, self.sealed = guard, 0, 0, False

    def preflight(self, connection, copies):
        """Bound the wire JSON in the same read transaction before fetching rows.

        Native JSON escapes ASCII controls; Python additionally escapes Unicode.
        Expand BMP/non-BMP characters to six/twelve ASCII bytes. Numeric
        BIGINTs and timestamps have small conservative formatting allowances.
        """
        estimated = len(json.dumps(self.value, ensure_ascii=True).encode())
        for definition, version, read in copies:
            self.guard()
            table = ({"version": version} if definition is None else
                     {"definition": definition, "version": version, "rows": []})
            estimated += len(json.dumps(table, ensure_ascii=True).encode()) + 2
            if read is not None:
                sizes = []
                for column in definition["schema"]:
                    name, kind = quoted(column["name"]), column["type"]
                    if kind == "BLOB":
                        value = "to_base64(" + name + ")"
                    elif kind in {"BOOLEAN", "VARCHAR[]"}:
                        value = name
                    else:
                        value = "CAST(" + name + " AS VARCHAR)"
                    wire = "coalesce(CAST(to_json(" + value + ") AS VARCHAR),'null')"
                    size = ("length(regexp_replace(" + wire
                            + ", '[^\\x00-\\x7E]', 'xxxxxx', 'g'))"
                            + " + 6 * length(regexp_replace(" + wire
                            + ", '[\\x00-\\x{FFFF}]', '', 'g'))")
                    if kind == "VARCHAR[]":
                        # Python's default JSON includes spaces between elements.
                        size += " + coalesce(array_length(" + name + "),0)"
                    elif kind in {"TIMESTAMP", "TIMESTAMPTZ"}:
                        size += " + 6"
                    sizes.append(size)
                # Brackets, cell separators and the next row separator.
                expression = " + ".join(sizes + [str(2 * len(sizes) + 2)])
                estimated += connection.execute("SELECT coalesce(sum(" + expression
                    + "),0) FROM (" + read[0] + ") AS colvenna_wire_source", read[2]).fetchone()[0]
            if estimated > MAX_BYTES:
                raise ControlError(413, "QUERY_SOURCE_QUOTA")
        self.guard()

    def observe(self, version):
        # A version-only table: the receipt's dataset when the SQL names none of its rows.
        table = {"version": version}
        self.bytes += len(json.dumps(table, ensure_ascii=True).encode())
        self.value["tables"].append(table)

    def load(self, definition, batches, version):
        columns = definition["schema"]
        table = {"definition": definition, "version": version, "rows": []}
        self.bytes += len(json.dumps(table, ensure_ascii=True).encode())
        for batch in batches:
            self.guard()
            for row in batch:
                if self.rows >= MAX_ROWS or sum(_value_bytes(value) * 2 for value in row) > MAX_BYTES:
                    raise ControlError(413, "QUERY_SOURCE_QUOTA")
                for column, value in zip(columns, row):
                    _native_value(column, value)
                wire = [json_value(value) for value in row]
                self.bytes += len(json.dumps(wire, ensure_ascii=True).encode()) + 2
                if self.bytes > MAX_BYTES:
                    raise ControlError(413, "QUERY_SOURCE_QUOTA")
                table["rows"].append(wire)
                self.rows += 1
        self.value["tables"].append(table)

    def seal(self):
        self.sealed = True

    def result(self):
        self.guard()
        if not self.sealed:
            raise ControlError(503, "QUERY_SOURCE_UNCONFIRMED")
        if len(json.dumps(self.value, ensure_ascii=True).encode()) > MAX_BYTES:
            raise ControlError(413, "QUERY_SOURCE_QUOTA")
        raw = canonical(self.value).encode()
        return {"snapshot": self.value, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}


def _decoded(column, value):
    if value is not None:
        kind = column["type"]
        if kind == "BIGINT" and type(value) is str and re.fullmatch("-?[0-9]+", value):
            value = int(value)
        elif kind.startswith("DECIMAL(") and type(value) is str:
            value = Decimal(value)
        elif kind == "DATE" and type(value) is str:
            value = date.fromisoformat(value)
        elif kind in {"TIMESTAMP", "TIMESTAMPTZ"} and type(value) is str:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        elif kind == "BLOB" and type(value) is str:
            value = base64.b64decode(value, validate=True)
    _native_value(column, value)
    return value


def load(value, configuration, target, guard, datasets):
    """Verify fixed payload before loading one private, sealed query session.

    Rows arrive only for datasets the SQL names; a version-only table binds the
    receipt's dataset otherwise.
    """
    guard()
    try:
        payload = value["snapshot"]
        raw = canonical(payload).encode()
        if (len(raw) != value["bytes"] or hashlib.sha256(raw).hexdigest() != value["sha256"]
                or len(json.dumps(payload, ensure_ascii=True).encode()) > MAX_BYTES
                or payload["format_version"] != 2 or payload["space_id"] != configuration["space_id"]
                or payload["generation"] != configuration["generation"]
                or not isinstance(payload["tables"], list)
                or len(payload["tables"]) > len(configuration["datasets"])):
            raise ValueError("Snapshot identity differs")
        rows, sequence = 0, None
        for table in payload["tables"]:
            guard()
            version, observed = table["version"], set(table) == {"version"}
            definition = (configuration["datasets"][version["dataset"]]["definition"] if observed
                          else table["definition"])
            if not observed:
                if definition["dataset"] not in datasets:
                    raise ValueError("Snapshot dataset differs")
                expected = copy.deepcopy(configuration["datasets"][definition["dataset"]]["definition"])
                # Native primary keys can strengthen nullability, while sparse ODS
                # deletes relax business-column input requirements. The source proof
                # binds this actual query schema, including each boolean.
                for column, actual in zip(expected["schema"], definition["schema"]):
                    if type(actual["nullable"]) is not bool:
                        raise ValueError("Snapshot nullability differs")
                    column["nullable"] = actual["nullable"]
                if definition != expected:
                    raise ValueError("Snapshot schema differs")
            if (version["dataset"] != definition["dataset"]
                    or version["generation"] != configuration["generation"]
                    or type(version["commit_sequence"]) is not int
                    or type(version["snapshot_sequence"]) is not int
                    or not 0 <= version["commit_sequence"] <= version["snapshot_sequence"]
                    or sequence is not None and sequence != version["snapshot_sequence"]):
                raise ValueError("Snapshot vector differs")
            sequence = version["snapshot_sequence"]
            if observed:
                target.observe(version)
                continue
            columns = definition["schema"]
            decoded = []
            for row in table["rows"]:
                if not isinstance(row, list) or len(row) != len(columns):
                    raise ValueError("Snapshot row differs")
                rows += 1
                if rows > MAX_ROWS:
                    raise ValueError("Snapshot row quota differs")
                decoded.append([_decoded(column, cell) for column, cell in zip(columns, row)])
            target.load(definition, (decoded[i:i + 512] for i in range(0, len(decoded), 512)), version)
        guard()
        target.seal()
    except ControlError:
        raise
    except Exception:
        raise ControlError(503, "QUERY_SOURCE_UNCONFIRMED") from None
