"""Execute SQL against a private, read-only database of approved dataset values.

Only server code loads the snapshot. User SQL never runs on a connection holding
native attachments, control ledgers, credentials, or another principal's data.
"""

import base64
import json
import math
import re
import uuid
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path

from colvenna_isle.contracts.validation import finite_json, parse_type, validate
from colvenna_isle.control.registry import ControlError
from colvenna_isle.writer.ingestion import quoted

SNAPSHOT_MAX_BYTES = 1073741824
RESULT_MAX_BYTES = 8388608
SAFE_INTEGER = 9007199254740991
PARSE_ONLY = {"enable_external_access": False, "autoload_known_extensions": False,
              "autoinstall_known_extensions": False, "allow_unsigned_extensions": False,
              "lock_configuration": True}


def explained(duckdb, query):
    """The statement inside EXPLAIN [ANALYZE] [(options)], located with the engine's lexer."""
    tokens = duckdb.tokenize(query)
    index = 1
    word = re.match(r"[A-Za-z]+", query[tokens[index][0]:])
    if word and word[0].upper() == "ANALYZE":
        index += 1
    if query[tokens[index][0]] == "(":
        depth = 0
        while index < len(tokens):
            position, kind = tokens[index]
            if kind == duckdb.token_type.operator:
                character = query[position]
                depth += (character == "(") - (character == ")")
            index += 1
            if depth == 0:
                break
    return query[tokens[index][0]:]


def references(statements, datasets):
    """Registered datasets that any statement names, found by the engine itself.

    The SQL never runs; a separate connection without external access parses
    it. The parse tree gives every base table (also in joins that name
    extraction cannot bind without columns), and get_table_names adds the
    tables that functions such as query_table bind. Names match as the binder
    resolves them (case-insensitive, any catalog). SQL that does not parse
    names nothing; the sealed session then returns its stable error.
    """
    import duckdb

    names = {}
    for dataset in datasets:
        names.setdefault(dataset.lower(), set()).add(dataset)
    found, pending = set(), [sql for sql in statements if isinstance(sql, str)]
    connection = duckdb.connect(config=PARSE_ONLY)

    def tables(sql):
        nodes = [json.loads(connection.execute("SELECT json_serialize_sql(?)", [sql]).fetchone()[0])]
        while nodes:
            node = nodes.pop()
            if isinstance(node, dict):
                if node.get("type") == "BASE_TABLE" and node.get("schema_name"):
                    found.update(names.get((node["schema_name"] + "." + node["table_name"]).lower(), ()))
                nodes.extend(node.values())
            elif isinstance(node, list):
                nodes.extend(node)

    try:
        while pending:
            try:
                for statement in connection.extract_statements(pending.pop()):
                    if statement.type == duckdb.StatementType.EXPLAIN:
                        pending.append(explained(duckdb, statement.query))
                    else:
                        tables(statement.query)
                    try:
                        bound = connection.get_table_names(statement.query, qualified=True)
                    except duckdb.Error:
                        bound = ()
                    for name in bound:
                        # The engine quoted this name; parsing it again resolves its parts.
                        tables("SELECT * FROM " + name)
            except (duckdb.Error, IndexError, KeyError, TypeError, ValueError, RecursionError):
                continue
    finally:
        connection.close()
    return sorted(found)


def json_value(value):
    """Preserve decimal precision, large integers, timestamps and nested values."""
    if value is None or type(value) in {str, bool}:
        return value
    if type(value) is int:
        return value if abs(value) <= SAFE_INTEGER else str(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ControlError(422, "NONFINITE_DECIMAL")
        return format(value, "f")
    if type(value) is float:
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return value
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc)
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, bytes):
        return base64.b64encode(value).decode()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (list, tuple)):
        return [json_value(member) for member in value]
    if isinstance(value, dict):
        if all(isinstance(key, str) for key in value):
            return {key: json_value(member) for key, member in value.items()}
        return {"entries": [[json_value(key), json_value(member)] for key, member in value.items()]}
    # Unhandled Python adapters must not silently lose native semantics.
    raise ControlError(422, "UNSUPPORTED_QUERY_RESULT_TYPE")


class Session:
    """One query's private copy; nothing is shared between sessions."""

    def __init__(self, directory, *, maximum_bytes=SNAPSHOT_MAX_BYTES):
        import duckdb

        self.duckdb = duckdb
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=False)
        self.path = self.directory / "snapshot.duckdb"
        self.maximum_bytes = maximum_bytes
        self.catalog = []
        self.data_versions = []
        self.unpublished = set()
        self.connection = duckdb.connect(str(self.path), config={
            "autoload_known_extensions": False, "autoinstall_known_extensions": False,
            "allow_unsigned_extensions": False})
        self.connection.execute("SET threads=2")
        self.connection.execute("SET memory_limit='128MB'")
        self.connection.execute("SET max_temp_directory_size='256MB'")
        self.connection.execute("SET TimeZone='UTC'")
        self.connection.execute("SET temp_directory=?", [str(self.directory / "spill")])
        self.sealed = False

    def _admit(self, dataset):
        if self.sealed:
            raise ControlError(409, "QUERY_SNAPSHOT_SEALED")
        if any(value["dataset"] == dataset for value in self.data_versions):
            raise ControlError(409, "DUPLICATE_QUERY_DATASET")

    def _table(self, definition):
        validate("dataset_registration", definition)
        self._admit(definition["dataset"])
        schema, table = definition["dataset"].split(".")
        for column in definition["schema"]:
            parse_type(column["type"])
        c = self.connection
        c.execute("CREATE SCHEMA IF NOT EXISTS " + quoted(schema))
        target = quoted(schema) + "." + quoted(table)
        # The schema and column/type strings passed the registry contract.
        c.execute("CREATE TABLE " + target + " (" + ",".join(
            quoted(column["name"]) + " " + column["type"]
            + ("" if column["nullable"] else " NOT NULL") for column in definition["schema"]) + ")")
        return target

    def _loaded(self, definition, version):
        self.catalog.append({"dataset": definition["dataset"], "schema": definition["schema"]})
        self.data_versions.append(version)
        self.require_capacity()

    def load(self, definition, batches, version):
        """The caller supplies an independently authorized native read stream."""
        import pyarrow as pa
        from colvenna_isle.pipeline.parquet import arrow_schema

        target, columns = self._table(definition), definition["schema"]
        schema = arrow_schema(columns)
        for batch in batches:
            if not batch:
                continue
            if any(len(row) != len(columns) for row in batch):
                raise ControlError(503, "NATIVE_QUERY_SCHEMA_CHANGED")
            # Exact Arrow types preserve Decimal(38,s), BIGINT, timezone and
            # NULL semantics without one native INSERT per row.
            relation = "colvenna_query_rows_" + uuid.uuid4().hex
            table = pa.Table.from_arrays([
                pa.array([row[index] for row in batch], type=field.type)
                for index, field in enumerate(schema)], schema=schema)
            self.connection.register(relation, table)
            try:
                self.connection.execute("INSERT INTO " + target + " SELECT * FROM " + quoted(relation))
            finally:
                self.connection.unregister(relation)
            self.require_capacity()
        self._loaded(definition, version)

    def bulk(self, definition, export, version):
        """export(path) writes one private Parquet file inside the caller's native read transaction.

        Only this server connection reads it, before sealing, and it is removed
        either way. The registered types must arrive exactly; NOT NULL holds.
        """
        target = self._table(definition)
        path = self.directory / "staging" / (uuid.uuid4().hex + ".parquet")
        path.parent.mkdir(exist_ok=True)
        try:
            export(path)
            self.require_capacity()
            source = "read_parquet('" + str(path).replace("'", "''") + "')"
            if self.connection.execute("SELECT column_name,column_type FROM (DESCRIBE SELECT * FROM "
                    + source + ")").fetchall() != [(column["name"], str(self.duckdb.sqltype(column["type"])))
                                                  for column in definition["schema"]]:
                raise ControlError(503, "NATIVE_QUERY_SCHEMA_CHANGED")
            self.connection.execute("INSERT INTO " + target + " SELECT * FROM " + source)
            self.require_capacity()
        finally:
            path.unlink(missing_ok=True)
        self._loaded(definition, version)

    def observe(self, version):
        """A version the request binds (receipt or exact release) whose rows its SQL never names."""
        self._admit(version["dataset"])
        self.data_versions.append(version)

    def require_capacity(self):
        # Includes DB, WAL and spill. The process supervisor also bounds the
        # working directory during native staging/execution, before publication.
        total = sum(path.stat().st_size for path in self.directory.rglob("*") if path.is_file())
        if total > self.maximum_bytes:
            raise ControlError(413, "QUERY_SNAPSHOT_TOO_LARGE")

    def publications_only(self, registered_datasets):
        """Record authorized datasets not staged as a publication; never substitute their current rows."""
        self.unpublished = set(registered_datasets) - {value["dataset"] for value in self.catalog}

    def seal(self):
        if self.sealed:
            return
        self.connection.execute("CHECKPOINT")
        self.require_capacity()
        self.connection.close()
        self.connection = self.duckdb.connect(str(self.path), read_only=True, config={
            "autoload_known_extensions": False, "autoinstall_known_extensions": False,
            "allow_unsigned_extensions": False})
        c = self.connection
        c.execute("SET threads=2")
        c.execute("SET memory_limit='128MB'")
        c.execute("SET max_temp_directory_size='256MB'")
        c.execute("SET temp_directory=?", [str(self.directory / "spill")])
        c.execute("SET TimeZone='UTC'")
        c.execute("SET enable_external_access=false")
        c.execute("SET lock_configuration=true")
        self.sealed = True

    def execute(self, sql, max_rows):
        if not self.sealed:
            raise ControlError(503, "QUERY_SNAPSHOT_NOT_SEALED")
        if (not isinstance(sql, str) or not sql.strip() or len(sql.encode()) > 65536
                or type(max_rows) is not int or not 1 <= max_rows <= 10000):
            raise ControlError(400, "INVALID_QUERY")
        view = None
        try:
            statements = self.connection.extract_statements(sql)
            if len(statements) != 1 or statements[0].type not in {
                    self.duckdb.StatementType.SELECT, self.duckdb.StatementType.EXPLAIN}:
                raise ControlError(403, "READ_ONLY_SQL_REQUIRED")
            # The engine rewrites PRAGMA into a SELECT; the contract admits only SELECT/WITH/EXPLAIN text.
            tokens = self.duckdb.tokenize(sql)
            first = re.match(r"[A-Za-z]+", sql[tokens[0][0]:]) if tokens else None
            if first and first[0].upper() == "PRAGMA":
                raise ControlError(403, "READ_ONLY_SQL_REQUIRED")
            if self.unpublished and references([sql], self.unpublished):
                raise ControlError(503, "PUBLICATION_NOT_AVAILABLE")
            statement = statements[0]
            if statement.type == self.duckdb.StatementType.EXPLAIN:
                # Use the engine's lexer (including nested/comment handling) and
                # parser to verify the explained statement as well. READ_ONLY
                # alone permits some scratch mutations such as temporary DDL.
                inner = self.connection.extract_statements(explained(self.duckdb, statement.query))
                if len(inner) != 1 or inner[0].type != self.duckdb.StatementType.SELECT:
                    raise ControlError(403, "READ_ONLY_SQL_REQUIRED")
                result = self.connection.execute(statement)
                columns = [{"name": column[0], "type": str(column[1]), "encoding": "native_json"}
                           for column in result.description]
            else:
                # Bind without executing. Give each output position a private,
                # unique name so duplicate user aliases preserve distinct values.
                relation = self.connection.sql(statement)
                columns = [{"name": name, "type": str(kind), "encoding": "native_json"}
                           for name, kind in zip(relation.columns, relation.types)]
                view = "__colvenna_result_" + uuid.uuid4().hex
                names = ["__column_" + str(index) for index in range(len(columns))]
                self.connection.execute("CREATE TEMP VIEW " + quoted(view) + "(" +
                    ",".join(quoted(name) for name in names) + ") AS " + statement.query)
                projections = []
                for name, column in zip(names, columns):
                    kind = column["type"]
                    if ("INTERVAL" in kind or "TIMESTAMP_NS" in kind or "TIME WITH TIME ZONE" in kind
                            or kind == "BIT"):
                        # Preserve native interval months, nanoseconds and
                        # timezone information instead of lossy Python adapters.
                        projections.append("CAST(" + quoted(name) + " AS VARCHAR)")
                        column["encoding"] = "native_text"
                    else:
                        projections.append(quoted(name))
                result = self.connection.execute("SELECT " + ",".join(projections) + " FROM " + quoted(view))
            rows = result.fetchmany(max_rows + 1)
            value = {"columns": columns, "rows": [[json_value(item) for item in row]
                for row in rows[:max_rows]], "data_versions": self.data_versions,
                "truncated": len(rows) > max_rows}
            finite_json(value)
            validate("query_result", value)
            if len(json.dumps(value, separators=(",", ":"), allow_nan=False).encode()) > RESULT_MAX_BYTES:
                raise ControlError(413, "QUERY_RESULT_TOO_LARGE")
            return value
        except self.duckdb.PermissionException:
            raise ControlError(403, "EXTERNAL_ACCESS_FORBIDDEN") from None
        except self.duckdb.OutOfMemoryException:
            raise ControlError(429, "QUERY_MEMORY_LIMIT") from None
        except (self.duckdb.ParserException, self.duckdb.BinderException, self.duckdb.CatalogException):
            # Native diagnostics can contain SQL fragments. Return a stable
            # code; only the owner sees their retained original SQL.
            raise ControlError(400, "INVALID_OR_UNAUTHORIZED_SQL") from None
        except self.duckdb.Error:
            raise ControlError(422, "QUERY_EXECUTION_FAILED") from None
        finally:
            if view is not None:
                self.connection.execute("DROP VIEW IF EXISTS " + quoted(view))

    def close(self):
        self.connection.close()
