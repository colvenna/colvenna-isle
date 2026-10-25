"""Direct-table mutations and their authoritative results in one native database."""

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from colvenna_isle.contracts.validation import validate, validate_value
from colvenna_isle.control.registry import ControlError, canonical
from colvenna_isle.timing import NOT_TIMED
from .ingestion import digest, quoted, wire
from .native_schema import physical


def write_id(space, generation, dataset, idempotency_key):
    # Principal is a binding, not a namespace: another principal cannot reuse
    # somebody else's key to create a second business operation.
    return "write_" + digest([space, generation, dataset, idempotency_key])


def table_name(dataset):
    schema, table = dataset.split(".")
    return "warehouse." + quoted(schema) + "." + quoted(table)


def typed_rows(request, definition):
    columns = {column["name"]: column for column in definition["schema"]}

    def convert(record):
        result = {}
        for name, value in record.items():
            column = columns[name]
            value = validate_value(column["type"], value, column["nullable"])
            result[name] = canonical(value) if column["type"] == "JSON" and value is not None else value
        return result

    operation = request["operation"]
    if operation in {"insert", "upsert"}:
        return [convert(record) for record in request["records"]]
    if operation == "delete":
        return [convert(key) for key in request["keys"]]
    return [{"key": convert(item["key"]), "set": convert(item["set"])} for item in request["updates"]]


def unchanged(stored, value):
    """Exact for every registered column type (none is floating point): the same type and an equal value."""
    return type(stored) is type(value) and stored == value


def business_binding(request, registered):
    definition = registered["definition"]
    validate("write_request", request, definition=definition)
    rows = typed_rows(request, definition)

    def portable(value):
        if isinstance(value, dict):
            return {name: portable(member) for name, member in value.items()}
        if isinstance(value, list):
            return [portable(member) for member in value]
        return wire(value)

    return digest([definition["dataset"], definition["schema_version"],
                   registered["definition_sha256"], request["operation"], portable(rows)])


def literal(value):
    """CHECK literals come from the typed registration contract, never SQL text."""
    if type(value) is bool:
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, Decimal)):
        return str(value)
    return "'" + wire(value).replace("'", "''") + "'"


class WriteStore:
    RETRY_DAYS = 30

    def __init__(self, ingestions, clock=lambda: datetime.now(timezone.utc)):
        self.native, self.clock = ingestions, clock
        self.ready = False

    @property
    def connection(self):
        return self.native.connection

    @property
    def configuration(self):
        return self.native.configuration

    def initialize(self):
        self.native.initialize()
        # DuckDB resolves FK references in the connection's current catalog and
        # forbids catalog-qualified REFERENCES, even for the target catalog.
        self.connection.execute("USE warehouse")
        if self.ready:
            return

        def create():
            definitions = {
                "writes": "(write_id VARCHAR PRIMARY KEY, principal_id VARCHAR NOT NULL, "
                          "binding_sha256 VARCHAR NOT NULL, dataset VARCHAR NOT NULL, "
                          "schema_version BIGINT NOT NULL, request VARCHAR NOT NULL, "
                          "registered_at TIMESTAMPTZ NOT NULL, retry_until TIMESTAMPTZ NOT NULL, "
                          "receipt VARCHAR)",
                "write_changes": "(write_id VARCHAR, item_number INTEGER, dataset VARCHAR NOT NULL, "
                                 "operation VARCHAR NOT NULL, commit_sequence BIGINT NOT NULL, "
                                 "key_values VARCHAR, before_partition VARCHAR, after_partition VARCHAR, "
                                 "PRIMARY KEY(write_id,item_number))",
                "dataset_watermarks": "(dataset VARCHAR PRIMARY KEY, commit_sequence BIGINT NOT NULL)",
                "direct_table_definitions": "(dataset VARCHAR PRIMARY KEY, definition_sha256 VARCHAR NOT NULL, "
                                            "physical_sha256 VARCHAR NOT NULL)",
            }
            for name, definition in definitions.items():
                self.connection.execute("CREATE TABLE IF NOT EXISTS warehouse.ctl." + name + " " + definition)
        self.native.transaction(create)
        self.ready = True

    def physical(self, dataset):
        return physical(self.connection, dataset)

    def ensure_table(self, registered):
        """Called within a guarded native transaction; do not adopt unknown tables."""
        definition = registered["definition"]
        validate("dataset_registration", definition)
        if definition.get("write_owner") != "direct":
            raise ControlError(403, "DIRECT_WRITE_TARGET_REQUIRED")
        dataset = definition["dataset"]
        prior = self.connection.execute(
            "SELECT definition_sha256,physical_sha256 FROM warehouse.ctl.direct_table_definitions "
            "WHERE dataset=?", [dataset]).fetchone()
        if prior:
            if (prior[0] != registered["definition_sha256"]
                    or prior[1] != digest(self.physical(dataset))):
                raise ControlError(409, "NATIVE_SCHEMA_IDENTITY_MISMATCH")
            return
        constraints = definition.get("constraints", {})
        for foreign in constraints.get("foreign_keys", []):
            parent = self.configuration["datasets"].get(foreign["references"])
            if not parent or parent["definition"].get("write_owner") != "direct":
                raise ControlError(409, "FOREIGN_KEY_TARGET_UNREGISTERED")
            self.ensure_table(parent)
        columns = {column["name"]: column for column in definition["schema"]}
        clauses = [
            quoted(column["name"]) + " " + column["type"] + ("" if column["nullable"] else " NOT NULL")
            for column in definition["schema"]
        ]

        def identifiers(names):
            return ",".join(quoted(name) for name in names)

        if constraints.get("primary_key"):
            clauses.append("PRIMARY KEY (" + identifiers(constraints["primary_key"]) + ")")
        for names in constraints.get("unique", []):
            clauses.append("UNIQUE (" + identifiers(names) + ")")
        for foreign in constraints.get("foreign_keys", []):
            parent_schema, parent_table = foreign["references"].split(".")
            clauses.append("FOREIGN KEY (" + identifiers(foreign["columns"]) + ") REFERENCES "
                           + quoted(parent_schema) + "." + quoted(parent_table) + " ("
                           + identifiers(foreign["referenced_columns"]) + ")")
        for check in constraints.get("checks", []):
            value = validate_value(columns[check["column"]]["type"], check["value"], False)
            clauses.append("CHECK (" + quoted(check["column"]) + " " + check["operator"] + " "
                           + literal(value) + ")")
        self.connection.execute("CREATE TABLE " + table_name(dataset) + " (" + ",".join(clauses) + ")")
        for index in definition.get("indexes", []):
            name = "_colvenna_idx_" + digest([dataset, index])[:24]
            self.connection.execute("CREATE INDEX " + quoted(name) + " ON " + table_name(dataset)
                                    + " (" + identifiers(index) + ")")
        self.connection.execute("INSERT INTO warehouse.ctl.direct_table_definitions VALUES (?,?,?)",
            [dataset, registered["definition_sha256"], digest(self.physical(dataset))])

    def original(self, identity, principal_id, expected_binding=None):
        self.initialize()
        row = self.connection.execute(
            "SELECT principal_id,binding_sha256,retry_until,receipt FROM warehouse.ctl.writes WHERE write_id=?",
            [identity]).fetchone()
        if row is None:
            return None
        if row[0] != principal_id:
            raise ControlError(403, "WRITE_OWNER_REQUIRED")
        if expected_binding is not None and row[1] != expected_binding:
            raise ControlError(409, "WRITE_BINDING_CONFLICT")
        return row

    def register(self, identity, principal_id, request, registered, recheck=lambda: None, retry_until=None,
                 timing=NOT_TIMED):
        self.initialize()
        expected = business_binding(request, registered)
        prior = self.original(identity, principal_id, expected)
        if prior:
            if self.clock() >= prior[2]:
                raise ControlError(409, "WRITE_IDENTITY_EXPIRED")
            return json.loads(prior[3]) if prior[3] else None
        now = self.clock()
        expiry = min(retry_until or now + timedelta(days=self.RETRY_DAYS), now + timedelta(days=self.RETRY_DAYS))
        if expiry <= now:
            raise ControlError(409, "WRITE_IDENTITY_EXPIRED")

        def create():
            self.ensure_table(registered)
            recheck()
            self.connection.execute("INSERT INTO warehouse.ctl.writes VALUES (?,?,?,?,?,?,?,?,NULL)",
                [identity, principal_id, expected, request["dataset"], request["schema_version"],
                 canonical({name: value for name, value in request.items() if name != "timeout_ms"}),
                 now, expiry])
        # This durable transaction precedes any business mutation. Recovery reads
        # this same original plus the atomic business result before resuming.
        self.native.transaction(create, recheck, timing, "identity_registration", "unit")
        return None

    def status(self, identity, principal_id, admin=False):
        self.initialize()
        row = self.connection.execute(
            "SELECT principal_id,receipt FROM warehouse.ctl.writes WHERE write_id=?", [identity]).fetchone()
        if row and row[0] != principal_id and not admin:
            raise ControlError(403, "WRITE_OWNER_REQUIRED")
        if row and row[1]:
            return json.loads(row[1])
        return {"write_id": identity, "status": "UNKNOWN"}

    def require_committed(self, identity, principal_id, admin=False):
        value = self.status(identity, principal_id, admin)
        if value["status"] != "COMMITTED":
            raise ControlError(409, "WRITE_NOT_COMMITTED")
        watermark = self.connection.execute(
            "SELECT commit_sequence FROM warehouse.ctl.dataset_watermarks WHERE dataset=?",
            [value["dataset"]]).fetchone()
        if not watermark or watermark[0] < value["commit_sequence"]:
            raise ControlError(503, "WRITE_WATERMARK_UNAVAILABLE")
        return value

    def commit(self, identity, principal_id, request, registered, recheck, before_commit=lambda: None,
               retry_until=None, timing=NOT_TIMED):
        prior = self.register(identity, principal_id, request, registered, recheck, retry_until, timing)
        if prior:
            return prior
        definition = registered["definition"]
        dataset, operation = definition["dataset"], request["operation"]
        keys = definition["key_columns"]
        names = [column["name"] for column in definition["schema"]]
        rows = typed_rows(request, definition)
        c, table = self.connection, table_name(dataset)
        retry_until = self.original(identity, principal_id)[2].isoformat()

        def select(key):
            result = c.execute("SELECT * FROM " + table + " WHERE "
                + " AND ".join(quoted(name) + "=?" for name in keys), [key[name] for name in keys]).fetchone()
            return dict(zip(names, result)) if result else None

        def partition(row):
            return (canonical({name: wire(row[name]) for name in definition["partition_columns"]})
                    if row is not None else None)

        def write():
            self.ensure_table(registered)
            sequence = c.execute(
                "UPDATE warehouse.ctl.commit_sequence SET sequence=sequence+1 WHERE slot=1 "
                "RETURNING sequence").fetchone()[0]
            affected = 0
            for index, row in enumerate(rows):
                key = (row["key"] if operation == "update" else {name: row[name] for name in keys})
                before = select(key) if keys else None
                where = " WHERE " + " AND ".join(quoted(name) + "=?" for name in keys)
                values = [key[name] for name in keys]
                if operation == "insert" or operation == "upsert" and before is None:
                    sql = "INSERT INTO " + table + " (" + ",".join(quoted(name) for name in names) + ") VALUES ("
                    sql += ",".join("?" for _ in names) + ") RETURNING *"
                    after = dict(zip(names, c.execute(sql, [row[name] for name in names]).fetchone()))
                elif operation == "delete":
                    found = c.execute("DELETE FROM " + table + where + " RETURNING *", values).fetchone()
                    after = None
                elif before is None:
                    after = None
                else:
                    # DuckDB 1.5.6 executes an UPDATE ... RETURNING, or an UPDATE assigning an indexed column, as
                    # delete + insert and checks foreign keys on that delete: on a referenced row both are rejected
                    # even when no value changes. Assign only the columns whose native value changes, without
                    # RETURNING, then read the stored row; a row with no change (or a key-only upsert) is still the
                    # one affected row.
                    assigned = (row["set"] if operation == "update" else
                                {name: row[name] for name in names if name not in keys})
                    changed = {name: value for name, value in assigned.items() if not unchanged(before[name], value)}
                    if changed:
                        c.execute("UPDATE " + table + " SET " + ",".join(quoted(name) + "=?" for name in changed)
                                  + where, list(changed.values()) + values)
                    after = select(key) if changed else before
                if after is not None or operation == "delete" and found is not None:
                    affected += 1
                    change_operation = ("DELETE" if operation == "delete" else
                                        "INSERT" if operation == "insert" or before is None else "UPDATE")
                    c.execute("INSERT INTO warehouse.ctl.write_changes VALUES (?,?,?,?,?,?,?,?)",
                        [identity, index, dataset, change_operation, sequence,
                         canonical({name: wire(value) for name, value in key.items()}) if keys else None,
                         partition(before), partition(after)])
            receipt = {
                "write_id": identity, "space_id": self.configuration["space_id"],
                "generation": self.configuration["generation"], "dataset": dataset,
                "schema_version": definition["schema_version"], "status": "COMMITTED",
                "commit_sequence": sequence, "dataset_watermark": sequence, "affected_rows": affected,
                "committed_at": self.clock().isoformat(), "retry_until": retry_until,
            }
            validate("write_receipt", receipt)
            c.execute("UPDATE warehouse.ctl.writes SET receipt=? WHERE write_id=?", [canonical(receipt), identity])
            c.execute("INSERT INTO warehouse.ctl.dataset_watermarks VALUES (?,?) ON CONFLICT(dataset) "
                      "DO UPDATE SET commit_sequence=excluded.commit_sequence", [dataset, sequence])
            recheck()
            return receipt
        try:
            def final_check():
                before_commit()
                recheck()
            return self.native.transaction(write, final_check, timing, "business", "unit")
        except Exception as error:
            import duckdb
            if not isinstance(error, duckdb.ConstraintException):
                raise
            # The native transaction has rolled back in full. Persist a stable,
            # value-free rejection for this original, without a commit sequence.
            receipt = {
                "write_id": identity, "space_id": self.configuration["space_id"],
                "generation": self.configuration["generation"], "dataset": dataset,
                "schema_version": definition["schema_version"], "status": "REJECTED",
                "code": "NATIVE_CONSTRAINT", "rejected_at": self.clock().isoformat(), "retry_until": retry_until,
            }
            validate("write_receipt", receipt)
            self.native.transaction(lambda: c.execute(
                "UPDATE warehouse.ctl.writes SET receipt=? WHERE write_id=?", [canonical(receipt), identity]),
                recheck, timing, "rejection", "unit")
            return receipt

    def probe(self, dataset):
        """Fixed bounded read for admitted qualification targets; never public SQL."""
        self.initialize()
        registered = self.configuration["datasets"].get(dataset)
        if not registered or registered["definition"].get("write_owner") != "direct":
            raise ControlError(403, "DIRECT_WRITE_TARGET_REQUIRED")
        physical = self.physical(dataset)
        exists = bool(physical["columns"])
        keys = registered["definition"]["key_columns"]
        sql = "SELECT * FROM " + table_name(dataset)
        if keys:
            sql += " ORDER BY " + ",".join(quoted(name) for name in keys)
        rows = self.connection.execute(sql + " LIMIT 1001").fetchall() if exists else []
        return {
            "columns": [column["name"] for column in registered["definition"]["schema"]],
            "rows": [[wire(value) for value in row] for row in rows], "physical": physical,
            "sequence": self.connection.execute(
                "SELECT sequence FROM warehouse.ctl.commit_sequence WHERE slot=1").fetchone()[0],
            "watermarks": self.connection.execute(
                "SELECT dataset,commit_sequence FROM warehouse.ctl.dataset_watermarks ORDER BY dataset").fetchall(),
            "receipts": [json.loads(row[0]) for row in self.connection.execute(
                "SELECT receipt FROM warehouse.ctl.writes WHERE receipt IS NOT NULL ORDER BY registered_at "
                "LIMIT 1001").fetchall()],
            "changes": self.connection.execute(
                "SELECT write_id,item_number,dataset,operation,commit_sequence,key_values,before_partition,"
                "after_partition FROM warehouse.ctl.write_changes ORDER BY commit_sequence,item_number "
                "LIMIT 1001").fetchall(),
        }
