"""Native ODS events and authoritative receipts committed in one database."""

import base64
import hashlib
import json
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal

from colvenna_isle.contracts.validation import validate_record, validate_value
from colvenna_isle.control.registry import canonical
from colvenna_isle.control.status import observation
from colvenna_isle.timing import NOT_TIMED, io_counters, io_delta


class IngestionError(ValueError):
    """Deterministic rejection, with a safe code and no business values."""

    def __init__(self, code):
        super().__init__(code)
        self.code = code


class CommitUnknown(RuntimeError):
    """Pause this native handle; a new approved process must reconcile."""


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def wire(value):
    """Stable typed identity, including numeric and timezone normalization."""
    if isinstance(value, Decimal):
        if value == 0:
            return "0"
        text = format(value, "f")
        return text.rstrip("0").rstrip(".") if "." in text else text
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bytes):
        return base64.b64encode(value).decode()
    if isinstance(value, list):
        return [wire(item) for item in value]
    return value


def quoted(name):
    # Identifiers already passed the registry contract; quote also covers keywords.
    return '"' + name.replace('"', '""') + '"'


def binding(message):
    return digest(message)


def arrow_type(pa, kind):
    if kind.startswith("DECIMAL("):
        precision, scale = map(int, kind[len("DECIMAL("):-1].split(","))
        return pa.decimal128(precision, scale)
    return {"BIGINT": pa.int64(), "BOOLEAN": pa.bool_(), "VARCHAR": pa.string(), "JSON": pa.string(),
            "VARCHAR[]": pa.list_(pa.string()), "DATE": pa.date32(), "TIMESTAMP": pa.timestamp("us"),
            "TIMESTAMPTZ": pa.timestamp("us", tz="UTC"), "BLOB": pa.binary()}[kind]


def insert_rows(connection, table, types, rows, suffix=""):
    """Insert validated rows with a bounded number of statements.

    DuckDB converts every Python parameter separately (about 0.6 ms each), so the writer image passes the rows as
    one exactly typed Arrow table. Runtimes without pyarrow fall back to
    multi-row statements whose parameters are cast to the column types."""
    if not rows:
        return
    try:
        import pyarrow as pa
    except ImportError:
        placeholder = "(" + ",".join("CAST(? AS " + kind + ")" for kind in types) + ")"
        for start in range(0, len(rows), 500):
            batch = rows[start:start + 500]
            connection.execute("INSERT INTO " + table + " VALUES " + ",".join(placeholder for _ in batch) + suffix,
                               [value for row in batch for value in row])
        return
    columns = list(zip(*rows))
    batch = pa.table([pa.array(list(values), arrow_type(pa, kind)) for values, kind in zip(columns, types)],
                     names=["c%d" % index for index in range(len(types))])
    name = "colvenna_rows_" + uuid.uuid4().hex
    connection.register(name, batch)
    try:
        connection.execute("INSERT INTO " + table + " SELECT * FROM " + name + suffix)
    finally:
        connection.unregister(name)


class IngestionStore:
    def __init__(self, connection, configuration, guard):
        self.connection, self.configuration, self.guard = connection, configuration, guard
        self.unknown = False
        self.ready = False

    def require(self):
        if self.unknown:
            raise CommitUnknown("NATIVE_RESULT_UNKNOWN")
        return self.guard()

    def transaction(self, body, before_commit=lambda: None, timing=NOT_TIMED, label="transaction", parent=None):
        self.require()
        c = self.connection
        with timing.segment(label, parent):
            c.execute("BEGIN")
            try:
                with timing.segment(label + ".sql", label):
                    result = body()
                with timing.segment(label + ".precommit_check", label):
                    before_commit()
                    self.require()
            except Exception:
                try:
                    c.execute("ROLLBACK")
                except Exception:
                    self.unknown = True
                    raise CommitUnknown("ROLLBACK_UNKNOWN") from None
                raise
            # objfs counters bracket COMMIT: its FileSync/SlateDB S3 requests,
            # not separately timed inside the native call.
            before = io_counters(c) if timing.active else None
            try:
                with timing.segment(label + ".commit", label):
                    c.execute("COMMIT")
            except Exception:
                # COMMIT may have reached durable storage. Do not label it FAILED,
                # issue another write, ACK, or reuse this uncertain native handle.
                self.unknown = True
                raise CommitUnknown("COMMIT_UNKNOWN") from None
            if before is not None:
                timing.count(label + ".commit_s3", io_delta(before, io_counters(c)))
        return result

    def initialize(self):
        if self.ready:
            self.require()
            return

        def create():
            c = self.connection
            definitions = {
                "space_identity": "(slot INTEGER PRIMARY KEY CHECK(slot=1), space_id VARCHAR NOT NULL, "
                                  "generation VARCHAR NOT NULL, format_version INTEGER NOT NULL)",
                "commit_sequence": "(slot INTEGER PRIMARY KEY CHECK(slot=1), sequence BIGINT NOT NULL)",
                "observation_sequence": "(slot INTEGER PRIMARY KEY CHECK(slot=1), sequence BIGINT NOT NULL)",
                "ingestion_observations": "(ingestion_id VARCHAR PRIMARY KEY, projection_version BIGINT UNIQUE "
                                          "NOT NULL, observation VARCHAR NOT NULL)",
                "ingestions": "(ingestion_id VARCHAR PRIMARY KEY, binding_sha256 VARCHAR NOT NULL, "
                              "commit_sequence BIGINT UNIQUE NOT NULL, receipt VARCHAR NOT NULL)",
                "events": "(dataset VARCHAR, event_id VARCHAR, event_sha256 VARCHAR NOT NULL, "
                          "ingestion_id VARCHAR NOT NULL, commit_sequence BIGINT NOT NULL, "
                          "operation VARCHAR NOT NULL, source_version BIGINT, key_sha256 VARCHAR, "
                          "disposition VARCHAR NOT NULL, PRIMARY KEY(dataset,event_id))",
                "key_state": "(dataset VARCHAR, key_sha256 VARCHAR, source_version BIGINT NOT NULL, "
                             "event_id VARCHAR NOT NULL, tombstone BOOLEAN NOT NULL, "
                             "commit_sequence BIGINT NOT NULL, PRIMARY KEY(dataset,key_sha256))",
                "ingestion_attempts": "(attempt_id VARCHAR PRIMARY KEY, ingestion_id VARCHAR NOT NULL, "
                                      "binding_sha256 VARCHAR NOT NULL, message_id VARCHAR NOT NULL, "
                                      "error_code VARCHAR NOT NULL, observed_at TIMESTAMPTZ NOT NULL)",
                "dlq_reconciliations": "(message_id VARCHAR PRIMARY KEY, ingestion_id VARCHAR NOT NULL, "
                                       "evidence VARCHAR NOT NULL, observed_at TIMESTAMPTZ NOT NULL)",
                "dataset_definitions": "(dataset VARCHAR PRIMARY KEY, definition_sha256 VARCHAR NOT NULL)",
            }
            for name, definition in definitions.items():
                c.execute("CREATE TABLE IF NOT EXISTS warehouse.ctl." + name + " " + definition)
            expected = (self.configuration["space_id"], self.configuration["generation"], 6)
            observed = c.execute(
                "SELECT space_id,generation,format_version FROM warehouse.ctl.space_identity WHERE slot=1").fetchone()
            if observed is None:
                c.execute("INSERT INTO warehouse.ctl.space_identity VALUES (1,?,?,?)", list(expected))
                c.execute("INSERT INTO warehouse.ctl.commit_sequence VALUES (1,0)")
                c.execute("INSERT INTO warehouse.ctl.observation_sequence VALUES (1,0)")
            elif observed != expected:
                raise IngestionError("NATIVE_SPACE_IDENTITY_MISMATCH")
        try:
            self.transaction(create)
        except IngestionError as error:
            if error.code == "NATIVE_SPACE_IDENTITY_MISMATCH":
                self.unknown = True
                raise CommitUnknown(error.code) from None
            raise
        self.ready = True

    def existing(self, message):
        self.initialize()
        row = self.connection.execute(
            "SELECT binding_sha256,receipt FROM warehouse.ctl.ingestions WHERE ingestion_id=?",
            [message["ingestion_id"]]).fetchone()
        if row is None:
            return None
        if row[0] != binding(message):
            raise IngestionError("INGESTION_BINDING_CONFLICT")
        return json.loads(row[1])

    def status(self, ingestion_id):
        self.initialize()
        row = self.connection.execute(
            "SELECT observation FROM warehouse.ctl.ingestion_observations WHERE ingestion_id=?",
            [ingestion_id]).fetchone()
        if row:
            return json.loads(row[0])
        return {"ingestion_id": ingestion_id, "status": "UNKNOWN"}

    def next_observation(self):
        """Called only inside the same native transaction as the observed state."""
        c = self.connection
        version = c.execute(
            "UPDATE warehouse.ctl.observation_sequence SET sequence=sequence+1 WHERE slot=1 "
            "RETURNING sequence").fetchone()[0]
        return version

    def save_observation(self, value):
        self.connection.execute(
            "INSERT INTO warehouse.ctl.ingestion_observations VALUES (?,?,?) "
            "ON CONFLICT(ingestion_id) DO UPDATE SET projection_version=excluded.projection_version,"
            "observation=excluded.observation",
            [value["ingestion_id"], value["projection_version"], canonical(value)])

    def observations(self, after):
        self.initialize()
        return [json.loads(row[0]) for row in self.connection.execute(
            "SELECT observation FROM warehouse.ctl.ingestion_observations "
            "WHERE projection_version>? ORDER BY projection_version LIMIT 64", [after]).fetchall()]

    def diagnostics(self, ingestion_id):
        self.initialize()
        return {"status": self.status(ingestion_id), "attempts": [
            {"message_id": row[0], "error_code": row[1], "observed_at": row[2].isoformat()}
            for row in self.connection.execute(
                "SELECT message_id,error_code,observed_at FROM warehouse.ctl.ingestion_attempts "
                "WHERE ingestion_id=? ORDER BY observed_at DESC LIMIT 32", [ingestion_id]).fetchall()],
            "dlq_reconciliations": [json.loads(row[0]) for row in self.connection.execute(
                "SELECT evidence FROM warehouse.ctl.dlq_reconciliations WHERE ingestion_id=? "
                "ORDER BY observed_at DESC LIMIT 32", [ingestion_id]).fetchall()]}

    def reconcile_dead_letter(self, evidence):
        self.initialize()
        def write():
            prior = self.connection.execute(
                "SELECT evidence FROM warehouse.ctl.dlq_reconciliations WHERE message_id=?",
                [evidence["message_id"]]).fetchone()
            if prior:
                if json.loads(prior[0]) != evidence:
                    raise IngestionError("DLQ_RECONCILIATION_CONFLICT")
                return
            self.connection.execute(
                "INSERT INTO warehouse.ctl.dlq_reconciliations VALUES (?,?,?,?)",
                [evidence["message_id"], evidence["ingestion_id"], canonical(evidence), datetime.now(timezone.utc)])
        self.transaction(write)

    def reject(self, message, message_id, code):
        """Trace rejected deliveries without replacing a COMMITTED receipt."""
        self.initialize()
        attempt = digest([binding(message), message_id, code])

        def write():
            c = self.connection
            if c.execute("SELECT 1 FROM warehouse.ctl.ingestion_attempts WHERE attempt_id=?", [attempt]).fetchone():
                return
            observed_at = datetime.now(timezone.utc)
            c.execute("INSERT INTO warehouse.ctl.ingestion_attempts VALUES (?,?,?,?,?,?)",
                [attempt, message["ingestion_id"], binding(message), message_id, code, observed_at])
            if not c.execute("SELECT 1 FROM warehouse.ctl.ingestions WHERE ingestion_id=?",
                             [message["ingestion_id"]]).fetchone():
                self.save_observation(observation({
                    "ingestion_id": message["ingestion_id"], "space_id": message["space_id"],
                    "generation": message["generation"], "dataset": message["dataset"],
                    "principal_id": message["principal_id"], "status": "FAILED", "error_code": code,
                }, self.next_observation(), observed_at.isoformat()))
        self.transaction(write)

    def commit(self, message, payload, registered, recheck, before_commit=lambda: None, timing=NOT_TIMED):
        prior = self.existing(message)
        if prior:
            return prior
        definition = registered["definition"]
        ingestion = definition["ingestion"]
        columns = definition["schema"]
        required = definition["key_columns"] + [ingestion["event_id_column"]]
        version_column = ingestion.get("source_version_column")
        if version_column:
            required.append(version_column)
        rows = []
        for record in payload["records"]:
            validate_record(columns, record, payload["operation"], required)
            typed = {}
            for column in columns:
                name = column["name"]
                value = record.get(name)
                typed[name] = validate_value(column["type"], value,
                    column["nullable"] or (payload["operation"] == "delete" and name not in required))
                if column["type"] == "JSON" and value is not None:
                    typed[name] = canonical(value)
            rows.append(typed)
        c = self.connection
        dataset = definition["dataset"]
        table = "warehouse.ods." + quoted(dataset.split(".", 1)[1])

        def write():
            previous = c.execute(
                "SELECT definition_sha256 FROM warehouse.ctl.dataset_definitions WHERE dataset=?", [dataset]).fetchone()
            if previous and previous[0] != registered["definition_sha256"]:
                raise IngestionError("NATIVE_SCHEMA_IDENTITY_MISMATCH")
            if not previous:
                # ODS retains sparse delete events; intake still enforces the
                # declared nonnullable business schema for append/upsert.
                declaration = ",".join(quoted(col["name"]) + " " + col["type"]
                    + (" NOT NULL" if col["name"] in required else "") for col in columns)
                c.execute("CREATE TABLE " + table + " (" + declaration +
                    ",_colvenna_ingestion_id VARCHAR NOT NULL,_colvenna_commit_sequence BIGINT NOT NULL,"
                    "_colvenna_operation VARCHAR NOT NULL,_colvenna_disposition VARCHAR NOT NULL,"
                    "_colvenna_received_at TIMESTAMPTZ NOT NULL)")
                c.execute("INSERT INTO warehouse.ctl.dataset_definitions VALUES (?,?)",
                          [dataset, registered["definition_sha256"]])
            sequence = c.execute(
                "SELECT sequence FROM warehouse.ctl.commit_sequence WHERE slot=1").fetchone()[0] + 1
            counts = {"inserted_events": 0, "duplicate_events": 0, "stale_events": 0, "tombstones": 0}
            observed_at = datetime.now(timezone.utc)
            # A few multi-row statements per batch, never one per record: each single-row INSERT of one DuckDB
            # transaction keeps its own transaction-local storage (about 160 KB), so a 4,000-record batch
            # exhausted the writer's 512 MB. Dispositions are still decided record by record, in order, against
            # the native state read once for the whole batch plus the batch's own earlier events.
            event_column, key_columns = ingestion["event_id_column"], definition["key_columns"]
            planned = [(row, row[event_column],
                        digest([payload["operation"], {name: wire(value) for name, value in row.items()}]),
                        digest([wire(row[name]) for name in key_columns]) if key_columns else None)
                       for row in rows]
            known = dict(c.execute(
                "SELECT event_id, event_sha256 FROM warehouse.ctl.events WHERE dataset=? AND event_id IN "
                "(SELECT unnest(?::VARCHAR[]))", [dataset, sorted({item[1] for item in planned})]).fetchall())
            versions = dict(c.execute(
                "SELECT key_sha256, source_version FROM warehouse.ctl.key_state WHERE dataset=? AND key_sha256 IN "
                "(SELECT unnest(?::VARCHAR[]))",
                [dataset, sorted({item[3] for item in planned if item[3] is not None})]).fetchall())
            ods_rows, event_rows, key_rows = [], [], {}
            for row, event_id, event_hash, key_hash in planned:
                if event_id in known:
                    if known[event_id] != event_hash:
                        raise IngestionError("EVENT_IDENTITY_CONFLICT")
                    counts["duplicate_events"] += 1
                    continue
                version = row[version_column] if version_column else None
                disposition = "RECORDED"
                if version is not None and key_hash is not None:
                    current = versions.get(key_hash)
                    if current is not None and version == current:
                        raise IngestionError("BUSINESS_VERSION_CONFLICT")
                    if current is not None and version < current:
                        disposition = "STALE"
                        counts["stale_events"] += 1
                    else:
                        disposition = "CURRENT"
                        versions[key_hash] = version
                        key_rows[key_hash] = [dataset, key_hash, version, event_id, payload["operation"] == "delete",
                                              sequence]
                known[event_id] = event_hash
                ods_rows.append([row[col["name"]] for col in columns] + [
                    message["ingestion_id"], sequence, payload["operation"], disposition, observed_at])
                event_rows.append([dataset, event_id, event_hash, message["ingestion_id"], sequence,
                                   payload["operation"], version, key_hash, disposition])
                counts["inserted_events"] += 1
                counts["tombstones"] += int(payload["operation"] == "delete")
            insert_rows(c, table, [col["type"] for col in columns] + [
                "VARCHAR", "BIGINT", "VARCHAR", "VARCHAR", "TIMESTAMPTZ"], ods_rows)
            insert_rows(c, "warehouse.ctl.events", ["VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR", "BIGINT", "VARCHAR",
                                                    "BIGINT", "VARCHAR", "VARCHAR"], event_rows)
            # One row per key (its last CURRENT event): an upsert cannot update one row twice in a statement.
            insert_rows(c, "warehouse.ctl.key_state", ["VARCHAR", "VARCHAR", "BIGINT", "VARCHAR", "BOOLEAN", "BIGINT"],
                        list(key_rows.values()),
                        " ON CONFLICT(dataset,key_sha256) DO UPDATE SET source_version=excluded.source_version,"
                        "event_id=excluded.event_id,tombstone=excluded.tombstone,"
                        "commit_sequence=excluded.commit_sequence")
            receipt = {
                "ingestion_id": message["ingestion_id"], "space_id": message["space_id"],
                "generation": message["generation"], "dataset": dataset,
                "principal_id": message["principal_id"], "payload_sha256": message["payload_sha256"],
                "source": message["source"], "status": "COMMITTED", "commit_sequence": sequence,
                "operation": payload["operation"], "input_records": len(rows), **counts,
                "committed_at": observed_at.isoformat(), "publication": {"status": "NOT_PUBLISHED"},
            }
            receipt = observation(receipt, self.next_observation(), observed_at.isoformat())
            c.execute("UPDATE warehouse.ctl.commit_sequence SET sequence=? WHERE slot=1", [sequence])
            c.execute("INSERT INTO warehouse.ctl.ingestions VALUES (?,?,?,?)",
                      [message["ingestion_id"], binding(message), sequence, canonical(receipt)])
            self.save_observation(receipt)
            recheck()
            return receipt
        return self.transaction(write, before_commit, timing, "native", "unit")

    def probe(self, dataset):
        """Bounded qualification read; caller enforces the synthetic namespace."""
        self.initialize()
        validate = self.configuration["datasets"].get(dataset)
        if not validate:
            raise IngestionError("DATASET_NOT_FOUND")
        table = "warehouse.ods." + quoted(dataset.split(".", 1)[1])
        exists = self.connection.execute(
            "SELECT count(*) FROM warehouse.ctl.dataset_definitions WHERE dataset=?", [dataset]).fetchone()[0]
        if exists:
            cursor = self.connection.execute("SELECT * FROM " + table +
                                             " ORDER BY _colvenna_commit_sequence LIMIT 101")
            columns = [entry[0] for entry in cursor.description]
            rows = [[wire(value) for value in row] for row in cursor.fetchall()]
        else:
            columns, rows = [], []
        return {
            "columns": columns, "rows": rows,
            "commit_sequence": self.connection.execute(
                "SELECT sequence FROM warehouse.ctl.commit_sequence WHERE slot=1").fetchone()[0],
            "ledger": [json.loads(row[0]) for row in self.connection.execute(
                "SELECT receipt FROM warehouse.ctl.ingestions ORDER BY commit_sequence LIMIT 101").fetchall()],
            "events": self.connection.execute("SELECT dataset,event_id,operation,source_version,disposition "
                                              "FROM warehouse.ctl.events ORDER BY event_id LIMIT 101").fetchall(),
            "keys": self.connection.execute("SELECT dataset,source_version,event_id,tombstone,commit_sequence "
                                           "FROM warehouse.ctl.key_state ORDER BY event_id LIMIT 101").fetchall(),
            "attempts": self.connection.execute(
                "SELECT ingestion_id,error_code,message_id FROM warehouse.ctl.ingestion_attempts "
                "ORDER BY observed_at LIMIT 101").fetchall(),
        }
