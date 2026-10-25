"""Consistent native backup and same-version isolated restore of one space generation.

A backup is one native read snapshot taken by the approved writer under the
native lock, at one commit sequence: each warehouse table's DDL, rows (Parquet
parts), row count, order-independent row digest and physical catalog
fingerprint, plus explicit indexes. Types that Parquet would not round-trip
exactly travel as text. Parts and the manifest are immutable objects in the
backup vault with SHA-256 checksums; an S3 copy alone never makes a backup.

Restore requires the backup's native runtime identity (DuckDB version and
extension hashes), the same space and generation, and an empty target
database. Every table loads in one native transaction that commits only when
every count, digest, fingerprint and the commit sequence match the manifest;
otherwise it rolls back and the target stays empty. No source database is
opened, so a restore never creates a second writer of the original database.
"""

import base64
import hashlib
import json
import re
import shutil
import threading
import time
from pathlib import Path

from botocore.exceptions import BotoCoreError, ClientError

from colvenna_isle.control.registry import ControlError, canonical
from .ingestion import digest, quoted
from .native_schema import physical
from .ownership import matches
from .scheduler import NotSubmitted

OPERATIONS = {"backup_native", "backup_status", "restore_native", "restore_status"}
FORMAT_VERSION = 1
PART_BYTES = 268435456
MAX_MANIFEST_BYTES = 8388608
IDENTITY = re.compile(r"(backup|restore)_[0-9a-f]{32}")


def text_type(kind):
    """Parquet would not round-trip these exactly (HUGEINT becomes DOUBLE); they travel as text."""
    return "HUGEINT" in kind or kind == "BIT" or "INTERVAL" in kind or "TIME WITH TIME ZONE" in kind


def row_digest(connection, schema, table):
    """Order-independent: row count, sum and XOR of each whole-row hash (same DuckDB version)."""
    name = quoted(schema) + "." + quoted(table)
    count, total, folded = connection.execute(
        "SELECT count(*), CAST(coalesce(sum(CAST(hash(r) AS HUGEINT)), 0) AS VARCHAR), "
        "CAST(coalesce(bit_xor(hash(r)), 0) AS VARCHAR) FROM (SELECT t AS r FROM warehouse." + name + " AS t)"
    ).fetchone()
    return {"rows": count, "hash_sum": total, "hash_xor": folded}


def runtime_identity():
    manifest = json.loads(Path("/opt/colvenna/artifacts/manifest.json").read_text())
    return {"duckdb_version": manifest["duckdb_version"], "extensions": manifest["files"]}


# The manifest's DDL is replayed on the warehouse connection, which has S3 access; first it must
# replay on a sealed in-memory catalog and render back unchanged.
SEALED = {"enable_external_access": False, "autoload_known_extensions": False,
          "autoinstall_known_extensions": False, "allow_unsigned_extensions": False, "lock_configuration": True}


def rehearse(manifest):
    """Each statement is one CREATE of exactly the recorded object, with exactly the recorded columns."""
    import duckdb

    references = {(value["schema"], value["table"]): [tuple(parent) for parent in value["references"]]
                  for value in manifest["tables"]}
    sandbox = duckdb.connect(config=SEALED)
    try:
        for schema in manifest["schemas"]:
            sandbox.execute("CREATE SCHEMA IF NOT EXISTS " + quoted(schema))
        recorded = [(table, "SELECT sql FROM duckdb_tables() WHERE schema_name=? AND table_name=?",
                     [table["schema"], table["table"]]) for table in ordered(manifest["tables"], references)]
        recorded += [(index, "SELECT sql FROM duckdb_indexes() WHERE schema_name=? AND table_name=? "
                             "AND index_name=?", [index["schema"], index["table"], index["name"]])
                     for index in manifest["indexes"]]
        for value, rendered, names in recorded:
            statements = sandbox.extract_statements(value["sql"])
            if len(statements) != 1 or statements[0].type != duckdb.StatementType.CREATE:
                raise ControlError(409, "RESTORE_MANIFEST_DDL_REJECTED")
            try:
                sandbox.execute(value["sql"])
            except duckdb.Error:
                raise ControlError(409, "RESTORE_MANIFEST_DDL_REJECTED") from None
            if sandbox.execute(rendered, names).fetchall() != [(value["sql"],)]:
                raise ControlError(409, "RESTORE_MANIFEST_DDL_REJECTED")
        for table in manifest["tables"]:
            columns = sandbox.execute("SELECT column_name,data_type FROM duckdb_columns() WHERE schema_name=? "
                                      "AND table_name=? ORDER BY column_index",
                                      [table["schema"], table["table"]]).fetchall()
            if [(name, kind) for name, kind, _ in table["columns"]] != columns or any(
                    text != text_type(kind) for _, kind, text in table["columns"]):
                raise ControlError(409, "RESTORE_MANIFEST_DDL_REJECTED")
    finally:
        sandbox.close()


def ordered(tables, references):
    """Referenced tables first, so each load satisfies its foreign keys."""
    remaining, result = {(value["schema"], value["table"]): value for value in tables}, []
    while remaining:
        ready = [key for key in sorted(remaining)
                 if not any(parent in remaining and parent != key for parent in references.get(key, ()))]
        if not ready:
            raise ControlError(409, "BACKUP_REFERENCE_CYCLE")
        for key in ready:
            result.append(remaining.pop(key))
    return result


class Backups:
    def __init__(self, runtime, s3, bucket, account, work=Path("/work")):
        self.runtime, self.s3, self.bucket, self.account = runtime, s3, bucket, account
        self.work = work
        self.jobs, self.guard_lock = {}, threading.Lock()

    def authorize(self, request):
        runtime = self.runtime
        if not matches(request.get("identity", {}), runtime.identity):
            raise ControlError(409, "WRITER_IDENTITY_CHANGED")
        if type(request.get("epoch")) is not int or request["epoch"] < 1:
            raise ControlError(400, "WRITER_EPOCH_REQUIRED")
        runtime.guard(request["epoch"])
        principal = runtime.direct.keys.authenticate_id(request.get("key_id", ""))
        principal.authorize(request.get("space_id"), "backup:manage")
        if (principal.principal_id != request.get("principal_id")
                or principal.generation != request.get("generation")):
            raise ControlError(403, "FORBIDDEN")
        configuration = runtime.registry._read(request["space_id"])
        if configuration["generation"] != request["generation"]:
            raise ControlError(403, "SPACE_GENERATION_CHANGED")
        return principal, configuration

    def execute(self, request):
        operation = request.get("operation")
        if operation not in OPERATIONS:
            raise ControlError(400, "UNKNOWN_BACKUP_OPERATION")
        if not self.bucket:
            raise ControlError(503, "BACKUP_VAULT_UNAVAILABLE")
        principal, configuration = self.authorize(request)
        identity = request.get("backup_id" if operation.startswith("backup") else "restore_id", "")
        if not IDENTITY.fullmatch(identity) or not identity.startswith(operation.split("_")[0]):
            raise ControlError(400, "INVALID_BACKUP_IDENTITY")
        space, generation = configuration["space_id"], configuration["generation"]
        if operation == "backup_status":
            return self.status(identity, self.prefix(space, generation, identity) + "manifest.json")
        if operation == "restore_status":
            with self.guard_lock:
                job = self.jobs.get(identity)
            return job or {"restore_id": identity, "state": "UNKNOWN", "code": "RESTORE_JOB_UNKNOWN"}
        if operation == "backup_native":
            action = lambda: self.backup(request, configuration, principal, identity)  # noqa: E731
            initial = {"backup_id": identity, "state": "RUNNING", "space_id": space, "generation": generation}
        else:
            reference = request.get("manifest")
            if (not isinstance(reference, dict) or set(reference) != {"bucket", "key", "version_id", "sha256"}
                    or reference["bucket"] != self.bucket or not re.fullmatch(r"[0-9a-f]{64}", reference["sha256"])
                    or not str(reference["key"]).startswith(f"backups/{space}/{generation}/backup_")):
                raise ControlError(400, "INVALID_BACKUP_MANIFEST_REFERENCE")
            action = lambda: self.restore(request, configuration, reference, identity)  # noqa: E731
            initial = {"restore_id": identity, "state": "RUNNING", "space_id": space, "generation": generation,
                       "manifest": reference}
        with self.guard_lock:
            if identity in self.jobs:
                return self.jobs[identity]
            if any(job["state"] == "RUNNING" for job in self.jobs.values()):
                raise ControlError(429, "BACKUP_EXECUTOR_BUSY")
            self.jobs[identity] = dict(initial, started_unix=time.time())
        threading.Thread(target=self._run, args=(identity, action), daemon=True,
                         name="native-" + operation).start()
        return dict(initial)

    def _run(self, identity, action):
        try:
            result = action()
            state = {"state": "COMPLETED", **result}
        except ControlError as error:
            state = {"state": "FAILED", "code": error.code}
        except Exception as error:
            # The unit may have reached a durable effect; report UNKNOWN, never FAILED.
            state = {"state": "UNKNOWN", "code": "BACKUP_RESULT_UNKNOWN", "exception_type": type(error).__name__}
        with self.guard_lock:
            self.jobs[identity].update(state, finished_unix=time.time())
        self.runtime.log("NATIVE_BACKUP_JOB", {"identity": identity, "state": state["state"],
                                               "code": state.get("code")})

    @staticmethod
    def prefix(space, generation, backup_id):
        return f"backups/{space}/{generation}/{backup_id}/native/"

    def unit(self, kind, configuration, action):
        try:
            return self.runtime.scheduler.submit(kind, "backup:" + digest([configuration["space_id"],
                                                                           time.monotonic_ns()]),
                                                 action, configuration["space_id"]).future.result()
        except NotSubmitted:
            raise ControlError(429, "WRITER_QUEUE_FULL") from None

    # -- backup -------------------------------------------------------------------------------
    def backup(self, request, configuration, principal, backup_id):
        space, generation = configuration["space_id"], configuration["generation"]
        directory = self.work / "backups" / backup_id
        shutil.rmtree(directory, ignore_errors=True)
        directory.mkdir(parents=True)
        timing = {}
        try:
            started = time.monotonic()
            snapshot = self.unit("export", configuration,
                                 lambda: self.snapshot(configuration, directory, request["epoch"], timing))
            timing["snapshot_and_queue_ms"] = round((time.monotonic() - started) * 1000, 3)
            prefix = self.prefix(space, generation, backup_id)
            uploaded = time.monotonic()
            uploaded_bytes = 0
            for table in snapshot["tables"]:
                files = []
                for part in sorted((directory / table["directory"]).iterdir()):
                    files.append(self.put_file(prefix + table["directory"] + "/" + part.name, part))
                    uploaded_bytes += files[-1]["bytes"]
                table["files"] = files
                table.pop("directory")
            timing["upload_ms"] = round((time.monotonic() - uploaded) * 1000, 3)
            # The approved writer and the original requester still hold authority at publication.
            self.authorize(request)
            manifest = {
                "format_version": FORMAT_VERSION, "kind": "colvenna_native_backup", "backup_id": backup_id,
                "space_id": space, "generation": generation, "route_version": configuration["route_version"],
                "point": snapshot["point"], "runtime": {
                    **runtime_identity(), "writer_source_commit": self.runtime.identity["source_commit"],
                    "writer_image_digest": self.runtime.identity["image_digest"],
                    "writer_task_arn": self.runtime.identity["task_arn"]},
                "requested_by": {"principal_id": principal.principal_id, "key_id": principal.key_id},
                "schemas": snapshot["schemas"], "tables": snapshot["tables"], "indexes": snapshot["indexes"],
                "uploaded_bytes": uploaded_bytes, "timing": timing,
            }
            raw = (canonical(manifest) + "\n").encode()
            if len(raw) > MAX_MANIFEST_BYTES:
                raise ControlError(413, "BACKUP_MANIFEST_TOO_LARGE")
            reference = self.put_bytes(prefix + "manifest.json", raw)
            return {"manifest": reference, "point": snapshot["point"], "tables": len(snapshot["tables"]),
                    "rows": sum(table["digest"]["rows"] for table in snapshot["tables"]),
                    "uploaded_bytes": uploaded_bytes, "timing": timing}
        finally:
            shutil.rmtree(directory, ignore_errors=True)

    def snapshot(self, configuration, directory, epoch, timing):
        """Runs on the scheduler under the native lock: one read transaction fixes the backup point."""
        started = time.monotonic()
        self.runtime.guard(epoch)
        database = self.runtime.database(configuration)
        store = database.ingestions
        store.initialize()
        c = database.connection
        c.execute("BEGIN")
        try:
            identity = c.execute("SELECT space_id,generation,format_version FROM warehouse.ctl.space_identity "
                                 "WHERE slot=1").fetchone()
            if identity != (configuration["space_id"], configuration["generation"], 6):
                raise ControlError(503, "NATIVE_SPACE_IDENTITY_MISMATCH")
            # Every native object must be a table (with constraints) or an explicit index.
            if (c.execute("SELECT count(*) FROM duckdb_views() WHERE database_name='warehouse' AND NOT internal")
                    .fetchone()[0] or c.execute("SELECT count(*) FROM duckdb_sequences() WHERE "
                                                "database_name='warehouse'").fetchone()[0]):
                raise ControlError(409, "BACKUP_OBJECT_UNSUPPORTED")
            schemas = [row[0] for row in c.execute(
                "SELECT schema_name FROM duckdb_schemas() WHERE database_name='warehouse' AND NOT internal "
                "ORDER BY schema_name").fetchall()]
            point = {"commit_sequence": c.execute(
                         "SELECT sequence FROM warehouse.ctl.commit_sequence WHERE slot=1").fetchone()[0],
                     "observation_sequence": c.execute(
                         "SELECT sequence FROM warehouse.ctl.observation_sequence WHERE slot=1").fetchone()[0],
                     "captured_unix": time.time()}
            tables = []
            for index, (schema, table, sql) in enumerate(c.execute(
                    "SELECT schema_name,table_name,sql FROM duckdb_tables() WHERE database_name='warehouse' "
                    "ORDER BY schema_name,table_name").fetchall()):
                columns = c.execute("SELECT column_name,data_type FROM duckdb_columns() WHERE "
                                    "database_name='warehouse' AND schema_name=? AND table_name=? "
                                    "ORDER BY column_index", [schema, table]).fetchall()
                projection = ",".join(("CAST(" + quoted(name) + " AS VARCHAR)" if text_type(kind) else quoted(name))
                                      + " AS " + quoted(name) for name, kind in columns)
                part = "t%03d" % index
                c.execute("COPY (SELECT " + projection + " FROM warehouse." + quoted(schema) + "." + quoted(table)
                          + ") TO '" + str(directory / part) + "' (FORMAT parquet, COMPRESSION zstd, "
                          "FILE_SIZE_BYTES " + str(PART_BYTES) + ")")
                tables.append({"schema": schema, "table": table, "sql": sql, "directory": part,
                               "columns": [[name, kind, text_type(kind)] for name, kind in columns],
                               "digest": row_digest(c, schema, table),
                               "physical_sha256": digest(physical(c, schema + "." + table))})
            indexes = [{"schema": schema, "table": table, "name": name, "sql": sql} for schema, table, name, sql in
                       c.execute("SELECT schema_name,table_name,index_name,sql FROM duckdb_indexes() WHERE "
                                 "database_name='warehouse' ORDER BY schema_name,table_name,index_name").fetchall()]
            references = c.execute("SELECT schema_name,table_name,referenced_table FROM duckdb_constraints() "
                                   "WHERE database_name='warehouse' AND constraint_type='FOREIGN KEY'").fetchall()
            for value in tables:
                value["references"] = sorted({(schema, parent) for schema, table, parent in references
                                              if (schema, table) == (value["schema"], value["table"])})
            c.execute("COMMIT")
        except BaseException:
            try:
                c.execute("ROLLBACK")
            except Exception:
                pass
            raise
        timing["native_lock_ms"] = round((time.monotonic() - started) * 1000, 3)
        return {"point": point, "schemas": schemas, "tables": tables, "indexes": indexes}

    # -- vault objects -------------------------------------------------------------------------
    def put_file(self, key, path):
        sha = hashlib.sha256()
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1048576), b""):
                sha.update(block)
        size = path.stat().st_size
        with path.open("rb") as stream:
            return self._put(key, stream, sha, size)

    def put_bytes(self, key, raw):
        sha = hashlib.sha256(raw)
        return self._put(key, raw, sha, len(raw))

    def _put(self, key, body, sha, size):
        checksum = base64.b64encode(sha.digest()).decode()
        try:
            response = self.s3.put_object(Bucket=self.bucket, Key=key, Body=body, IfNoneMatch="*",
                                          ExpectedBucketOwner=self.account, ChecksumSHA256=checksum,
                                          Metadata={"sha256": sha.hexdigest()})
            head = self.s3.head_object(Bucket=self.bucket, Key=key, VersionId=response["VersionId"],
                                       ExpectedBucketOwner=self.account, ChecksumMode="ENABLED")
        except (ClientError, BotoCoreError, KeyError):
            raise ControlError(503, "BACKUP_OBJECT_UNCONFIRMED") from None
        if (head.get("ChecksumSHA256") != checksum or head["ContentLength"] != size
                or head.get("VersionId") in {None, "", "null"}):
            raise ControlError(503, "BACKUP_OBJECT_UNCONFIRMED")
        return {"bucket": self.bucket, "key": key, "version_id": head["VersionId"], "sha256": sha.hexdigest(),
                "bytes": size}

    def get(self, reference, destination=None, maximum=MAX_MANIFEST_BYTES):
        """One exact object version, streamed to a new file or returned; its SHA-256 must match."""
        sha, total, chunks = hashlib.sha256(), 0, []
        try:
            response = self.s3.get_object(Bucket=reference["bucket"], Key=reference["key"],
                                          VersionId=reference["version_id"], ExpectedBucketOwner=self.account)
            out = destination.open("xb") if destination else None
            try:
                with response["Body"] as stream:
                    for block in iter(lambda: stream.read(1048576), b""):
                        sha.update(block)
                        total += len(block)
                        if out:
                            out.write(block)
                        elif total > maximum:
                            raise ControlError(413, "BACKUP_MANIFEST_TOO_LARGE")
                        else:
                            chunks.append(block)
            finally:
                if out:
                    out.close()
        except (ClientError, BotoCoreError):
            raise ControlError(503, "BACKUP_OBJECT_UNCONFIRMED") from None
        if response.get("VersionId") != reference["version_id"] or sha.hexdigest() != reference["sha256"] or (
                "bytes" in reference and total != reference["bytes"]):
            raise ControlError(409, "BACKUP_OBJECT_CHANGED")
        return b"".join(chunks)

    def status(self, backup_id, manifest_key):
        with self.guard_lock:
            job = self.jobs.get(backup_id)
        if job:
            return job
        # A writer restart loses the in-flight job; only the published manifest is authority.
        try:
            head = self.s3.head_object(Bucket=self.bucket, Key=manifest_key, ExpectedBucketOwner=self.account,
                                       ChecksumMode="ENABLED")
        except ClientError as error:
            if error.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
                return {"backup_id": backup_id, "state": "UNKNOWN", "code": "BACKUP_MANIFEST_NOT_PUBLISHED"}
            raise ControlError(503, "BACKUP_OBJECT_UNCONFIRMED") from None
        return {"backup_id": backup_id, "state": "COMPLETED", "manifest": {
            "bucket": self.bucket, "key": manifest_key, "version_id": head["VersionId"],
            "sha256": base64.b64decode(head["ChecksumSHA256"]).hex(), "bytes": head["ContentLength"]}}

    # -- restore -------------------------------------------------------------------------------
    def restore(self, request, configuration, reference, restore_id):
        manifest = json.loads(self.get(reference))
        space, generation = configuration["space_id"], configuration["generation"]
        if (manifest.get("format_version") != FORMAT_VERSION or manifest.get("kind") != "colvenna_native_backup"
                or manifest["space_id"] != space or manifest["generation"] != generation
                or reference["key"] != self.prefix(space, generation, manifest["backup_id"]) + "manifest.json"):
            raise ControlError(409, "BACKUP_IDENTITY_MISMATCH")
        actual = runtime_identity()
        if {name: manifest["runtime"][name] for name in actual} != actual:
            raise ControlError(409, "BACKUP_RUNTIME_MISMATCH")
        rehearse(manifest)
        directory = self.work / "restores" / restore_id
        shutil.rmtree(directory, ignore_errors=True)
        directory.mkdir(parents=True)
        timing = {}
        try:
            started = time.monotonic()
            for index, table in enumerate(manifest["tables"]):
                part = directory / ("t%03d" % index)
                part.mkdir()
                table["local"] = []
                for number, file in enumerate(table["files"]):
                    if file["bucket"] != self.bucket or not file["key"].startswith(
                            self.prefix(space, generation, manifest["backup_id"])):
                        raise ControlError(409, "BACKUP_IDENTITY_MISMATCH")
                    path = part / ("part_%04d.parquet" % number)
                    self.get(file, path)
                    table["local"].append(str(path))
            timing["download_ms"] = round((time.monotonic() - started) * 1000, 3)
            loaded = time.monotonic()
            result = self.unit("import", configuration,
                               lambda: self.load(request, configuration, manifest, timing))
            timing["load_and_queue_ms"] = round((time.monotonic() - loaded) * 1000, 3)
            return {"restore_id": restore_id, "backup_id": manifest["backup_id"], "point": manifest["point"],
                    "tables": len(manifest["tables"]), "rows": result["rows"], "timing": timing}
        finally:
            shutil.rmtree(directory, ignore_errors=True)

    def load(self, request, configuration, manifest, timing):
        """Runs on the scheduler under the native lock: all tables in one native transaction."""
        started = time.monotonic()
        self.runtime.guard(request["epoch"])
        database = self.runtime.database(configuration)
        c = database.connection
        references = {(value["schema"], value["table"]): [tuple(parent) for parent in value["references"]]
                      for value in manifest["tables"]}
        rows = 0
        c.execute("BEGIN")
        try:
            for schema, table in self.pristine(c, configuration):
                c.execute("DROP TABLE warehouse." + quoted(schema) + "." + quoted(table))
            for schema in manifest["schemas"]:
                c.execute("CREATE SCHEMA IF NOT EXISTS warehouse." + quoted(schema))
            # DuckDB resolves the recorded unqualified DDL and FK references in the current catalog.
            c.execute("USE warehouse")
            tables = ordered(manifest["tables"], references)
            for table in tables:
                c.execute(table["sql"])
            for table in tables:
                columns = ",".join(("CAST(" + quoted(name) + " AS " + kind + ")" if text else quoted(name))
                                   for name, kind, text in table["columns"])
                c.execute("INSERT INTO warehouse." + quoted(table["schema"]) + "." + quoted(table["table"])
                          + " SELECT " + columns + " FROM read_parquet(?)", [table["local"]])
            for index in manifest["indexes"]:
                c.execute(index["sql"])
            for table in tables:
                if (row_digest(c, table["schema"], table["table"]) != table["digest"]
                        or digest(physical(c, table["schema"] + "." + table["table"])) != table["physical_sha256"]):
                    raise ControlError(409, "RESTORE_VERIFICATION_FAILED")
                rows += table["digest"]["rows"]
            identity = c.execute("SELECT space_id,generation,format_version FROM warehouse.ctl.space_identity "
                                 "WHERE slot=1").fetchone()
            sequence = c.execute("SELECT sequence FROM warehouse.ctl.commit_sequence WHERE slot=1").fetchone()
            if (identity != (configuration["space_id"], configuration["generation"], 6)
                    or sequence != (manifest["point"]["commit_sequence"],)):
                raise ControlError(409, "RESTORE_VERIFICATION_FAILED")
            self.authorize(request)
        except BaseException:
            try:
                c.execute("ROLLBACK")
            except Exception:
                database.ingestions.unknown = True
                raise ControlError(503, "RESTORE_ROLLBACK_UNKNOWN") from None
            raise
        try:
            c.execute("COMMIT")
        except Exception:
            # The load may be durable; the handle is retired until an approved process reconciles.
            database.ingestions.unknown = True
            raise ControlError(503, "RESTORE_COMMIT_UNKNOWN") from None
        timing["native_lock_ms"] = round((time.monotonic() - started) * 1000, 3)
        return {"rows": rows}

    @staticmethod
    def pristine(c, configuration):
        """Tables a restore may replace: none, or only the writer's own initial ledger of this
        generation with no commit (sequence 0) and no other rows. Anything else is refused."""
        tables = c.execute("SELECT schema_name,table_name FROM duckdb_tables() WHERE database_name='warehouse' "
                           "ORDER BY schema_name,table_name").fetchall()
        if not tables:
            return []
        names = {schema + "." + table for schema, table in tables}
        counters = {"ctl.space_identity", "ctl.commit_sequence", "ctl.observation_sequence"}
        if (not counters <= names or any(not schema == "ctl" for schema, _ in tables)
                or c.execute("SELECT space_id,generation,format_version FROM warehouse.ctl.space_identity "
                             "WHERE slot=1").fetchone()
                != (configuration["space_id"], configuration["generation"], 6)
                or c.execute("SELECT sequence FROM warehouse.ctl.commit_sequence").fetchall() != [(0,)]
                or c.execute("SELECT sequence FROM warehouse.ctl.observation_sequence").fetchall() != [(0,)]
                or any(c.execute("SELECT count(*) FROM warehouse.ctl." + quoted(table)).fetchone()[0]
                       for _, table in tables if "ctl." + table not in counters)):
            raise ControlError(409, "RESTORE_TARGET_NOT_EMPTY")
        return tables
