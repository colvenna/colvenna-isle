"""Bounded staged-Parquet contention probe; it does not publish a pipeline version."""

import hashlib
import re
import threading
import time

from colvenna_isle.control.registry import ControlError, canonical
from .ingestion import digest, quoted
from .ownership import matches
from .scheduler import NotSubmitted


class QualificationImports:
    CHUNK_ROWS = 500
    MAX_ROWS = 100000
    MAX_BYTES = 67108864
    SCHEMA = [{"name": "id", "type": "BIGINT", "nullable": False},
              {"name": "value", "type": "VARCHAR", "nullable": False}]

    def __init__(self, runtime):
        self.runtime, self.jobs, self.lock = runtime, {}, threading.Lock()

    def authorize(self, request):
        runtime = self.runtime
        if not matches(request.get("identity", {}), runtime.identity):
            raise ControlError(409, "WRITER_IDENTITY_CHANGED")
        if type(request.get("epoch")) is not int or request["epoch"] < 1:
            raise ControlError(400, "WRITER_EPOCH_REQUIRED")
        runtime.guard(request["epoch"])
        configuration = runtime.registry._read(request.get("space_id"))
        runtime.write_qualification.require(configuration)
        if configuration["generation"] != request.get("generation"):
            raise ControlError(409, "SPACE_GENERATION_CHANGED")
        registered = configuration["datasets"].get("dwd.import_probe")
        if (not registered or registered["definition"].get("write_owner") != "pipeline"
                or registered["definition"]["schema"] != self.SCHEMA):
            raise ControlError(409, "QUALIFICATION_IMPORT_TARGET_REQUIRED")
        return configuration, registered

    def execute(self, request):
        configuration, registered = self.authorize(request)
        identity = request.get("import_id")
        if not re.fullmatch(r"import_[0-9a-f]{64}", identity or ""):
            raise ControlError(400, "INVALID_QUALIFICATION_IMPORT")
        if request.get("phase") == "status":
            with self.runtime.lock:
                database = self.runtime.database(configuration)
                return self.progress(database, identity)
        if request.get("phase") != "start":
            raise ControlError(400, "INVALID_QUALIFICATION_IMPORT")
        source = request.get("source")
        prefix = (f"spaces/{configuration['space_id']}/qualification-import/"
                  f"{configuration['generation']}/{identity}.parquet")
        if (not isinstance(source, dict) or set(source) != {"bucket", "key", "version_id", "sha256", "bytes", "rows"}
                or source["bucket"] != self.runtime.consumer.bucket or source["key"] != prefix
                or not isinstance(source["version_id"], str) or not 1 <= len(source["version_id"]) <= 1024
                or source["version_id"] == "null" or not re.fullmatch(r"[0-9a-f]{64}", source["sha256"])
                or type(source["bytes"]) is not int or not 1 <= source["bytes"] <= self.MAX_BYTES
                or type(source["rows"]) is not int or not 1 <= source["rows"] <= self.MAX_ROWS):
            raise ControlError(400, "INVALID_QUALIFICATION_IMPORT_SOURCE")
        binding = digest([configuration["space_id"], configuration["generation"],
                          self.runtime.identity["source_commit"], registered["definition_sha256"], source])
        with self.lock:
            prior = self.jobs.get(identity)
            if prior:
                if prior["binding"] != binding:
                    raise ControlError(409, "QUALIFICATION_IMPORT_CONFLICT")
                return {"import_id": identity, "state": prior["state"], "scope": "CONTENTION_PROBE_NOT_PUBLISHING"}
            if len(self.jobs) >= 2:
                raise ControlError(429, "QUALIFICATION_IMPORT_LIMIT")
            job = {"binding": binding, "state": "PREPARING"}
            self.jobs[identity] = job
        threading.Thread(target=self.run, args=(request, configuration, registered, identity, job),
                         daemon=True, name="qualification-import-" + identity[-8:]).start()
        return {"import_id": identity, "state": "PREPARING", "scope": "CONTENTION_PROBE_NOT_PUBLISHING"}

    def prepare(self, database, identity, source):
        path = database.work / (identity + ".parquet")
        value = self.runtime.consumer.s3.get_object(
            Bucket=source["bucket"], Key=source["key"], VersionId=source["version_id"],
            ExpectedBucketOwner=self.runtime.consumer.account)
        if value.get("VersionId") != source["version_id"]:
            raise ControlError(409, "QUALIFICATION_IMPORT_VERSION_CHANGED")
        sha, count = hashlib.sha256(), 0
        with value["Body"] as stream, path.open("xb") as output:
            while block := stream.read(1048576):
                count += len(block)
                if count > self.MAX_BYTES or count > source["bytes"]:
                    raise ControlError(413, "QUALIFICATION_IMPORT_BYTES_EXCEEDED")
                sha.update(block)
                output.write(block)
        if count != source["bytes"] or sha.hexdigest() != source["sha256"]:
            raise ControlError(409, "QUALIFICATION_IMPORT_HASH_CHANGED")
        import duckdb
        # File decoding/preparation uses memory only, without another attachment.
        reader = duckdb.connect(config={"threads": 1, "memory_limit": "128MB",
                                       "autoload_known_extensions": False, "autoinstall_known_extensions": False})
        columns = reader.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
        if [(column[0], column[1]) for column in columns] != [("id", "BIGINT"), ("value", "VARCHAR")]:
            reader.close()
            raise ControlError(409, "QUALIFICATION_IMPORT_SCHEMA_CHANGED")
        if reader.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0] != source["rows"]:
            reader.close()
            raise ControlError(409, "QUALIFICATION_IMPORT_ROW_COUNT_CHANGED")
        return path, reader

    def initialize(self, database, identity, binding, rows, recheck):
        native, c = database.ingestions, database.connection
        native.initialize()
        candidate = "_colvenna_import_" + identity.removeprefix("import_")[:24]

        def create():
            c.execute("CREATE TABLE IF NOT EXISTS warehouse.ctl.qualification_imports "
                      "(import_id VARCHAR PRIMARY KEY,binding_sha256 VARCHAR NOT NULL,"
                      "candidate VARCHAR NOT NULL,row_count BIGINT NOT NULL)")
            c.execute("CREATE TABLE IF NOT EXISTS warehouse.ctl.qualification_import_chunks "
                      "(import_id VARCHAR,chunk_number INTEGER,chunk_sha256 VARCHAR NOT NULL,"
                      "commit_sequence BIGINT UNIQUE NOT NULL,rows BIGINT NOT NULL,receipt VARCHAR NOT NULL,"
                      "PRIMARY KEY(import_id,chunk_number))")
            prior = c.execute("SELECT binding_sha256,candidate,row_count FROM warehouse.ctl.qualification_imports "
                              "WHERE import_id=?", [identity]).fetchone()
            if prior:
                if prior != (binding, candidate, rows):
                    raise ControlError(409, "QUALIFICATION_IMPORT_CONFLICT")
                return
            c.execute("CREATE TABLE warehouse.dwd." + quoted(candidate) + " (id BIGINT PRIMARY KEY,value VARCHAR NOT NULL)")
            c.execute("INSERT INTO warehouse.ctl.qualification_imports VALUES (?,?,?,?)",
                      [identity, binding, candidate, rows])
        native.transaction(create, recheck)
        return candidate

    def chunk(self, database, identity, candidate, number, rows, recheck):
        native, c, expected = database.ingestions, database.connection, digest(rows)
        prior = c.execute("SELECT chunk_sha256 FROM warehouse.ctl.qualification_import_chunks "
                          "WHERE import_id=? AND chunk_number=?", [identity, number]).fetchone()
        if prior:
            if prior[0] != expected:
                raise ControlError(409, "QUALIFICATION_IMPORT_CHUNK_CONFLICT")
            return

        def write():
            count = c.execute("SELECT count(*) FROM warehouse.ctl.qualification_import_chunks WHERE import_id=?",
                              [identity]).fetchone()[0]
            if count != number:
                raise ControlError(409, "QUALIFICATION_IMPORT_ORDER_CHANGED")
            sequence = c.execute(
                "UPDATE warehouse.ctl.commit_sequence SET sequence=sequence+1 WHERE slot=1 "
                "RETURNING sequence").fetchone()[0]
            for row in rows:
                c.execute("INSERT INTO warehouse.dwd." + quoted(candidate) + " VALUES (?,?)", list(row))
            receipt = {"import_id": identity, "chunk_number": number, "commit_sequence": sequence,
                       "rows": len(rows), "sha256": expected}
            c.execute("INSERT INTO warehouse.ctl.qualification_import_chunks VALUES (?,?,?,?,?,?)",
                      [identity, number, expected, sequence, len(rows), canonical(receipt)])
        native.transaction(write, recheck)

    def unit(self, identity, number, action):
        while True:
            try:
                return self.runtime.scheduler.submit("import", identity + "/" + str(number), action).future.result()
            except NotSubmitted:
                # Known queue rejection, not an uncertain Native COMMIT.
                self.runtime.guard()
                time.sleep(0.05)

    def run(self, request, configuration, registered, identity, job):
        reader = None
        try:
            with self.runtime.lock:
                self.authorize(request)
                database = self.runtime.database(configuration)
            path, reader = self.prepare(database, identity, request["source"])

            def recheck():
                current, target = self.authorize(request)
                if (current["route_version"] != configuration["route_version"]
                        or target["definition_sha256"] != registered["definition_sha256"]):
                    raise ControlError(409, "QUALIFICATION_IMPORT_CONFIGURATION_CHANGED")
            candidate = self.unit(identity, "identity", lambda: self.initialize(
                database, identity, job["binding"], request["source"]["rows"], recheck))
            job["state"] = "IMPORTING"
            for number, offset in enumerate(range(0, request["source"]["rows"], self.CHUNK_ROWS)):
                rows = reader.execute("SELECT id,value FROM read_parquet(?) ORDER BY id LIMIT ? OFFSET ?",
                                      [str(path), self.CHUNK_ROWS, offset]).fetchall()
                self.unit(identity, number, lambda rows=rows, number=number:
                          self.chunk(database, identity, candidate, number, rows, recheck))
            job["state"] = "COMMITTED"
        except Exception as error:
            job["state"] = "UNKNOWN"
            self.runtime.log("QUALIFICATION_IMPORT_UNKNOWN", {
                "import_id": identity, "exception_type": type(error).__name__})
        finally:
            if reader:
                reader.close()

    def progress(self, database, identity):
        database.ingestions.initialize()
        c = database.connection
        exists = c.execute(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name='warehouse' AND schema_name='ctl' "
            "AND table_name='qualification_imports'").fetchone()[0]
        row = c.execute("SELECT candidate,row_count FROM warehouse.ctl.qualification_imports WHERE import_id=?",
                        [identity]).fetchone() if exists else None
        if not row:
            return {"import_id": identity, "state": "UNKNOWN"}
        chunks = c.execute("SELECT chunk_number,commit_sequence,rows FROM warehouse.ctl.qualification_import_chunks "
                           "WHERE import_id=? ORDER BY chunk_number LIMIT 201", [identity]).fetchall()
        observed = c.execute("SELECT count(*),min(id),max(id),sum(id) FROM warehouse.dwd." + quoted(row[0])).fetchone()
        if observed[0] != sum(chunk[2] for chunk in chunks):
            raise ControlError(503, "QUALIFICATION_IMPORT_LEDGER_MISMATCH")
        return {"import_id": identity, "state": "COMMITTED" if observed[0] == row[1] else "UNKNOWN",
                "expected_rows": row[1], "observed": observed, "chunks": chunks,
                "scope": "CONTENTION_PROBE_NOT_PUBLISHING"}
