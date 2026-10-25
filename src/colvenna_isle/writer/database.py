"""Hash-checked native handles; no local database files or runtime installation."""

import hashlib
import json
from pathlib import Path

from colvenna_isle.region import region


def native_uri(configuration):
    # DuckDB reserves attached file URLs across the whole process, even when
    # distinct instances have independent native VFS roots.
    return "duckdb_objfs://warehouse_" + hashlib.sha256(
        configuration["database_uri"].encode()).hexdigest() + ".db"


class Database:
    def __init__(self, configuration, guard, work=Path("/work")):
        import duckdb

        guard()
        artifacts = Path("/opt/colvenna/artifacts")
        manifest = json.loads((artifacts / "manifest.json").read_text())
        if duckdb.__version__ != manifest["duckdb_version"]:
            raise RuntimeError("Native package identity mismatch")
        for name, expected in manifest["files"].items():
            path = artifacts / name
            if path.parent != artifacts or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                raise RuntimeError("Native extension identity mismatch")
        self.guard = guard
        self.configuration = configuration
        def stage(name):
            print(json.dumps({"event": "NATIVE_STAGE", "stage": name,
                              "space_id": configuration["space_id"],
                              "generation": configuration["generation"]}), flush=True)
        stage("VERIFY_ARTIFACTS")
        self.work = work / configuration["space_id"] / configuration["generation"]
        self.work.mkdir(parents=True, exist_ok=False)
        self.cache_initial_entries = sorted(path.name for path in self.work.iterdir())
        self.connection = duckdb.connect(config={
            "allow_unsigned_extensions": True, "autoload_known_extensions": False,
            "autoinstall_known_extensions": False})
        connection = self.connection
        for name in ["httpfs", "aws", "duckdb_object_storage"]:
            connection.execute("LOAD '" + str(artifacts / f"{name}.duckdb_extension") + "'")
        stage("EXTENSIONS_LOADED")
        connection.execute("SET threads = 2")
        connection.execute("SET memory_limit = '512MB'")
        connection.execute("SET TimeZone = 'UTC'")
        connection.execute("SET duckdb_objfs_memory_cache_size = 67108864")
        connection.execute("SET duckdb_objfs_metadata_cache_size = 16777216")
        connection.execute("SET temp_directory = ?", [str(self.work)])
        uri = configuration["database_uri"]
        bucket, prefix = uri.removeprefix("s3://").split("/", 1)
        for setting, value in [("duckdb_objfs_backend", "s3"), ("duckdb_objfs_bucket", bucket),
                               ("duckdb_objfs_root", prefix)]:
            connection.execute(f"SET {setting} = ?", [value])
        # Default AWS SDK chain includes ECS task-role credentials. Keep refresh
        # enabled; materialized credentials remain internal, never selected/logged.
        connection.execute(
            "CREATE SECRET task_role (TYPE S3, PROVIDER credential_chain, REFRESH 'auto', "
            "REGION '" + region() + "', SCOPE '" + uri.replace("'", "''") + "')")
        stage("TASK_ROLE_SECRET_CREATED")
        guard()
        connection.execute("ATTACH '" + native_uri(configuration) + "' AS warehouse")
        stage("NATIVE_DATABASE_ATTACHED")
        for schema in ["ods", "dwd", "dws", "ads", "ctl"]:
            guard()
            connection.execute(f"CREATE SCHEMA IF NOT EXISTS warehouse.{schema}")
        stage("NATIVE_SCHEMAS_READY")
        self.open_transaction = False
        from colvenna_isle.writer.ingestion import IngestionStore
        self.ingestions = IngestionStore(connection, configuration, guard)
        from colvenna_isle.writer.writes import WriteStore
        self.writes = WriteStore(self.ingestions)

    def checkpoint(self):
        self.guard()
        self.ingestions.require()
        if self.open_transaction:
            raise RuntimeError("Unfinished qualification transaction")
        self.connection.execute("CHECKPOINT warehouse")
        return {"checkpoint": "CONFIRMED"}

    def catalog(self):
        """Bounded, fixed internal read through the writer's own native instance."""
        self.guard()
        self.ingestions.require()
        if self.open_transaction:
            raise RuntimeError("Unfinished qualification transaction")
        return {
            "schemas": self.connection.execute(
                "SELECT schema_name FROM duckdb_schemas() WHERE database_name='warehouse' "
                "ORDER BY schema_name LIMIT 16").fetchall(),
            "tables": self.connection.execute(
                "SELECT schema_name,table_name FROM duckdb_tables() WHERE database_name='warehouse' "
                "ORDER BY schema_name,table_name LIMIT 128").fetchall(),
        }

    def probe_commit(self, row_id, text):
        """Fixed qualification table, never arbitrary SQL or customer dataset."""
        self.guard()
        if self.open_transaction:
            raise RuntimeError("Unfinished qualification transaction")
        c = self.connection
        c.execute("CREATE TABLE IF NOT EXISTS warehouse.ctl.writer_probe "
                  "(id BIGINT PRIMARY KEY, value VARCHAR NOT NULL)")
        c.execute("BEGIN")
        try:
            c.execute("INSERT INTO warehouse.ctl.writer_probe VALUES (?, ?)", [row_id, text])
            permit = self.guard()
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        return {"id": row_id, "value": text, "permit_epoch": permit["epoch"],
                "committed": True}

    def probe_open(self):
        self.guard()
        self.connection.execute("BEGIN")
        self.connection.execute("INSERT INTO warehouse.ctl.writer_probe VALUES (999, 'UNCOMMITTED')")
        self.open_transaction = True
        return {"open_transaction": True, "uncommitted_id": 999}

    def probe_read(self):
        self.guard()
        if self.open_transaction:
            raise RuntimeError("Unfinished qualification transaction")
        return {"rows": self.connection.execute(
            "SELECT id,value FROM warehouse.ctl.writer_probe ORDER BY id").fetchall(),
            "cache_initial_entries": self.cache_initial_entries,
            "io_stats": self.connection.execute("SELECT * FROM duckdb_objfs_io_stats()").fetchall(),
            "cache_stats": self.connection.execute("SELECT * FROM duckdb_objfs_cache_stats()").fetchall()}
