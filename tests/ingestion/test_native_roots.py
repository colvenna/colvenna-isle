"""Real native VFS regression for independent roots in one writer process."""

import tempfile
import unittest
from pathlib import Path

import duckdb

from colvenna_isle.writer.database import native_uri


class NativeRoots(unittest.TestCase):
    def test_two_live_roots_and_cold_reopen_preserve_independent_rows(self):
        self.assertEqual(duckdb.__version__, "1.5.6")
        connections = []
        with tempfile.TemporaryDirectory() as folder:
            def open_root(name):
                c = duckdb.connect(config={"allow_unsigned_extensions": True,
                    "autoload_known_extensions": False, "autoinstall_known_extensions": False})
                connections.append(c)
                for extension in ["httpfs", "aws", "duckdb_object_storage"]:
                    c.execute("LOAD '/opt/colvenna/artifacts/" + extension + ".duckdb_extension'")
                c.execute("SET duckdb_objfs_backend='local'")
                c.execute("SET duckdb_objfs_root=?", [str(Path(folder) / name)])
                configuration = {"database_uri": "s3://test-bucket/spaces/" + name + "/gen_fixed/warehouse"}
                c.execute("ATTACH '" + native_uri(configuration) + "' AS warehouse")
                return c
            try:
                for name in ["primary", "other"]:
                    c = open_root(name)
                    c.execute("CREATE TABLE warehouse.proof(name VARCHAR)")
                    c.execute("INSERT INTO warehouse.proof VALUES (?)", [name])
                self.assertEqual(connections[0].execute("SELECT * FROM warehouse.proof").fetchall(), [("primary",)])
                self.assertEqual(connections[1].execute("SELECT * FROM warehouse.proof").fetchall(), [("other",)])
                for c in connections:
                    c.execute("CHECKPOINT warehouse")
                    c.close()
                connections.clear()
                for name in ["primary", "other"]:
                    self.assertEqual(open_root(name).execute("SELECT * FROM warehouse.proof").fetchall(), [(name,)])
            finally:
                for c in connections:
                    c.close()


if __name__ == "__main__":
    unittest.main()
