"""Real DuckDB isolation: values, types, native capabilities and concurrent users."""

import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from colvenna_isle.control.registry import ControlError
from colvenna_isle.query.sql import Session, references


def definition():
    return {"dataset": "ods.orders", "schema_version": 1, "write_owner": "direct",
        "schema": [{"name": "id", "type": "BIGINT", "nullable": False},
                   {"name": "amount", "type": "DECIMAL(18,2)", "nullable": False},
                   {"name": "at", "type": "TIMESTAMPTZ", "nullable": False}],
        "key_columns": ["id"], "partition_columns": [],
        "constraints": {"primary_key": ["id"]}}


class QuerySQL(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.session = self.make_session("one", Decimal("12.30"))

    def tearDown(self):
        self.session.close()
        self.temporary.cleanup()

    def make_session(self, name, amount):
        session = Session(Path(self.temporary.name) / name)
        session.load(definition(), [[(1, amount, datetime(2026, 10, 16, tzinfo=timezone.utc)),
                                   (9007199254740993, Decimal("0.10"),
                                    datetime(2026, 10, 16, tzinfo=timezone.utc))]],
            {"dataset": "ods.orders", "release_id": None, "commit_sequence": 7,
             "generation": "gen_fixture", "snapshot_sequence": 7})
        session.seal()
        return session

    def denied(self, sql, codes):
        with self.assertRaises(ControlError) as caught:
            self.session.execute(sql, 100)
        self.assertIn(caught.exception.code, codes)

    def test_select_with_aggregate_and_explain_return_real_values(self):
        result = self.session.execute('SELECT id,amount,"at" FROM ods.orders ORDER BY id', 100)
        self.assertEqual(result["rows"], [
            [1, "12.30", "2026-10-16T00:00:00Z"],
            ["9007199254740993", "0.10", "2026-10-16T00:00:00Z"]])
        result = self.session.execute(
            "WITH amounts AS (SELECT amount FROM ods.orders) SELECT sum(amount),count(*) FROM amounts", 100)
        self.assertEqual(result["rows"], [["12.40", 2]])
        self.assertIn("DECIMAL", result["columns"][0]["type"])
        plan = self.session.execute("EXPLAIN SELECT sum(amount) FROM ods.orders", 100)
        self.assertIn("SEQ_SCAN", plan["rows"][0][1])

    def test_write_setting_extension_and_multi_statement_sql_is_denied(self):
        for sql in [
            "INSERT INTO ods.orders VALUES (2,1.00,now())", "DELETE FROM ods.orders",
            "UPDATE ods.orders SET amount=999", "CREATE TABLE p AS SELECT 1",
            "COPY ods.orders TO '/tmp/export.csv'", "ATTACH ':memory:' AS peer",
            "SET enable_external_access=true", "INSTALL httpfs", "LOAD httpfs",
            "PRAGMA enable_profiling", "PRAGMA database_list", "  /* c */ pragma show_tables", "SELECT 1; SELECT 2",
        ]:
            self.denied(sql, {"READ_ONLY_SQL_REQUIRED"})
        self.assertEqual(self.session.execute("SELECT amount FROM ods.orders WHERE id=1", 10)["rows"],
                         [["12.30"]])

    def test_ledgers_unpublished_generations_and_other_space_do_not_exist(self):
        for sql in [
            "SELECT * FROM ctl.writes", "SELECT * FROM warehouse.ods.orders",
            "SELECT * FROM other_space.ods.orders", "SELECT * FROM ods.orders_unpublished",
            "SELECT * FROM query_table('ctl.ingestions')",
        ]:
            self.denied(sql, {"INVALID_OR_UNAUTHORIZED_SQL"})
        self.denied("SELECT * FROM duckdb_secrets()", {"EXTERNAL_ACCESS_FORBIDDEN"})
        self.assertEqual(self.session.catalog, [{"dataset": "ods.orders", "schema": definition()["schema"]}])

    def test_external_file_network_and_glob_access_is_disabled_by_engine(self):
        fixture = Path(self.temporary.name) / "private.csv"
        fixture.write_text("secret\nanother-principal\n")
        for sql in [
            f"SELECT * FROM read_csv('{fixture}')",
            f"SELECT * FROM read_text('{fixture}')",
            "SELECT * FROM read_parquet('s3://private-customer/secret.parquet')",
            "SELECT * FROM glob('/tmp/*')",
        ]:
            self.denied(sql, {"EXTERNAL_ACCESS_FORBIDDEN", "INVALID_OR_UNAUTHORIZED_SQL"})

    def test_shared_process_and_concurrent_users_cannot_read_each_others_values(self):
        other = self.make_session("two", Decimal("987.65"))
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(lambda session: session.execute(
                    "SELECT amount FROM ods.orders WHERE id=1", 10), [self.session, other]))
            self.assertEqual([value["rows"] for value in results], [[["12.30"]], [["987.65"]]])
        finally:
            other.close()

    def test_result_limit_is_explicit_and_preserves_snapshot_version(self):
        result = self.session.execute("SELECT id FROM ods.orders ORDER BY id", 1)
        self.assertEqual(result["rows"], [[1]])
        self.assertTrue(result["truncated"])
        self.assertEqual(result["data_versions"], [
            {"dataset": "ods.orders", "release_id": None, "commit_sequence": 7,
             "generation": "gen_fixture", "snapshot_sequence": 7}])

    def test_explain_analyze_cannot_execute_a_write_on_readonly_snapshot(self):
        for sql in ["EXPLAIN ANALYZE DELETE FROM ods.orders",
                    "EXPLAIN /* nested /* comment */ ends */ ANALYZE CREATE TEMP TABLE hidden AS SELECT 1",
                    "EXPLAIN (FORMAT JSON) INSERT INTO ods.orders VALUES (2,1.00,now())"]:
            self.denied(sql, {"READ_ONLY_SQL_REQUIRED"})
        self.assertEqual(self.session.execute("SELECT count(*) FROM ods.orders", 10)["rows"], [[2]])

    def test_duplicate_aliases_and_lossless_interval_nanosecond_values(self):
        result = self.session.execute(
            "SELECT 1 AS a,2 AS a,INTERVAL '1 month', "
            "'2026-10-16 00:00:00.123456789'::TIMESTAMP_NS", 10)
        self.assertEqual(result["rows"], [[1, 2, "1 month", "2026-10-16 00:00:00.123456789"]])
        self.assertEqual([column["name"] for column in result["columns"][:2]], ["a", "a"])
        self.assertEqual([column["encoding"] for column in result["columns"][2:]],
                         ["native_text", "native_text"])

    def test_explain_options_and_nested_comments_use_the_engine_parser(self):
        result = self.session.execute(
            "-- prefix\nEXPLAIN /* nested /* inner */ end */ (FORMAT JSON) "
            "WITH v AS (SELECT id FROM ods.orders) SELECT * FROM v", 10)
        self.assertIn("SEQ_SCAN", result["rows"][0][1])

    def test_data_loading_cannot_change_a_sealed_session(self):
        with self.assertRaises(ControlError) as caught:
            self.session.load(definition(), [], {})
        self.assertEqual(caught.exception.code, "QUERY_SNAPSHOT_SEALED")

    def test_references_use_the_engine_parser_without_running_sql(self):
        fixture = Path(self.temporary.name) / "private.csv"
        fixture.write_text("secret\n")
        registered = ["ods.orders", "dwd.a", "dws.b", "dwd.Mixed", "ads.t"]
        for sql, expected in [
            ("SELECT 1", []), ("SELECT * FROM ODS.Orders", ["ods.orders"]),
            ('SELECT * FROM "ods"."orders"', ["ods.orders"]), ("SELECT * FROM snapshot.ods.orders", ["ods.orders"]),
            ("WITH v AS (SELECT * FROM dwd.a) SELECT * FROM v JOIN dws.b USING (id)", ["dwd.a", "dws.b"]),
            ("SELECT * FROM dwd.a NATURAL JOIN dws.b", ["dwd.a", "dws.b"]),
            ("SELECT (SELECT max(id) FROM ads.t) FROM dwd.mixed WHERE id IN (SELECT id FROM dws.b)",
             ["ads.t", "dwd.Mixed", "dws.b"]),
            ("-- prefix\nEXPLAIN /* nested /* inner */ end */ (FORMAT JSON) SELECT * FROM dwd.a", ["dwd.a"]),
            ("EXPLAIN ANALYZE WITH v AS (SELECT * FROM ads.t) SELECT * FROM v", ["ads.t"]),
            ("SELECT * FROM query_table('dws.b')", ["dws.b"]), ("DESCRIBE ads.t", ["ads.t"]),
            ("SELECT * FROM ods.unregistered, \"ods.orders\"", []),
            (f"SELECT * FROM read_csv('{fixture}')", []), ("SELECT * FROM", []), ("", []),
        ]:
            with self.subTest(sql=sql):
                self.assertEqual(references([sql], registered), expected)
        self.assertEqual(references(["SELECT * FROM dwd.a", None, "SELECT * FROM ads.t"], registered),
                         ["ads.t", "dwd.a"])

    def test_published_session_checks_only_referenced_unstaged_datasets(self):
        self.session.publications_only(["ods.orders", "dwd.hidden"])
        result = self.session.execute("SELECT count(*) FROM ods.orders a JOIN ods.orders b USING (id)", 10)
        self.assertEqual(result["rows"], [[2]])
        for sql in ["SELECT * FROM DWD.HIDDEN", "SELECT * FROM ods.orders JOIN dwd.hidden USING (id)",
                    "SELECT * FROM query_table('dwd.hidden')"]:
            self.denied(sql, {"PUBLICATION_NOT_AVAILABLE"})
        self.denied("SELECT * FROM", {"INVALID_OR_UNAUTHORIZED_SQL"})


if __name__ == "__main__":
    unittest.main()
