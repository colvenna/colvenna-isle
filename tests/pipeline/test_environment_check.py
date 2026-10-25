"""The environment check runs the controlled SQL task path with the installed DuckDB."""

import unittest
from unittest.mock import patch

from colvenna_isle.pipeline import sql_task
from colvenna_isle.pipeline.environment_adapter import sql_check


class EnvironmentSqlCheck(unittest.TestCase):
    def test_the_installed_duckdb_runs_the_controlled_sql_path(self):
        sql_check()

    def test_an_sql_task_that_cannot_stage_its_output_fails_the_check(self):
        # SQL tasks stream their result into Parquet; a check that skips this path qualifies nothing.
        with patch.object(sql_task, "write", side_effect=RuntimeError("SQL_OUTPUT_UNAVAILABLE")), \
                self.assertRaisesRegex(RuntimeError, "SQL_OUTPUT_UNAVAILABLE"):
            sql_check()


if __name__ == "__main__":
    unittest.main()
