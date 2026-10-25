"""The writer's ingestion projector: one opened database's failure never stalls the others."""

import threading
import unittest
from types import SimpleNamespace

from colvenna_isle.control.registry import ControlError
from colvenna_isle.writer.runtime import Runtime


class Ingestions:
    def __init__(self, pages=None, failure=None):
        self.pages, self.failure = pages or [], failure

    def observations(self, cursor):
        if self.failure:
            raise self.failure
        return [value for value in self.pages if value["projection_version"] > cursor]


class Projection:
    def __init__(self):
        self.written = []

    def write(self, value):
        self.written.append(value["ingestion_id"])
        return "STORED"


class ProjectionIsolationTests(unittest.TestCase):
    def test_a_failing_database_is_logged_with_its_code_and_the_other_still_projects(self):
        logged = []
        runtime = SimpleNamespace(
            lock=threading.Lock(), projector_paused=False, projection=Projection(),
            log=lambda event, fields=None: logged.append((event, fields)),
            databases={
                "broken/gen_a": SimpleNamespace(ingestions=Ingestions(failure=ControlError(503, "NATIVE_BUSY"))),
                "healthy/gen_b": SimpleNamespace(ingestions=Ingestions([
                    {"ingestion_id": "ing_one", "projection_version": 1},
                    {"ingestion_id": "ing_two", "projection_version": 2}]))})
        cursors = {}
        Runtime.project_once(runtime, cursors)
        self.assertEqual(runtime.projection.written, ["ing_one", "ing_two"])
        self.assertEqual(cursors, {"healthy/gen_b": 2})
        self.assertEqual(logged, [("PROJECTION_UNAVAILABLE", {"exception_type": "ControlError",
                                                              "code": "NATIVE_BUSY", "database": "broken/gen_a"})])
        Runtime.project_once(runtime, cursors)
        self.assertEqual(runtime.projection.written, ["ing_one", "ing_two"])


if __name__ == "__main__":
    unittest.main()
