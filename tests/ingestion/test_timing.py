"""Per-request segment timing: nesting, approved fields only, and the off switch."""

import contextlib
import io
import json
import os
import unittest
from unittest.mock import patch

import test_direct as fixture_module
from colvenna_isle.timing import NOT_TIMED, Timing, io_delta

APPROVED = {"event", "path", "request_id", "space_id", "generation", "write_id", "ingestion_id", "message_id",
            "receive_count", "dataset", "operation", "outcome", "process", "total_ms", "segments",
            "unattributed_ms", "counters"}


class TimingTests(unittest.TestCase):
    def test_nested_segments_report_parent_remainders_and_adopted_remote_segments(self):
        with patch.dict(os.environ, {"COLVENNA_TIMING": "on"}):
            timing = Timing("sync_write", request_id="r1")
            with timing.segment("writer_rpc"):
                pass
            timing.adopt({"process": "writer", "total_ms": 3.0,
                          "segments": [{"name": "unit", "parent": None, "ms": 2.0},
                                       {"name": "business", "parent": "unit", "ms": 1.5}]}, "writer_rpc")
            timing.record("queue_wait", 40, source="SQS_SERVICE_TIMESTAMPS")
            value = timing.value("sync")
        names = {segment["name"]: segment for segment in value["segments"]}
        self.assertEqual(names["writer.unit"]["parent"], "writer_rpc")
        self.assertEqual(names["writer.business"]["parent"], "writer.unit")
        self.assertEqual(names["writer"]["parent"], "writer_rpc")
        # The service-timestamp interval is reported, never added to this process's own timeline.
        self.assertGreaterEqual(value["unattributed_ms"]["total"], 0)
        self.assertAlmostEqual(value["unattributed_ms"]["writer.unit"], 0.5)
        self.assertAlmostEqual(value["unattributed_ms"]["writer_rpc"], names["writer_rpc"]["ms"] - 2.0 - 3.0, 3)

    def test_off_switch_records_and_emits_nothing(self):
        with patch.dict(os.environ, {"COLVENNA_TIMING": "off"}):
            timing = Timing("sync_write", request_id="r1")
            with timing.segment("authenticate"):
                pass
            stream = io.StringIO()
            with contextlib.redirect_stdout(stream):
                self.assertIsNone(timing.emit("200:COMMITTED"))
        self.assertEqual((timing.segments, stream.getvalue()), ([], ""))
        with NOT_TIMED.segment("unit"):
            NOT_TIMED.bind(write_id="w")
        self.assertEqual(NOT_TIMED.identity, {})

    def test_objfs_counter_delta_reports_only_changed_operations(self):
        self.assertEqual(io_delta({"write": (2, 100, 10.0), "read": (5, 50, 5.0)},
                                  {"write": (3, 180, 14.5), "read": (5, 50, 5.0)}),
                         {"write": {"requests": 1, "bytes": 80, "latency_ms": 4.5}})
        self.assertIsNone(io_delta(None, {}))

    def test_every_direct_operation_is_timed_and_commits(self):
        fixture = fixture_module.DirectTransactionTests()
        fixture.setUp()
        try:
            stream = io.StringIO()
            forms = [("insert", {"records": [{"id": 5, "code": "five", "amount": "1.00", "region": "west"}]}),
                     ("upsert", {"records": [{"id": 5, "code": "five", "amount": "2.00", "region": "east"}]}),
                     ("update", {"updates": [{"key": {"id": 5}, "set": {"amount": "3.00"}},
                                             {"key": {"id": 99}, "set": {"region": "none"}}]}),
                     ("delete", {"keys": [{"id": 5}, {"id": 99}]})]
            with patch.dict(os.environ, {"COLVENNA_TIMING": "on"}), contextlib.redirect_stdout(stream):
                results = [fixture.direct.execute(fixture.rpc(operation, {
                    "dataset": "dwd.orders", "schema_version": 1, "operation": operation, **form}))
                    for operation, form in forms]
            self.assertEqual([value["status"] for value in results], [200, 200, 200, 200])
            lines = [json.loads(line) for line in stream.getvalue().splitlines() if "COLVENNA_TIMING" in line]
            self.assertEqual([(line["operation"], line["counters"]["records"]) for line in lines],
                             [("insert", 1), ("upsert", 1), ("update", 2), ("delete", 2)])
        finally:
            fixture.tearDown()

    def test_direct_write_line_names_segments_and_carries_no_business_value_or_credential(self):
        fixture = fixture_module.DirectTransactionTests()
        fixture.setUp()
        try:
            stream = io.StringIO()
            with patch.dict(os.environ, {"COLVENNA_TIMING": "on"}), contextlib.redirect_stdout(stream):
                value = fixture.direct.execute({**fixture.rpc(), "request_id": "gateway-request-1"})
            self.assertEqual(value["status"], 200)
            lines = [json.loads(line) for line in stream.getvalue().splitlines() if "COLVENNA_TIMING" in line]
            self.assertEqual(len(lines), 1)
            line = lines[0]
            self.assertLessEqual(set(line), APPROVED)
            self.assertEqual((line["request_id"], line["write_id"], line["outcome"]),
                             ("gateway-request-1", fixture.identity, "COMMITTED"))
            names = {segment["name"] for segment in line["segments"]}
            self.assertLessEqual({"authorize", "intent", "admission_wait", "execution_wait", "scheduler_wait",
                                  "unit", "identity_registration", "business", "business.sql",
                                  "business.precommit_check", "business.commit"}, names)
            self.assertEqual(value["timing"]["segments"], line["segments"])
            rendered = json.dumps(line)
            payload = fixture.rpc()["payload"]
            # Numeric business values could coincide with a duration's digits; check the textual ones.
            for record in payload["records"]:
                for member in record.values():
                    if isinstance(member, str) and len(member) > 3 and not member.replace(".", "").isdigit():
                        self.assertNotIn(member, rendered)
            self.assertNotIn('"amount"', rendered)
            self.assertNotIn("key_fixture", rendered)
            self.assertNotIn("owner", rendered)
        finally:
            fixture.tearDown()


if __name__ == "__main__":
    unittest.main()
