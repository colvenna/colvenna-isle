"""Native DuckDB 1.5.6 retention custody and trial preview; no AWS acceptance claim."""

import copy
import tempfile
from contextlib import closing
import time
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from colvenna_isle.control.registry import ControlError, Principal, canonical
from colvenna_isle.pipeline.input_exports import InputExports
from colvenna_isle.pipeline.publications import resolve_release
from colvenna_isle.query.native import stage
from colvenna_isle.query.sql import Session
from test_publications import EXACT, NativePublications
import test_writer_publications as commands


class ReleaseRetention(NativePublications):
    def test_only_unprotected_history_is_dropped_and_never_substituted(self):
        old = self.release("retain_old", [[1, Decimal("1.00")]])
        middle = self.release("retain_middle", [[2, Decimal("2.00")]], old["release_id"])
        current = self.release("retain_current", [[3, EXACT]], middle["release_id"])
        candidates = self.store.retention_candidates()
        by_id = {value["release_id"]: value for value in candidates["releases"]}
        self.assertTrue(by_id[current["release_id"]]["current"])
        self.assertIsNotNone(by_id[old["release_id"]]["published_unix"])
        self.assertIn(current["release_id"], candidates["protected"])
        guard = lambda: None
        with self.assertRaisesRegex(ControlError, "PUBLICATION_RELEASE_RETAINED"):
            self.store.collect("col_one", [current["release_id"]], [], [], guard)
        with self.assertRaisesRegex(ControlError, "PUBLICATION_RELEASE_RETAINED"):
            self.store.collect("col_one", [middle["release_id"]], [], [middle["release_id"]], guard)
        evidence = self.store.collect("col_one", [old["release_id"]], [], [], guard)
        self.assertEqual([(value["kind"], value["identity"]) for value in evidence], [("release", old["release_id"])])
        self.assertEqual(self.store.collect("col_two", [old["release_id"]], [], [], guard), [])
        tables = {row[0] for row in self.writer.execute(
            "SELECT table_name FROM duckdb_tables() WHERE schema_name='_colvenna_data'").fetchall()}
        self.assertNotIn(old["physical"]["table"], tables)
        self.assertIn(current["physical"]["table"], tables)
        with self.assertRaisesRegex(ControlError, "PUBLICATION_RELEASE_COLLECTED"):
            resolve_release(self.reader, self.configuration, "dwd.result", old["release_id"])
        # An explicit historical read fails with the collected identity; it never reads another release.
        with self.assertRaisesRegex(ControlError, "PUBLICATION_RELEASE_COLLECTED"):
            with closing(Session(Path(self.temporary.name) / "query")) as snapshot:
                stage(self.reader, self.configuration, snapshot, lambda: None, consistency="published",
                      release_id=old["release_id"])
        self.assertEqual(self.rows(resolve_release(self.reader, self.configuration, "dwd.result")), [(3, EXACT)])
        self.assertEqual(self.rows(middle), [(2, Decimal("2.00"))])
        recorded = self.writer.execute("SELECT kind,identity,collection_id FROM warehouse.ctl.retention_collections"
                                       ).fetchall()
        self.assertEqual(recorded, [("release", old["release_id"], "col_one")])
        # Durable across a reopened native handle.
        self.reopen()
        with self.assertRaisesRegex(ControlError, "PUBLICATION_RELEASE_COLLECTED"):
            resolve_release(self.reader, self.configuration, "dwd.result", old["release_id"])

    def test_revoked_candidate_generation_is_dropped_but_a_published_one_is_retained(self):
        published = self.release("retain_published", [[1, Decimal("1.00")]])
        candidate = self.select(self.grant(job="revoked_job", previous=published["release_id"]), [1])
        self.store.revoke("revoked_job", expected_epoch=candidate["epoch"])
        generations = {value["job_id"]: value for value in self.store.retention_candidates()["generations"]}
        self.assertEqual((generations["revoked_job"]["leftover"], generations["revoked_job"]["reason"]),
                         (True, "REVOKED_CANDIDATE"))
        self.assertFalse(generations["retain_published"]["leftover"])
        with self.assertRaisesRegex(ControlError, "PUBLICATION_GENERATION_RETAINED"):
            self.store.collect("col_one", [], [generations["retain_published"]["generation_id"]], [], lambda: None)
        evidence = self.store.collect("col_one", [], [generations["revoked_job"]["generation_id"]], [], lambda: None)
        self.assertEqual(evidence[0]["reason"], "REVOKED_CANDIDATE")
        self.assertNotIn("revoked_job", {value["job_id"] for value in self.store.retention_candidates()["generations"]})
        self.assertEqual(self.rows(published), [(1, Decimal("1.00"))])


class SampledExport(NativePublications):
    def test_trial_sample_reads_at_most_the_requested_rows_of_the_same_exact_version(self):
        from colvenna_isle.pipeline.native_inputs import capture

        release = self.release("sampled", [[number, Decimal("1.00")] for number in range(1, 8)])
        binding = {"source": {"dataset": "dwd.result", "selection": {"mode": "snapshot"}}}
        sampled = capture(self.reader, self.configuration, binding, Path(self.temporary.name) / "sample",
                          maximum_bytes=1048576, maximum_rows=100, guard=lambda: None, sample_rows=3)
        full = capture(self.reader, self.configuration, binding, Path(self.temporary.name) / "full",
                       maximum_bytes=1048576, maximum_rows=100, guard=lambda: None)
        self.assertEqual((sampled["inputs"]["source"]["file"]["rows"], full["inputs"]["source"]["file"]["rows"]), (3, 7))
        self.assertEqual(sampled["inputs"]["source"]["native_version"], full["inputs"]["source"]["native_version"])
        self.assertEqual(sampled["sample_rows"], 3)
        self.assertEqual(sampled["inputs"]["source"]["native_version"]["release_id"], release["release_id"])
        with self.assertRaisesRegex(ControlError, "INPUT_EXPORT_SAMPLE"):
            capture(self.reader, self.configuration, binding, Path(self.temporary.name) / "bad",
                    maximum_bytes=1048576, maximum_rows=100, guard=lambda: None, sample_rows=0)


class ExportRetention(NativePublications):
    def record(self, identity, state, *, age=0, until=-1, references=None, release=None):
        now = time.time()
        value = {"format_version": 1, "export_id": identity, "execution_id": "exec_" + identity,
                 "binding_sha256": "f" * 64, "space_id": "owned",
                 "generation": "gen_owned", "principal_id": "owner", "state": state, "created_unix": now - age,
                 "retain_until_unix": now + until, "references": references or {}, "retained_bytes": 64,
                 "bindings": {"source": {"dataset": "ods.source", "selection": {"mode": "snapshot"},
                                         **({"release_id": release} if release else {})}},
                 "source_commit": "a" * 40, "image_digest": "sha256:" + "b" * 64}
        self.writer.execute("INSERT INTO warehouse.ctl.input_exports VALUES (?,?,?)",
                            [identity, "f" * 64, canonical(value)])
        return value

    def test_collection_marks_custody_before_deletion_and_active_pins_win(self):
        exports = InputExports(self.native)
        exports.initialize()
        self.record("input_expired", "READY", references={"job": {"state": "RELEASED", "retain_until_unix": 0,
                                                                   "released_unix": 0}})
        self.record("input_pinned", "READY", references={"job": {"state": "ACTIVE", "retain_until_unix": 0}})
        self.record("input_fresh", "READY", until=600)
        self.record("input_failed", "FAILED", age=7200)
        self.record("input_abandoned", "STAGED", until=-90000)
        reasons = {value["export_id"]: value["reason"] for value in exports.candidates()}
        self.assertEqual(reasons, {"input_expired": "RETENTION_EXPIRED", "input_pinned": None, "input_fresh": None,
                                   "input_failed": "FAILED_ORIGINAL", "input_abandoned": "ABANDONED_ORIGINAL"})
        self.assertIsNone({value["export_id"]: value["reason"]
                           for value in exports.candidates(["input_failed"])}["input_failed"])
        with self.assertRaisesRegex(ControlError, "INPUT_EXPORT_RETAINED"):
            exports.mark_collecting("input_pinned", "col_one", [], lambda: None)
        marked = exports.mark_collecting("input_expired", "col_one", [], lambda: None)
        self.assertEqual((marked["state"], marked["collection"]["previous_state"]), ("COLLECTING", "READY"))
        # No consumer can pin or read an original whose objects are being deleted.
        with self.assertRaisesRegex(ControlError, "INPUT_EXPORT_COLLECTED"):
            exports.retain(marked, "late_job", time.time() + 600, lambda: None)
        self.assertFalse(exports.available(marked))
        with self.assertRaisesRegex(ControlError, "INPUT_EXPORT_COLLECTION_CONFLICT"):
            exports.mark_collected("input_expired", "col_other", {"objects": []}, lambda: None)
        done = exports.mark_collected("input_expired", "col_one", {"objects": [{"key": "k", "version_id": "v",
                                                                                "bytes": 64}]}, lambda: None)
        self.assertEqual((done["state"], done["retained_bytes"], done["collection"]["bytes"]), ("COLLECTED", 0, 64))
        self.assertFalse(exports.live(done))
        self.assertEqual(exports.mark_collected("input_expired", "col_one", {"objects": []}, lambda: None)["state"],
                         "COLLECTED")


    def test_only_originals_that_may_still_be_read_bind_their_releases(self):
        exports = InputExports(self.native)
        exports.initialize()
        self.record("input_pinned", "READY", release="release_pinned",
                    references={"job": {"state": "ACTIVE", "retain_until_unix": 0}})
        self.record("input_done", "READY", release="release_done",
                    references={"job": {"state": "RELEASED", "retain_until_unix": 0, "released_unix": 0}})
        self.assertEqual(exports.pinned_releases(), {"release_pinned"})
        exports.mark_collecting("input_done", "col_one", [], lambda: None)
        self.assertEqual(exports.pinned_releases(), {"release_pinned"})
        exports.mark_collected("input_done", "col_one", {"objects": []}, lambda: None)
        # A collected original can never be read again, so its release is collectable by policy.
        self.assertEqual(exports.pinned_releases(), {"release_pinned"})
        self.assertNotIn("release_done", exports.pinned_releases())


class TrialPreview(commands.WriterPublicationCommands):
    def prepared(self, directory, rows):
        schema = [{"name": "id", "type": "BIGINT", "nullable": False},
                  {"name": "amount", "type": "DECIMAL(38,2)", "nullable": False}]
        table = pa.table({"id": pa.array([value[0] for value in rows], pa.int64()),
                          "amount": pa.array([value[1] for value in rows], pa.decimal128(38, 2))})
        output = Path(directory) / "result"
        output.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, output / "part-0.parquet")
        return {"completion": {}, "files": [{"rows": len(rows)}], "directory": str(output), "state": None,
                "manifest": {"schema": schema, "row_count": len(rows),
                             "files": [{"path": "part-0.parquet", "rows": len(rows), "bytes": 100}]}}

    def preview(self, rows, actor_scopes=("pipeline:run", "query:read"), prepare=None):
        from colvenna_isle.pipeline.candidates import Candidates

        self.actor = Principal("owner", frozenset(["owned"]), frozenset(actor_scopes), "key_owner", "gen_owned")
        value = self.fixture.descriptor()
        reference = self.fixture.reference(self.descriptor)
        completion = {"s3_uri": "s3://test-bucket/x/completion.json", "object_version": "c1", "sha256": "c" * 64,
                      "bytes": 10}
        with tempfile.TemporaryDirectory() as directory:
            values = [[9007199254740993, EXACT], [1, Decimal("0.10")], [2, Decimal("0.20")]]
            with patch("colvenna_isle.pipeline.task_objects.TaskObjects.read",
                       return_value=canonical(self.descriptor).encode()), \
                    patch.object(Candidates, "prepare", side_effect=prepare or (
                        lambda *args, **kwargs: self.prepared(directory, values))):
                return self.operation("publication_preview", descriptor=reference, execution_id="exec_one",
                                      completion=completion, rows=rows)

    def test_preview_validates_like_an_import_and_returns_exact_rows_without_a_native_job(self):
        result = self.preview(2)
        self.assertEqual(result["validation"], {"status": "PASSED", "error_code": None, "row_count": 3, "files": 1,
                                                "bytes": 100})
        self.assertEqual(result["preview"]["rows"], [["9007199254740993", str(EXACT)], [1, "0.10"]])
        self.assertTrue(result["preview"]["truncated"])
        with self.assertRaisesRegex(ControlError, "PUBLICATION_JOB_NOT_FOUND"):
            self.fixture.store.status("job_one")
        self.assertEqual(self.preview(0)["validation"]["status"], "PASSED")
        with self.assertRaisesRegex(ControlError, "PREVIEW_QUERY_SCOPE_REQUIRED"):
            self.preview(2, actor_scopes=("pipeline:run",))

    def test_rejected_candidate_is_reported_not_raised(self):
        def reject(*args, **kwargs):
            raise ControlError(422, "PUBLICATION_COMPLETION_BINDING")
        result = self.preview(5, prepare=reject)
        self.assertEqual((result["validation"]["status"], result["validation"]["error_code"], result["preview"]),
                         ("REJECTED", "PUBLICATION_COMPLETION_BINDING", None))


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for case in [ReleaseRetention, SampledExport, ExportRetention, TrialPreview]:
        names = [name for name in loader.getTestCaseNames(case) if name in case.__dict__]
        suite.addTests(case(name) for name in names)
    return suite


if __name__ == "__main__":
    unittest.main()
