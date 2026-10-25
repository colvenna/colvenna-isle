"""Real native DuckDB 1.5.6 transaction/MVCC checks; no AWS acceptance claim."""

import copy
import tempfile
import unittest
from contextlib import closing
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from colvenna_isle.control.registry import ControlError, canonical
from colvenna_isle.control.pipelines import DEFINITION_EPOCH
from colvenna_isle.pipeline.native_inputs import capture
from colvenna_isle.pipeline.publications import CHUNK_ROWS, Publications, resolve_release
from colvenna_isle.pipeline.task_descriptor import descriptor_key
from colvenna_isle.query.native import stage
from colvenna_isle.query.sql import Session
from colvenna_isle.writer.ingestion import CommitUnknown, IngestionStore, digest

COLUMNS = [{"name": "id", "type": "BIGINT", "nullable": False},
           {"name": "amount", "type": "DECIMAL(38,2)", "nullable": False}]
EXACT = Decimal("999999999999999999999999999999999999.99")
AUTHORITY = {"principal_id": "owner", "input_export_id": "export_one", "input_manifest": {}}


class CommitReplyLost:
    """The real COMMIT succeeds once; only its caller's reply is lost."""

    def __init__(self, connection):
        self.connection = connection
        self.armed = True

    def execute(self, sql, *args):
        result = self.connection.execute(sql, *args)
        if sql == "COMMIT" and self.armed:
            self.armed = False
            raise OSError("Original native commit reply lost")
        return result


class NativePublications(unittest.TestCase):
    def setUp(self):
        self.assertEqual(duckdb.__version__, "1.5.6")
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "native.duckdb"
        definition = {"dataset": "dwd.result", "schema_version": 1, "write_owner": "pipeline",
            "schema": copy.deepcopy(COLUMNS), "key_columns": ["id"], "partition_columns": []}
        self.configuration = {"space_id": "owned", "generation": "gen_owned",
            "database_uri": "s3://test-bucket/spaces/owned/warehouse/gen_owned",
            "datasets": {"dwd.result": {"definition": definition, "definition_sha256": digest(definition)}}}
        self.authorization = lambda descriptor: None
        self.open()

    def open(self):
        self.writer = duckdb.connect()
        self.writer.execute("ATTACH '" + str(self.path) + "' AS warehouse")
        for name in ["ctl", "ods", "dwd", "dws", "ads"]:
            self.writer.execute("CREATE SCHEMA IF NOT EXISTS warehouse." + name)
        self.reader = self.writer.cursor()
        self.native = IngestionStore(self.writer, self.configuration, lambda: {"epoch": 1})
        self.store = Publications(self.native, lambda value: self.authorization(value))
        self.store.initialize()

    def reopen(self):
        self.reader.close()
        self.writer.close()
        self.open()

    def tearDown(self):
        self.reader.close()
        self.writer.close()
        self.temporary.cleanup()

    def descriptor(self, job="job_one", attempt="attempt_one", previous=None):
        task = {"task_id": "transform", "language": "python", "executor": "lambda",
            "resource_profile": "python_lambda_standard", "entrypoint": "transform.py",
            "runtime_environment": {"environment_id": "analysis", "version": "v1"},
            "input_mode": "local_files", "processing": {"mode": "full"}, "timeout_seconds": 30,
            "max_attempts": 2, "inputs": {"source": {"dataset": "ods.source", "selection": "snapshot"}},
            "output": {"dataset": "dwd.result", "write_mode": "versioned_replace",
                "schema": copy.deepcopy(COLUMNS), "key_columns": ["id"], "allow_empty": True}}
        binding = {"dataset": "ods.source", "release_id": "input_one", "watermark": {"ods_sequence": 1},
            "schema": copy.deepcopy(COLUMNS), "files": [{
                "s3_uri": "s3://test-bucket/spaces/owned/inputs/gen_owned/export_one/exec_one/part.parquet",
                "object_version": "input-v1", "sha256": "f" * 64, "rows": 1, "bytes": 100}],
            "selection": {"mode": "snapshot"}}
        binding["manifest_sha256"] = digest({"contract_version": 2,
            **{name: binding[name] for name in ["dataset", "release_id", "watermark", "schema", "files"]}})
        return {"contract_version": 2, "space_id": "owned", "generation": "gen_owned",
            "pipeline_id": "demo", "pipeline_version": "v1", "definition_epoch": DEFINITION_EPOCH,
            "snapshot_sha256": "f" * 64, "run_id": "run_one", "job_id": job, "attempt_id": attempt,
            "task": task, "package": {"s3_uri":
                "s3://test-bucket/spaces/owned/pipeline-code/gen_owned/demo/versions/v1/package.zip",
                "object_version": "code-v1", "sha256": "a" * 64, "bytes": 200},
            "environment": {"source_commit": "a" * 40, "adapter_sha256": "b" * 64,
                "specification_sha256": "c" * 64, "dependency_lock_sha256": "d" * 64,
                "reference": task["runtime_environment"], "executor_binding": {"executor": "lambda",
                    "profile": "python_lambda_standard", "lambda_mode": "standard",
                    "function_arn": "arn:aws:lambda:us-west-2:123456789012:function:owned:1"}},
            "parameters": {}, "processing": {"mode": "full"},
            "output_state": {"expected_previous_release": previous, "replace_partitions": []},
            "inputs": {"source": binding}}

    def reference(self, descriptor):
        return {"s3_uri": "s3://test-bucket/" + descriptor_key(descriptor), "object_version": "descriptor-v1",
                "sha256": digest(descriptor), "bytes": len(canonical(descriptor).encode())}

    def grant(self, **args):
        descriptor = self.descriptor(**args)
        return self.store.grant(self.reference(descriptor), descriptor, AUTHORITY)

    def select(self, value, counts, execution="execution_one", *, state_counts=None):
        d = value["descriptor"]
        prefix = (f"s3://test-bucket/spaces/owned/staging/gen_owned/demo/run_one/"
            f"{d['job_id']}/{d['attempt_id']}/{execution}/")
        completion = {"s3_uri": prefix + "completion.json", "object_version": "completion-v1",
            "sha256": "b" * 64, "bytes": 100}
        files = [{"s3_uri": prefix + "result/part-" + str(i) + ".parquet", "object_version": "file-v1",
            "sha256": "c" * 64, "bytes": 100, "rows": count} for i, count in enumerate(counts)]
        state_files = None if state_counts is None else [
            {"s3_uri": prefix + "state/part-" + str(i) + ".parquet", "object_version": "state-v1",
             "sha256": "d" * 64, "bytes": 100, "rows": count}
            for i, count in enumerate(state_counts)]
        return self.store.select(d["job_id"], value["epoch"], d["attempt_id"], execution, completion, files,
                                 state_files=state_files)

    def chunk(self, value, file, chunk, rows, *, image="data"):
        d = value["descriptor"]
        return self.store.chunk(d["job_id"], value["epoch"], d["attempt_id"], value["execution_id"],
                                file, chunk, rows, image=image)

    def publish(self, value):
        d = value["descriptor"]
        return self.store.publish(d["job_id"], value["epoch"], d["attempt_id"], value["execution_id"])

    def release(self, job, rows, previous=None):
        value = self.select(self.grant(job=job, previous=previous), [len(rows)])
        self.chunk(value, 0, 0, rows)
        return self.publish(value)

    def sequence(self):
        return self.writer.execute("SELECT sequence FROM warehouse.ctl.commit_sequence").fetchone()[0]

    def rows(self, release, connection=None):
        return (connection or self.writer).execute(
            "SELECT * FROM warehouse._colvenna_data." + release["physical"]["table"] + " ORDER BY id").fetchall()

    def test_query_explicit_historical_release_never_substitutes_current_values(self):
        old = self.release("query_old", [[1, Decimal("12.30")]])
        newer = self.release("query_new", [[9007199254740993, EXACT]], old["release_id"])
        sequence = self.sequence()
        with closing(Session(Path(self.temporary.name) / "query")) as snapshot:
            stage(self.reader, self.configuration, snapshot, lambda: None,
                  consistency="published", release_id=old["release_id"])
            result = snapshot.execute("SELECT id,amount FROM dwd.result", 10)
            self.assertEqual(result["rows"], [[1, "12.30"]])
            self.assertEqual(result["data_versions"], [{"dataset": "dwd.result",
                "release_id": old["release_id"], "commit_sequence": old["commit_sequence"],
                "snapshot_sequence": sequence, "generation": "gen_owned"}])
        self.assertEqual(self.sequence(), sequence)
        self.assertEqual(resolve_release(self.reader, self.configuration, "dwd.result"), newer)

    def test_query_published_current_returns_actual_release_and_exact_native_types(self):
        released = self.release("query_published", [[9007199254740993, EXACT]])
        with closing(Session(Path(self.temporary.name) / "query")) as snapshot:
            stage(self.reader, self.configuration, snapshot, lambda: None, consistency="published")
            result = snapshot.execute("SELECT id,amount FROM dwd.result", 10)
            self.assertEqual(result["rows"], [["9007199254740993", str(EXACT)]])
            self.assertEqual(result["data_versions"][0]["release_id"], released["release_id"])

    def test_query_missing_explicit_release_is_not_ready_and_snapshot_unusable(self):
        self.release("query_current", [[1, Decimal("12.30")]])
        with closing(Session(Path(self.temporary.name) / "query")) as snapshot:
            with self.assertRaisesRegex(ControlError, "PUBLICATION_RELEASE_NOT_READY"):
                stage(self.reader, self.configuration, snapshot, lambda: None,
                      consistency="published", release_id="release_not_observed")
            self.assertFalse(snapshot.sealed)
        self.assertEqual(self.reader.execute("SELECT 1").fetchone(), (1,))

    def test_query_without_any_publication_fails_before_sealing_snapshot(self):
        with closing(Session(Path(self.temporary.name) / "query")) as snapshot:
            with self.assertRaisesRegex(ControlError, "PUBLICATION_NOT_AVAILABLE"):
                stage(self.reader, self.configuration, snapshot, lambda: None, consistency="published")
            self.assertFalse(snapshot.sealed)

    def test_query_versions_an_exact_release_and_publications_only_for_named_datasets(self):
        with closing(Session(Path(self.temporary.name) / "unnamed")) as snapshot:
            stage(self.reader, self.configuration, snapshot, lambda: None, consistency="published", datasets=[])
            self.assertEqual(snapshot.execute("SELECT 1", 10)["data_versions"], [])
        with closing(Session(Path(self.temporary.name) / "unpublished")) as snapshot:
            with self.assertRaisesRegex(ControlError, "PUBLICATION_NOT_AVAILABLE"):
                stage(self.reader, self.configuration, snapshot, lambda: None, consistency="published",
                      datasets=["dwd.result"])
        old = self.release("query_old", [[1, Decimal("12.30")]])
        self.release("query_new", [[2, EXACT]], old["release_id"])
        version = {"dataset": "dwd.result", "release_id": old["release_id"],
                   "commit_sequence": old["commit_sequence"], "snapshot_sequence": self.sequence(),
                   "generation": "gen_owned"}
        with closing(Session(Path(self.temporary.name) / "query")) as snapshot:
            stage(self.reader, self.configuration, snapshot, lambda: None, consistency="published",
                  release_id=old["release_id"], datasets=[])
            self.assertEqual((snapshot.execute("SELECT 1", 10)["data_versions"], snapshot.catalog), ([version], []))
            with self.assertRaisesRegex(ControlError, "INVALID_OR_UNAUTHORIZED_SQL"):
                snapshot.execute("SELECT * FROM dwd.result", 10)
        with closing(Session(Path(self.temporary.name) / "named")) as snapshot:
            stage(self.reader, self.configuration, snapshot, lambda: None, consistency="published",
                  release_id=old["release_id"], datasets=["dwd.result"])
            result = snapshot.execute("SELECT id,amount FROM dwd.result", 10)
            self.assertEqual((result["rows"], result["data_versions"]), ([[1, "12.30"]], [version]))
        with closing(Session(Path(self.temporary.name) / "missing")) as snapshot:
            with self.assertRaisesRegex(ControlError, "PUBLICATION_RELEASE_NOT_READY"):
                stage(self.reader, self.configuration, snapshot, lambda: None, consistency="published",
                      release_id="release_not_observed", datasets=[])
            self.assertFalse(snapshot.sealed)

    def test_query_published_cannot_read_direct_table_as_an_analytical_release(self):
        self.release("query_published", [[1, Decimal("12.30")]])
        from colvenna_isle.writer.writes import WriteStore, write_id
        definition = {"dataset": "dwd.source", "schema_version": 1, "write_owner": "direct",
            "schema": copy.deepcopy(COLUMNS), "key_columns": ["id"], "partition_columns": [],
            "constraints": {"primary_key": ["id"]}}
        registered = {"definition": definition, "definition_sha256": digest(definition)}
        self.configuration["datasets"]["dwd.source"] = registered
        WriteStore(self.native).commit(write_id("owned", "gen_owned", "dwd.source", "query_direct"), "owner",
            {"dataset": "dwd.source", "schema_version": 1, "operation": "insert",
             "records": [{"id": 7, "amount": "99.99"}]}, registered, lambda: None)
        with closing(Session(Path(self.temporary.name) / "query")) as snapshot:
            stage(self.reader, self.configuration, snapshot, lambda: None, consistency="published")
            snapshot.publications_only(self.configuration["datasets"])
            self.assertEqual([value["dataset"] for value in snapshot.catalog], ["dwd.result"])
            for sql in ["SELECT * FROM dwd.source", "EXPLAIN SELECT * FROM dwd.source",
                        "WITH x AS (SELECT * FROM dwd.source) SELECT * FROM x",
                        "SELECT * FROM query_table('dwd.source')"]:
                with self.assertRaisesRegex(ControlError, "PUBLICATION_NOT_AVAILABLE"):
                    snapshot.execute(sql, 10)
            self.assertEqual(snapshot.execute("SELECT * FROM dwd.result", 10)["rows"], [[1, "12.30"]])

    def test_partial_candidate_invisible_and_mvcc_returns_complete_old_or_new_exact_values(self):
        old = self.release("job_old", [[1, Decimal("12.30")]])
        candidate = self.select(self.grant(job="job_new", previous=old["release_id"]), [CHUNK_ROWS + 1])
        first = [[i + 2, Decimal("1.00")] for i in range(CHUNK_ROWS)]
        self.chunk(candidate, 0, 0, first)
        self.assertEqual(resolve_release(self.reader, self.configuration, "dwd.result"), old)
        self.reader.execute("BEGIN")
        pinned = resolve_release(self.reader, self.configuration, "dwd.result")
        self.chunk(candidate, 0, 1, [[9007199254740993, EXACT]])
        new = self.publish(candidate)
        self.assertEqual(resolve_release(self.reader, self.configuration, "dwd.result"), pinned)
        self.assertEqual(self.rows(pinned, self.reader), [(1, Decimal("12.30"))])
        self.reader.execute("COMMIT")
        self.assertEqual(resolve_release(self.reader, self.configuration, "dwd.result"), new)
        self.assertEqual(self.rows(new)[-1], (9007199254740993, EXACT))
        self.assertEqual(self.rows(old), [(1, Decimal("12.30"))])
        self.assertEqual(new["input_watermarks"]["source"]["watermark"], {"ods_sequence": 1})

    def test_duplicate_chunk_and_publication_preserve_original_receipts_and_sequence(self):
        value = self.select(self.grant(), [1])
        original = self.chunk(value, 0, 0, [[1, EXACT]])
        sequence = self.sequence()
        self.assertEqual(self.chunk(value, 0, 0, [[1, EXACT]]), original)
        self.assertEqual(self.sequence(), sequence)
        with self.assertRaisesRegex(ControlError, "PUBLICATION_CHUNK_CONFLICT"):
            self.chunk(value, 0, 0, [[1, Decimal("1.00")]])
        release = self.publish(value)
        sequence = self.sequence()
        self.assertEqual(self.publish(value), release)
        self.assertEqual(self.sequence(), sequence)
        self.assertEqual(self.rows(release), [(1, EXACT)])

    def test_missing_chunks_and_cross_file_duplicate_roll_back_without_partial_publication(self):
        value = self.select(self.grant(), [1, 1])
        self.chunk(value, 0, 0, [[1, EXACT]])
        sequence = self.sequence()
        with self.assertRaisesRegex(ControlError, "PUBLICATION_CHUNKS_INCOMPLETE"):
            self.publish(value)
        with self.assertRaisesRegex(ControlError, "PUBLICATION_DUPLICATE_KEY"):
            self.chunk(value, 1, 0, [[1, Decimal("0.00")]])
        self.assertEqual(self.sequence(), sequence)
        self.assertIsNone(resolve_release(self.reader, self.configuration, "dwd.result"))
        self.chunk(value, 1, 0, [[2, Decimal("0.00")]])
        self.assertEqual(self.publish(value)["row_count"], 2)

    def test_expected_previous_release_cas_rejects_another_complete_candidate(self):
        a = self.select(self.grant(job="job_a"), [1])
        b = self.select(self.grant(job="job_b"), [1])
        self.chunk(a, 0, 0, [[1, EXACT]])
        self.chunk(b, 0, 0, [[2, EXACT]])
        release = self.publish(a)
        with self.assertRaisesRegex(ControlError, "PUBLICATION_PREVIOUS_RELEASE_CONFLICT"):
            self.publish(b)
        self.assertEqual(resolve_release(self.reader, self.configuration, "dwd.result"), release)
        with self.assertRaisesRegex(ControlError, "PUBLICATION_PREVIOUS_RELEASE_CONFLICT"):
            self.grant(job="job_c")

    def test_replacement_epoch_rejects_late_candidates_attempt_reuse_and_changed_job_binding(self):
        old = self.select(self.grant(), [1])
        descriptor = self.descriptor(attempt="attempt_two")
        fresh = self.store.grant(self.reference(descriptor), descriptor, AUTHORITY, expected_epoch=1)
        self.assertEqual(fresh["epoch"], 2)
        for action in [lambda: self.chunk(old, 0, 0, [[1, EXACT]]), lambda: self.publish(old)]:
            with self.assertRaisesRegex(ControlError, "PUBLICATION_ELIGIBILITY_CHANGED"):
                action()
        changed = self.descriptor(attempt="attempt_three")
        changed["parameters"] = {"variant": "different"}
        with self.assertRaisesRegex(ControlError, "PUBLICATION_JOB_BINDING_CONFLICT"):
            self.store.grant(self.reference(changed), changed, AUTHORITY, expected_epoch=2)
        descriptor = self.descriptor()
        with self.assertRaisesRegex(ControlError, "PUBLICATION_ATTEMPT_REUSED"):
            self.store.grant(self.reference(descriptor), descriptor, AUTHORITY, expected_epoch=2)

    def test_revocation_is_idempotent_and_cannot_claim_cancellation_after_publish(self):
        value = self.select(self.grant(), [1])
        revoked = self.store.revoke("job_one", expected_epoch=1)
        self.assertEqual(revoked["epoch"], 2)
        self.assertEqual(self.store.revoke("job_one", expected_epoch=1), revoked)
        with self.assertRaisesRegex(ControlError, "PUBLICATION_ELIGIBILITY_CHANGED"):
            self.chunk(value, 0, 0, [[1, EXACT]])
        release = self.release("job_other", [[2, EXACT]])
        self.assertEqual(self.store.revoke("job_other", expected_epoch=1)["release"], release)
        for bad in [None, True, 0]:
            with self.subTest(bad=bad), self.assertRaisesRegex(ControlError, "PUBLICATION_EPOCH_REQUIRED"):
                self.store.revoke("job_other", expected_epoch=bad)

    def test_original_execution_and_object_scope_are_bound_by_native_selection(self):
        grant = self.grant()
        value = self.select(grant, [1])
        self.assertEqual(self.select(grant, [1]), value)
        with self.assertRaisesRegex(ControlError, "PUBLICATION_EXECUTION_CONFLICT"):
            self.select(grant, [1], execution="execution_other")
        descriptor = self.descriptor(job="job_other")
        reference = self.reference(descriptor)
        reference["s3_uri"] = reference["s3_uri"].replace("test-bucket", "foreign-bucket")
        with self.assertRaisesRegex(ControlError, "TASK_OBJECT_SCOPE"):
            self.store.grant(reference, descriptor, AUTHORITY)
        old = self.descriptor(job="job_old_epoch")
        old["definition_epoch"] = "0" * 64
        with self.assertRaisesRegex(ControlError, "PUBLICATION_DEFINITION_CHANGED"):
            self.store.grant(self.reference(old), old, AUTHORITY)
        plan = value["selection"]
        files = copy.deepcopy(plan["files"])
        files[0]["s3_uri"] = files[0]["s3_uri"].replace("/owned/", "/foreign/")
        with self.assertRaisesRegex(ControlError, "TASK_OBJECT_SCOPE"):
            self.store.select("job_one", 1, "attempt_one", "execution_one", plan["completion"], files)

    def test_import_restart_uses_original_generation_chunk_and_fixed_inputs(self):
        value = self.select(self.grant(), [1, 1])
        original = self.chunk(value, 0, 0, [[1, EXACT]])
        self.reopen()
        self.assertEqual(self.store.status("job_one")["generation_id"], value["generation_id"])
        self.assertEqual(self.chunk(value, 0, 0, [[1, EXACT]]), original)
        self.chunk(value, 1, 0, [[2, EXACT]])
        self.assertEqual(self.publish(value)["row_count"], 2)

    def test_chunk_commit_reply_loss_pauses_handle_then_reconciles_the_original(self):
        value = self.select(self.grant(), [1])
        self.native.connection = CommitReplyLost(self.writer)
        with self.assertRaisesRegex(CommitUnknown, "COMMIT_UNKNOWN"):
            self.chunk(value, 0, 0, [[1, EXACT]])
        with self.assertRaisesRegex(CommitUnknown, "NATIVE_RESULT_UNKNOWN"):
            self.store.status("job_one")
        self.reopen()
        sequence = self.sequence()
        self.assertEqual(self.chunk(value, 0, 0, [[1, EXACT]])["commit_sequence"], sequence)
        self.assertEqual(self.sequence(), sequence)
        self.assertEqual(self.publish(value)["row_count"], 1)

    def test_publication_commit_reply_loss_recovers_original_pointer_and_watermark_atomically(self):
        value = self.select(self.grant(), [1])
        self.chunk(value, 0, 0, [[9007199254740993, EXACT]])
        self.native.connection = CommitReplyLost(self.writer)
        with self.assertRaisesRegex(CommitUnknown, "COMMIT_UNKNOWN"):
            self.publish(value)
        self.reopen()
        sequence = self.sequence()
        original = self.store.status("job_one")["release"]
        self.assertEqual(self.publish(value), original)
        self.assertEqual(resolve_release(self.reader, self.configuration, "dwd.result"), original)
        self.assertEqual(self.sequence(), sequence)
        self.assertEqual(self.rows(original), [(9007199254740993, EXACT)])
        observed = self.writer.execute("SELECT record FROM warehouse.ctl.pipeline_task_watermarks").fetchone()[0]
        self.assertEqual(observed, canonical(original))

    def test_fresh_authorization_at_precommit_rolls_back_rows_and_all_chunk_ledger_effects(self):
        value = self.select(self.grant(), [1])
        calls = []

        def authorize(descriptor):
            calls.append(1)
            if len(calls) == 3:
                raise ControlError(403, "PERMIT_REVOKED")

        self.authorization = authorize
        with self.assertRaisesRegex(ControlError, "PERMIT_REVOKED"):
            self.chunk(value, 0, 0, [[1, EXACT]])
        self.authorization = lambda descriptor: None
        self.assertEqual(self.sequence(), 0)
        self.assertEqual(self.writer.execute("SELECT count(*) FROM warehouse.ctl.publication_chunks").fetchone(), (0,))
        self.assertEqual(self.writer.execute("SELECT count(*) FROM warehouse.ctl.generation_keys").fetchone(), (0,))
        self.chunk(value, 0, 0, [[1, EXACT]])
        self.assertEqual(self.publish(value)["row_count"], 1)

    def test_physical_custody_and_current_registered_target_fail_closed(self):
        release = self.release("job_one", [[1, EXACT]])
        table = release["physical"]["table"]
        self.writer.execute("ALTER TABLE warehouse._colvenna_data." + table + " ADD COLUMN unexpected VARCHAR")
        with self.assertRaisesRegex(ControlError, "PUBLICATION_PHYSICAL_CUSTODY_UNKNOWN"):
            resolve_release(self.reader, self.configuration, "dwd.result")
        self.configuration["datasets"]["dwd.result"]["definition"]["write_owner"] = "direct"
        with self.assertRaisesRegex(ControlError, "PUBLICATION_TARGET_FORBIDDEN"):
            self.store.status("job_one")

    def test_controlled_native_read_and_full_input_export_use_only_the_published_pointer(self):
        from colvenna_isle.query.native import describe

        candidate = self.select(self.grant(), [1])
        self.chunk(candidate, 0, 0, [[9007199254740993, EXACT]])
        self.assertEqual(describe(self.reader, self.configuration, lambda: None), ([], []))
        release = self.publish(candidate)
        catalog, versions = describe(self.reader, self.configuration, lambda: None)
        self.assertEqual(catalog, [{"dataset": "dwd.result", "schema": COLUMNS}])
        self.assertEqual(versions[0]["release_id"], release["release_id"])
        exported = capture(self.reader, self.configuration,
            {"source": {"dataset": "dwd.result", "selection": {"mode": "snapshot"}}},
            Path(self.temporary.name) / "export", maximum_bytes=1048576, maximum_rows=10, guard=lambda: None)
        value = exported["inputs"]["source"]
        self.assertEqual(value["native_version"]["release_id"], release["release_id"])
        row = pq.read_table(value["file"]["local_path"]).to_pylist()[0]
        self.assertEqual(row, {"id": 9007199254740993, "amount": EXACT})
        replacement = self.select(self.grant(job="job_next", previous=release["release_id"]), [1])
        self.chunk(replacement, 0, 0, [[2, Decimal("1.00")]])
        self.assertEqual(describe(self.reader, self.configuration, lambda: None)[1][0]["release_id"],
            release["release_id"])

    def bind_state(self):
        definition = self.configuration["datasets"]["dwd.result"]["definition"]
        definition["publication_state"] = {"protocol": "complete_image_v1",
            "schema": [copy.deepcopy(COLUMNS[0]), {"name": "version", "type": "BIGINT", "nullable": False},
                       {"name": "deleted", "type": "BOOLEAN", "nullable": False},
                       copy.deepcopy(COLUMNS[1])], "key_columns": ["id"], "allow_empty": False}
        self.configuration["datasets"]["dwd.result"]["definition_sha256"] = digest(definition)

    def state_rows(self, release, connection=None):
        return (connection or self.writer).execute(
            "SELECT * FROM warehouse._colvenna_data." + release["state"]["physical"]["table"] +
            " ORDER BY id").fetchall()

    def test_complete_data_state_pairs_publish_atomically_and_old_mvcc_pair_is_retained(self):
        self.bind_state()
        first = self.select(self.grant(job="job_old"), [1], state_counts=[2])
        self.chunk(first, 0, 0, [[1, Decimal("12.30")]])
        self.chunk(first, 0, 0, [[1, 1, False, Decimal("12.30")], [2, 5, True, EXACT]], image="state")
        old = self.publish(first)
        candidate = self.select(self.grant(job="job_new", previous=old["release_id"]), [1], state_counts=[1])
        self.chunk(candidate, 0, 0, [[9007199254740993, EXACT]])
        with self.assertRaisesRegex(ControlError, "PUBLICATION_CHUNKS_INCOMPLETE"):
            self.publish(candidate)
        self.reader.execute("BEGIN")
        pinned = resolve_release(self.reader, self.configuration, "dwd.result")
        self.chunk(candidate, 0, 0, [[9007199254740993, 6, False, EXACT]], image="state")
        new = self.publish(candidate)
        self.assertEqual(resolve_release(self.reader, self.configuration, "dwd.result"), pinned)
        self.assertEqual(self.rows(pinned, self.reader), [(1, Decimal("12.30"))])
        self.assertEqual(self.state_rows(pinned, self.reader),
                         [(1, 1, False, Decimal("12.30")), (2, 5, True, EXACT)])
        self.reader.execute("COMMIT")
        self.assertEqual(resolve_release(self.reader, self.configuration, "dwd.result"), new)
        self.assertEqual(self.rows(new), [(9007199254740993, EXACT)])
        self.assertEqual(self.state_rows(new), [(9007199254740993, 6, False, EXACT)])
        self.assertEqual(resolve_release(self.reader, self.configuration, "dwd.result", old["release_id"]), old)
        pointer = self.writer.execute("SELECT release_id,state_record FROM warehouse.ctl.state_pointers").fetchone()
        self.assertEqual(pointer, (new["release_id"], canonical(new["state"])))
        self.assertEqual(self.writer.execute("SELECT record FROM warehouse.ctl.pipeline_task_watermarks").fetchone(),
                         (canonical(new),))

    def test_state_chunk_and_publication_lost_commit_replies_reconcile_exact_original_pair(self):
        self.bind_state()
        value = self.select(self.grant(), [1], state_counts=[1])
        self.chunk(value, 0, 0, [[9007199254740993, EXACT]])
        self.native.connection = CommitReplyLost(self.writer)
        with self.assertRaisesRegex(CommitUnknown, "COMMIT_UNKNOWN"):
            self.chunk(value, 0, 0, [[9007199254740993, 8, False, EXACT]], image="state")
        self.reopen()
        sequence = self.sequence()
        replay = self.chunk(value, 0, 0, [[9007199254740993, 8, False, EXACT]], image="state")
        self.assertEqual(replay["commit_sequence"], sequence)
        self.assertEqual(self.sequence(), sequence)
        self.native.connection = CommitReplyLost(self.writer)
        with self.assertRaisesRegex(CommitUnknown, "COMMIT_UNKNOWN"):
            self.publish(value)
        self.reopen()
        sequence = self.sequence()
        original = self.store.status("job_one")["release"]
        self.assertEqual(self.publish(value), original)
        self.assertEqual(resolve_release(self.reader, self.configuration, "dwd.result"), original)
        self.assertEqual(self.state_rows(original), [(9007199254740993, 8, False, EXACT)])
        self.assertEqual(self.sequence(), sequence)

    def test_required_state_plan_missing_chunk_duplicate_state_key_and_cas_fail_closed(self):
        self.bind_state()
        grant = self.grant(job="job_a")
        with self.assertRaisesRegex(ControlError, "PUBLICATION_STATE_PLAN_REQUIRED"):
            self.select(grant, [1])
        self.assertEqual(self.writer.execute("SELECT count(*) FROM warehouse.ctl.publication_generations").fetchone(), (0,))
        a = self.select(grant, [1], state_counts=[1, 1])
        b = self.select(self.grant(job="job_b"), [1], state_counts=[1])
        self.chunk(a, 0, 0, [[1, EXACT]])
        self.chunk(a, 0, 0, [[1, 1, False, EXACT]], image="state")
        with self.assertRaisesRegex(ControlError, "PUBLICATION_CHUNKS_INCOMPLETE"):
            self.publish(a)
        sequence = self.sequence()
        with self.assertRaisesRegex(ControlError, "PUBLICATION_DUPLICATE_KEY"):
            self.chunk(a, 1, 0, [[1, 2, True, EXACT]], image="state")
        self.assertEqual(self.sequence(), sequence)
        self.chunk(a, 1, 0, [[2, 2, True, EXACT]], image="state")
        release = self.publish(a)
        self.chunk(b, 0, 0, [[3, EXACT]])
        self.chunk(b, 0, 0, [[3, 3, False, EXACT]], image="state")
        with self.assertRaisesRegex(ControlError, "PUBLICATION_PREVIOUS_RELEASE_CONFLICT"):
            self.publish(b)
        self.assertEqual(resolve_release(self.reader, self.configuration, "dwd.result"), release)
        self.assertEqual(self.state_rows(release), [(1, 1, False, EXACT), (2, 2, True, EXACT)])

    def test_state_cancellation_contract_change_and_physical_or_pointer_tamper_are_rejected(self):
        self.bind_state()
        value = self.select(self.grant(), [1], state_counts=[1])
        self.store.revoke("job_one", expected_epoch=1)
        with self.assertRaisesRegex(ControlError, "PUBLICATION_ELIGIBILITY_CHANGED"):
            self.chunk(value, 0, 0, [[1, 1, False, EXACT]], image="state")
        value = self.select(self.grant(job="job_other"), [1], state_counts=[1])
        self.chunk(value, 0, 0, [[1, EXACT]])
        self.chunk(value, 0, 0, [[1, 1, False, EXACT]], image="state")
        registered = self.configuration["datasets"]["dwd.result"]
        original = copy.deepcopy(registered)
        registered["definition"]["publication_state"]["allow_empty"] = True
        registered["definition_sha256"] = digest(registered["definition"])
        with self.assertRaisesRegex(ControlError, "PUBLICATION_TARGET_CHANGED"):
            self.publish(value)
        registered.clear()
        registered.update(original)
        release = self.publish(value)
        self.assertEqual(self.store.revoke("job_other", expected_epoch=1)["release"], release)
        self.writer.execute("UPDATE warehouse.ctl.state_pointers SET state_record='null'")
        with self.assertRaisesRegex(ControlError, "PUBLICATION_STATE_POINTER_CUSTODY_UNKNOWN"):
            resolve_release(self.reader, self.configuration, "dwd.result")
        self.writer.execute("UPDATE warehouse.ctl.state_pointers SET state_record=?", [canonical(release["state"])])
        self.writer.execute("ALTER TABLE warehouse._colvenna_data." + release["state"]["physical"]["table"] +
                            " ADD COLUMN unexpected VARCHAR")
        with self.assertRaisesRegex(ControlError, "PUBLICATION_STATE_CUSTODY_UNKNOWN"):
            resolve_release(self.reader, self.configuration, "dwd.result")


if __name__ == "__main__":
    unittest.main()
