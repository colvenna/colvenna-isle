"""Private writer commands over real Native ledgers and exact-version IO doubles."""

import copy
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from colvenna_isle.control.pipelines import DEFINITION_EPOCH
from colvenna_isle.control.registry import ControlError, Principal, canonical
from colvenna_isle.pipeline.input_exports import InputExports
from colvenna_isle.writer.publications import Publisher
from colvenna_isle.writer.scheduler import Scheduler
from colvenna_isle.writer.ingestion import digest
from colvenna_isle.writer.writes import WriteStore, write_id
import test_publications as native_fixture


class WriterPublicationCommands(unittest.TestCase):
    def setUp(self):
        self.fixture = native_fixture.NativePublications()
        self.fixture.setUp()
        configuration = self.fixture.configuration
        configuration["route_version"] = 1
        source = copy.deepcopy(configuration["datasets"]["dwd.result"]["definition"])
        source.update(dataset="ods.source", write_owner="direct", constraints={"primary_key": ["id"]})
        configuration["datasets"]["ods.source"] = {"definition": source, "definition_sha256": digest(source)}
        self.writes = WriteStore(self.fixture.native)
        self.writes.commit(write_id("owned", "gen_owned", "ods.source", "original"), "owner",
            {"dataset": "ods.source", "schema_version": 1, "operation": "insert",
             "records": [{"id": 1, "amount": "12.30"}]},
            configuration["datasets"]["ods.source"], lambda: None)
        self.actor = Principal("owner", frozenset(["owned"]), frozenset(["pipeline:run"]),
                               "key_owner", "gen_owned")
        self.active, self.permit = True, True
        self.identity = {"task_arn": "original", "task_definition": "writer:1",
            "source_commit": "a" * 40, "image_digest": "sha256:" + "b" * 64,
            "pid": 1, "start_ticks": 2, "boot_id": "boot", "process_nonce": "original", "address": "10.0.0.2"}
        self.descriptor = self.fixture.descriptor()
        self.descriptor["definition_epoch"] = DEFINITION_EPOCH
        environment = self.descriptor["environment"]
        self.environment = {"reference": environment["reference"], "executor_binding": environment["executor_binding"],
            "specification_sha256": environment["specification_sha256"], "ready": {
                "runtime": {name: environment[name] for name in ["source_commit", "adapter_sha256"]},
                "dependency_lock": {"sha256": environment["dependency_lock_sha256"]}}}
        definition = {"spec_version": 1, "pipeline_id": "demo", "name": "Fixture", "schedule": {"owner": "none"},
            "max_active_runs": 1, "coalesce_pending": True, "tasks": [self.descriptor["task"]]}
        self.snapshot = {"definition_epoch": DEFINITION_EPOCH, "space_id": "owned", "generation": "gen_owned",
            "pipeline_id": "demo", "pipeline_version": "v1", "definition": definition,
            "datasets": {name: value["definition_sha256"] for name, value in configuration["datasets"].items()},
            "environments": {"transform": self.environment},
            "executors": {"transform": environment["executor_binding"]}}
        self.descriptor["snapshot_sha256"] = digest(self.snapshot)
        package = self.descriptor["package"]
        self.published = {"definition_epoch": DEFINITION_EPOCH, "snapshot": self.snapshot,
            "snapshot_sha256": digest(self.snapshot), "package": {
                "package_uri": package["s3_uri"], "package_object_version": package["object_version"],
                "package_sha256": package["sha256"], "package_bytes": package["bytes"]}}
        self.native_inputs = InputExports(self.fixture.native)
        self.native_inputs.initialize()
        binding = self.descriptor["inputs"]["source"]
        manifest = {"contract_version": 2, **{name: binding[name] for name in [
            "dataset", "release_id", "watermark", "schema", "files"]}}
        portable = {"space_id": "owned", "generation": "gen_owned", "export_id": "input_original",
            "inputs": {"source": {"manifest": manifest, "selection": {"mode": "snapshot"}}}}
        # Seed a trusted Native READY fixture, separate from the untrusted task
        # descriptor. Input-export production itself has its own integration suite.
        self.input_record = {"format_version": 1, "export_id": "input_original", "binding_sha256": "f" * 64,
            "space_id": "owned", "generation": "gen_owned", "principal_id": "owner", "state": "READY",
            "retain_until_unix": time.time() + 600, "manifest": {"sha256": digest(portable)},
            "definitions": {"source": configuration["datasets"]["ods.source"]["definition_sha256"]},
            "bindings": {"source": {"dataset": "ods.source", "selection": {"mode": "snapshot"}}},
            "references": {"job_one": {"state": "ACTIVE", "retain_until_unix": time.time() + 600}}}
        self.fixture.writer.execute("INSERT INTO warehouse.ctl.input_exports VALUES (?,?,?)",
            ["input_original", self.input_record["binding_sha256"], canonical(self.input_record)])
        self.authority = {"principal_id": "owner", "input_export_id": "input_original", "input_manifest": portable}
        registry = SimpleNamespace(_read=lambda space: copy.deepcopy(configuration), client=object(), table="control")
        database = SimpleNamespace(ingestions=self.fixture.native, connection=self.fixture.writer,
                                   work=self.fixture.path.parent)
        self.runtime = SimpleNamespace(identity=self.identity, registry=registry, lock=threading.Lock(),
            direct=SimpleNamespace(keys=SimpleNamespace(authenticate_id=self.authenticate)),
            consumer=SimpleNamespace(s3=None, bucket="test-bucket", account="123456789012"),
            database=lambda config: database, inputs=SimpleNamespace(authorize=self.authorize))
        self.runtime.scheduler = Scheduler(self.runtime.lock)
        self.publisher = Publisher(self.runtime, {})
        self.publisher.pipelines._read = lambda *args, **kwargs: copy.deepcopy(self.published)
        self.publisher.pipelines.environments.pipeline_binding = lambda *args: copy.deepcopy(self.environment)
        self.request = {"identity": self.identity, "epoch": 1, "space_id": "owned", "generation": "gen_owned",
            "principal_id": "owner", "key_id": "key_owner", "job_id": "job_one"}

    def tearDown(self):
        self.fixture.tearDown()

    def authenticate(self, identity):
        if not self.active or identity != self.actor.key_id:
            raise ControlError(403, "FORBIDDEN")
        return self.actor

    def authorize(self, request):
        if not self.permit or request["identity"] != self.identity or request["epoch"] != 1:
            raise ControlError(409, "WRITER_IDENTITY_CHANGED")
        actor = self.authenticate(request["key_id"])
        actor.authorize(request["space_id"], "pipeline:run")
        if request["principal_id"] != actor.principal_id or request["generation"] != actor.generation:
            raise ControlError(403, "FORBIDDEN")
        return actor, copy.deepcopy(self.fixture.configuration)

    def store(self):
        return self.publisher.unit("fixture_store", lambda: self.publisher.store(
            self.request, self.actor, self.fixture.configuration)[1])

    def grant(self):
        store = self.store()
        return self.publisher.unit("fixture_grant",
            lambda: store.grant(self.fixture.reference(self.descriptor), self.descriptor, self.authority))

    def operation(self, operation, **fields):
        return self.publisher.execute({**self.request, **fields, "operation": operation})

    def change_input(self, action):
        action(self.input_record)
        self.fixture.writer.execute("UPDATE warehouse.ctl.input_exports SET record=? WHERE export_id=?",
            [canonical(self.input_record), "input_original"])

    def test_private_status_and_revoke_are_native_fairly_scheduled_and_freshly_authorized(self):
        self.grant()
        status = self.operation("publication_status")
        self.assertEqual(status, {"job_id": "job_one", "epoch": 1, "status": "GRANTED"})
        self.assertNotIn("descriptor", status)
        revoked = self.operation("publication_revoke", eligibility_epoch=1)
        self.assertEqual(revoked["epoch"], 2)
        self.assertEqual(revoked["status"], "REVOKED")
        self.assertEqual(self.operation("publication_revoke", eligibility_epoch=1), revoked)
        self.assertGreaterEqual(self.runtime.scheduler.observation()["finished"]["import"], 4)
        self.active = False
        with self.assertRaisesRegex(ControlError, "FORBIDDEN"):
            self.operation("publication_status")

    def test_stored_bound_selection_fields_are_authority_not_a_mismatch(self):
        # A fixed ODS bound (or interval/release/partitions) stays in the
        # stored binding; only its dataset/selection must equal the manifest.
        self.change_input(lambda record: record["bindings"]["source"].update(to_sequence=1))
        self.assertEqual(self.grant()["status"], "GRANTED")
        self.change_input(lambda record: record["bindings"]["source"].update(selection={"mode": "changes"}))
        with self.assertRaisesRegex(ControlError, "PUBLICATION_INPUT_AUTHORITY_CHANGED"):
            self.operation("publication_status")

    def test_current_definition_environment_and_input_retention_cannot_be_substituted_by_ddb_projection(self):
        self.grant()
        changes = [lambda: self.published.update(definition_epoch="0" * 64),
            lambda: self.environment["ready"]["runtime"].update(source_commit="0" * 40),
            lambda: self.change_input(lambda value: value["references"].clear()),
            lambda: self.change_input(lambda value: value["manifest"].update(sha256="0" * 64))]
        for change in changes:
            published, environment, original = copy.deepcopy(self.published), copy.deepcopy(self.environment), \
                copy.deepcopy(self.input_record)
            change()
            with self.subTest(change=change), self.assertRaises(ControlError):
                self.operation("publication_status")
            self.published, self.environment, self.input_record = published, environment, original
            self.change_input(lambda value: None)
        self.assertIsNone(native_fixture.resolve_release(
            self.fixture.reader, self.fixture.configuration, "dwd.result"))

    def test_replacing_input_or_attempt_without_native_cas_cannot_publish(self):
        self.grant()
        changed = copy.deepcopy(self.descriptor)
        changed["inputs"]["source"]["watermark"]["ods_sequence"] = 900
        manifest = {"contract_version": 2, **{name: changed["inputs"]["source"][name] for name in [
            "dataset", "release_id", "watermark", "schema", "files"]}}
        changed["inputs"]["source"]["manifest_sha256"] = digest(manifest)
        store = self.store()
        changed["job_id"] = "job_foreign"
        self.input_record["references"]["job_foreign"] = {"state": "ACTIVE"}
        self.change_input(lambda value: None)
        with self.assertRaisesRegex(ControlError, "PUBLICATION_INPUT_AUTHORITY_CHANGED"):
            self.publisher.unit("forged_input", lambda: store.grant(
                self.fixture.reference(changed), changed, self.authority))
        self.assertEqual(self.operation("publication_status")["epoch"], 1)

    def test_writer_identity_and_foreign_principal_fail_before_native_status(self):
        self.grant()
        for change in [{"principal_id": "foreign"}, {"generation": "gen_other"},
                       {"identity": {**self.identity, "process_nonce": "foreign"}}, {"epoch": 2}]:
            with self.subTest(change=change), self.assertRaises(ControlError):
                self.publisher.execute({**self.request, **change, "operation": "publication_status"})
        self.permit = False
        with self.assertRaisesRegex(ControlError, "WRITER_IDENTITY_CHANGED"):
            self.operation("publication_revoke", eligibility_epoch=1)

    def test_terminal_original_receipt_survives_positive_input_release_expiry_and_reclamation(self):
        self.grant()
        value = self.fixture.select(self.fixture.store.status("job_one"), [1])
        self.fixture.chunk(value, 0, 0, [[9007199254740993, native_fixture.EXACT]])
        published = self.fixture.publish(value)
        self.assertEqual(self.operation("publication_status")["release"], published)
        sequence = self.fixture.sequence()
        self.change_input(lambda record: record.update(references={
            "job_one": {"state": "RELEASED"}}, retain_until_unix=time.time() - 1))
        request = {"eligibility_epoch": 1, "attempt_id": "attempt_one", "execution_id": value["execution_id"],
                   "completion": value["selection"]["completion"]}
        for reclaimed in [False, True]:
            if reclaimed:
                self.fixture.writer.execute("DELETE FROM warehouse.ctl.input_exports WHERE export_id='input_original'")
            with self.subTest(reclaimed=reclaimed):
                self.assertEqual(self.operation("publication_status")["release"], published)
                self.assertEqual(self.operation("publication_import", **request)["release"], published)
                self.assertEqual(self.operation("publication_revoke", eligibility_epoch=1)["release"], published)
                self.assertEqual(self.fixture.sequence(), sequence)
        self.active = False
        with self.assertRaisesRegex(ControlError, "FORBIDDEN"):
            self.operation("publication_status")

    def test_revoked_original_status_survives_released_consumers_but_cannot_regain_eligibility(self):
        self.grant()
        revoked = self.operation("publication_revoke", eligibility_epoch=1)
        self.change_input(lambda record: record.update(references={}, retain_until_unix=time.time() - 1))
        self.assertEqual(self.operation("publication_status"), revoked)
        self.assertEqual(self.operation("publication_revoke", eligibility_epoch=1), revoked)
        with self.assertRaisesRegex(ControlError, "PUBLICATION_ELIGIBILITY_CHANGED"):
            self.operation("publication_import", eligibility_epoch=1, attempt_id="attempt_one",
                execution_id="execution_one", completion={
                    "s3_uri": "s3://test-bucket/spaces/owned/staging/gen_owned/demo/run_one/"
                        "job_one/attempt_one/execution_one/completion.json",
                    "object_version": "original", "sha256": "a" * 64, "bytes": 10})

    def test_revocation_is_sticky_for_a_new_attempt(self):
        self.grant()
        self.operation("publication_revoke", eligibility_epoch=1)
        replacement = copy.deepcopy(self.descriptor)
        replacement["attempt_id"] = "attempt_two"
        store = self.store()
        with self.assertRaisesRegex(ControlError, "PUBLICATION_ELIGIBILITY_CHANGED"):
            self.publisher.unit("fixture_regrant", lambda: store.grant(
                self.fixture.reference(replacement), replacement, self.authority, expected_epoch=2))

    def test_output_preparation_allows_ingress_then_chunks_publish_original_atomic_state(self):
        from colvenna_isle.pipeline.candidates import Candidates

        self.grant()
        value = self.fixture.select(self.fixture.store.status("job_one"), [513])
        # Keep verification outside Native: use its independent checker seam
        # (tested with real Parquet in test_candidates) to block original IO.
        completion, files = value["selection"]["completion"], value["selection"]["files"]
        entered, release = threading.Event(), threading.Event()

        def prepare(*args, **kwargs):
            entered.set()
            self.assertTrue(release.wait(5))
            return {"completion": completion, "files": files, "state": None}

        rows = [[i + 2, native_fixture.EXACT] for i in range(512)]
        with patch.object(Candidates, "prepare", side_effect=prepare), patch.object(Candidates, "chunks",
                return_value=iter([(0, 0, rows), (0, 1, [[9007199254740993, native_fixture.EXACT]])])):
            request = {"eligibility_epoch": 1, "attempt_id": "attempt_one",
                       "execution_id": value["execution_id"], "completion": completion}
            self.operation("publication_import", **request)
            self.assertTrue(entered.wait(3))
            try:
                def ingress():
                    return self.writes.commit(write_id("owned", "gen_owned", "ods.source", "during_import"), "owner",
                        {"dataset": "ods.source", "schema_version": 1, "operation": "insert",
                         "records": [{"id": 2, "amount": "99.99"}]},
                        self.fixture.configuration["datasets"]["ods.source"], lambda: None)
                committed = self.runtime.scheduler.submit("ingestion", "actual_traffic", ingress).future.result(timeout=3)
                self.assertEqual(committed["status"], "COMMITTED")
                self.assertIsNone(native_fixture.resolve_release(
                    self.fixture.reader, self.fixture.configuration, "dwd.result"))
            finally:
                release.set()
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                status = self.operation("publication_status")
                if status["status"] == "PUBLISHED":
                    break
                if status.get("worker", {}).get("state") in {"REJECTED", "UNKNOWN"}:
                    self.fail(str(status["worker"]))
                time.sleep(0.01)
            self.assertEqual(status["status"], "PUBLISHED")
            self.assertEqual(status["release"]["row_count"], 513)
            self.assertEqual(status["release"]["input_watermarks"]["source"]["watermark"], {"ods_sequence": 1})
            sequence = self.fixture.sequence()
            self.assertEqual(self.operation("publication_import", **request)["release"], status["release"])
            self.assertEqual(self.fixture.sequence(), sequence)

    def test_background_writer_import_commits_a_complete_bound_data_state_pair(self):
        from colvenna_isle.pipeline.candidates import Candidates

        self.fixture.bind_state()
        self.snapshot["datasets"]["dwd.result"] = self.fixture.configuration["datasets"]["dwd.result"]["definition_sha256"]
        self.descriptor["snapshot_sha256"] = self.published["snapshot_sha256"] = digest(self.snapshot)
        self.grant()
        value = self.fixture.select(self.fixture.store.status("job_one"), [1], state_counts=[1])
        completion = value["selection"]["completion"]
        prepared = {"completion": completion, "files": value["selection"]["files"],
                    "state": {"files": value["selection"]["state_files"]}}
        data = [(0, 0, [[9007199254740993, native_fixture.EXACT]])]
        state = [(0, 0, [[9007199254740993, 8, False, native_fixture.EXACT]])]
        with patch.object(Candidates, "prepare", return_value=prepared), \
                patch.object(Candidates, "chunks", return_value=iter(data)), \
                patch.object(Candidates, "state_chunks", return_value=iter(state)):
            request = {"eligibility_epoch": 1, "attempt_id": "attempt_one",
                       "execution_id": value["execution_id"], "completion": completion}
            self.operation("publication_import", **request)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                status = self.operation("publication_status")
                if status["status"] == "PUBLISHED":
                    break
                if status.get("worker", {}).get("state") in {"REJECTED", "UNKNOWN"}:
                    self.fail(str(status["worker"]))
                time.sleep(0.01)
        self.assertEqual(status["status"], "PUBLISHED")
        release = status["release"]
        self.assertEqual(self.fixture.rows(release), [(9007199254740993, native_fixture.EXACT)])
        self.assertEqual(self.fixture.state_rows(release), [(9007199254740993, 8, False, native_fixture.EXACT)])
        self.assertEqual(native_fixture.resolve_release(self.fixture.reader, self.fixture.configuration, "dwd.result"), release)


if __name__ == "__main__":
    unittest.main()
