"""Real Native timing and process exits; authorization uses a local controller double."""

import copy
import json
import subprocess
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from colvenna_isle.control.registry import ControlError
from colvenna_isle.pipeline.task_descriptor import staging_prefix
from colvenna_isle.writer.ingestion import CommitUnknown
from colvenna_isle.writer.publication_qualification import PublicationQualification
import test_publications as native_fixture


class PublicationFaults(unittest.TestCase):
    def setUp(self):
        self.fixture = native_fixture.NativePublications()
        native_open = self.fixture.open

        def first_open():
            # Install the qualification identity before Native initialization.
            # An existing database's space identity must never be rewritten.
            configuration = self.fixture.configuration
            configuration["space_id"] = "probe_publication_local"
            configuration["database_uri"] = configuration["database_uri"].replace(
                "/owned/", "/probe_publication_local/")
            native_open()

        with patch.object(self.fixture, "open", side_effect=first_open):
            self.fixture.setUp()
        self.configuration = self.fixture.configuration
        self.fixture.bind_state()
        self.active = True
        self.logs = []
        self.runtime = SimpleNamespace(probe_space="probe_publication_local", probe_generation="gen_owned",
            log=lambda event, fields: self.logs.append({"event": event, **fields}))
        self.publisher = SimpleNamespace(runtime=self.runtime, actor=self.actor,
            store=lambda *args: (None, self.fixture.store), unit=lambda key, action: action())
        self.qualification = PublicationQualification(self.publisher)
        self.descriptor = self.fixture.descriptor()
        self.descriptor["space_id"] = "probe_publication_local"
        self.descriptor["package"]["s3_uri"] = self.descriptor["package"]["s3_uri"].replace(
            "/owned/", "/probe_publication_local/")
        self.grant = self.fixture.store.grant(
            self.fixture.reference(self.descriptor), self.descriptor, native_fixture.AUTHORITY)
        self.execution = "execution_one"
        prefix = "s3://test-bucket/" + staging_prefix(self.descriptor, self.execution)
        completion = {"s3_uri": prefix + "completion.json", "object_version": "completion-v1",
                      "sha256": "a" * 64, "bytes": 100}
        files = lambda namespace: [{"s3_uri": prefix + namespace + "/part.parquet",
            "object_version": "file-v1", "sha256": "b" * 64, "rows": 1, "bytes": 100}]
        self.value = self.fixture.store.select("job_one", 1, "attempt_one", self.execution,
            completion, files("result"), state_files=files("state"))
        self.request = {"space_id": "probe_publication_local", "generation": "gen_owned",
            "principal_id": "owner", "key_id": "key_owner", "job_id": "job_one",
            "eligibility_epoch": 1, "attempt_id": "attempt_one", "execution_id": self.execution}

    def tearDown(self):
        self.fixture.tearDown()

    def actor(self, request):
        if (not self.active or request["space_id"] != self.configuration["space_id"]
                or request["generation"] != self.configuration["generation"]):
            raise ControlError(403, "FORBIDDEN")
        return "verified_local_owner", {**copy.deepcopy(self.configuration), "route_version": 1}

    def data(self):
        return self.fixture.chunk(self.value, 0, 0, [[9007199254740993, native_fixture.EXACT]])

    def state(self):
        return self.fixture.chunk(self.value, 0, 0,
            [[9007199254740993, 8, False, native_fixture.EXACT]], image="state")

    def arm(self, phase, **fields):
        return self.qualification.execute({**self.request, "phase": phase,
            "fault": "commit_reply_lost", **fields})

    def test_fault_is_disabled_without_exact_installed_generation_and_fresh_actor(self):
        self.runtime.probe_space = ""
        with self.assertRaisesRegex(ControlError, "QUALIFICATION_DISABLED"):
            self.arm("publish")
        self.runtime.probe_space = self.request["space_id"]
        self.runtime.probe_generation = "gen_other"
        with self.assertRaisesRegex(ControlError, "QUALIFICATION_DISABLED"):
            self.arm("publish")
        self.runtime.probe_generation = self.request["generation"]
        self.active = False
        with self.assertRaisesRegex(ControlError, "FORBIDDEN"):
            self.arm("publish")
        self.assertFalse(self.qualification.faults)

    def test_stale_original_binding_and_conflicting_fault_are_rejected(self):
        for change in [{"eligibility_epoch": 2}, {"attempt_id": "attempt_old"},
                       {"execution_id": "execution_other"}]:
            with self.subTest(change=change), self.assertRaisesRegex(ControlError, "PUBLICATION_ELIGIBILITY_CHANGED"):
                self.arm("publish", **change)
        self.arm("publish")
        self.assertEqual(self.arm("publish")["fault"], "commit_reply_lost")
        with self.assertRaisesRegex(ControlError, "QUALIFICATION_FAULT_CONFLICT"):
            self.arm("publish", fault="after_commit_abort")

    def test_original_chunk_must_be_declared_and_not_previously_committed(self):
        with self.assertRaisesRegex(ControlError, "PUBLICATION_CHUNK_SCOPE"):
            self.arm("chunk", image="state", file_number=1, chunk_number=0)
        self.data()
        with self.assertRaisesRegex(ControlError, "QUALIFICATION_CHUNK_ALREADY_COMMITTED"):
            self.arm("chunk", image="data", file_number=0, chunk_number=0)
        self.assertFalse(self.qualification.faults)

    def test_revoked_actor_after_arming_cannot_trigger_any_publication_commit(self):
        self.data()
        self.state()
        sequence = self.fixture.sequence()
        self.arm("publish")
        self.active = False
        with self.assertRaisesRegex(ControlError, "FORBIDDEN"):
            self.qualification.run(self.fixture.store, self.request, "publish",
                lambda: self.fixture.publish(self.value))
        self.assertEqual(self.fixture.sequence(), sequence)
        self.assertIsNone(native_fixture.resolve_release(
            self.fixture.reader, self.configuration, "dwd.result"))
        self.assertFalse(self.logs)

    def test_real_state_chunk_commit_lost_reply_pauses_then_reconciles_original_sequence(self):
        self.data()
        self.arm("chunk", image="state", file_number=0, chunk_number=0)
        with self.assertRaisesRegex(CommitUnknown, "COMMIT_UNKNOWN"):
            self.qualification.run(self.fixture.store, self.request, "chunk", self.state,
                image="state", file_number=0, chunk_number=0)
        with self.assertRaisesRegex(CommitUnknown, "NATIVE_RESULT_UNKNOWN"):
            self.fixture.store.status("job_one")
        self.assertEqual(self.logs[-1]["effect"], "REAL_COMMIT_RETURNED")
        self.assertEqual(self.logs[-1]["image"], "state")
        self.assertFalse(self.qualification.faults)
        self.fixture.reopen()
        sequence = self.fixture.sequence()
        self.assertEqual(self.state()["commit_sequence"], sequence)
        self.assertEqual(self.fixture.sequence(), sequence)
        self.assertEqual(self.fixture.publish(self.value)["state"]["row_count"], 1)

    def test_real_pair_publication_lost_reply_recovers_pointers_and_original_receipt(self):
        self.data()
        self.state()
        self.arm("publish")
        with self.assertRaisesRegex(CommitUnknown, "COMMIT_UNKNOWN"):
            self.qualification.run(self.fixture.store, self.request, "publish",
                lambda: self.fixture.publish(self.value))
        self.fixture.reopen()
        original = self.fixture.store.status("job_one")["release"]
        sequence = self.fixture.sequence()
        self.assertEqual(native_fixture.resolve_release(
            self.fixture.reader, self.configuration, "dwd.result"), original)
        self.assertEqual(self.fixture.publish(self.value), original)
        self.assertEqual(self.fixture.sequence(), sequence)
        self.assertEqual(self.fixture.state_rows(original),
                         [(9007199254740993, 8, False, native_fixture.EXACT)])

    def test_real_child_process_exits_before_or_after_publication_commit(self):
        self.data()
        self.state()
        # Close the local writer before the separate actual process acquires its
        # file. These are real exit137 tests, not mocked os._exit calls.
        self.fixture.reader.close()
        self.fixture.writer.close()
        script = '''
import json,sys,duckdb
from colvenna_isle.writer.ingestion import IngestionStore
from colvenna_isle.pipeline.publications import Publications
from colvenna_isle.writer.publication_qualification import _CommitFault
c=duckdb.connect()
c.execute("ATTACH '"+sys.argv[1]+"' AS warehouse")
n=IngestionStore(c,json.loads(sys.argv[2]),lambda:{"epoch":1})
p=Publications(n,lambda value:None)
p.initialize()
n.connection=_CommitFault(c,sys.argv[3],{"phase":"publish"},lambda e,r:print(json.dumps(r),flush=True))
p.publish("job_one",1,"attempt_one","execution_one")
raise RuntimeError("Fault did not exit the real process")
'''
        for fault, terminal in [("before_commit_abort", "IMPORTING"), ("after_commit_abort", "PUBLISHED")]:
            process = subprocess.run([sys.executable, "-c", script, str(self.fixture.path),
                json.dumps(self.configuration), fault], capture_output=True, text=True, timeout=10)
            self.assertEqual(process.returncode, 137, process.stderr)
            self.fixture.open()
            self.assertEqual(self.fixture.store.status("job_one")["status"], terminal)
            current = native_fixture.resolve_release(self.fixture.reader, self.configuration, "dwd.result")
            if terminal == "IMPORTING":
                self.assertIsNone(current)
            else:
                self.assertEqual(self.fixture.rows(current), [(9007199254740993, native_fixture.EXACT)])
                self.assertEqual(self.fixture.state_rows(current),
                                 [(9007199254740993, 8, False, native_fixture.EXACT)])
            self.fixture.reader.close()
            self.fixture.writer.close()
        self.fixture.open()


if __name__ == "__main__":
    unittest.main()
