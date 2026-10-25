"""Actual typed Parquet/staging followed by independent writer re-verification."""

import copy
import hashlib
import io
import unittest
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq

from colvenna_isle.control.registry import ControlError, canonical
from colvenna_isle.pipeline.candidates import Candidates
from colvenna_isle.pipeline.output_validation import OutputValidator
from colvenna_isle.pipeline.parquet import write
from colvenna_isle.pipeline.task_descriptor import context_for
from colvenna_isle.pipeline.task_objects import TaskObjects
import test_task_adapter as task_fixture


class WriterCandidates(unittest.TestCase):
    def setUp(self):
        self.fixture = task_fixture.PythonAdapter()
        self.fixture.setUp()
        event = self.fixture.event()
        # Produce real typed Parquet without a user dependency such as pandas.
        # Task subprocesses are tested in the locked compute environment; this
        # consumer suite also runs in the actual Writer image.
        execution = "exec_candidate"
        output = self.fixture.root / "attempt_one" / execution / "result"
        output.mkdir(parents=True)
        context = context_for(self.fixture.descriptor, execution, output)
        context["inputs"]["source"]["files"][0]["local_path"] = str(self.fixture.root / "source.parquet")
        file = write(output / "part.parquet", task_fixture.COLUMNS, [[task_fixture.ROW]],
                     maximum_bytes=1048576, maximum_rows=10)
        manifest = {"contract_version": 2, **{name: context[name] for name in [
            "job_id", "attempt_id", "execution_id"]}, "output_name": "result", "format": "parquet",
            "schema": task_fixture.COLUMNS, "row_count": 1,
            "files": [{**file, "path": "part.parquet", "partition_values": {}}]}
        (output / "output-manifest.json").write_text(canonical(manifest))
        checked = OutputValidator(maximum_bytes=1048576, maximum_rows=10).verify(
            context, workspace=self.fixture.root)
        completion = self.fixture.adapter._stage(event["descriptor"], self.fixture.descriptor,
            context, checked, {"exit_code": 0}, SimpleNamespace(peak_bytes=0), lambda: None)
        self.result = {"execution_id": execution, "completion": completion}
        self.grant = {"status": "GRANTED", "descriptor": self.fixture.descriptor, "reference": event["descriptor"]}
        self.verifier = Candidates(TaskObjects(self.fixture.store, "test-bucket", "owned", "gen_owned"))
        self.index = 0

    def tearDown(self):
        self.fixture.tearDown()

    def prepare(self, reference=None, guard=lambda: None):
        self.index += 1
        directory = self.fixture.root / ("writer_" + str(self.index))
        directory.mkdir()
        return self.verifier.prepare(self.grant, self.result["execution_id"],
            reference or self.result["completion"], directory, guard)

    def changed_completion(self, change):
        objects = self.verifier.objects
        reference = self.result["completion"]
        from colvenna_isle.control.http import decode_json

        value = decode_json(objects.read(reference, "staging", maximum_bytes=1048576))
        change(value)
        return self.fixture.store.seed(objects.key(reference, "staging"), canonical(value).encode())

    def test_writer_reopens_original_files_with_exact_decimal_and_large_integer(self):
        prepared = self.prepare()
        chunks = list(self.verifier.chunks(prepared, lambda: None))
        self.assertEqual(chunks, [(0, 0, [(task_fixture.ROW[0], task_fixture.ROW[1])])])
        self.assertEqual(prepared["completion"], self.result["completion"])
        for file in prepared["files"]:
            self.assertNotIn("local_path", file)
        # A different latest object cannot change an already bound VersionId.
        key = self.verifier.objects.key(self.result["completion"], "staging")
        self.fixture.store.seed(key, b'{"status":"PUBLISHED"}')
        self.assertEqual(list(self.verifier.chunks(self.prepare(), lambda: None)), chunks)

    def test_altered_target_watermark_descriptor_or_epoch_cannot_grant_publication(self):
        changes = [lambda value: value["output"].update(dataset="dwd.other"),
            lambda value: value["input_manifest_hashes"].update(source="0" * 64),
            lambda value: value["descriptor"].update(sha256="0" * 64),
            lambda value: value.update(definition_epoch="0" * 64)]
        for change in changes:
            with self.subTest(change=change), self.assertRaisesRegex(ControlError, "PUBLICATION_COMPLETION_BINDING"):
                self.prepare(self.changed_completion(change))

    def test_missing_original_file_fails_even_when_completion_claims_a_complete_valid_output(self):
        from colvenna_isle.control.http import decode_json

        completion = decode_json(self.verifier.objects.read(
            self.result["completion"], "staging", maximum_bytes=1048576))
        key = self.verifier.objects.key({name: completion["files"][0][name] for name in [
            "s3_uri", "object_version", "sha256", "bytes"]}, "staging")
        del self.fixture.store.objects[key]
        with self.assertRaisesRegex(ControlError, "TASK_OBJECT_UNCONFIRMED"):
            self.prepare()

    def test_file_list_substitution_and_foreign_execution_are_rejected_before_native_import(self):
        def foreign(value):
            value["files"][0]["s3_uri"] = value["files"][0]["s3_uri"].replace("/owned/", "/foreign/")

        with self.assertRaisesRegex(ControlError, "TASK_OBJECT_SCOPE"):
            self.prepare(self.changed_completion(foreign))
        reference = copy.deepcopy(self.result["completion"])
        reference["s3_uri"] = reference["s3_uri"].replace(self.result["execution_id"], "execution_foreign")
        with self.assertRaisesRegex(ControlError, "PUBLICATION_EXECUTION_SCOPE"):
            self.prepare(reference)

    def test_revocation_during_exact_object_read_returns_no_verified_plan(self):
        calls = []

        def guard():
            calls.append(1)
            if len(calls) == 6:
                raise ControlError(403, "PERMIT_REVOKED")

        with self.assertRaisesRegex(ControlError, "PERMIT_REVOKED"):
            self.prepare(guard=guard)

    def test_matching_producer_hash_and_manifest_cannot_hide_a_wrong_actual_parquet_type(self):
        def wrong_type(value):
            buffer = io.BytesIO()
            pq.write_table(pa.table({"id": [str(task_fixture.ROW[0])], "amount": pa.array(
                [task_fixture.ROW[1]], type=pa.decimal128(38, 2))}), buffer)
            raw = buffer.getvalue()
            file = value["files"][0]
            metadata = {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
            value["manifest"]["files"][0].update(metadata)
            file.update(metadata)
            original = self.fixture.store.seed(file["s3_uri"].split("test-bucket/", 1)[1], raw)
            file["object_version"] = original["object_version"]
            manifest_raw = canonical(value["manifest"]).encode()
            value["manifest_sha256"] = hashlib.sha256(manifest_raw).hexdigest()
            value["output_manifest"] = self.fixture.store.seed(
                value["output_manifest"]["s3_uri"].split("test-bucket/", 1)[1], manifest_raw)

        with self.assertRaisesRegex(ControlError, "PARQUET_COLUMN_TYPE"):
            self.prepare(self.changed_completion(wrong_type))

    def paired_completion(self):
        objects = self.verifier.objects
        reference = self.result["completion"]
        from colvenna_isle.control.http import decode_json

        completion = decode_json(objects.read(reference, "staging", maximum_bytes=1048576))
        schema = copy.deepcopy(task_fixture.COLUMNS) + [{"name": "seen", "type": "BIGINT", "nullable": False}]
        self.grant["state_contract"] = {"protocol": "complete_image_v1", "schema": schema,
                                       "key_columns": ["id"], "allow_empty": False}
        directory = self.fixture.root / "state_producer"
        directory.mkdir()
        metadata = write(directory / "part.parquet", schema,
            [[(*task_fixture.ROW, 9007199254740993)]], maximum_bytes=1048576, maximum_rows=10)
        manifest = {**{name: completion["manifest"][name] for name in [
            "contract_version", "job_id", "attempt_id", "execution_id", "output_name", "format"]},
            "schema": schema, "row_count": 1,
            "files": [{**metadata, "path": "part.parquet", "partition_values": {}}]}
        prefix = objects.key(reference, "staging").removesuffix("completion.json")
        state_file = self.fixture.store.seed(prefix + "state/part.parquet", (directory / "part.parquet").read_bytes())
        raw = canonical(manifest).encode()
        state_manifest = self.fixture.store.seed(prefix + "state-manifest.json", raw)
        completion["state"] = {"contract_sha256": hashlib.sha256(
            canonical(self.grant["state_contract"]).encode()).hexdigest(), "manifest": manifest,
            "manifest_sha256": hashlib.sha256(raw).hexdigest(), "output_manifest": state_manifest,
            "files": [{**manifest["files"][0], "s3_uri": state_file["s3_uri"],
                       "object_version": state_file["object_version"]}]}
        self.result["completion"] = self.fixture.store.seed(objects.key(reference, "staging"), canonical(completion).encode())
        return completion

    def test_paired_state_is_independently_verified_with_exact_types_and_scoped_files(self):
        self.paired_completion()
        prepared = self.prepare()
        self.assertEqual(list(self.verifier.chunks(prepared, lambda: None)),
                         [(0, 0, [(task_fixture.ROW[0], task_fixture.ROW[1])])])
        self.assertEqual(list(self.verifier.state_chunks(prepared, lambda: None)),
                         [(0, 0, [(*task_fixture.ROW, 9007199254740993)])])
        self.assertIn("/state/", prepared["state"]["files"][0]["s3_uri"])

    def test_missing_state_file_unbound_contract_or_absent_state_cannot_return_verified_pair(self):
        completion = self.paired_completion()
        with self.assertRaisesRegex(ControlError, "PUBLICATION_STATE_BINDING"):
            self.prepare(self.changed_completion(lambda value: value["state"].update(contract_sha256="0" * 64)))
        with self.assertRaisesRegex(ControlError, "PUBLICATION_COMPLETION_BINDING"):
            self.prepare(self.changed_completion(lambda value: value.pop("state")))
        state_key = self.verifier.objects.key(
            {name: completion["state"]["files"][0][name] for name in ["s3_uri", "object_version", "sha256", "bytes"]},
            "staging")
        del self.fixture.store.objects[state_key]
        with self.assertRaisesRegex(ControlError, "TASK_OBJECT_UNCONFIRMED"):
            self.prepare()

    def test_forged_state_parquet_type_is_rejected_even_with_matching_all_producer_hashes(self):
        self.paired_completion()

        def wrong_state(value):
            state = value["state"]
            buffer = io.BytesIO()
            pq.write_table(pa.table({"id": [str(task_fixture.ROW[0])],
                "amount": pa.array([task_fixture.ROW[1]], type=pa.decimal128(38, 2)),
                "seen": pa.array([9007199254740993], type=pa.int64())}), buffer)
            raw = buffer.getvalue()
            metadata = {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
            state["manifest"]["files"][0].update(metadata)
            state["files"][0].update(metadata)
            key = self.verifier.objects.key(
                {name: state["files"][0][name] for name in ["s3_uri", "object_version", "sha256", "bytes"]},
                "staging")
            state["files"][0]["object_version"] = self.fixture.store.seed(key, raw)["object_version"]
            manifest_raw = canonical(state["manifest"]).encode()
            state["manifest_sha256"] = hashlib.sha256(manifest_raw).hexdigest()
            key = self.verifier.objects.key(state["output_manifest"], "staging")
            state["output_manifest"] = self.fixture.store.seed(key, manifest_raw)

        with self.assertRaisesRegex(ControlError, "PARQUET_COLUMN_TYPE"):
            self.prepare(self.changed_completion(wrong_state))


if __name__ == "__main__":
    unittest.main()
