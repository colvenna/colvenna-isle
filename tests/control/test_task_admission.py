"""Admission goes through fixed publication and pipeline:run, never arbitrary code/context."""

import copy
import json
import unittest
from unittest.mock import patch

import test_pipelines

from colvenna_isle.contracts.validation import ContractError
from colvenna_isle.control.registry import ControlError, Principal
from colvenna_isle.pipeline.task_descriptor import TaskAdmission
from colvenna_isle.pipeline.task_objects import TaskObjects


class TaskAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.fixture = test_pipelines.PipelineTests()
        self.fixture.setUp()
        self.version = self.fixture.publish()["pipeline_version"]
        self.runner = Principal("runner", frozenset(["space_one"]), frozenset(["pipeline:run"]),
                                generation=self.fixture.actor.generation)
        self.objects = TaskObjects(self.fixture.objects, "test-bucket", "space_one", self.runner.generation)
        self.provider_calls = []
        self.original = {"state": "READY", "space_id": "space_one", "generation": self.runner.generation,
            "manifest": {"space_id": "space_one", "generation": self.runner.generation,
                "export_id": "input_original", "inputs": {"source": {
                    "selection": {"mode": "snapshot"},
                    "manifest": {"contract_version": 2, "dataset": "ods.source", "release_id": "input_original",
                        "schema": [{"name": "id", "type": "BIGINT", "nullable": False}],
                        "watermark": {"ods_sequence": 1}, "files": [{
                            "s3_uri": f"s3://test-bucket/spaces/space_one/inputs/{self.runner.generation}/"
                                      "input_original/exec_original/part.parquet",
                            "object_version": "original-file", "sha256": "a" * 64, "rows": 1, "bytes": 512}]}}}}}
        self.admission = TaskAdmission(self.fixture.pipelines, self.provider, self.objects,
            lambda *args: {"dataset": "dwd.result", "expected_previous_release": None})

    def provider(self, actor, identity):
        self.provider_calls.append((actor, identity))
        return copy.deepcopy(self.original)

    def prepare(self, actor=None, parameters=None, version=None):
        return self.admission.prepare(actor or self.runner, "space_one", "demo", version or self.version,
            "transform", run_id="run_original", job_id="job_original", attempt_id="attempt_original",
            export_id="input_original", parameters=parameters or {})

    def test_published_snapshot_environment_and_original_inputs_are_fixed_before_context_put(self):
        event = self.prepare()
        descriptor = json.loads(self.objects.read(
            event["descriptor"], "task-contexts", maximum_bytes=1048576))
        bound = self.fixture.pipelines.bind_run(self.runner, "space_one", "demo", self.version)
        self.assertEqual(descriptor["pipeline_version"], self.version)
        self.assertEqual(descriptor["package"]["sha256"], bound["package"]["package_sha256"])
        self.assertEqual(descriptor["environment"]["executor_binding"],
                         bound["snapshot"]["executors"]["transform"])
        self.assertEqual(descriptor["inputs"]["source"]["files"][0]["object_version"], "original-file")
        self.assertNotIn("directory", descriptor["output_state"])
        self.assertEqual(self.provider_calls, [(self.runner, "input_original")])

    def test_manager_cross_space_old_generation_and_unpublished_version_cannot_admit(self):
        actors = [self.fixture.actor,
            Principal("foreign", frozenset(["other"]), frozenset(["pipeline:run"]),
                      generation=self.runner.generation),
            Principal("old", frozenset(["space_one"]), frozenset(["pipeline:run"]), generation="gen_old")]
        for actor in actors:
            with self.subTest(actor=actor), self.assertRaises(ControlError):
                self.prepare(actor)
        with self.assertRaisesRegex(ControlError, "PIPELINE_VERSION_NOT_FOUND"):
            self.prepare(version="unpublished")
        self.assertFalse(self.provider_calls)
        self.assertFalse(any("/task-contexts/" in key for key in self.fixture.objects.objects))

    def test_original_input_generation_dataset_scope_or_portable_paths_cannot_be_overridden(self):
        original = copy.deepcopy(self.original)
        changes = [
            lambda: self.original.update(generation="gen_foreign"),
            lambda: self.original["manifest"].update(export_id="input_different"),
            lambda: self.original["manifest"]["inputs"]["source"]["manifest"].update(dataset="ods.foreign"),
            lambda: self.original["manifest"]["inputs"]["source"]["manifest"]["files"][0].update(
                s3_uri="s3://test-bucket/spaces/foreign/inputs/gen_other/file.parquet"),
            lambda: self.original["manifest"]["inputs"]["source"]["manifest"]["files"][0].update(
                local_path="/untrusted/file.parquet"),
        ]
        for change in changes:
            self.original = copy.deepcopy(original)
            change()
            with self.subTest(change=change), self.assertRaises((ControlError, ContractError)):
                self.prepare()
        self.assertFalse(any("/task-contexts/" in key for key in self.fixture.objects.objects))

    def test_unregistered_parameter_cannot_become_a_role_target_or_api_credential(self):
        for parameters in [{"target": "ads.other"}, {"api_key": "synthetic-only"}, {"role": "foreign"}]:
            with self.subTest(parameters=parameters), self.assertRaises(ContractError):
                self.prepare(parameters=parameters)
        self.assertFalse(self.provider_calls)

    def test_rebuild_during_original_descriptor_put_retains_old_key_and_never_admits_new_generation(self):
        old_generation = self.runner.generation
        put = self.fixture.objects.put_object

        def put_then_rebuild(**request):
            result = put(**request)
            self.fixture.registry.rebuild("space_one", self.fixture.registry._read("space_one"))
            return result

        with patch.object(self.fixture.objects, "put_object", side_effect=put_then_rebuild):
            with self.assertRaisesRegex(ControlError, "TASK_UPLOAD_ORIGINAL_UNKNOWN") as failure:
                self.prepare()
        original = failure.exception.original_upload
        self.assertIn("/" + old_generation + "/", original["s3_uri"])
        self.assertIn("object_version", original)
        current = self.fixture.registry._read("space_one")["generation"]
        self.assertNotEqual(current, old_generation)
        self.assertFalse(any(f"/task-contexts/{current}/" in key for key in self.fixture.objects.objects))

    def test_original_previous_release_is_fixed_by_writer_and_cannot_redirect_output(self):
        self.admission.output_provider = lambda *args: {
            "dataset": "dwd.result", "expected_previous_release": "release_original"}
        event = self.prepare()
        descriptor = json.loads(self.objects.read(event["descriptor"], "task-contexts", maximum_bytes=1048576))
        self.assertEqual(descriptor["output_state"]["expected_previous_release"], "release_original")
        self.admission.output_provider = lambda *args: {
            "dataset": "dwd.foreign", "expected_previous_release": "release_original"}
        with self.assertRaisesRegex(ControlError, "TASK_OUTPUT_AUTHORITY_CHANGED"):
            self.prepare()

    def test_full_python_admission_cannot_omit_a_required_bound_state_image(self):
        self.admission.output_provider = lambda *args: {
            "dataset": "dwd.result", "expected_previous_release": None,
            "state_contract": {"protocol": "complete_image_v1"}}
        with self.assertRaisesRegex(ControlError, "TASK_STATE_AUTHORITY_NOT_READY"):
            self.prepare()
        self.assertFalse(any("/task-contexts/" in key for key in self.fixture.objects.objects))


if __name__ == "__main__":
    unittest.main()
