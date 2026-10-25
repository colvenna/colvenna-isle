"""Submit one fixed Colvenna Isle pipeline version from Airflow and wait for its publication.

Colvenna Isle owns the pipeline's internal DAG, fixed inputs, imports and publication;
Airflow only orchestrates across systems. The Idempotency-Key is derived from
the Airflow DAG run and task (not the try number), so retries, worker restarts
and reconnects observe the same Colvenna Isle run. An Airflow timeout never cancels the
Colvenna Isle run; cancellation is an explicit, separate API call.
"""

import json
import re
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.models import Variable

TERMINAL = {"PUBLISHED", "FAILED", "CANCELLED"}


def idempotency_key(dag_id, run_id, task_id):
    raw = "airflow:" + dag_id + ":" + run_id + ":" + task_id
    value = re.sub(r"[^A-Za-z0-9_.:-]", "-", raw)
    if len(value) > 128:
        import hashlib

        value = "airflow:" + hashlib.sha256(raw.encode()).hexdigest()
    return value


class ColvennaClient:
    def __init__(self, api_url, token, *, timeout=30):
        self.api_url, self.token, self.timeout = api_url.rstrip("/"), token, timeout

    def request(self, method, path, body=None, headers=None):
        request = Request(self.api_url + path, method=method, data=None if body is None else json.dumps(body).encode(),
                          headers={"Authorization": "Bearer " + self.token, "Content-Type": "application/json",
                                   **(headers or {})})
        try:
            with urlopen(request, timeout=self.timeout) as response:
                return response.status, json.loads(response.read() or b"null")
        except HTTPError as error:
            with error:
                return error.code, json.loads(error.read() or b"null")


class ColvennaPipelineRunOperator(BaseOperator):
    """Run a fixed Colvenna Isle pipeline version; succeed only once Colvenna Isle reports PUBLISHED."""

    template_fields = ("pipeline_version", "parameters")

    def __init__(self, *, space_id, pipeline_id, pipeline_version, api_url_variable="colvenna_api_url",
                 token_variable="colvenna_pipeline_token", parameters=None, poll_seconds=15,
                 wait_seconds=3600, **kwargs):
        super().__init__(**kwargs)
        self.space_id, self.pipeline_id, self.pipeline_version = space_id, pipeline_id, pipeline_version
        self.api_url_variable, self.token_variable = api_url_variable, token_variable
        self.parameters, self.poll_seconds, self.wait_seconds = parameters or {}, poll_seconds, wait_seconds

    def execute(self, context):
        client = ColvennaClient(Variable.get(self.api_url_variable), Variable.get(self.token_variable))
        key = idempotency_key(context["dag"].dag_id, context["run_id"], self.task_id)
        body = {"pipeline_version": self.pipeline_version, "mode": "snapshot"}
        if self.parameters:
            body["parameters"] = self.parameters
        path = f"/v1/spaces/{self.space_id}/pipelines/{self.pipeline_id}/runs"
        for _ in range(5):
            try:
                status, value = client.request("POST", path, body, {"Idempotency-Key": key})
            except (URLError, TimeoutError):
                # The same key returns the same run; a lost reply is retried.
                time.sleep(self.poll_seconds)
                continue
            if status != 503:
                break
            time.sleep(self.poll_seconds)
        else:
            raise AirflowException("Colvenna Isle run admission unconfirmed; retry this task with the same key")
        if status != 202:
            raise AirflowException(f"Colvenna Isle rejected the run request: {status} {value}")
        run_id = value["run_id"]
        self.log.info("Colvenna Isle run %s (Idempotency-Key %s)", run_id, key)
        deadline = time.monotonic() + self.wait_seconds
        while time.monotonic() < deadline:
            try:
                status, value = client.request("GET", f"{path}/{run_id}")
            except (URLError, TimeoutError):
                time.sleep(self.poll_seconds)
                continue
            if status == 200 and value["status"] in TERMINAL:
                if value["status"] != "PUBLISHED":
                    raise AirflowException(f"Colvenna Isle run {run_id} ended {value['status']}")
                releases = {task["task_id"]: task.get("release_id") for task in value["tasks"]}
                self.log.info("Colvenna Isle run %s PUBLISHED: %s", run_id, releases)
                return {"run_id": run_id, "status": value["status"], "releases": releases,
                        "observed_at": value["observed_at"]}
            if status not in {200, 503}:
                raise AirflowException(f"Colvenna Isle run status unavailable: {status} {value}")
            time.sleep(self.poll_seconds)
        # Timing out stops waiting only; the Colvenna Isle run keeps its own outcome.
        raise AirflowException(f"Colvenna Isle run {run_id} not published within {self.wait_seconds}s (not cancelled)")
