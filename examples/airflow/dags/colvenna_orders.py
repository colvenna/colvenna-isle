"""External orchestration: release downstream work only after Colvenna Isle publishes."""

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from colvenna_operator import ColvennaPipelineRunOperator


def downstream(**context):
    result = context["ti"].xcom_pull(task_ids="colvenna_orders")
    if result["status"] != "PUBLISHED" or not all(result["releases"].values()):
        raise ValueError("Colvenna Isle run is not published")
    # A real downstream system would read these exact releases.
    print({"colvenna_run": result["run_id"], "releases": result["releases"]})


with DAG("colvenna_orders", start_date=datetime(2026, 10, 1), schedule=None, catchup=False, max_active_runs=1) as dag:
    colvenna = ColvennaPipelineRunOperator(
        task_id="colvenna_orders", space_id=Variable.get("colvenna_space_id", default_var="space"),
        pipeline_id="orders", pipeline_version="{{ var.value.colvenna_orders_version }}",
        poll_seconds=10, wait_seconds=1800, retries=2)
    release = PythonOperator(task_id="downstream_after_publication", python_callable=downstream)
    colvenna >> release
