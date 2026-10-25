"""Retention collector Lambda: hourly scheduled sweeps for enabled spaces and queued requests.

It has no permission on SlateDB database prefixes. Reserved concurrency is one,
and a per-space lease additionally serializes any overlapping invocation.
"""

import json
import os
import time
from functools import lru_cache

import boto3

from colvenna_isle.region import region


@lru_cache(maxsize=1)
def services():
    from botocore.config import Config
    from colvenna_isle.intake.backpressure import Admission
    from colvenna_isle.writer.ownership import Ownership
    from .keys import KeyStore
    from .pipeline_artifacts import PipelineArtifacts
    from .pipelines import Pipelines
    from .quotas import Quotas
    from .registry import Registry
    from .retention import Collector, Retention
    from .runs import Runs
    from .status import Ledger
    from .writer_client import WriterClient

    options = Config(connect_timeout=3, read_timeout=10, retries={"total_max_attempts": 2})
    client = boto3.client("dynamodb", region_name=region(), config=options)
    s3 = boto3.client("s3", region_name=region(), config=options)
    lam = boto3.client("lambda", region_name=region(), config=Config(connect_timeout=3, read_timeout=28, retries={"total_max_attempts": 1}))
    table, bucket = os.environ["COLVENNA_CONTROL_TABLE"], os.environ["COLVENNA_DATA_BUCKET"]
    registry, keys, quotas = Registry(client, table, bucket), KeyStore(client, table), Quotas(client, table)
    ownership = Ownership(client, table)
    writer = WriterClient(ownership, lam, os.environ["COLVENNA_WRITER_CONTROL_FUNCTION"])
    # Reads only: the collector never dispatches compute or changes run state.
    pipelines = Pipelines(registry, PipelineArtifacts(s3, bucket), None, keys)
    runs = Runs(pipelines, keys, writer, None, None)
    ledger = Ledger(ownership, lam, os.environ["COLVENNA_WRITER_CONTROL_FUNCTION"], os.environ["COLVENNA_WRITER_SOURCE_COMMIT"])
    admission = Admission(boto3.client("sqs", region_name=region(), config=options), boto3.client("cloudwatch", region_name=region(), config=options), ledger,
                          os.environ["COLVENNA_INGRESS_QUEUE"], os.environ["COLVENNA_DLQ"])
    return Collector(Retention(registry, keys, runs), writer, s3, bucket, admission, ledger, quotas)


def handler(event, context):
    collector = services()
    deadline = time.monotonic() + max(10, context.get_remaining_time_in_millis() / 1000 - 30)
    scheduled = collector.schedule() if (event or {}).get("source") == "colvenna.retention" else []
    result = collector.tick(deadline)
    print(json.dumps({"event": "RETENTION_TICK", "source_commit": os.environ.get("COLVENNA_SOURCE_COMMIT"),
                      "scheduled": scheduled, "collections": result["collections"]}, default=str), flush=True)
    return {"scheduled": scheduled, "collections": result["collections"]}
