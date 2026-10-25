"""Approved S3 input helper: explicit VersionId/hash, one bounded object per batch iterator."""

import json
from pathlib import Path

from .task_adapter import MAX_INPUT_BYTES
from .task_objects import TaskObjects, input_reference

from colvenna_isle.region import region


def input_batches(file, *, batch_size=512):
    """Never pass s3_uri alone to a library that could silently read latest."""
    import boto3
    import pyarrow as pa
    import pyarrow.parquet as pq

    if type(batch_size) is not int or not 1 <= batch_size <= 512:
        raise ValueError("Input batch size must be between 1 and 512")
    environment = json.loads(Path("/opt/colvenna/environment.json").read_text())
    objects = TaskObjects(boto3.client("s3", region_name=region()), environment["bucket"],
                          environment["space_id"], environment["generation"])
    raw = objects.read(input_reference(file), "inputs", maximum_bytes=MAX_INPUT_BYTES)
    parquet = pq.ParquetFile(pa.BufferReader(raw))
    if parquet.metadata.num_rows != file["rows"]:
        raise ValueError("INPUT_ROW_COUNT")
    yield from parquet.iter_batches(batch_size=batch_size)
