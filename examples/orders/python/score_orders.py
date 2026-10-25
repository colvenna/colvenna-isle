#!/usr/bin/env python3
"""Example Python transform: fixed Parquet inputs -> Parquet plus output manifest."""

import argparse
import hashlib
import json
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ARROW_SCHEMA = pa.schema(
    [
        pa.field("order_id", pa.int64(), nullable=False),
        pa.field("high_value", pa.bool_(), nullable=False),
        pa.field("value_band", pa.string(), nullable=False),
    ]
)


def run(context_path: Path, output_dir: Path) -> None:
    context = json.loads(context_path.read_text(encoding="utf-8"))
    if context["contract_version"] != 2 or context["input_mode"] != "local_files":
        raise ValueError("This example requires contract v2 with local_files inputs")
    if output_dir.resolve() != Path(context["output"]["directory"]).resolve():
        raise ValueError("Output directory must match the runner's context")
    threshold = Decimal(context["parameters"]["high_value_threshold"])
    if not threshold.is_finite() or threshold < 0:
        raise ValueError("high_value_threshold must be a finite non-negative decimal")

    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "part-00000.parquet"
    manifest_file = output_dir / "output-manifest.json"
    if output_file.exists() or manifest_file.exists():
        raise FileExistsError("Each execution must use a fresh attempt directory")

    row_count = 0
    with pq.ParquetWriter(output_file, ARROW_SCHEMA, compression="zstd") as writer:
        for file in context["inputs"]["orders"]["files"]:
            parquet = pq.ParquetFile(file["local_path"])
            for batch in parquet.iter_batches(batch_size=65536, columns=["order_id", "amount"]):
                source = batch.to_pandas()
                if source["order_id"].isna().any() or source["amount"].isna().any():
                    raise ValueError("Input order_id and amount must be non-null")
                # Parquet DECIMAL becomes decimal.Decimal; retain exact money values.
                high_value = source["amount"].ge(threshold)
                band = pd.Series("REGULAR", index=source.index, dtype="string")
                band.loc[high_value] = "HIGH"
                band.loc[source["amount"].lt(Decimal("0"))] = "REFUND"
                result = pd.DataFrame(
                    {
                        "order_id": source["order_id"],
                        "high_value": high_value,
                        "value_band": band,
                    }
                )
                table = pa.Table.from_pandas(result, schema=ARROW_SCHEMA, preserve_index=False)
                writer.write_table(table)
                row_count += table.num_rows

    with output_file.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    manifest = {
        "contract_version": 2,
        "job_id": context["job_id"],
        "attempt_id": context["attempt_id"],
        "execution_id": context["execution_id"],
        "output_name": "result",
        "format": "parquet",
        "schema": context["output"]["schema"],
        "row_count": row_count,
        "files": [
            {
                "path": output_file.name,
                "rows": row_count,
                "bytes": output_file.stat().st_size,
                "sha256": digest,
                "partition_values": {},
            }
        ],
        "metrics": {"scored_orders": row_count},
    }
    # Write the manifest last. A partial file without a manifest is not a result.
    temporary = output_dir / "output-manifest.json.tmp"
    temporary.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    temporary.replace(manifest_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    run(args.context, args.output_dir)
