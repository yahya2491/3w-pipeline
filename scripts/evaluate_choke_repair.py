"""Mask short known-valid choke stretches and measure bounded last-value error.

Run after Silver: python scripts/evaluate_choke_repair.py --env dev --output output/choke_repair.json
"""

import argparse
import json
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from threew.config import load_config
from threew.silver.prepare import OPENINGS, observed_valid
from threew.silver.rules import QualityRules
from threew.spark import get_spark


def evaluate_masked_repairs(observations, rules: QualityRules):
    """Use original block-start anchors, hiding the following 1..limit observations."""
    period = max(600, rules.choke_forward_fill_max_seconds + 1)
    frame = (
        Window.partitionBy("_source_path", "_file_sha256", "sequence_segment_id")
        .orderBy("_row_in_file")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    results = []
    for name in OPENINGS:
        valid = observed_valid(name, rules)
        block_start = (F.col("_row_in_file") % period == 0) & valid & F.col("sequence_usable")
        df = observations.withColumn(
            "_anchor",
            F.last(
                F.when(block_start, F.struct(F.col("timestamp").alias("time"), F.col(name).alias("value"))),
                ignorenulls=True,
            ).over(frame),
        )
        age = (F.col("timestamp") - F.col("_anchor.time")).cast("decimal(20,6)").cast("double")
        df = df.withColumn("_gap_seconds", age).filter(
            F.col("sequence_usable")
            & valid
            & (F.col("_row_in_file") % period).between(1, rules.choke_forward_fill_max_seconds)
            & (F.col("_gap_seconds") == F.col("_row_in_file") % period)
        )
        error = F.col(name) - F.col("_anchor.value")
        rows = (
            df.groupBy("source_type", "_gap_seconds")
            .agg(
                F.count("*").alias("masked_valid_measurements"),
                F.avg(F.abs(error)).alias("mean_absolute_error_pct_points"),
                F.sqrt(F.avg(error * error)).alias("root_mean_squared_error_pct_points"),
                F.max(F.abs(error)).alias("maximum_absolute_error_pct_points"),
            )
            .orderBy("source_type", "_gap_seconds")
            .collect()
        )
        results.extend({"sensor": name, **row.asDict()} for row in rows)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", required=True, choices=("dev", "prod"))
    parser.add_argument("--silver-root", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    cfg = load_config(args.env)
    root = args.silver_root or cfg.storage.silver
    marker = json.loads((root / "silver_ready.json").read_text())
    if marker.get("env") != args.env or marker.get("contract_version") != 2:
        raise ValueError("Select a successful contract-v2 Silver result for the requested environment")
    rules = QualityRules(**marker["quality_rules"]["resolved"])
    spark = get_spark(cfg)
    try:
        data = spark.read.parquet(marker["tables"]["silver_observations"]["path"])
        result = {
            "silver_run_id": marker["run_id"],
            "quality_rules": marker["quality_rules"],
            "method": "Mask short stretches after every 600th original row (or longer if configured).",
            "limitations": "Known-valid masked readings are not actual sensor faults; results do not validate event forecasting.",
            "results": evaluate_masked_repairs(data, rules),
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n")
        print(f"Repair evaluation saved to {args.output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
