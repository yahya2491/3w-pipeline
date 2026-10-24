"""Build Gold from the current validated clean-only Silver publication.

Run on the VM with the locked pipeline environment:
    python gold/build_gold_table.py --silver-root /mnt/silver-data/silver-schedule
"""

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from threew.silver.output import CONTRACT_VERSION

SILVER_ROOT = Path("/mnt/silver-data/silver-schedule")
GOLD = "/mnt/silver-data/gold/gold_table"
STRIDE = 60
RARE_LABEL, RARE_STRIDE = 2, 10
CAP = 200_000
SEED = 42
TEST_FRACTION = 0.2
VALUE_LIMIT = 1e12


def resolve_silver_input(root):
    """Read one ready marker; never mix publications or guess an output folder."""
    root = Path(root)
    marker = json.loads((root / "silver_ready.json").read_text())
    if marker.get("contract_version") != CONTRACT_VERSION or marker.get("reconcile", {}).get("passed") is not True:
        raise ValueError("Gold requires a verified current Silver publication; rebuild Silver first")
    path = Path(marker["tables"]["silver_observations"]["path"])
    if path.resolve() != root.resolve() / "runs" / marker["run_id"] / "silver_observations" or not path.is_dir():
        raise ValueError("Silver observations do not belong to the selected ready publication")
    return path


def prepare_gold_rows(obs):
    """Select clean targets and usable history; retain inference provenance."""
    sensors = sorted(c[: -len("_count_120s")] for c in obs.columns if c.endswith("_count_120s"))
    if not sensors:
        raise ValueError("Silver has no temporal sensor features")
    # Gold's existing class_code name now contains the single clean Silver target.
    df = obs.filter(F.col("class_code_clean").isNotNull() & (F.col("class_label_status") == "consistent")).withColumn(
        "class_code", F.col("class_code_clean")
    )
    df = df.withColumn(
        "label",
        F.when(F.col("class_code") >= 100, F.col("class_code") - 100).otherwise(F.col("class_code")).cast("int"),
    )
    feature_cols = []
    updates = {}
    for sensor in sensors:
        names = [f"{sensor}_{stat}" for stat in ("mean_120s", "stddev_120s", "change")]
        ok = F.col(f"{sensor}_count_120s") == 120
        for name in names:
            ok = ok & F.coalesce(F.abs(F.col(name)) <= VALUE_LIMIT, F.lit(True))
        updates.update({name: F.when(ok, F.col(name)) for name in names})
        feature_cols += names
    df = df.withColumns(updates)
    df = df.filter(F.coalesce(*[F.col(c) for c in feature_cols]).isNotNull())
    stride = F.when(F.col("label") == RARE_LABEL, RARE_STRIDE).otherwise(STRIDE)
    df = df.filter(F.col("_row_in_file") % stride == 0)
    keep = [
        "_source_path",
        "source_type",
        "well_id",
        "_row_in_file",
        "class_code",
        "class_was_inferred",
        "label",
    ] + feature_cols
    return df.select(*keep).withColumnRenamed("_source_path", "recording_id")


def split_gold_rows(df):
    """Split complete recordings; inferred targets are training-only."""
    rec = df.groupBy("recording_id").agg(
        F.max("label").alias("recording_event"), F.first("source_type").alias("rec_source")
    )
    ordered = Window.partitionBy("recording_event", "rec_source").orderBy(F.xxhash64("recording_id", F.lit(SEED)))
    rec = rec.withColumn("rank", F.percent_rank().over(ordered))
    rec = rec.withColumn("split", F.when(F.col("rank") < TEST_FRACTION, "test").otherwise("train"))
    df = df.join(F.broadcast(rec.select("recording_id", "recording_event", "split")), on="recording_id")
    # Held-out scores continue to use supplied labels, not backfilled targets.
    df = df.filter((F.col("split") == "train") | ~F.col("class_was_inferred"))
    counts = {row["label"]: row["count"] for row in df.groupBy("label").count().collect()}
    fractions = {label: min(1.0, CAP / count) for label, count in counts.items()}
    return df.sampleBy("label", fractions, seed=SEED)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-root", type=Path, default=SILVER_ROOT)
    parser.add_argument("--output", default=GOLD)
    args = parser.parse_args()
    source = resolve_silver_input(args.silver_root)
    spark = SparkSession.builder.appName("gold_table").getOrCreate()
    prepared = None
    try:
        prepared = prepare_gold_rows(spark.read.parquet(str(source))).cache()
        split_gold_rows(prepared).coalesce(1).write.mode("overwrite").parquet(args.output)
        print("Gold table written to", args.output)
    finally:
        if prepared is not None:
            prepared.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
