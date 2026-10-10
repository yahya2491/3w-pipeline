"""Read-only profile of the Bronze table used as input to Silver.

Usage:
    python -m threew.silver.profile --env dev
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from threew.bronze.inspect import load_contract
from threew.config import Config, load_config
from threew.spark import get_spark


def coverage_by_source_and_label(df: DataFrame) -> DataFrame:
    """Count rows, source-file versions, and missing row labels per recording category."""
    return (
        df.groupBy("source_type", "label")
        .agg(
            F.count("*").alias("rows"),
            F.countDistinct(F.struct("_source_path", "_file_sha256")).alias("recordings"),
            F.count(F.when(F.col("class").isNull(), 1)).alias("class_null"),
            F.count(F.when(F.col("state").isNull(), 1)).alias("state_null"),
        )
        .orderBy("source_type", "label")
    )


def sensor_missingness(df: DataFrame, sensor_names: tuple[str, ...]) -> list[dict[str, int | str]]:
    """Count null, NaN, and infinite values for each sensor using one Spark aggregation."""
    missing_columns = set(sensor_names) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Bronze table is missing sensor columns: {', '.join(sorted(missing_columns))}")
    if not sensor_names:
        return []

    expressions = []
    for index, name in enumerate(sensor_names):
        column = F.col(name)
        expressions.extend(
            (
                F.count(F.when(column.isNull(), 1)).alias(f"sensor_{index}_null"),
                F.count(F.when(F.isnan(column), 1)).alias(f"sensor_{index}_nan"),
                F.count(F.when(column == float("inf"), 1)).alias(f"sensor_{index}_positive_infinity"),
                F.count(F.when(column == float("-inf"), 1)).alias(f"sensor_{index}_negative_infinity"),
            )
        )

    counts = df.agg(*expressions).first().asDict()
    return [
        {
            "sensor": name,
            "null": counts[f"sensor_{index}_null"],
            "nan": counts[f"sensor_{index}_nan"],
            "positive_infinity": counts[f"sensor_{index}_positive_infinity"],
            "negative_infinity": counts[f"sensor_{index}_negative_infinity"],
        }
        for index, name in enumerate(sensor_names)
    ]


def _read_ready_marker(cfg: Config) -> dict[str, Any]:
    path = cfg.storage.bronze_ready
    if not path.is_file():
        raise FileNotFoundError(f"Bronze is not marked ready at {path}; run the Bronze pipeline first.")

    marker = json.loads(path.read_text(encoding="utf-8"))
    if marker.get("env") != cfg.env:
        raise ValueError(f"Bronze marker environment {marker.get('env')!r} does not match {cfg.env!r}.")
    if Path(marker.get("bronze_table", "")).resolve() != cfg.storage.bronze:
        raise ValueError("Bronze marker points to a different table than the current environment config.")
    reconcile = marker.get("reconcile")
    if not isinstance(reconcile, dict) or reconcile.get("passed") is not True:
        raise ValueError("Bronze ready marker does not report a successful reconciliation.")
    return marker


def profile_bronze(cfg: Config) -> None:
    """Print the Bronze schema, a few examples, coverage, and sensor missing-value counts."""
    marker = _read_ready_marker(cfg)
    sensor_names = tuple(
        name for name, type_name in load_contract(cfg.schema.contract).items() if type_name == "double"
    )

    spark: SparkSession = get_spark(cfg)
    spark.sparkContext.setLogLevel("WARN")
    try:
        df = spark.read.parquet(str(cfg.storage.bronze))

        print(f"Bronze run: {marker['run_id']}")
        print(f"Source revision: {marker['source']['revision']}")
        print("\nBronze schema:")
        df.printSchema()
        print("\nFirst five rows (examples only):")
        df.show(5, truncate=False)
        print("\nRows and missing labels by source type and recording label:")
        coverage_by_source_and_label(df).show(50, truncate=False)

        print("\nMissing or non-finite sensor values:")
        for counts in sensor_missingness(df, sensor_names):
            print(
                f"{counts['sensor']}: null={counts['null']:,}, NaN={counts['nan']:,}, "
                f"+infinity={counts['positive_infinity']:,}, -infinity={counts['negative_infinity']:,}"
            )
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", choices=("dev", "prod"), required=True)
    args = parser.parse_args(argv)
    profile_bronze(load_config(args.env))
    return 0


if __name__ == "__main__":
    sys.exit(main())
