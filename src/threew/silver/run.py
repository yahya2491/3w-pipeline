"""Build, save, and reconcile Silver outputs from a ready Bronze table.

Usage:
    python -m threew.silver.run --env dev

This first version writes only to the dev environment. Each run gets its own folder under
<storage.root>/silver/runs/, and silver_ready.json points to the latest successful run.
"""

import argparse
import json
import os
import sys
import uuid
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from threew.config import Config, load_config
from threew.silver.input import current_manifest_rows, read_bronze_ready_marker
from threew.silver.quality import SENSOR_FIELDS, build_quality_summaries
from threew.silver.transform import build_silver_observations
from threew.spark import get_spark

SparkFactory = Callable[[Config], AbstractContextManager[SparkSession]]
RECORDING_KEY = ("_source_path", "_file_sha256")


@contextmanager
def spark_session(cfg: Config) -> Iterator[SparkSession]:
    """Create one Spark session for the Silver run and always stop it afterwards."""
    session = get_spark(cfg)
    session.sparkContext.setLogLevel("WARN")
    try:
        yield session
    finally:
        session.stop()


def _sum_rows(df: DataFrame, column: str) -> int:
    value = df.agg(F.sum(F.col(column))).first()[0]
    return int(value or 0)


def _recording_count_mismatches(expected: DataFrame, actual: DataFrame, actual_count: str) -> int:
    """Count recordings missing from either table or having different row counts."""
    joined = expected.join(actual, on=list(RECORDING_KEY), how="full_outer")
    return joined.filter(
        F.col("bronze_rows").isNull() | F.col(actual_count).isNull() | (F.col("bronze_rows") != F.col(actual_count))
    ).count()


def _write_json_atomically(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def run_silver(cfg: Config, *, spark_factory: SparkFactory = spark_session) -> dict[str, Any]:
    """Write a versioned dev run and publish its marker only after read-back checks pass."""
    if cfg.env != "dev":
        raise ValueError("Silver writes are limited to --env dev until the production run is reviewed.")

    bronze_marker = read_bronze_ready_marker(cfg)
    manifest_rows = current_manifest_rows(cfg)
    now = datetime.now(UTC)
    run_id = f"{now:%Y%m%dT%H%M%S%fZ}-{uuid.uuid4().hex[:8]}"
    run_root = cfg.storage.silver / "runs" / run_id
    if run_root.exists():
        raise FileExistsError(f"Silver run folder already exists: {run_root}")

    table_paths = {
        "silver_observations": run_root / "silver_observations",
        "instance_quality": run_root / "instance_quality",
        "sensor_availability": run_root / "sensor_availability",
    }

    with spark_factory(cfg) as spark:
        bronze = spark.read.parquet(str(cfg.storage.bronze))
        silver = build_silver_observations(bronze, manifest_rows)
        summaries = build_quality_summaries(silver)
        try:
            silver.write.mode("errorifexists").parquet(str(table_paths["silver_observations"]))
            summaries.instance_quality.write.mode("errorifexists").parquet(str(table_paths["instance_quality"]))
            summaries.sensor_availability.write.mode("errorifexists").parquet(str(table_paths["sensor_availability"]))
        finally:
            summaries.unpersist()

        # Re-read the saved tables so the checks cover what downstream users will consume.
        silver_saved = spark.read.parquet(str(table_paths["silver_observations"]))
        quality_saved = spark.read.parquet(str(table_paths["instance_quality"]))
        sensors_saved = spark.read.parquet(str(table_paths["sensor_availability"]))

        bronze_counts = (
            bronze.groupBy(*RECORDING_KEY)
            .count()
            .withColumnRenamed("count", "bronze_rows")
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        silver_counts = (
            silver_saved.groupBy(*RECORDING_KEY)
            .count()
            .withColumnRenamed("count", "silver_rows")
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        try:
            bronze_recordings = bronze_counts.count()
            bronze_rows = _sum_rows(bronze_counts, "bronze_rows")
            silver_recordings = silver_counts.count()
            silver_rows = _sum_rows(silver_counts, "silver_rows")
            instance_quality_rows = quality_saved.count()
            sensor_availability_rows = sensors_saved.count()

            quality_counts = quality_saved.select(
                *RECORDING_KEY,
                F.col("recording_rows").alias("quality_rows"),
            )
            sensor_recording_shape = sensors_saved.groupBy(*RECORDING_KEY).agg(
                F.count("*").alias("sensor_rows"),
                F.countDistinct("sensor_name").alias("distinct_sensor_rows"),
                F.count(F.when(F.col("sensor_name").isNull() | ~F.col("sensor_name").isin(*SENSOR_FIELDS), 1)).alias(
                    "unknown_sensor_rows"
                ),
            )
            sensor_recording_issues = sensor_recording_shape.filter(
                (F.col("sensor_rows") != len(SENSOR_FIELDS))
                | (F.col("distinct_sensor_rows") != len(SENSOR_FIELDS))
                | (F.col("unknown_sensor_rows") != 0)
            ).count()

            checks = {
                "silver_observation_rows_match_bronze": silver_rows == bronze_rows,
                "silver_observation_row_counts_match_per_recording": (
                    _recording_count_mismatches(bronze_counts, silver_counts, "silver_rows") == 0
                ),
                "instance_quality_has_one_matching_row_per_recording": (
                    instance_quality_rows == bronze_recordings
                    and _recording_count_mismatches(bronze_counts, quality_counts, "quality_rows") == 0
                ),
                "sensor_availability_has_27_unique_sensors_per_recording": (
                    sensor_availability_rows == bronze_recordings * len(SENSOR_FIELDS)
                    and sensor_recording_shape.count() == bronze_recordings
                    and sensor_recording_issues == 0
                ),
            }
        finally:
            bronze_counts.unpersist()
            silver_counts.unpersist()

    passed = all(checks.values())
    counts = {
        "bronze_rows": bronze_rows,
        "silver_observation_rows": silver_rows,
        "bronze_recordings": bronze_recordings,
        "silver_recordings": silver_recordings,
        "instance_quality_rows": instance_quality_rows,
        "sensor_availability_rows": sensor_availability_rows,
    }
    reconciliation = {"passed": passed, "checks": checks, "counts": counts}
    report = {
        "run_id": run_id,
        "env": cfg.env,
        "started_at": now.isoformat(),
        "source": {
            "bronze_run_id": bronze_marker["run_id"],
            "repo_id": cfg.source.repo_id,
            "revision": bronze_marker["source"]["revision"],
        },
        "tables": {
            name: {"path": str(path), "rows": counts[row_key]}
            for name, path, row_key in (
                ("silver_observations", table_paths["silver_observations"], "silver_observation_rows"),
                ("instance_quality", table_paths["instance_quality"], "instance_quality_rows"),
                ("sensor_availability", table_paths["sensor_availability"], "sensor_availability_rows"),
            )
        },
        "reconcile": reconciliation,
    }
    report_path = cfg.storage.silver_runs / f"{run_id}.json"
    _write_json_atomically(report_path, report)
    if not passed:
        failed = [name for name, check_passed in checks.items() if not check_passed]
        raise RuntimeError(f"Silver reconciliation failed: {', '.join(failed)}. See {report_path}.")

    marker = {
        "run_id": run_id,
        "published_at": datetime.now(UTC).isoformat(),
        "env": cfg.env,
        "source": report["source"],
        "tables": report["tables"],
        "reconcile": reconciliation,
        "report": str(report_path),
    }
    _write_json_atomically(cfg.storage.silver_ready, marker)

    print(f"Bronze run: {bronze_marker['run_id']} ({bronze_rows:,} rows, {bronze_recordings:,} recordings)")
    print(f"Silver run: {run_id}")
    print(f"Silver observations: {silver_rows:,} rows")
    print(f"Instance quality: {instance_quality_rows:,} rows")
    print(f"Sensor availability: {sensor_availability_rows:,} rows")
    print(f"Reconciliation: all {len(checks)} checks passed")
    print(f"Ready marker: {cfg.storage.silver_ready}")
    return marker


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", choices=("dev",), required=True)
    args = parser.parse_args(argv)
    run_silver(load_config(args.env))
    return 0


if __name__ == "__main__":
    sys.exit(main())
