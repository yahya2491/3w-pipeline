"""Read-only profile of the Bronze table used as input to Silver.

Usage:
    python -m threew.silver.profile --env dev
"""

import argparse
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from threew.bronze.inspect import load_contract
from threew.config import Config, load_config
from threew.silver.input import read_bronze_ready_marker
from threew.silver.quality import is_finite
from threew.spark import get_spark


def coverage_by_source_and_label(df: DataFrame) -> DataFrame:
    """Count rows, source-file versions, and missing row labels per recording category."""
    return (
        df.groupBy("source_type", "label")  # Keep real, simulated and drawn coverage separate.
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

    counts = df.agg(F.count("*").alias("total_rows"), *expressions).first().asDict()  # Scan once for all sensors.
    return [
        {
            "sensor": name,
            "total_rows": counts["total_rows"],
            "null": counts[f"sensor_{index}_null"],
            "nan": counts[f"sensor_{index}_nan"],
            "positive_infinity": counts[f"sensor_{index}_positive_infinity"],
            "negative_infinity": counts[f"sensor_{index}_negative_infinity"],
        }
        for index, name in enumerate(sensor_names)
    ]


def recording_sensor_counts(df: DataFrame, sensor_names: tuple[str, ...]) -> DataFrame:
    """Count sensors with finite readings in each source-file version."""
    recording_keys = ("source_type", "label", "_source_path", "_file_sha256")
    missing_columns = (set(sensor_names) | set(recording_keys)) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Bronze table is missing columns: {', '.join(sorted(missing_columns))}")
    if not sensor_names:
        return df.select(*recording_keys).distinct().withColumn("sensors_present", F.lit(0))

    present_columns = []  # Build one presence indicator per sensor before counting usable channels.
    for index, name in enumerate(sensor_names):
        present_columns.append(F.max(F.when(is_finite(F.col(name)), 1).otherwise(0)).alias(f"sensor_{index}_present"))

    per_sensor = df.groupBy(*recording_keys).agg(*present_columns)  # Reduce to recording-level coverage.
    count_expression = sum(F.col(f"sensor_{index}_present") for index in range(len(sensor_names)))
    return per_sensor.withColumn("sensors_present", count_expression)


def recording_sensor_summary(per_recording: DataFrame) -> DataFrame:
    """Summarize how many sensors have values in recordings of each category."""
    return (
        per_recording.groupBy("source_type", "label")
        .agg(
            F.count("*").alias("recordings"),
            F.min("sensors_present").alias("min_sensors_present"),
            F.round(F.avg("sensors_present"), 2).alias("avg_sensors_present"),
            F.max("sensors_present").alias("max_sensors_present"),
            F.count(F.when(F.col("sensors_present") == 0, 1)).alias("recordings_with_no_sensors"),
        )
        .orderBy("source_type", "label")
    )


def sensor_recording_availability(per_recording: DataFrame, sensor_names: tuple[str, ...]) -> DataFrame:
    """Count recordings with usable values for each sensor, summarized by source type."""
    if not sensor_names:
        return per_recording.sparkSession.createDataFrame(
            [],
            "source_type string, sensor string, recordings_present long, recordings_absent long, recordings_total long",
        )

    counts = per_recording.groupBy("source_type").agg(
        F.count("*").alias("recordings_total"),
        *[
            F.sum(F.col(f"sensor_{index}_present")).alias(f"sensor_{index}_recordings_present")
            for index in range(len(sensor_names))
        ],
    )
    sensor_entries = F.array(  # Turn the wide per-sensor totals into rows for display.
        *[
            F.struct(
                F.lit(name).alias("sensor"),
                F.col(f"sensor_{index}_recordings_present").alias("recordings_present"),
            )
            for index, name in enumerate(sensor_names)
        ]
    )
    return (
        counts.withColumn("_sensor", F.explode(sensor_entries))
        .select(
            "source_type",
            F.col("_sensor.sensor").alias("sensor"),
            F.col("_sensor.recordings_present").alias("recordings_present"),
            (F.col("recordings_total") - F.col("_sensor.recordings_present")).alias("recordings_absent"),
            "recordings_total",
        )
        .orderBy("source_type", "sensor")
    )


def timestamp_quality_by_recording(df: DataFrame) -> DataFrame:
    """Measure missing, duplicate, repeated, backward, and non-1-second times per recording."""
    required = {"source_type", "label", "_source_path", "_file_sha256", "_row_in_file", "timestamp"}
    missing_columns = required - set(df.columns)
    if missing_columns:
        raise ValueError(f"Bronze table is missing columns: {', '.join(sorted(missing_columns))}")

    recording_keys = ("source_type", "label", "_source_path", "_file_sha256")  # Keep file versions separate.
    ordered = Window.partitionBy("_source_path", "_file_sha256").orderBy("_row_in_file")  # Keep original order.
    with_previous = (
        df.withColumn("_profile_timestamp", F.col("timestamp"))
        .withColumn("_profile_previous_timestamp", F.lag("_profile_timestamp").over(ordered))
        .withColumn(
            "_profile_interval_seconds",
            (F.col("_profile_timestamp") - F.col("_profile_previous_timestamp")).cast("decimal(20,6)").cast("double"),
        )
    )
    stats = with_previous.groupBy(*recording_keys).agg(
        F.count("*").alias("rows"),
        F.count(F.when(F.col("timestamp").isNull(), 1)).alias("null_timestamps"),
        F.count("timestamp").alias("non_null_timestamps"),
        F.countDistinct("timestamp").alias("distinct_timestamps"),
        F.count(F.when(F.col("_profile_interval_seconds").isNotNull(), 1)).alias("intervals_compared"),
        F.count(F.when(F.col("_profile_interval_seconds") == 1, 1)).alias("one_second_intervals"),
        F.count(F.when(F.col("_profile_interval_seconds") == 0, 1)).alias("repeated_adjacent_timestamps"),
        F.count(F.when(F.col("_profile_interval_seconds") < 0, 1)).alias("backward_intervals"),
        F.count(F.when(F.col("_profile_interval_seconds") > 1, 1)).alias("gaps_over_one_second"),
        F.max(F.when(F.col("_profile_interval_seconds") > 1, F.col("_profile_interval_seconds"))).alias(
            "largest_gap_seconds"
        ),
    )
    return stats.withColumn("duplicate_timestamp_rows", F.col("non_null_timestamps") - F.col("distinct_timestamps"))


def timestamp_quality_summary(per_recording: DataFrame) -> DataFrame:
    """Summarize timestamp quality counts by source type and recording label."""
    return (
        per_recording.groupBy("source_type", "label")
        .agg(
            F.count("*").alias("recordings"),
            F.sum("rows").alias("rows"),
            F.sum("null_timestamps").alias("null_timestamps"),
            F.sum("duplicate_timestamp_rows").alias("duplicate_timestamp_rows"),
            F.sum(F.when(F.col("duplicate_timestamp_rows") > 0, 1).otherwise(0)).alias(
                "recordings_with_duplicate_times"
            ),
            F.sum("intervals_compared").alias("intervals_compared"),
            F.sum("one_second_intervals").alias("one_second_intervals"),
            F.sum("repeated_adjacent_timestamps").alias("repeated_adjacent_timestamps"),
            F.sum("backward_intervals").alias("backward_intervals"),
            F.sum("gaps_over_one_second").alias("gaps_over_one_second"),
            F.max("largest_gap_seconds").alias("largest_gap_seconds"),
        )
        .orderBy("source_type", "label")
    )


def profile_bronze(cfg: Config) -> None:
    """Print a read-only overview of Bronze sensors, labels, and timestamp consistency."""
    marker = read_bronze_ready_marker(cfg)  # Profiling still requires a verified Bronze publication.
    sensor_names = tuple(
        name for name, type_name in load_contract(cfg.schema.contract).items() if type_name == "double"
    )

    spark: SparkSession = get_spark(cfg)
    spark.sparkContext.setLogLevel("WARN")  # Keep the printed profile readable among Spark messages.
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
        missingness = sensor_missingness(df, sensor_names)
        total_rows = missingness[0]["total_rows"] if missingness else 0
        total_sensor_values = total_rows * len(sensor_names)
        null_sensor_values = sum(int(counts["null"]) for counts in missingness)
        null_percent = 100 * null_sensor_values / total_sensor_values if total_sensor_values else 0.0
        print(
            f"Across all sensors: {null_sensor_values:,} of {total_sensor_values:,} values are null "
            f"({null_percent:.2f}%)."
        )
        all_null_sensors = [
            str(counts["sensor"])
            for counts in missingness
            if counts["total_rows"]
            and counts["null"] == counts["total_rows"]
            and counts["nan"] == 0
            and counts["positive_infinity"] == 0
            and counts["negative_infinity"] == 0
        ]
        print(f"Completely null sensor columns: {len(all_null_sensors)} ({', '.join(all_null_sensors) or 'none'})")
        for counts in missingness:
            sensor_null_percent = 100 * counts["null"] / counts["total_rows"] if counts["total_rows"] else 0.0
            print(
                f"{counts['sensor']}: null={counts['null']:,}/{counts['total_rows']:,} "
                f"({sensor_null_percent:.2f}%), NaN={counts['nan']:,}, "
                f"+infinity={counts['positive_infinity']:,}, -infinity={counts['negative_infinity']:,}"
            )

        per_recording_sensors = recording_sensor_counts(df, sensor_names)
        print("\nUsable sensor counts per recording (a sensor is usable if it has a finite value):")
        recording_sensor_summary(per_recording_sensors).show(100, truncate=False)
        print("\nRecordings containing each sensor, grouped by source type:")
        sensor_recording_availability(per_recording_sensors, sensor_names).show(100, truncate=False)

        per_recording_times = timestamp_quality_by_recording(df)
        print("\nTimestamp checks by source type and label (intervals compare adjacent rows in original file order):")
        timestamp_quality_summary(per_recording_times).show(100, truncate=False)
    finally:
        spark.stop()  # Release the session even when a profiling query fails.


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", choices=("dev", "prod"), required=True)
    args = parser.parse_args(argv)
    profile_bronze(load_config(args.env))
    return 0


if __name__ == "__main__":
    sys.exit(main())
