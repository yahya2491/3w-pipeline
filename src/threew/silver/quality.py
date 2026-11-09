"""Build separate recording-level and sensor-level Silver quality outputs."""

from typing import NamedTuple

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from threew.silver.reference import RECORDING_KEY, SENSOR_FIELDS

RECORDING_FIELDS = (
    "source_type",
    "instance_label",
    "instance_event_name",
    "_source_path",
    "_file_sha256",
    "instance_id",
    "well_id",
    "instance_start",
)


class QualitySummaries(NamedTuple):
    """The two summary DataFrames and their shared cached recording metrics."""

    instance_quality: DataFrame
    sensor_availability: DataFrame
    recording_metrics: DataFrame

    def unpersist(self) -> None:
        """Release the small shared recording-level cache after both outputs are consumed."""
        self.recording_metrics.unpersist()  # Both summaries share this cache, so release it after both are consumed.


def is_finite(value: Column) -> Column:
    """Whether a reading is finite; domain validity is checked separately."""
    return value.isNotNull() & ~F.isnan(value) & (value != float("inf")) & (value != float("-inf"))


def with_timestamp_intervals(silver: DataFrame) -> DataFrame:
    """Compare source-order neighbors without changing original timestamps or rows."""
    ordered = Window.partitionBy(*RECORDING_KEY).orderBy("_row_in_file")
    return (
        silver.withColumn("_quality_timestamp", F.col("timestamp"))
        .withColumn(
            "_quality_previous_timestamp",
            F.lag("_quality_timestamp").over(ordered),  # Compare in source order, even if time goes backwards.
        )
        .withColumn(
            "_quality_interval_seconds",
            (F.col("_quality_timestamp") - F.col("_quality_previous_timestamp")).cast("decimal(20,6)").cast("double"),
        )
    )


def _recording_metrics(silver: DataFrame) -> DataFrame:
    required = {
        *RECORDING_FIELDS,
        "_row_in_file",
        "class_code",
        "state_code",
        "timestamp",
        "row_event_phase",
        "class_label_status",
        *SENSOR_FIELDS,
    }
    missing_columns = required - set(silver.columns)
    if missing_columns:
        raise ValueError(f"Silver table is missing columns: {', '.join(sorted(missing_columns))}")

    timed = with_timestamp_intervals(silver)  # Add intervals without replacing any source timestamp.

    sensor_metrics = []
    for index, name in enumerate(SENSOR_FIELDS):
        finite = is_finite(F.col(name))  # Coverage measures presence; domain checks are handled elsewhere.
        sensor_metrics.append(F.sum(F.when(finite, 1).otherwise(0)).alias(f"_sensor_{index}_finite_rows"))

    metrics = timed.groupBy(*RECORDING_FIELDS).agg(  # Collapse the wide observations to one row per recording.
        F.count("*").alias("recording_rows"),
        F.count(F.when(F.col("class_code").isNull(), 1)).alias("unlabeled_rows"),
        F.count(F.when(F.col("state_code").isNull(), 1)).alias("null_state_rows"),
        F.count(F.when(F.col("timestamp").isNull(), 1)).alias("null_timestamp_rows"),
        F.count("timestamp").alias("_non_null_timestamp_rows"),  # Null clocks do not count as duplicate timestamps.
        F.countDistinct("timestamp").alias("_distinct_timestamp_rows"),  # Repeated clocks may be non-adjacent too.
        F.count(F.when(F.col("_quality_interval_seconds").isNotNull(), 1)).alias("timestamp_intervals_compared"),
        F.count(F.when(F.col("_quality_interval_seconds") == 1, 1)).alias("one_second_intervals"),
        F.count(F.when(F.col("_quality_interval_seconds") == 0, 1)).alias("repeated_adjacent_timestamps"),
        F.count(F.when(F.col("_quality_interval_seconds") < 0, 1)).alias("backward_intervals"),
        F.count(F.when(F.col("_quality_interval_seconds") > 1, 1)).alias("gaps_over_one_second"),
        F.max(F.when(F.col("_quality_interval_seconds") > 1, F.col("_quality_interval_seconds"))).alias(
            "largest_gap_seconds"
        ),
        F.count(F.when(F.col("class_label_status") == "inconsistent", 1)).alias("inconsistent_class_rows"),
        F.count(F.when(F.col("row_event_phase") == "unrecognized", 1)).alias("unrecognized_class_rows"),
        *sensor_metrics,
    )

    metrics = metrics.withColumn(
        "duplicate_timestamp_rows",
        F.col("_non_null_timestamp_rows") - F.col("_distinct_timestamp_rows"),
    )
    sensors_present = F.lit(0)  # Count a sensor as present if it has at least one finite reading.
    for index in range(len(SENSOR_FIELDS)):
        sensors_present = sensors_present + F.when(F.col(f"_sensor_{index}_finite_rows") > 0, 1).otherwise(0)

    return (
        metrics.withColumn("sensors_present", sensors_present)
        .withColumn("sensors_missing_entire_recording", F.lit(len(SENSOR_FIELDS)) - F.col("sensors_present"))
        .withColumn("no_sensors_present", F.col("sensors_present") == 0)
    )


def build_quality_summaries(silver: DataFrame) -> QualitySummaries:
    """Build one quality row per recording and one availability row per recording and sensor.

    A sensor is considered present only when it has at least one finite value.
    Domain validity is checked separately in the issue table and clean columns.
    """
    metrics = _recording_metrics(silver).persist(StorageLevel.MEMORY_AND_DISK)  # Share this small summary.

    instance_quality_columns = [
        *RECORDING_FIELDS,
        "recording_rows",
        "unlabeled_rows",
        "null_state_rows",
        "null_timestamp_rows",
        "duplicate_timestamp_rows",
        "timestamp_intervals_compared",
        "one_second_intervals",
        "repeated_adjacent_timestamps",
        "backward_intervals",
        "gaps_over_one_second",
        "largest_gap_seconds",
        "inconsistent_class_rows",
        "unrecognized_class_rows",
        "sensors_present",
        "sensors_missing_entire_recording",
        "no_sensors_present",
    ]
    instance_quality = metrics.select(*instance_quality_columns)  # Leave temporary per-sensor totals out of this table.

    sensor_entries = F.array(  # Bundle sensor totals so we can turn them into separate rows.
        *[
            F.struct(
                F.lit(name).alias("sensor_name"),
                F.col(f"_sensor_{index}_finite_rows").alias("rows_with_finite_values"),
            )
            for index, name in enumerate(SENSOR_FIELDS)
        ]
    )
    sensor_availability = (
        metrics.withColumn("_sensor", F.explode(sensor_entries))  # Produce one row per sensor per recording.
        .select(
            *RECORDING_FIELDS,
            "recording_rows",
            F.col("_sensor.sensor_name").alias("sensor_name"),
            F.col("_sensor.rows_with_finite_values").alias("rows_with_finite_values"),
        )
        .withColumn("rows_without_finite_values", F.col("recording_rows") - F.col("rows_with_finite_values"))
        .withColumn(
            "percent_rows_with_finite_values",
            F.round(F.lit(100.0) * F.col("rows_with_finite_values") / F.col("recording_rows"), 2),
        )
        .withColumn("has_finite_values", F.col("rows_with_finite_values") > 0)
    )

    return QualitySummaries(instance_quality, sensor_availability, metrics)
