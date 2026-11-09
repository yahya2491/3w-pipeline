"""Add trailing sensor features for the planned Gold classifier."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from threew.silver.quality import is_finite
from threew.silver.reference import OPENINGS, RECORDING_KEY
from threew.silver.rules import QualityRules


def add_sensor_features(observations: DataFrame, rules: QualityRules) -> DataFrame:
    """Use clean readings within one recording/segment; never read future values."""
    df = observations.withColumn("_feature_us", F.unix_micros(F.col("timestamp").cast("timestamp")))
    segment = (*RECORDING_KEY, "sequence_segment_id")  # No feature history can cross a recording or segment boundary.
    ordered = Window.partitionBy(*segment).orderBy("_row_in_file")
    prefix = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)  # Include this row in the streak.
    recent = (
        Window.partitionBy(*segment)
        .orderBy("_feature_us")
        .rangeBetween(-rules.feature_window_seconds * 1_000_000 + 1, 0)  # 120 readings at 1 Hz, including this row.
    )
    usable = F.col("sequence_usable")
    columns = []
    for name in rules.feature_sensors:
        value = F.col(f"{name}_clean")  # Use the cleaned sensor, not its raw Bronze value.
        columns.extend(
            [
                F.when(usable, F.avg(value).over(recent)).alias(f"{name}_mean_120s"),
                F.when(usable, F.stddev_samp(value).over(recent)).alias(f"{name}_stddev_120s"),  # Needs two readings.
                F.when(usable, value - F.lag(value).over(ordered)).alias(f"{name}_change"),  # Both values are needed.
                F.when(usable, F.count(value).over(recent)).alias(f"{name}_count_120s"),  # Show incomplete history.
            ]
        )
    for name in OPENINGS:
        value = F.col(name)  # Time the original invalid streak, even if we repaired the clean value.
        invalid = F.coalesce(
            usable & is_finite(value) & ~value.between(rules.opening_min_pct, rules.opening_max_pct), F.lit(False)
        )
        last_other_row = F.last(F.when(~invalid, F.col("_row_in_file")), ignorenulls=True).over(prefix)
        first_row = F.min("_row_in_file").over(prefix)  # A streak can begin at the first row of a segment.
        run_start = F.coalesce(last_other_row + 1, first_row)  # Start just after the last row that broke the streak.
        elapsed = (F.col("_row_in_file") - run_start) * rules.expected_interval_seconds
        columns.append(
            F.when(usable, F.when(invalid, elapsed).otherwise(0)).cast("long").alias(f"{name}_invalid_duration_seconds")
        )
    return df.select(*observations.columns, *columns)
