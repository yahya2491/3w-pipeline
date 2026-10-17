"""Add the 30 observation-level sensor features used by downstream Gold models."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from threew.silver.issues import is_finite
from threew.silver.reference import SENSOR_RENAMES
from threew.silver.rules import QualityRules

OPENINGS = tuple(target for source, target in SENSOR_RENAMES.items() if source.startswith("ABER-"))


def add_sensor_features(observations: DataFrame, rules: QualityRules) -> DataFrame:
    """Use clean readings within one recording/segment; never read future values."""
    df = observations.withColumn("_feature_us", F.unix_micros(F.col("timestamp").cast("timestamp")))
    segment = ("_source_path", "_file_sha256", "sequence_segment_id")
    ordered = Window.partitionBy(*segment).orderBy("_row_in_file")
    prefix = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    # (current time - 120 seconds, current time]: exactly 120 readings at 1 Hz.
    recent = (
        Window.partitionBy(*segment)
        .orderBy("_feature_us")
        .rangeBetween(-rules.feature_window_seconds * 1_000_000 + 1, 0)
    )
    usable = F.col("sequence_usable")
    columns = []
    for name in rules.feature_sensors:
        value = F.col(f"{name}_clean")
        columns.extend(
            [
                F.when(usable, F.avg(value).over(recent)).alias(f"{name}_mean_120s"),
                F.when(usable, F.stddev_samp(value).over(recent)).alias(f"{name}_stddev_120s"),
                F.when(usable, value - F.lag(value).over(ordered)).alias(f"{name}_change"),
                F.when(usable, F.count(value).over(recent)).alias(f"{name}_count_120s"),
            ]
        )
    for name in OPENINGS:
        value = F.col(name)
        invalid = F.coalesce(
            usable & is_finite(value) & ~value.between(rules.opening_min_pct, rules.opening_max_pct), F.lit(False)
        )
        # Original readings define the run, even when a short clean repair exists.
        last_other_row = F.last(F.when(~invalid, F.col("_row_in_file")), ignorenulls=True).over(prefix)
        first_row = F.min("_row_in_file").over(prefix)
        run_start = F.coalesce(last_other_row + 1, first_row)
        elapsed = (F.col("_row_in_file") - run_start) * rules.expected_interval_seconds
        columns.append(
            F.when(usable, F.when(invalid, elapsed).otherwise(0)).cast("long").alias(f"{name}_invalid_duration_seconds")
        )
    return df.select(*observations.columns, *columns)
