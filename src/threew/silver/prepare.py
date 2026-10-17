"""Causal measurement preparation and elapsed-time features using Spark SQL only."""

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from threew.silver.issues import is_finite
from threew.silver.reference import SENSOR_RENAMES
from threew.silver.rules import QualityRules

RECORDING = ("_source_path", "_file_sha256")
OPENINGS = tuple(target for source, target in SENSOR_RENAMES.items() if source.startswith("ABER-"))
PRESSURE_DROP = "production_choke_pressure_drop_pa"


def observed_valid(name, rules):
    value = F.col(name)
    valid = is_finite(value)
    if name in OPENINGS:
        valid = valid & value.between(rules.opening_min_pct, rules.opening_max_pct)
    elif name.endswith("_valve_state"):
        valid = valid & value.isin(*rules.valve_states)
    return F.coalesce(valid, F.lit(False))


def prepare_observations(observations: DataFrame, rules: QualityRules) -> DataFrame:
    """Retain source columns/rows; add clean values, repair provenance and causal features."""
    original = observations.columns
    ordered = Window.partitionBy(*RECORDING).orderBy("_row_in_file")
    prefix = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df = observations.withColumn("_prep_us", F.unix_micros(F.col("timestamp").cast("timestamp")))
    df = df.select(
        "*",
        F.lag("_prep_us").over(ordered).alias("_prep_prev_us"),
        F.lag("_row_in_file").over(ordered).alias("_prep_prev_row"),
    )
    step = (F.col("_prep_us") - F.col("_prep_prev_us")) / F.lit(1_000_000.0)
    df = df.withColumn("_prep_step", step).withColumn(
        "sequence_usable",
        F.col("_prep_us").isNotNull() & (F.col("_prep_prev_us").isNull() | (F.col("_prep_step") > 0)),
    )
    df = df.withColumn("_prep_prev_usable", F.lag("sequence_usable").over(ordered))
    boundary = (
        F.col("_prep_prev_us").isNull()
        | F.col("_prep_us").isNull()
        | (F.col("_prep_step") != rules.expected_interval_seconds)
        | (F.col("_row_in_file") != F.col("_prep_prev_row") + 1)
        | ~F.coalesce(F.col("_prep_prev_usable"), F.lit(False))
    )
    df = df.withColumn("sequence_segment_id", F.sum(F.when(boundary, 1).otherwise(0)).over(prefix))
    segment = Window.partitionBy(*RECORDING, "sequence_segment_id").orderBy("_row_in_file")
    history = segment.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    previous = segment.rowsBetween(Window.unboundedPreceding, -1)
    df = df.select(
        "*",
        F.first("_prep_us", ignorenulls=True).over(history).alias("_prep_start_us"),
        *[observed_valid(name, rules).alias(f"{name}_is_valid") for name in SENSOR_RENAMES.values()],
    )
    clean_columns = []
    for name in SENSOR_RENAMES.values():
        valid = F.col(f"{name}_is_valid")
        cleaned = F.when(valid, F.col(name))
        if name in OPENINGS:
            anchor_name = f"_prep_anchor_{name}"
            df = df.withColumn(
                anchor_name,
                F.last(
                    F.when(
                        valid & F.col("sequence_usable"),
                        F.struct(F.col("_prep_us").alias("us"), F.col(name).alias("value")),
                    ),
                    ignorenulls=True,
                ).over(previous),
            )
            age = (F.col("_prep_us") - F.col(f"{anchor_name}.us")) / F.lit(1_000_000.0)
            repair = F.coalesce(
                F.col("sequence_usable")
                & is_finite(F.col(name))
                & ~valid
                & (age > 0)
                & (age <= rules.choke_forward_fill_max_seconds),
                F.lit(False),
            )
            df = df.select(
                "*",
                repair.alias(f"{name}_was_imputed"),
                F.when(F.col("sequence_usable"), F.when(valid, F.lit(0.0)).otherwise(age)).alias(
                    f"{name}_source_age_seconds"
                ),
            )
            cleaned = cleaned.when(repair, F.col(f"{anchor_name}.value"))
        clean_columns.append(cleaned.cast("double").alias(f"{name}_clean"))
    df = df.select("*", *clean_columns)
    valid_count = reduce(lambda a, b: a + b, (F.col(f"{n}_is_valid").cast("int") for n in SENSOR_RENAMES.values()))
    prepared_count = reduce(
        lambda a, b: a + b, (F.col(f"{n}_clean").isNotNull().cast("int") for n in SENSOR_RENAMES.values())
    )
    df = df.select(
        "*",
        valid_count.alias("observed_valid_sensor_count"),
        prepared_count.alias("prepared_sensor_count"),
        (valid_count > 0).alias("has_observed_sensor_evidence"),
        (
            F.col("production_choke_upstream_pressure_pa_clean")
            - F.col("production_choke_downstream_pressure_pa_clean")
        ).alias(PRESSURE_DROP),
    )
    timed = Window.partitionBy(*RECORDING, "sequence_segment_id").orderBy("_prep_us")
    usable = F.col("sequence_usable")
    short, long = rules.short_window_seconds, rules.long_window_seconds
    feature_columns = []
    for name in rules.sensors:
        value = F.col(f"{name}_clean")
        short_frame = timed.rangeBetween(-short * 1_000_000 + 1, 0)
        long_frame = timed.rangeBetween(-long * 1_000_000 + 1, 0)
        past_frame = timed.rangeBetween(-short * 1_000_000, -1)
        observed = F.when(F.col(f"{name}_is_valid") & usable, 1)
        short_count = F.count(observed).over(short_frame)
        long_count = F.count(observed).over(long_frame)
        past_count = F.count(observed).over(past_frame)
        short_ok = usable & (short_count >= rules.minimum_count(short))
        long_ok = usable & (long_count >= rules.minimum_count(long))
        mean_short = F.when(short_ok, F.avg(value).over(short_frame))
        mean_long = F.when(long_ok, F.avg(value).over(long_frame))
        feature_columns.extend(
            [
                mean_short.alias(f"{name}_mean_{short}s"),
                F.when(short_ok & (short_count >= 2), F.stddev_samp(value).over(short_frame)).alias(
                    f"{name}_stddev_{short}s"
                ),
                mean_long.alias(f"{name}_mean_{long}s"),
                F.when(
                    usable & (past_count >= rules.minimum_count(short)), value - F.avg(value).over(past_frame)
                ).alias(f"{name}_deviation_{short}s"),
            ]
        )
    # Extract all compatible windows together, avoiding per-sensor row/time re-sorts.
    df = df.select("*", *feature_columns)
    return df.select(
        *original, *[name for name in df.columns if name not in original and not name.startswith("_prep_")]
    )


def summarize_preparation(observations: DataFrame, rules: QualityRules) -> dict:
    expressions = [
        F.count("*").alias("rows"),
        F.count(F.when(~F.col("has_observed_sensor_evidence"), 1)).alias("rows_without_observed_valid_sensors"),
        F.count(F.when(~F.col("sequence_usable"), 1)).alias("rows_without_usable_time"),
    ]
    for name in OPENINGS:
        expressions.extend(
            [
                F.count(F.when(F.col(f"{name}_was_imputed"), 1)).alias(f"{name}_repaired"),
                F.count(F.when(F.col(name).isNotNull() & F.col(f"{name}_clean").isNull(), 1)).alias(
                    f"{name}_unresolved"
                ),
            ]
        )
    for name in rules.sensors:
        expressions.append(F.count(f"{name}_mean_{rules.short_window_seconds}s").alias(f"{name}_rows_with_short_mean"))
    return observations.agg(*expressions).first().asDict()
