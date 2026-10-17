"""Validate and clean observation-level sensor readings using Spark SQL."""

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from threew.silver.issues import is_finite
from threew.silver.reference import SENSOR_RENAMES
from threew.silver.rules import QualityRules

RECORDING = ("_source_path", "_file_sha256")
OPENINGS = tuple(target for source, target in SENSOR_RENAMES.items() if source.startswith("ABER-"))


def observed_valid(name, rules):
    value = F.col(name)
    valid = is_finite(value)
    if name in OPENINGS:
        valid = valid & value.between(rules.opening_min_pct, rules.opening_max_pct)
    elif name.endswith("_valve_state"):
        valid = valid & value.isin(*rules.valve_states)
    return F.coalesce(valid, F.lit(False))


def prepare_observations(observations: DataFrame, rules: QualityRules) -> DataFrame:
    """Retain source columns/rows; add clean values and repair provenance."""
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
    previous = segment.rowsBetween(Window.unboundedPreceding, -1)
    df = df.select(
        "*",
        *[observed_valid(name, rules).alias(f"_prep_valid_{name}") for name in SENSOR_RENAMES.values()],
    )
    clean_columns = []
    for name in SENSOR_RENAMES.values():
        valid = F.col(f"_prep_valid_{name}")
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
    return df.select(
        *original, *[name for name in df.columns if name not in original and not name.startswith("_prep_")]
    )


def summarize_preparation(observations: DataFrame, rules: QualityRules) -> dict:
    has_valid_sensor = reduce(
        lambda a, b: a | b,
        (observed_valid(name, rules) for name in SENSOR_RENAMES.values()),
    )
    expressions = [
        F.count("*").alias("rows"),
        F.count(F.when(~has_valid_sensor, 1)).alias("rows_without_observed_valid_sensors"),
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
    return observations.agg(*expressions).first().asDict()
