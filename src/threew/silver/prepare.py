"""Validate and clean observation-level sensor readings using Spark SQL."""

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from threew.silver.features import add_sensor_features
from threew.silver.labels import prepare_class_labels
from threew.silver.quality import is_finite
from threew.silver.reference import OPENINGS, RECORDING_KEY, SENSOR_FIELDS
from threew.silver.rules import QualityRules


def observed_valid(name, rules):
    value = F.col(name)
    valid = is_finite(value)  # Being finite is enough unless this sensor has a documented domain.
    if name in OPENINGS:
        valid = valid & value.between(rules.opening_min_pct, rules.opening_max_pct)  # Opening limits are inclusive.
    elif name.endswith("_valve_state"):
        valid = valid & value.isin(*rules.valve_states)  # Valve codes are discrete; we do not round them.
    return F.coalesce(valid, F.lit(False))  # A null reading should give false rather than an unknown condition.


def prepare_observations(observations: DataFrame, rules: QualityRules) -> DataFrame:
    """Retain source rows; add clean values, repair provenance and 30 sensor features."""
    original = observations.columns  # Keep source columns while building the clean values alongside them.
    ordered = Window.partitionBy(*RECORDING_KEY).orderBy("_row_in_file")  # Keep source order.
    prefix = ordered.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df = prepare_class_labels(observations, rules).withColumn(
        "_prep_us",
        F.unix_micros(F.col("timestamp").cast("timestamp")),  # Use microseconds for repair ages.
    )
    df = df.select(
        "*",
        F.lag("_prep_us").over(ordered).alias("_prep_prev_us"),
        F.lag("_row_in_file").over(ordered).alias("_prep_prev_row"),  # Check row gaps as well as clock gaps.
    )
    step = (F.col("_prep_us") - F.col("_prep_prev_us")) / F.lit(1_000_000.0)
    df = df.withColumn("_prep_step", step).withColumn(
        "sequence_usable",
        F.col("_prep_us").isNotNull() & (F.col("_prep_prev_us").isNull() | (F.col("_prep_step") > 0)),
    )
    df = df.withColumn("_prep_prev_usable", F.lag("sequence_usable").over(ordered))
    boundary = (  # Here we reset history at time or row gaps, so repairs cannot cross them.
        F.col("_prep_prev_us").isNull()
        | F.col("_prep_us").isNull()
        | (F.col("_prep_step") != rules.expected_interval_seconds)  # Even a positive time gap starts a new segment.
        | (F.col("_row_in_file") != F.col("_prep_prev_row") + 1)
        | ~F.coalesce(F.col("_prep_prev_usable"), F.lit(False))  # Start fresh after an unusable timestamp too.
    )
    df = df.withColumn("sequence_segment_id", F.sum(F.when(boundary, 1).otherwise(0)).over(prefix))
    segment = Window.partitionBy(*RECORDING_KEY, "sequence_segment_id").orderBy("_row_in_file")
    previous = segment.rowsBetween(Window.unboundedPreceding, -1)  # Only look at earlier rows in this segment.
    df = df.select(
        "*",
        *[observed_valid(name, rules).alias(f"_prep_valid_{name}") for name in SENSOR_FIELDS],
    )
    clean_columns = []
    for name in SENSOR_FIELDS:
        valid = F.col(f"_prep_valid_{name}")
        cleaned = F.when(valid, F.col(name))  # Keep valid readings unchanged; everything else starts as null.
        if name in OPENINGS:  # Only choke openings can receive a replacement sensor value.
            anchor_name = f"_prep_anchor_{name}"
            df = df.withColumn(
                anchor_name,
                F.last(
                    F.when(
                        valid & F.col("sequence_usable"),
                        F.struct(F.col("_prep_us").alias("us"), F.col(name).alias("value")),
                    ),
                    ignorenulls=True,
                ).over(previous),  # Keep the original value and its time; repairs never become anchors.
            )
            age = (F.col("_prep_us") - F.col(f"{anchor_name}.us")) / F.lit(1_000_000.0)
            repair = F.coalesce(
                F.col("sequence_usable")
                & is_finite(F.col(name))  # We leave missing and non-finite readings unfilled.
                & ~valid
                & (age > 0)
                & (age <= rules.choke_forward_fill_max_seconds),  # Stop carrying the value once it is too old.
                F.lit(False),
            )
            df = df.select(
                "*",
                repair.alias(f"{name}_was_imputed"),  # Let downstream users distinguish estimates from observations.
                F.when(F.col("sequence_usable"), F.when(valid, F.lit(0.0)).otherwise(age)).alias(
                    f"{name}_source_age_seconds"
                ),
            )
            cleaned = cleaned.when(repair, F.col(f"{anchor_name}.value"))  # Use the anchor value; do not clip.
        clean_columns.append(cleaned.cast("double").alias(f"{name}_clean"))
    df = df.select("*", *clean_columns)
    cleaned = df.select(
        *original, *[name for name in df.columns if name not in original and not name.startswith("_prep_")]
    )
    return add_sensor_features(cleaned, rules)  # Features use clean values and the segments built above.


def summarize_preparation(
    observations: DataFrame, rules: QualityRules, source_observations: DataFrame | None = None
) -> dict:
    has_valid_sensor = reduce(
        lambda a, b: a | b,
        (observed_valid(name, rules) for name in SENSOR_FIELDS),
    )
    expressions = [
        F.count("*").alias("rows"),
        F.count(F.when(F.col("class_was_inferred"), 1)).alias("class_code_inferred_rows"),
        F.count(F.when(F.col("class_code_clean").isNull(), 1)).alias("class_code_unresolved_rows"),  # Count unknowns.
        F.count(F.when(~F.col("sequence_usable"), 1)).alias("rows_without_usable_time"),
    ]
    for name in OPENINGS:
        expressions.extend(
            [
                F.count(F.when(F.col(f"{name}_was_imputed"), 1)).alias(f"{name}_repaired"),
            ]
        )
    for name in rules.feature_sensors:
        expressions.append(
            F.count(F.when(F.col(f"{name}_count_120s") == rules.feature_window_seconds, 1)).alias(
                f"{name}_complete_120s_rows"
            )
        )
    result = observations.agg(*expressions).first().asDict()
    source = observations if source_observations is None else source_observations  # Count original problems.
    source_expressions = [
        F.count(F.when(~has_valid_sensor, 1)).alias("rows_without_observed_valid_sensors"),
        *[
            F.count(F.when(F.col(name).isNotNull() & ~observed_valid(name, rules), 1)).alias(f"{name}_invalid")
            for name in OPENINGS
        ],
    ]
    original = source.agg(*source_expressions).first().asDict()  # Read source validity separately from repair counts.
    result["rows_without_observed_valid_sensors"] = original["rows_without_observed_valid_sensors"]
    for name in OPENINGS:
        result[f"{name}_unresolved"] = original[f"{name}_invalid"] - result[f"{name}_repaired"]
    return result
