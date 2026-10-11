"""Locate quality problems and explain their effect without repairing source values.

Expected missing sensor cells stay in sensor_availability, avoiding billions of
issue entries. Missing state codes remain counted in instance_quality because
their domain is not documented. Quarantine is a field/use-specific review list;
the complete original observation remains in silver_observations.
"""

from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from threew.silver.quality import with_timestamp_intervals
from threew.silver.reference import SENSOR_DEFINITIONS

OBSERVATION_KEY = ("_source_path", "_file_sha256", "_row_in_file")
ISSUE_KEY = (*OBSERVATION_KEY, "field_name", "issue_code")
ISSUE_SCHEMA = (
    "_source_path string, _file_sha256 string, _row_in_file long, field_name string, "
    "issue_code string, severity string, affected_use string, reason string, "
    "recommended_action string, observed_value string"
)
USAGE_POLICY = {
    "timestamps": "Preserve source time and row order; flag problems. Never infer replacement timestamps.",
    "duplicate_identity": "Stop on repeated source-path/file-hash/row-position keys, including identical copies.",
    "duplicate_timestamp": "Keep distinct observations; flag every row sharing a timestamp within its recording.",
    "missing_sensors": "Individual missing sensors stay in availability summaries. A row with no finite sensors is flagged for sensor use.",
    "missing_class": "Keep sensor readings; exclude this label from analyses requiring a known class.",
    "quarantine": "Review error issues for their affected field/use; join their keys to observations for complete readings.",
    "source_versions": "Use Bronze's current manifest; a new file hash is a new source version, not a row duplicate.",
    "domains": "Check documented valve codes and percentage openings; do not invent pressure/flow/temperature limits.",
    "state_code": "Domain not documented: preserve values, count missingness, and do not invent an allowed-code list.",
}


def is_finite(value: Column) -> Column:
    return value.isNotNull() & ~F.isnan(value) & (value != float("inf")) & (value != float("-inf"))


def build_quality_issues(observations: DataFrame) -> DataFrame:
    """One issue per affected row/field/rule, with scope-specific usage guidance."""
    timed = with_timestamp_intervals(observations).withColumn(
        "_same_timestamp_rows",
        F.count("*").over(Window.partitionBy("_source_path", "_file_sha256", "timestamp")),
    )
    entries = []

    def add(condition, field, code, severity, use, reason, action, value):
        entries.append(
            F.when(
                condition,
                F.struct(
                    F.lit(field).alias("field_name"),
                    F.lit(code).alias("issue_code"),
                    F.lit(severity).alias("severity"),
                    F.lit(use).alias("affected_use"),
                    F.lit(reason).alias("reason"),
                    F.lit(action).alias("recommended_action"),
                    value.cast("string").alias("observed_value"),
                ),
            )
        )

    add(
        F.col("class_code").isNull(),
        "class_code",
        "missing_class",
        "warning",
        "labeled_analysis",
        "No class label was supplied.",
        "Keep readings; exclude the label from tasks requiring known classes.",
        F.col("class_code"),
    )
    add(
        F.col("row_event_phase") == "unrecognized",
        "class_code",
        "unsupported_class",
        "error",
        "labeled_analysis",
        "The class code has no supported event/phase definition.",
        "Review the label; other measurements remain available.",
        F.col("class_code"),
    )
    add(
        (F.col("class_label_status") == "inconsistent") & (F.col("row_event_phase") != "unrecognized"),
        "class_code",
        "class_recording_mismatch",
        "error",
        "labeled_analysis",
        "The class/phase is not supported for this recording's event category.",
        "Review the label against the source; do not automatically relabel readings.",
        F.col("class_code"),
    )
    add(
        F.col("timestamp").isNull(),
        "timestamp",
        "missing_timestamp",
        "error",
        "time_sequence",
        "No timestamp was supplied.",
        "Keep the source row; review before time-based analysis.",
        F.col("timestamp"),
    )
    add(
        F.col("timestamp").isNotNull() & (F.col("_same_timestamp_rows") > 1),
        "timestamp",
        "duplicate_timestamp",
        "error",
        "time_sequence",
        "Multiple source rows in this recording share the timestamp.",
        "Keep every reading; resolve timing only with source evidence.",
        F.col("timestamp"),
    )
    interval = F.col("_quality_interval_seconds")
    add(
        interval < 0,
        "timestamp",
        "backward_timestamp",
        "error",
        "time_sequence",
        "Time moves backward relative to the previous original source row.",
        "Review the sequence; do not change timestamps or hide the source order.",
        F.col("timestamp"),
    )
    add(
        interval > 1,
        "timestamp",
        "timestamp_gap",
        "warning",
        "time_sequence",
        "The source-order interval is longer than the expected one second.",
        "Keep the gap visible; account for it in time-based analysis.",
        F.col("timestamp"),
    )
    add(
        (interval > 0) & (interval < 1),
        "timestamp",
        "short_timestamp_interval",
        "warning",
        "time_sequence",
        "The source-order interval is shorter than the expected one second.",
        "Preserve fractional time; review the sampling assumption.",
        F.col("timestamp"),
    )

    for definition in SENSOR_DEFINITIONS:
        field = definition["sensor_name"]
        value = F.col(field)
        finite = is_finite(value)
        add(
            value.isNotNull() & ~finite,
            field,
            "nonfinite_sensor",
            "error",
            "sensor_measurement",
            "The supplied measurement is NaN or positive/negative infinity.",
            "Exclude this measurement from numeric analysis; keep the source and other sensors.",
            value,
        )
        if definition["allowed_values"] is not None:
            add(
                finite & ~value.isin(*definition["allowed_values"]),
                field,
                "unsupported_valve_state",
                "error",
                "sensor_measurement",
                "The documented valve states are 0, 0.5 and 1.",
                "Review this sensor value; do not round or replace it.",
                value,
            )
        if definition["minimum"] is not None:
            add(
                finite & ~value.between(definition["minimum"], definition["maximum"]),
                field,
                "opening_out_of_range",
                "error",
                "sensor_measurement",
                "A choke opening percentage must be between 0 and 100.",
                "Review this opening value; do not clip it.",
                value,
            )

    any_sensor = reduce(lambda a, b: a | b, (is_finite(F.col(d["sensor_name"])) for d in SENSOR_DEFINITIONS))
    add(
        ~any_sensor,
        "sensor_readings",
        "no_finite_sensor_readings",
        "error",
        "sensor_measurement",
        "None of the 27 sensors has a finite reading in this row.",
        "Keep the row and labels; exclude it from tasks requiring a sensor measurement.",
        F.lit(None),
    )
    return timed.select(
        *OBSERVATION_KEY, F.explode(F.filter(F.array(*entries), lambda entry: entry.isNotNull())).alias("issue")
    ).select(*OBSERVATION_KEY, "issue.*")


def quarantine_issues(issues: DataFrame) -> DataFrame:
    """An inspectable error-only subset; it does not delete observations."""
    return issues.filter(F.col("severity") == "error")


def summarize_quality(issues: DataFrame, quality: DataFrame, sensors: DataFrame, observation_rows: int) -> dict:
    """Small per-load report; multiple issues on one row do not inflate row rates."""
    by_rule = [
        row.asDict()
        for row in issues.groupBy("issue_code", "severity", "affected_use").count().orderBy("issue_code").collect()
    ]
    issue_rows = issues.select(*OBSERVATION_KEY).distinct().count()
    quarantine_rows = quarantine_issues(issues).select(*OBSERVATION_KEY).distinct().count()
    quality_fields = (
        "unlabeled_rows",
        "null_state_rows",
        "null_timestamp_rows",
        "duplicate_timestamp_rows",
        "backward_intervals",
        "gaps_over_one_second",
        "inconsistent_class_rows",
    )
    recording = (
        quality.agg(
            *[F.coalesce(F.sum(name), F.lit(0)).alias(name) for name in quality_fields],
            F.count(F.when(F.col("no_sensors_present"), 1)).alias("recordings_without_finite_sensors"),
        )
        .first()
        .asDict()
    )
    availability = [
        row.asDict()
        for row in sensors.groupBy("sensor_name")
        .agg(
            F.sum("recording_rows").alias("observation_rows"),
            F.sum("rows_with_finite_values").alias("finite_readings"),
            F.sum("rows_without_finite_values").alias("missing_or_nonfinite_readings"),
        )
        .orderBy("sensor_name")
        .collect()
    ]
    return {
        "observation_rows": observation_rows,
        "issue_entries": sum(row["count"] for row in by_rule),
        "rows_with_issues": issue_rows,
        "quarantine_issue_entries": sum(row["count"] for row in by_rule if row["severity"] == "error"),
        "rows_with_quarantine_issues": quarantine_rows,
        "quarantine_row_percent": round(100 * quarantine_rows / observation_rows, 4) if observation_rows else 0.0,
        "has_quality_errors": quarantine_rows > 0,
        "by_rule": by_rule,
        "recording_totals": recording,
        "sensor_availability": availability,
    }
