"""Check input coverage and saved Silver content before publishing readiness."""

from collections.abc import Mapping, Sequence
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, TimestampNTZType

from threew.silver.issues import ISSUE_KEY, build_quality_issues, quarantine_issues
from threew.silver.output import export_observations
from threew.silver.prepare import prepare_observations
from threew.silver.quality import build_quality_summaries
from threew.silver.reference import (
    OBSERVATION_KEY,
    RECORDING_KEY,
    SENSOR_FIELDS,
    SENSOR_RENAMES,
    build_reference_tables,
)
from threew.silver.transform import BRONZE_TYPES, build_silver_observations
from threew.spark import cleanup_spark

RENAMES = {**SENSOR_RENAMES, "label": "instance_label", "class": "class_code", "state": "state_code"}
BASE_OBSERVATION_TYPES = {
    **{RENAMES.get(name, name): value for name, value in BRONZE_TYPES.items()},
    "instance_id": StringType(),
    "well_id": IntegerType(),
    "instance_start": TimestampNTZType(),
    **{
        name: StringType()
        for name in ("instance_event_name", "row_event_name", "row_event_phase", "class_label_status")
    },
}
DETAIL_FIELDS = tuple(
    name for name in BASE_OBSERVATION_TYPES if name not in RENAMES.values() and name not in BRONZE_TYPES
)
REQUIRED_OBSERVATION_FIELDS = (
    *OBSERVATION_KEY,
    "_ingest_run_id",
    "source_type",
    "instance_label",
    "instance_id",
    "instance_event_name",
    "row_event_phase",
    "class_label_status",
)


def same_rows(left: DataFrame, right: DataFrame) -> bool:
    """Exact logical equality, including duplicates and nulls, regardless of row order."""
    weight = "__threew_row_weight"  # Equal rows cancel out, including their duplicate counts.
    while weight in left.columns:  # Avoid overwriting a real data column with our temporary weight.
        weight += "_"
    balance = weight + "_balance"
    while balance in left.columns:
        balance += "_"
    signed = left.withColumn(weight, F.lit(1)).unionByName(right.select(*left.columns).withColumn(weight, F.lit(-1)))
    differences = signed.groupBy(*left.columns).agg(F.sum(weight).alias(balance))  # Equal rows sum to zero.
    return differences.filter(F.col(balance) != 0).limit(1).count() == 0


def schema_matches(df: DataFrame, types: Mapping[str, Any]) -> bool:
    actual = {field.name: field.dataType for field in df.schema}
    return len(df.columns) == len(types) and actual == dict(types)


def unique_non_null_keys(df: DataFrame, fields: Sequence[str]) -> bool:
    missing = F.lit(False)  # Check every part of the key; one null is enough to reject the row.
    for name in fields:
        missing = missing | F.col(name).isNull()
    return (
        df.filter(missing).limit(1).count() == 0  # Finding one bad key is enough; no need to count them all.
        and df.groupBy(*fields).count().filter("count > 1").limit(1).count() == 0
    )


def input_checks(
    spark: SparkSession,
    bronze_counts: DataFrame,
    marker: Mapping[str, Any],
    manifest_counts: Sequence[Mapping[str, Any]],
) -> dict[str, bool]:
    expected = spark.createDataFrame(
        [dict(row) for row in manifest_counts], "_source_path string, _file_sha256 string, bronze_rows long"
    )
    total = bronze_counts.agg(F.sum("bronze_rows")).first()[0] or 0
    return {
        "bronze_rows_match_ready_marker": total == marker["rows"],
        "bronze_recordings_match_ready_marker": bronze_counts.count() == marker["files"],
        "bronze_recording_counts_match_manifest": same_rows(expected, bronze_counts),  # Totals can hide mismatches.
    }


def output_checks(
    bronze: DataFrame,
    bronze_counts: DataFrame,
    manifest_rows: Sequence[Mapping[str, Any]],
    observations: DataFrame,
    quality: DataFrame,
    sensors: DataFrame,
    additional: Mapping[str, DataFrame] | None = None,
    rules=None,
) -> dict[str, bool]:
    """Validate saved tables against Bronze, the contract and freshly computed summaries."""
    source = build_silver_observations(bronze, manifest_rows)  # Rebuild expected values from Bronze.
    expected = export_observations(prepare_observations(source, rules)) if rules is not None else source
    summaries = build_quality_summaries(source)
    issues = None
    try:
        checks = {  # Check saved output here; explicit-value tests check the rules themselves.
            "silver_schema_matches_contract": schema_matches(
                observations,
                {f.name: f.dataType for f in expected.schema} if rules is not None else BASE_OBSERVATION_TYPES,
            ),
            "instance_quality_schema_matches_contract": schema_matches(
                quality, {f.name: f.dataType for f in summaries.instance_quality.schema}
            ),
            "sensor_availability_schema_matches_contract": schema_matches(
                sensors, {f.name: f.dataType for f in summaries.sensor_availability.schema}
            ),
        }
        if not all(checks.values()):  # Stop before comparing values if the schemas are incompatible.
            return checks
        required_null = F.lit(False)  # Required context cannot disappear during the export.
        for name in REQUIRED_OBSERVATION_FIELDS:
            if name in expected.columns:
                required_null = required_null | F.col(name).isNull()
        retained = [name for name in BRONZE_TYPES if RENAMES.get(name, name) in expected.columns]
        checks.update(
            {
                "silver_required_fields_are_non_null": observations.filter(required_null).limit(1).count() == 0,
                "bronze_observation_keys_are_unique_and_non_null": unique_non_null_keys(bronze, OBSERVATION_KEY),
                "silver_observation_keys_are_unique_and_non_null": unique_non_null_keys(observations, OBSERVATION_KEY),
                "silver_observation_keys_match_bronze": same_rows(
                    bronze.select(*OBSERVATION_KEY), observations.select(*OBSERVATION_KEY)
                ),
                "silver_observation_rows_match_bronze": observations.count()
                == bronze_counts.agg(F.sum("bronze_rows")).first()[0],
                "silver_observation_row_counts_match_per_recording": same_rows(
                    bronze_counts,
                    observations.groupBy(*RECORDING_KEY).count().withColumnRenamed("count", "bronze_rows"),
                ),
                "silver_preserved_values_match_bronze": same_rows(
                    bronze.select(*[F.col(name).alias(RENAMES.get(name, name)) for name in retained]),
                    observations.select(*[RENAMES.get(name, name) for name in retained]),
                ),
                "silver_recording_and_event_details_match": same_rows(
                    expected.select(*OBSERVATION_KEY, *DETAIL_FIELDS),
                    observations.select(*OBSERVATION_KEY, *DETAIL_FIELDS),
                ),
                "instance_quality_has_one_matching_row_per_recording": unique_non_null_keys(quality, RECORDING_KEY)
                and same_rows(
                    bronze_counts, quality.select(*RECORDING_KEY, F.col("recording_rows").alias("bronze_rows"))
                ),
                "sensor_availability_has_27_unique_sensors_per_recording": unique_non_null_keys(
                    sensors, (*RECORDING_KEY, "sensor_name")
                )
                and same_rows(
                    bronze_counts.select(*RECORDING_KEY).crossJoin(
                        bronze.sparkSession.createDataFrame([(name,) for name in SENSOR_FIELDS], "sensor_name string")
                    ),
                    sensors.select(*RECORDING_KEY, "sensor_name"),
                ),
                "instance_quality_values_match_observations": same_rows(summaries.instance_quality, quality),
                "sensor_availability_values_match_observations": same_rows(summaries.sensor_availability, sensors),
            }
        )
        if rules is not None:
            derived = [name for name in expected.columns if name not in BASE_OBSERVATION_TYPES]
            reference = expected.select(*OBSERVATION_KEY, *derived)  # Also compare prepared fields against the rules.
            checks["silver_prepared_values_match_rules"] = same_rows(
                reference, observations.select(*OBSERVATION_KEY, *derived)
            )
        if additional is not None:
            issues = build_quality_issues(source, rules).persist(StorageLevel.MEMORY_AND_DISK)
            expected_additional = {
                "quality_issues": issues,
                "quality_quarantine": quarantine_issues(issues),
                **build_reference_tables(bronze.sparkSession, rules),
            }
            for name, reference in expected_additional.items():
                candidate = additional[name]  # Check the saved issue and reference tables against fresh results.
                correct_schema = schema_matches(candidate, {f.name: f.dataType for f in reference.schema})
                checks[f"{name}_schema_matches"] = correct_schema
                checks[f"{name}_values_match"] = correct_schema and same_rows(reference, candidate)
            checks["quality_issue_keys_are_unique_and_non_null"] = checks[
                "quality_issues_schema_matches"
            ] and unique_non_null_keys(additional["quality_issues"], ISSUE_KEY)
        return checks
    finally:
        if issues is not None:
            cleanup_spark(issues.unpersist, "release expected issues")  # Release the cache even if comparison failed.
        cleanup_spark(summaries.unpersist, "release expected summaries")
