"""Build the row-level Silver observations DataFrame from Bronze and current manifest rows."""

from collections.abc import Mapping, Sequence
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

SENSOR_RENAMES = {
    "ABER-CKGL": "gas_lift_choke_opening_pct",
    "ABER-CKP": "production_choke_opening_pct",
    "ESTADO-DHSV": "downhole_safety_valve_state",
    "ESTADO-M1": "production_master_valve_state",
    "ESTADO-M2": "annulus_master_valve_state",
    "ESTADO-PXO": "pig_crossover_valve_state",
    "ESTADO-SDV-GL": "gas_lift_shutdown_valve_state",
    "ESTADO-SDV-P": "production_shutdown_valve_state",
    "ESTADO-W1": "production_wing_valve_state",
    "ESTADO-W2": "annulus_wing_valve_state",
    "ESTADO-XO": "crossover_valve_state",
    "P-ANULAR": "annulus_pressure_pa",
    "P-JUS-BS": "service_pump_downstream_pressure_pa",
    "P-JUS-CKGL": "gas_lift_choke_downstream_pressure_pa",
    "P-JUS-CKP": "production_choke_downstream_pressure_pa",
    "P-MON-CKGL": "gas_lift_choke_upstream_pressure_pa",
    "P-MON-CKP": "production_choke_upstream_pressure_pa",
    "P-MON-SDV-P": "production_shutdown_valve_upstream_pressure_pa",
    "P-PDG": "downhole_pdg_pressure_pa",
    "PT-P": "subsea_production_line_pressure_pa",
    "P-TPT": "subsea_tree_tpt_pressure_pa",
    "QBS": "service_pump_flow_rate_m3_s",
    "QGL": "gas_lift_flow_rate_m3_s",
    "T-JUS-CKP": "production_choke_downstream_temperature_c",
    "T-MON-CKP": "production_choke_upstream_temperature_c",
    "T-PDG": "downhole_pdg_temperature_c",
    "T-TPT": "subsea_tree_tpt_temperature_c",
}

MANIFEST_SCHEMA = StructType(
    [
        StructField("_source_path", StringType(), nullable=False),
        StructField("_file_sha256", StringType(), nullable=False),
        StructField("_manifest_source_type", StringType(), nullable=False),
        StructField("_manifest_label", IntegerType(), nullable=False),
        StructField("instance_id", StringType(), nullable=False),
        StructField("well_id", IntegerType(), nullable=True),
        StructField("instance_start", TimestampNTZType(), nullable=True),
    ]
)

BRONZE_TYPES = {
    "source_type": StringType(),
    "label": IntegerType(),
    "class": ShortType(),
    "state": ShortType(),
    "timestamp": TimestampNTZType(),
    "_source_path": StringType(),
    "_file_sha256": StringType(),
    "_ingest_run_id": StringType(),
    "_row_in_file": LongType(),
    **{name: DoubleType() for name in SENSOR_RENAMES},
}


def _validate_manifest_rows(rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        raise ValueError("Cannot build Silver observations without current manifest rows.")

    seen_paths = set()
    for row in rows:
        for name in ("_source_path", "_file_sha256", "_manifest_source_type", "_manifest_label", "instance_id"):
            if row.get(name) is None:
                raise ValueError(f"Manifest row is missing required field {name!r}.")
        source_path = row["_source_path"]
        if not isinstance(source_path, str) or not source_path:
            raise ValueError("Manifest source paths must be non-empty strings.")
        if source_path in seen_paths:
            raise ValueError(f"Manifest has more than one current recording for {source_path!r}.")
        seen_paths.add(source_path)
        if not isinstance(row["_file_sha256"], str) or not row["_file_sha256"]:
            raise ValueError(f"Manifest row {source_path!r} has no file hash.")
        if not isinstance(row["_manifest_source_type"], str) or not row["_manifest_source_type"]:
            raise ValueError(f"Manifest row {source_path!r} has no source type.")
        if not isinstance(row["instance_id"], str) or not row["instance_id"]:
            raise ValueError(f"Manifest row {source_path!r} has no instance ID.")


def _manifest_dataframe(spark: SparkSession, rows: Sequence[Mapping[str, Any]]) -> DataFrame:
    _validate_manifest_rows(rows)
    return spark.createDataFrame([dict(row) for row in rows], schema=MANIFEST_SCHEMA)


def build_silver_observations(bronze: DataFrame, manifest_rows: Sequence[Mapping[str, Any]]) -> DataFrame:
    """Rename and enrich Bronze rows, failing when recording metadata is ambiguous or inconsistent."""
    missing = set(BRONZE_TYPES) - set(bronze.columns)
    if missing:
        raise ValueError(f"Bronze table is missing required columns: {', '.join(sorted(missing))}")

    actual_types = {field.name: field.dataType for field in bronze.schema.fields}
    wrong_types = [
        f"{name}: expected {expected.simpleString()}, got {actual_types[name].simpleString()}"
        for name, expected in BRONZE_TYPES.items()
        if actual_types[name] != expected
    ]
    if wrong_types:
        raise ValueError("Bronze table has unexpected field types: " + "; ".join(wrong_types))

    manifest = _manifest_dataframe(bronze.sparkSession, manifest_rows)
    joined = bronze.join(F.broadcast(manifest), on=["_source_path", "_file_sha256"], how="left")
    checks = joined.agg(
        F.count(F.when(F.col("instance_id").isNull(), 1)).alias("unmatched_rows"),
        F.count(F.when(~F.col("source_type").eqNullSafe(F.col("_manifest_source_type")), 1)).alias(
            "source_type_mismatches"
        ),
        F.count(F.when(~F.col("label").eqNullSafe(F.col("_manifest_label")), 1)).alias("label_mismatches"),
    ).first()

    if checks["unmatched_rows"]:
        raise ValueError(f"{checks['unmatched_rows']:,} Bronze rows have no current manifest match.")
    if checks["source_type_mismatches"]:
        raise ValueError("Bronze source_type does not match the current manifest.")
    if checks["label_mismatches"]:
        raise ValueError("Bronze label does not match the current manifest.")

    columns = [
        F.col("source_type"),
        F.col("label").cast(IntegerType()).alias("instance_label"),
        F.col("class").cast(ShortType()).alias("class_code"),
        F.col("state").cast(ShortType()).alias("state_code"),
        F.col("timestamp"),
        F.col("_source_path"),
        F.col("_file_sha256"),
        F.col("_ingest_run_id"),
        F.col("_row_in_file").cast(LongType()),
        F.col("instance_id"),
        F.col("well_id"),
        F.col("instance_start"),
        *[F.col(source).cast(DoubleType()).alias(target) for source, target in SENSOR_RENAMES.items()],
    ]
    return joined.select(*columns)
