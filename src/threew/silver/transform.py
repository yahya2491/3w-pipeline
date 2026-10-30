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

from threew.silver.reference import (
    EVENT_NAMES,
    NORMAL_LABELS,
    SENSOR_RENAMES,
    SOURCE_TYPES,
    TRANSIENT_LABELS,
)

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

    seen_paths = set()  # We need one current recording per source path.
    for row in rows:
        for name in ("_source_path", "_file_sha256", "_manifest_source_type", "_manifest_label", "instance_id"):
            if row.get(name) is None:
                raise ValueError(f"Manifest row is missing required field {name!r}.")
        source_path = row["_source_path"]
        if not isinstance(source_path, str) or not source_path:
            raise ValueError("Manifest source paths must be non-empty strings.")
        if source_path in seen_paths:  # Reject duplicates before a join can multiply observations.
            raise ValueError(f"Manifest has more than one current recording for {source_path!r}.")
        seen_paths.add(source_path)
        if not isinstance(row["_file_sha256"], str) or not row["_file_sha256"]:
            raise ValueError(f"Manifest row {source_path!r} has no file hash.")
        if not isinstance(row["_manifest_source_type"], str) or not row["_manifest_source_type"]:
            raise ValueError(f"Manifest row {source_path!r} has no source type.")
        if row["_manifest_source_type"] not in SOURCE_TYPES:
            raise ValueError(f"Manifest row {source_path!r} has an unsupported source type.")
        if type(row["_manifest_label"]) is not int or row["_manifest_label"] not in EVENT_NAMES:
            raise ValueError(f"Manifest row {source_path!r} has an unsupported event label.")
        if row["_manifest_source_type"] != "REAL" and (
            row.get("well_id") is not None or row.get("instance_start") is not None
        ):
            raise ValueError(f"Synthetic manifest row {source_path!r} must not have real well/start details.")
        if not isinstance(row["instance_id"], str) or not row["instance_id"]:
            raise ValueError(f"Manifest row {source_path!r} has no instance ID.")


def _manifest_dataframe(spark: SparkSession, rows: Sequence[Mapping[str, Any]]) -> DataFrame:
    _validate_manifest_rows(rows)  # Check the small catalogue before turning it into a DataFrame.
    return spark.createDataFrame([dict(row) for row in rows], schema=MANIFEST_SCHEMA)


def build_silver_observations(bronze: DataFrame, manifest_rows: Sequence[Mapping[str, Any]]) -> DataFrame:
    """Rename and enrich Bronze rows, failing when recording metadata is ambiguous or inconsistent."""
    missing = set(BRONZE_TYPES) - set(bronze.columns)  # Do not silently run with an incomplete input schema.
    if missing:
        raise ValueError(f"Bronze table is missing required columns: {', '.join(sorted(missing))}")

    extra = set(bronze.columns) - set(BRONZE_TYPES)  # New source columns need a deliberate contract update.
    if extra:
        raise ValueError(f"Bronze table has unexpected columns: {', '.join(sorted(extra))}")

    actual_types = {field.name: field.dataType for field in bronze.schema.fields}
    wrong_types = [
        f"{name}: expected {expected.simpleString()}, got {actual_types[name].simpleString()}"
        for name, expected in BRONZE_TYPES.items()
        if actual_types[name] != expected
    ]
    if wrong_types:
        raise ValueError("Bronze table has unexpected field types: " + "; ".join(wrong_types))

    manifest = _manifest_dataframe(bronze.sparkSession, manifest_rows)  # Small catalogue for the join below.
    joined = bronze.join(F.broadcast(manifest), on=["_source_path", "_file_sha256"], how="left")
    checks = joined.agg(  # The left join keeps unmatched file versions visible so we can reject them.
        F.count(F.when(F.col("instance_id").isNull(), 1)).alias("unmatched_rows"),
        F.count(F.when(~F.col("source_type").eqNullSafe(F.col("_manifest_source_type")), 1)).alias(
            "source_type_mismatches"
        ),
        F.count(F.when(~F.col("label").eqNullSafe(F.col("_manifest_label")), 1)).alias("label_mismatches"),
    ).first()

    if checks["unmatched_rows"]:  # Every observation must belong to a current manifest file version.
        raise ValueError(f"{checks['unmatched_rows']:,} Bronze rows have no current manifest match.")
    if checks["source_type_mismatches"]:
        raise ValueError("Bronze source_type does not match the current manifest.")
    if checks["label_mismatches"]:
        raise ValueError("Bronze label does not match the current manifest.")

    columns = [  # Keep source identity while renaming the sensor and label fields.
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
    silver = joined.select(*columns)

    return add_event_descriptions(silver)  # These first descriptions refer to the original row class.


def add_event_descriptions(observations: DataFrame, class_column: str = "class_code") -> DataFrame:
    """Describe the selected class column while retaining recording-event context."""
    event_name_map = F.create_map(*[item for code, name in EVENT_NAMES.items() for item in (F.lit(code), F.lit(name))])
    class_code = F.col(class_column).cast(IntegerType())
    instance_label = F.col("instance_label")  # The recording event is context, not a replacement row label.
    row_event_code = (
        F.when(class_code == 0, F.lit(0))
        .when(class_code.between(1, 9), class_code)
        .when(class_code.between(101, 109), class_code - F.lit(100))  # Map transient classes to event names.
        .otherwise(F.lit(None).cast(IntegerType()))
    )

    valid_normal_period = instance_label.isin(*NORMAL_LABELS) & (class_code == 0)
    valid_steady_event = instance_label.between(1, 9) & (class_code == instance_label)
    valid_transient_event = instance_label.isin(*TRANSIENT_LABELS) & (class_code == instance_label + F.lit(100))
    class_matches_recording = valid_normal_period | valid_steady_event | valid_transient_event  # Check allowed phases.

    return (
        observations.withColumn("instance_event_name", F.element_at(event_name_map, instance_label))
        .withColumn("row_event_name", F.element_at(event_name_map, row_event_code))
        .withColumn(
            "row_event_phase",
            F.when(class_code.isNull(), F.lit("unknown"))
            .when(class_code == 0, F.lit("normal"))
            .when(class_code.between(1, 9), F.lit("steady"))
            .when(class_code.between(101, 109), F.lit("transient"))
            .otherwise(F.lit("unrecognized")),
        )
        .withColumn(
            "class_label_status",
            F.when(class_code.isNull(), F.lit("unlabeled"))
            .when(class_matches_recording, F.lit("consistent"))
            .otherwise(F.lit("inconsistent")),
        )
    )
