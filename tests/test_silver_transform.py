from datetime import datetime

import pytest
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

from threew.silver.transform import SENSOR_RENAMES, build_silver_observations


def _bronze_fixture(spark):
    fields = [
        StructField("_source_path", StringType(), nullable=False),
        StructField("_file_sha256", StringType(), nullable=False),
        StructField("_ingest_run_id", StringType(), nullable=False),
        StructField("_row_in_file", LongType(), nullable=False),
        StructField("source_type", StringType(), nullable=False),
        StructField("label", IntegerType(), nullable=False),
        StructField("timestamp", TimestampNTZType(), nullable=False),
        StructField("class", ShortType(), nullable=True),
        StructField("state", ShortType(), nullable=True),
        *[StructField(name, DoubleType(), nullable=True) for name in SENSOR_RENAMES],
    ]
    schema = StructType(fields)
    source_rows = [
        {
            "_source_path": "data/1/WELL-00001.parquet",
            "_file_sha256": "hash-real",
            "_ingest_run_id": "bronze-run-1",
            "_row_in_file": 0,
            "source_type": "REAL",
            "label": 1,
            "timestamp": datetime.fromisoformat("2020-01-01T00:00:00"),
            "class": 0,
            "state": 1,
        },
        {
            "_source_path": "data/1/WELL-00001.parquet",
            "_file_sha256": "hash-real",
            "_ingest_run_id": "bronze-run-1",
            "_row_in_file": 1,
            "source_type": "REAL",
            "label": 1,
            "timestamp": datetime.fromisoformat("2020-01-01T00:00:01"),
            "class": 101,
            "state": None,
        },
        {
            "_source_path": "data/1/SIMULATED_00001.parquet",
            "_file_sha256": "hash-sim",
            "_ingest_run_id": "bronze-run-1",
            "_row_in_file": 0,
            "source_type": "SIMULATED",
            "label": 1,
            "timestamp": datetime.fromisoformat("1970-01-01T00:00:00"),
            "class": 1,
            "state": 2,
        },
    ]
    unlabeled_row = dict(source_rows[0])
    unlabeled_row.update(
        {
            "_row_in_file": 2,
            "timestamp": datetime.fromisoformat("2020-01-01T00:00:02"),
            "class": None,
        }
    )
    mismatched_row = dict(source_rows[0])
    mismatched_row.update(
        {
            "_row_in_file": 3,
            "timestamp": datetime.fromisoformat("2020-01-01T00:00:03"),
            "class": 2,
        }
    )
    source_rows.extend([unlabeled_row, mismatched_row])

    for index, row in enumerate(source_rows):
        row.update({name: float(index + 10) for name in SENSOR_RENAMES})
    source_rows[1]["QGL"] = None
    source_rows[0]["P-PDG"] = 101.25
    return spark.createDataFrame(
        [tuple(row.get(field.name) for field in fields) for row in source_rows],
        schema,
    )


def _manifest_rows():
    return [
        {
            "_source_path": "data/1/WELL-00001.parquet",
            "_file_sha256": "hash-real",
            "_manifest_source_type": "REAL",
            "_manifest_label": 1,
            "instance_id": "WELL-00001",
            "well_id": 1,
            "instance_start": datetime.fromisoformat("2020-01-01T00:00:00"),
        },
        {
            "_source_path": "data/1/SIMULATED_00001.parquet",
            "_file_sha256": "hash-sim",
            "_manifest_source_type": "SIMULATED",
            "_manifest_label": 1,
            "instance_id": "SIMULATED_00001",
            "well_id": None,
            "instance_start": None,
        },
    ]


def test_transform_keeps_one_row_shape_and_adds_recording_metadata(spark):
    bronze = _bronze_fixture(spark)
    silver = build_silver_observations(bronze, _manifest_rows())

    rows = silver.orderBy("source_type", "_row_in_file").collect()
    assert len(rows) == bronze.count() == 5
    assert len(SENSOR_RENAMES) == 27
    assert len(set(SENSOR_RENAMES.values())) == 27
    assert "P-PDG" not in silver.columns
    assert "downhole_pdg_pressure_pa" in silver.columns
    assert "QGL" not in silver.columns
    assert "gas_lift_flow_rate_m3_s" in silver.columns

    real_first = rows[0].asDict()
    assert real_first["instance_label"] == 1
    assert real_first["instance_event_name"] == "Abrupt Increase of BSW"
    assert real_first["class_code"] == 0
    assert real_first["row_event_name"] == "Normal Operation"
    assert real_first["row_event_phase"] == "normal"
    assert real_first["class_label_status"] == "consistent"
    assert real_first["downhole_pdg_pressure_pa"] == 101.25
    assert real_first["well_id"] == 1
    assert real_first["instance_start"] == datetime.fromisoformat("2020-01-01T00:00:00")
    assert real_first["timestamp"] == datetime.fromisoformat("2020-01-01T00:00:00")
    assert isinstance(real_first["timestamp"], datetime)

    real_second = rows[1].asDict()
    assert real_second["class_code"] == 101
    assert real_second["instance_event_name"] == "Abrupt Increase of BSW"
    assert real_second["row_event_name"] == "Abrupt Increase of BSW"
    assert real_second["row_event_phase"] == "transient"
    assert real_second["class_label_status"] == "consistent"
    assert real_second["state_code"] is None
    assert real_second["gas_lift_flow_rate_m3_s"] is None

    unlabeled = rows[2].asDict()
    assert unlabeled["class_code"] is None
    assert unlabeled["row_event_name"] is None
    assert unlabeled["row_event_phase"] == "unknown"
    assert unlabeled["class_label_status"] == "unlabeled"

    mismatched = rows[3].asDict()
    assert mismatched["class_code"] == 2
    assert mismatched["row_event_name"] == "Spurious Closure of DHSV"
    assert mismatched["row_event_phase"] == "steady"
    assert mismatched["class_label_status"] == "inconsistent"

    simulated = rows[4].asDict()
    assert simulated["source_type"] == "SIMULATED"
    assert simulated["instance_id"] == "SIMULATED_00001"
    assert simulated["well_id"] is None
    assert simulated["instance_start"] is None
    assert simulated["row_event_name"] == "Abrupt Increase of BSW"
    assert simulated["row_event_phase"] == "steady"
    assert simulated["class_label_status"] == "consistent"
    assert simulated["timestamp"] == datetime.fromisoformat("1970-01-01T00:00:00")
    assert silver.schema["timestamp"].dataType == TimestampNTZType()


def test_transform_fails_if_bronze_row_has_no_manifest_match(spark):
    bronze = _bronze_fixture(spark)
    only_real = _manifest_rows()[:1]

    with pytest.raises(ValueError, match="no current manifest match"):
        build_silver_observations(bronze, only_real)


def test_transform_fails_if_manifest_label_disagrees(spark):
    bronze = _bronze_fixture(spark)
    rows = _manifest_rows()
    rows[0]["_manifest_label"] = 2

    with pytest.raises(ValueError, match="label does not match"):
        build_silver_observations(bronze, rows)


def test_transform_fails_if_manifest_source_type_disagrees(spark):
    bronze = _bronze_fixture(spark)
    rows = _manifest_rows()
    rows[0]["_manifest_source_type"] = "SIMULATED"

    with pytest.raises(ValueError, match="source_type does not match"):
        build_silver_observations(bronze, rows)


def test_transform_fails_on_duplicate_current_manifest_recording(spark):
    bronze = _bronze_fixture(spark)
    rows = _manifest_rows()
    rows.append(dict(rows[0]))

    with pytest.raises(ValueError, match="more than one current recording"):
        build_silver_observations(bronze, rows)


def test_transform_rejects_an_unexpected_bronze_sensor_type(spark):
    bronze = _bronze_fixture(spark).withColumn("P-PDG", F.col("P-PDG").cast("string"))

    with pytest.raises(ValueError, match="unexpected field types"):
        build_silver_observations(bronze, _manifest_rows())


def test_recording_specific_class_rules_flag_normal_class_for_event_3(spark):
    bronze = _bronze_fixture(spark)
    real_path = "data/1/WELL-00001.parquet"
    bronze = bronze.withColumn(
        "label",
        F.when(F.col("_source_path") == real_path, F.lit(3)).otherwise(F.col("label")),
    ).withColumn(
        "class",
        F.when(
            (F.col("_source_path") == real_path) & (F.col("_row_in_file") == 1),
            F.lit(103).cast(ShortType()),
        ).otherwise(F.col("class")),
    )
    manifest_rows = _manifest_rows()
    manifest_rows[0]["_manifest_label"] = 3

    rows = (
        build_silver_observations(bronze, manifest_rows)
        .filter(F.col("_source_path") == real_path)
        .orderBy("_row_in_file")
        .collect()
    )
    assert rows[0]["instance_event_name"] == "Severe Slugging"
    assert rows[0]["row_event_name"] == "Normal Operation"
    assert rows[0]["class_label_status"] == "inconsistent"
    assert rows[1]["row_event_phase"] == "transient"
    assert rows[1]["class_label_status"] == "inconsistent"
