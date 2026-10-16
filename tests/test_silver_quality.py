from datetime import datetime

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

from threew.silver.quality import SENSOR_FIELDS, build_quality_summaries


def _silver_fixture(spark):
    fields = [
        StructField("source_type", StringType(), nullable=False),
        StructField("instance_label", IntegerType(), nullable=False),
        StructField("instance_event_name", StringType(), nullable=False),
        StructField("_source_path", StringType(), nullable=False),
        StructField("_file_sha256", StringType(), nullable=False),
        StructField("instance_id", StringType(), nullable=False),
        StructField("well_id", IntegerType(), nullable=True),
        StructField("instance_start", TimestampNTZType(), nullable=True),
        StructField("_row_in_file", LongType(), nullable=False),
        StructField("class_code", ShortType(), nullable=True),
        StructField("state_code", ShortType(), nullable=True),
        StructField("timestamp", TimestampNTZType(), nullable=True),
        StructField("row_event_phase", StringType(), nullable=False),
        StructField("class_label_status", StringType(), nullable=False),
        *[StructField(name, DoubleType(), nullable=True) for name in SENSOR_FIELDS],
    ]

    real_path = "data/1/WELL-00001.parquet"
    real_start = datetime.fromisoformat("2020-01-01T00:00:00")
    row_specs = [
        ("REAL", real_path, "hash-real", "WELL-00001", 1, real_start, 0, 1, 0, "normal", "consistent"),
        ("REAL", real_path, "hash-real", "WELL-00001", 1, real_start, 1, 101, None, "transient", "consistent"),
        ("REAL", real_path, "hash-real", "WELL-00001", 1, real_start, 2, None, 1, "unknown", "unlabeled"),
        ("REAL", real_path, "hash-real", "WELL-00001", 1, real_start, 3, 2, 1, "steady", "inconsistent"),
        (
            "SIMULATED",
            "data/1/SIMULATED_00001.parquet",
            "hash-sim",
            "SIMULATED_00001",
            None,
            None,
            0,
            1,
            2,
            "steady",
            "consistent",
        ),
    ]

    rows = []
    for (
        source_type,
        path,
        file_hash,
        instance_id,
        well_id,
        start,
        row_number,
        class_code,
        state_code,
        phase,
        status,
    ) in row_specs:
        timestamp = (
            datetime.fromisoformat(f"2020-01-01T00:00:{row_number:02d}")
            if source_type == "REAL"
            else datetime.fromisoformat("1970-01-01T00:00:00")
        )
        row = {
            "source_type": source_type,
            "instance_label": 1,
            "instance_event_name": "Abrupt Increase of BSW",
            "_source_path": path,
            "_file_sha256": file_hash,
            "instance_id": instance_id,
            "well_id": well_id,
            "instance_start": start,
            "_row_in_file": row_number,
            "class_code": class_code,
            "state_code": state_code,
            "timestamp": timestamp,
            "row_event_phase": phase,
            "class_label_status": status,
            **{name: 1.0 for name in SENSOR_FIELDS},
        }
        rows.append(row)

    rows[1]["gas_lift_flow_rate_m3_s"] = None
    rows[4]["gas_lift_flow_rate_m3_s"] = float("inf")
    return spark.createDataFrame(rows, StructType(fields))


def test_quality_summaries_report_recordings_and_sensor_coverage(spark, request):
    summaries = build_quality_summaries(_silver_fixture(spark))
    request.addfinalizer(summaries.unpersist)

    quality = {row["_source_path"]: row.asDict() for row in summaries.instance_quality.collect()}
    assert len(quality) == 2

    real = quality["data/1/WELL-00001.parquet"]
    assert real["recording_rows"] == 4
    assert real["unlabeled_rows"] == 1
    assert real["null_state_rows"] == 1
    assert real["null_timestamp_rows"] == 0
    assert real["duplicate_timestamp_rows"] == 0
    assert real["timestamp_intervals_compared"] == 3
    assert real["one_second_intervals"] == 3
    assert real["repeated_adjacent_timestamps"] == 0
    assert real["backward_intervals"] == 0
    assert real["gaps_over_one_second"] == 0
    assert real["inconsistent_class_rows"] == 1
    assert real["unrecognized_class_rows"] == 0
    assert real["sensors_present"] == 27
    assert real["sensors_missing_entire_recording"] == 0
    assert real["no_sensors_present"] is False

    simulated = quality["data/1/SIMULATED_00001.parquet"]
    assert simulated["recording_rows"] == 1
    assert simulated["timestamp_intervals_compared"] == 0
    assert simulated["sensors_present"] == 26
    assert simulated["sensors_missing_entire_recording"] == 1

    availability_rows = summaries.sensor_availability.collect()
    assert len(availability_rows) == 2 * len(SENSOR_FIELDS)
    availability = {(row["_source_path"], row["sensor_name"]): row.asDict() for row in availability_rows}

    real_flow = availability[("data/1/WELL-00001.parquet", "gas_lift_flow_rate_m3_s")]
    assert real_flow["recording_rows"] == 4
    assert real_flow["rows_with_finite_values"] == 3
    assert real_flow["rows_without_finite_values"] == 1
    assert real_flow["percent_rows_with_finite_values"] == 75.0
    assert real_flow["has_finite_values"] is True

    simulated_flow = availability[("data/1/SIMULATED_00001.parquet", "gas_lift_flow_rate_m3_s")]
    assert simulated_flow["rows_with_finite_values"] == 0
    assert simulated_flow["rows_without_finite_values"] == 1
    assert simulated_flow["percent_rows_with_finite_values"] == 0.0
    assert simulated_flow["has_finite_values"] is False


def test_quality_preserves_fractional_time_and_reports_abnormal_sequences(spark, request):
    from pyspark.sql import functions as F

    source = _silver_fixture(spark).filter("source_type = 'REAL'").limit(1)
    times = [
        "2020-01-01 00:00:00.5",
        "2020-01-01 00:00:01.5",
        "2020-01-01 00:00:01.5",
        "2020-01-01 00:00:03",
        "2020-01-01 00:00:02.75",
        None,
        "2020-01-01 00:00:06",
    ]
    values = spark.createDataFrame(list(enumerate(times)), "position long, time string")
    observations = (
        source.drop("timestamp", "_row_in_file")
        .crossJoin(values)
        .withColumn("timestamp", F.col("time").cast("timestamp_ntz"))
        .withColumnRenamed("position", "_row_in_file")
        .drop("time")
    )
    summaries = build_quality_summaries(observations)
    request.addfinalizer(summaries.unpersist)
    row = summaries.instance_quality.first()
    assert row["null_timestamp_rows"] == 1
    assert row["duplicate_timestamp_rows"] == 1
    assert row["timestamp_intervals_compared"] == 4
    assert row["one_second_intervals"] == 1
    assert row["repeated_adjacent_timestamps"] == 1
    assert row["backward_intervals"] == 1
    assert row["gaps_over_one_second"] == 1
    assert row["largest_gap_seconds"] == 1.5


def test_quality_distinguishes_zero_from_missing_and_nonfinite_values(spark, request):
    from pyspark.sql import functions as F

    source = _silver_fixture(spark).filter("source_type = 'REAL'").limit(1)
    source = source.drop(*SENSOR_FIELDS, "_row_in_file")
    values = spark.createDataFrame(
        list(enumerate([0.0, None, float("nan"), float("inf"), float("-inf")])), "position long, value double"
    )
    observations = source.crossJoin(values).withColumnRenamed("position", "_row_in_file")
    for sensor in SENSOR_FIELDS:
        observations = observations.withColumn(sensor, F.lit(None).cast("double"))
    observations = observations.withColumn(SENSOR_FIELDS[0], F.col("value")).drop("value")
    summaries = build_quality_summaries(observations)
    request.addfinalizer(summaries.unpersist)
    quality = summaries.instance_quality.first()
    assert quality["sensors_present"] == 1
    sensor = summaries.sensor_availability.filter(F.col("sensor_name") == SENSOR_FIELDS[0]).first()
    assert sensor["rows_with_finite_values"] == 1
    assert sensor["rows_without_finite_values"] == 4
    assert sensor["percent_rows_with_finite_values"] == 20.0
    assert sensor["has_finite_values"] is True

    empty = build_quality_summaries(observations.withColumn(SENSOR_FIELDS[0], F.lit(None).cast("double")))
    request.addfinalizer(empty.unpersist)
    row = empty.instance_quality.first()
    assert row["no_sensors_present"] is True
    assert row["sensors_present"] == 0
    assert row["sensors_missing_entire_recording"] == 27
