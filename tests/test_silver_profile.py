from pyspark.sql import functions as F

from threew.silver.profile import (
    coverage_by_source_and_label,
    recording_sensor_counts,
    recording_sensor_summary,
    sensor_missingness,
    sensor_recording_availability,
    timestamp_quality_by_recording,
    timestamp_quality_summary,
)


def test_profile_counts_recordings_labels_and_sensor_values(spark):
    rows = [
        ("REAL", 0, "real.parquet", "hash-real", 0, 0, 1.5),
        ("REAL", 0, "real.parquet", "hash-real", None, 0, None),
        ("REAL", 0, "real.parquet", "hash-real", 1, 0, float("nan")),
        ("SIMULATED", 1, "simulated.parquet", "hash-simulated", 101, None, float("inf")),
    ]
    schema = "source_type string, label int, _source_path string, _file_sha256 string, class short, state short, sensor double"
    df = spark.createDataFrame(rows, schema)

    coverage = {(row.source_type, row.label): row.asDict() for row in coverage_by_source_and_label(df).collect()}
    assert coverage[("REAL", 0)]["rows"] == 3
    assert coverage[("REAL", 0)]["recordings"] == 1
    assert coverage[("REAL", 0)]["class_null"] == 1
    assert coverage[("REAL", 0)]["state_null"] == 0
    assert coverage[("SIMULATED", 1)]["recordings"] == 1
    assert coverage[("SIMULATED", 1)]["state_null"] == 1

    missingness = sensor_missingness(df, ("sensor",))[0]
    assert missingness == {
        "sensor": "sensor",
        "total_rows": 4,
        "null": 1,
        "nan": 1,
        "positive_infinity": 1,
        "negative_infinity": 0,
    }

    per_recording = recording_sensor_counts(df, ("sensor",))
    sensor_counts = {(row.source_type, row.label): row.sensors_present for row in per_recording.collect()}
    assert sensor_counts == {("REAL", 0): 1, ("SIMULATED", 1): 0}

    summary = {(row.source_type, row.label): row.asDict() for row in recording_sensor_summary(per_recording).collect()}
    assert summary[("REAL", 0)]["min_sensors_present"] == 1
    assert summary[("SIMULATED", 1)]["recordings_with_no_sensors"] == 1

    availability = {
        (row.source_type, row.sensor): row.asDict()
        for row in sensor_recording_availability(per_recording, ("sensor",)).collect()
    }
    assert availability[("REAL", "sensor")]["recordings_present"] == 1
    assert availability[("SIMULATED", "sensor")]["recordings_absent"] == 1


def test_timestamp_quality_counts_duplicates_and_non_one_second_intervals(spark):
    rows = [
        ("REAL", 0, "real.parquet", "hash-real", 0, "2020-01-01 00:00:00"),
        ("REAL", 0, "real.parquet", "hash-real", 1, "2020-01-01 00:00:01"),
        ("REAL", 0, "real.parquet", "hash-real", 2, "2020-01-01 00:00:01"),
        ("REAL", 0, "real.parquet", "hash-real", 3, "2020-01-01 00:00:04"),
        ("REAL", 0, "real.parquet", "hash-real", 4, "2020-01-01 00:00:03"),
        ("SIMULATED", 1, "simulated.parquet", "hash-simulated", 0, "2020-01-01 00:00:00"),
        ("SIMULATED", 1, "simulated.parquet", "hash-simulated", 1, None),
        ("SIMULATED", 1, "simulated.parquet", "hash-simulated", 2, "2020-01-01 00:00:02"),
    ]
    schema = (
        "source_type string, label int, _source_path string, _file_sha256 string, _row_in_file long, timestamp string"
    )
    df = spark.createDataFrame(rows, schema).withColumn("timestamp", F.col("timestamp").cast("timestamp_ntz"))

    per_recording = {row.source_type: row.asDict() for row in timestamp_quality_by_recording(df).collect()}
    real = per_recording["REAL"]
    assert real["null_timestamps"] == 0
    assert real["duplicate_timestamp_rows"] == 1
    assert real["intervals_compared"] == 4
    assert real["one_second_intervals"] == 1
    assert real["repeated_adjacent_timestamps"] == 1
    assert real["gaps_over_one_second"] == 1
    assert real["backward_intervals"] == 1
    assert real["largest_gap_seconds"] == 3

    simulated = per_recording["SIMULATED"]
    assert simulated["null_timestamps"] == 1
    assert simulated["intervals_compared"] == 0

    summary = {
        row.source_type: row.asDict() for row in timestamp_quality_summary(timestamp_quality_by_recording(df)).collect()
    }
    assert summary["REAL"]["recordings_with_duplicate_times"] == 1
    assert summary["SIMULATED"]["null_timestamps"] == 1
