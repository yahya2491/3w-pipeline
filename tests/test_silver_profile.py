from threew.silver.profile import coverage_by_source_and_label, sensor_missingness


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
        "null": 1,
        "nan": 1,
        "positive_infinity": 1,
        "negative_infinity": 0,
    }
