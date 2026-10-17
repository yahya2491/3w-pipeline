from dataclasses import replace
from datetime import datetime, timedelta
from runpy import run_path
from statistics import stdev

import pytest
import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampNTZType

from threew.config import REPO_ROOT
from threew.silver.prepare import PRESSURE_DROP, prepare_observations, summarize_preparation
from threew.silver.reference import SENSOR_RENAMES
from threew.silver.rules import DEFAULT_RULES_PATH, load_rules

CHOKE = "production_choke_opening_pct"
PDG = "downhole_pdg_pressure_pa"


def readings(spark, times, overrides=None, path="a"):
    schema = StructType(
        [
            StructField("_source_path", StringType()),
            StructField("_file_sha256", StringType()),
            StructField("_row_in_file", LongType()),
            StructField("timestamp", TimestampNTZType()),
            *[StructField(name, DoubleType()) for name in SENSOR_RENAMES.values()],
        ]
    )
    rows = []
    for index, seconds in enumerate(times):
        row = {
            "_source_path": path,
            "_file_sha256": "hash",
            "_row_in_file": index,
            "timestamp": None
            if seconds is None
            else datetime.fromisoformat("2020-01-01T00:00:00.500000") + timedelta(seconds=seconds),
            **{name: 0.5 if name.endswith("_valve_state") else 50.0 for name in SENSOR_RENAMES.values()},
        }
        row.update((overrides or {}).get(index, {}))
        rows.append(row)
    return spark.createDataFrame(rows, schema)


def test_choke_repair_uses_original_anchor_for_at_most_three_seconds(spark):
    values = [40.0, 120.0, -99.99, 101.0, 120.0, None, 42.0, float("nan"), float("inf"), 0.0, 100.0]
    source = readings(spark, range(len(values)), {i: {CHOKE: value} for i, value in enumerate(values)})
    rules = replace(load_rules(), sensors=(CHOKE,))
    prepared = prepare_observations(source, rules)
    rows = prepared.orderBy("_row_in_file").collect()
    assert [r[f"{CHOKE}_clean"] for r in rows] == [40.0, 40.0, 40.0, 40.0, None, None, 42.0, None, None, 0.0, 100.0]
    assert [r[f"{CHOKE}_was_imputed"] for r in rows] == [
        False,
        True,
        True,
        True,
        False,
        False,
        False,
        False,
        False,
        False,
        False,
    ]
    assert [rows[i][f"{CHOKE}_source_age_seconds"] for i in range(5)] == [0.0, 1.0, 2.0, 3.0, 4.0]
    assert rows[2][CHOKE] == -99.99
    assert rows[3][f"{CHOKE}_is_valid"] is False
    assert rows[3]["prepared_sensor_count"] == 27
    assert rows[3]["observed_valid_sensor_count"] == 26
    assert f"{CHOKE}_invalid_duration_seconds" not in rows[4].asDict()
    summary = summarize_preparation(prepared, rules)
    assert summary[f"{CHOKE}_repaired"] == 3
    assert summary[f"{CHOKE}_unresolved"] == 3  # stale opening, NaN and infinity


def test_time_breaks_and_recordings_reset_repairs_and_empty_rows_remain(spark):
    source = readings(spark, [0, 1, 3, 3, 4, None, 6, 5, 6], {i: {CHOKE: 120.0} for i in range(1, 9)})
    empty = readings(spark, [0], {0: {name: None for name in SENSOR_RENAMES.values()}}, path="b")
    rules = replace(load_rules(), sensors=(CHOKE,))
    rows = prepare_observations(source.unionByName(empty), rules).orderBy("_source_path", "_row_in_file").collect()
    assert len(rows) == 10
    assert rows[1][f"{CHOKE}_clean"] == 50.0
    assert all(r[f"{CHOKE}_clean"] is None for r in rows[2:])
    for index in (3, 5, 7):
        assert rows[index]["sequence_usable"] is False
    assert f"{CHOKE}_coverage_120s" not in rows[3].asDict()
    assert rows[-1]["has_observed_sensor_evidence"] is False
    assert rows[-1]["prepared_sensor_count"] == 0
    assert f"{CHOKE}_delta" not in rows[2].asDict()


def test_elapsed_windows_and_pressure_relationship(spark):
    rules = replace(load_rules(), sensors=(PDG,))
    overrides = {
        i: {
            PDG: None if i == 100 else float(i),
            "production_choke_upstream_pressure_pa": float(1000 + i),
            "production_choke_downstream_pressure_pa": 500.0,
        }
        for i in range(321)
    }
    rows = prepare_observations(readings(spark, range(321), overrides), rules).orderBy("_row_in_file").collect()
    assert rows[58][f"{PDG}_mean_120s"] is None
    assert rows[59][f"{PDG}_mean_120s"] == 29.5
    assert f"{PDG}_coverage_120s" not in rows[59].asDict()
    expected = [float(i) for i in range(1, 121) if i != 100]
    assert rows[120][f"{PDG}_mean_120s"] == pytest.approx(sum(expected) / len(expected))
    assert rows[120][f"{PDG}_stddev_120s"] == pytest.approx(stdev(expected))
    assert f"{PDG}_coverage_120s" not in rows[120].asDict()
    previous = [float(i) for i in range(120) if i != 100]
    assert rows[120][f"{PDG}_deviation_120s"] == pytest.approx(120 - sum(previous) / len(previous))
    assert f"{PDG}_delta" not in rows[120].asDict()
    assert f"{PDG}_rate_per_second" not in rows[102].asDict()
    assert rows[149][f"{PDG}_mean_300s"] is None
    expected_long = [float(i) for i in range(1, 301) if i != 100]
    assert rows[300][f"{PDG}_mean_300s"] == pytest.approx(sum(expected_long) / len(expected_long))
    assert f"{PDG}_trend_120s_300s" not in rows[300].asDict()
    assert rows[120][PRESSURE_DROP] == 620.0
    assert f"{PRESSURE_DROP}_rate_per_second" not in rows[120].asDict()
    assert f"{PRESSURE_DROP}_delta" not in rows[120].asDict()
    assert rows[120]["timestamp"].microsecond == 500000


def test_future_readings_do_not_change_earlier_features(spark):
    rules = replace(load_rules(), sensors=(PDG, CHOKE))
    source = readings(spark, range(160), {i: {PDG: float(i)} for i in range(160)})
    changed = source.withColumn(PDG, F.when(F.col("_row_in_file") > 120, F.lit(9999.0)).otherwise(F.col(PDG)))
    changed = changed.withColumn(CHOKE, F.when(F.col("_row_in_file") > 120, F.lit(-99.99)).otherwise(F.col(CHOKE)))
    left = prepare_observations(source, rules).filter("_row_in_file <= 120").orderBy("_row_in_file").collect()
    right = prepare_observations(changed, rules).filter("_row_in_file <= 120").orderBy("_row_in_file").collect()
    assert left == right


def test_masked_repair_experiment_reports_known_errors(spark):
    evaluate_masked_repairs = run_path(str(REPO_ROOT / "scripts" / "evaluate_choke_repair.py"))[
        "evaluate_masked_repairs"
    ]

    rules = replace(load_rules(), sensors=(CHOKE,))
    source = readings(spark, range(4), {i: {CHOKE: float(40 + 2 * i)} for i in range(4)})
    data = prepare_observations(source.withColumn("source_type", F.lit("REAL")), rules)
    results = [row for row in evaluate_masked_repairs(data, rules) if row["sensor"] == CHOKE]
    assert [row["_gap_seconds"] for row in results] == [1.0, 2.0, 3.0]
    assert [row["mean_absolute_error_pct_points"] for row in results] == [2.0, 4.0, 6.0]
    assert all(row["masked_valid_measurements"] == 1 for row in results)


def test_configured_domains_agree_in_cleaning_issues_and_references(spark):
    from threew.silver.issues import build_quality_issues
    from threew.silver.reference import build_reference_tables

    rules = replace(load_rules(), opening_max_pct=80.0, sensors=(CHOKE,))
    source = readings(spark, [0, 1], {1: {CHOKE: 90.0}})
    source = (
        source.withColumn("class_code", F.lit(0))
        .withColumn("row_event_phase", F.lit("normal"))
        .withColumn("class_label_status", F.lit("consistent"))
    )
    prepared = prepare_observations(source, rules).orderBy("_row_in_file").collect()
    assert prepared[1][f"{CHOKE}_clean"] == 50.0
    assert prepared[1][f"{CHOKE}_was_imputed"] is True
    issues = build_quality_issues(source, rules).filter(F.col("field_name") == CHOKE).collect()
    assert len(issues) == 1 and issues[0]["issue_code"] == "opening_out_of_range"
    reference = build_reference_tables(spark, rules)["sensor_reference"].filter(F.col("sensor_name") == CHOKE).first()
    assert reference["maximum"] == 80.0


def test_canonical_preparation_can_reuse_build_cache(spark):
    rules = replace(load_rules(), sensors=(PDG,))
    source = readings(spark, range(4))
    built = prepare_observations(source, rules).cache()
    try:
        built.count()
        reference = prepare_observations(source, rules)
        assert "InMemoryRelation" in reference._jdf.queryExecution().withCachedData().toString()
    finally:
        built.unpersist()


def test_rolling_statistics_use_clean_values_without_counting_repairs_as_observations(spark):
    rules = replace(load_rules(), sensors=(CHOKE,))
    overrides = {i: {CHOKE: 120.0} for i in (60, 61, 62, 63)}
    overrides[120] = {"downhole_safety_valve_state": 0.75}
    source = readings(spark, range(121), overrides)
    row = prepare_observations(source, rules).filter("_row_in_file = 120").first()
    assert f"{CHOKE}_prepared_count_120s" not in row.asDict()
    assert f"{CHOKE}_coverage_120s" not in row.asDict()
    assert f"{CHOKE}_observed_count_120s" not in row.asDict()
    assert f"{CHOKE}_observed_count_300s" not in row.asDict()
    assert row[f"{CHOKE}_mean_120s"] == 50.0
    assert row[f"{CHOKE}_stddev_120s"] == 0.0
    assert row["downhole_safety_valve_state"] == 0.75
    assert row["downhole_safety_valve_state_clean"] is None
    assert row["downhole_safety_valve_state_is_valid"] is False


@pytest.mark.parametrize(
    "section,key,value",
    [
        ("temporal", "short_window_seconds", True),
        ("temporal", "minimum_observed_fraction", 0),
        ("temporal", "long_window_seconds", 100),
        ("temporal", "sensors", ["class_code"]),
        ("cleaning", "missing_classes", "forward_fill"),
        ("domains", "opening_min_pct", float("nan")),
        ("temporal", "typo", 120),
    ],
)
def test_rules_reject_invalid_or_unsupported_settings(tmp_path, section, key, value):
    raw = yaml.safe_load(DEFAULT_RULES_PATH.read_text())
    raw[section][key] = value
    path = tmp_path / "rules.yaml"
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError):
        load_rules(path)
