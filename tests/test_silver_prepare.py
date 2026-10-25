from dataclasses import replace
from datetime import datetime, timedelta

import pytest
import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, ShortType, StringType, StructField, StructType, TimestampNTZType

from threew.silver.prepare import prepare_observations, summarize_preparation
from threew.silver.reference import OPENINGS, SENSOR_RENAMES
from threew.silver.rules import DEFAULT_RULES_PATH, FEATURE_SENSORS, load_rules

CHOKE = "production_choke_opening_pct"
PDG = "downhole_pdg_pressure_pa"


def readings(spark, times, overrides=None, path="a"):
    schema = StructType(
        [
            StructField("_source_path", StringType()),
            StructField("_file_sha256", StringType()),
            StructField("_row_in_file", LongType()),
            StructField("timestamp", TimestampNTZType()),
            StructField("class_code", ShortType()),
            StructField("class_label_status", StringType()),
            *[StructField(name, DoubleType()) for name in SENSOR_RENAMES.values()],
        ]
    )
    rows = []
    for index, seconds in enumerate(times):
        row = {
            "_source_path": path,
            "_file_sha256": "hash",
            "_row_in_file": index,
            "class_code": None,
            "class_label_status": "unlabeled",
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
    rules = replace(load_rules(), choke_forward_fill_max_seconds=3)
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
    assert f"{CHOKE}_is_valid" not in prepared.columns
    assert "prepared_sensor_count" not in prepared.columns
    assert "observed_valid_sensor_count" not in prepared.columns
    summary = summarize_preparation(prepared, rules)
    assert summary[f"{CHOKE}_repaired"] == 3
    assert summary[f"{CHOKE}_unresolved"] == 3  # stale opening, NaN and infinity


def test_extended_default_window_stops_at_original_anchor_age_for_both_openings(spark):
    rules = load_rules()
    assert rules.choke_forward_fill_max_seconds == 360
    limit = rules.choke_forward_fill_max_seconds
    values = [40.0] + [120.0] * (limit + 1) + [None, float("nan"), float("inf")]
    source = readings(spark, range(len(values)), {i: dict.fromkeys(OPENINGS, value) for i, value in enumerate(values)})
    prepared = prepare_observations(source, rules)
    rows = prepared.orderBy("_row_in_file").collect()
    for opening in OPENINGS:
        assert [row[f"{opening}_clean"] for row in rows[: limit + 1]] == [40.0] * (limit + 1)
        assert all(row[f"{opening}_clean"] is None for row in rows[limit + 1 :])
        assert all(row[f"{opening}_was_imputed"] for row in rows[1 : limit + 1])
        assert not rows[0][f"{opening}_was_imputed"]
        assert not any(row[f"{opening}_was_imputed"] for row in rows[limit + 1 :])
        assert rows[limit][f"{opening}_source_age_seconds"] == float(limit)
        assert rows[limit + 1][f"{opening}_source_age_seconds"] == float(limit + 1)
        assert rows[limit][opening] == 120.0
    summary = summarize_preparation(prepared, rules)
    for opening in OPENINGS:
        assert summary[f"{opening}_repaired"] == limit
        assert summary[f"{opening}_unresolved"] == 3  # Expired finite value, NaN and infinity.


def test_time_breaks_and_recordings_reset_repairs_and_empty_rows_remain(spark):
    source = readings(spark, [0, 1, 3, 3, 4, None, 6, 5, 6], {i: {CHOKE: 120.0} for i in range(1, 9)})
    empty = readings(spark, [0], {0: {name: None for name in SENSOR_RENAMES.values()}}, path="b")
    rules = load_rules()
    prepared = prepare_observations(source.unionByName(empty), rules)
    rows = prepared.orderBy("_source_path", "_row_in_file").collect()
    assert len(rows) == 10
    assert rows[1][f"{CHOKE}_clean"] == 50.0
    assert all(r[f"{CHOKE}_clean"] is None for r in rows[2:])
    assert rows[0]["sequence_segment_id"] == rows[1]["sequence_segment_id"]
    assert rows[1]["sequence_segment_id"] != rows[2]["sequence_segment_id"]
    assert rows[2]["sequence_segment_id"] != rows[3]["sequence_segment_id"]
    assert rows[-1]["sequence_segment_id"] == 1  # A different recording starts its own sequence.
    for index in (3, 5, 7):
        assert rows[index]["sequence_usable"] is False
    assert all(rows[-1][f"{name}_clean"] is None for name in SENSOR_RENAMES.values())
    assert "has_observed_sensor_evidence" not in rows[-1].asDict()
    assert summarize_preparation(prepared, rules)["rows_without_observed_valid_sensors"] == 1


def test_clean_sensor_rows_keep_source_and_add_only_selected_features(spark):
    source = readings(
        spark,
        [0, 1],
        {
            0: {
                PDG: 120.0,
                "production_choke_upstream_pressure_pa": 1000.0,
                "production_choke_downstream_pressure_pa": 500.0,
            },
            1: {
                PDG: None,
                "production_choke_upstream_pressure_pa": 1001.0,
                "production_choke_downstream_pressure_pa": 500.0,
            },
        },
    )
    rows = prepare_observations(source, load_rules()).orderBy("_row_in_file").collect()
    assert rows[0][f"{PDG}_clean"] == 120.0
    assert rows[1][f"{PDG}_clean"] is None
    assert rows[1][PDG] is None
    assert f"{PDG}_is_valid" not in rows[0].asDict()
    assert "production_choke_pressure_drop_pa" not in rows[0].asDict()
    assert "prepared_sensor_count" not in rows[0].asDict()
    assert rows[0]["timestamp"].microsecond == 500000
    removed_features = (
        "_delta",
        "_rate_per_second",
        "_observed_count_120s",
        "_observed_count_300s",
        "_coverage_120s",
        "_prepared_count_120s",
        "_mean_300s",
        "_deviation_120s",
        "_trend_120s_300s",
    )
    assert not any(name.endswith(removed_features) for name in rows[0].asDict())
    expected = {
        f"{name}_{suffix}"
        for name in FEATURE_SENSORS
        for suffix in ("mean_120s", "stddev_120s", "change", "count_120s")
    } | {f"{name}_invalid_duration_seconds" for name in ("gas_lift_choke_opening_pct", CHOKE)}
    assert expected <= rows[0].asDict().keys()
    assert len(rows[0]) == len(source.columns) + 65
    assert "annulus_pressure_pa_mean_120s" not in rows[0].asDict()


def test_rolling_features_use_exactly_the_current_and_previous_119_seconds(spark):
    source = readings(spark, range(122), {i: {PDG: float(i)} for i in range(122)})
    rows = prepare_observations(source, load_rules()).orderBy("_row_in_file").collect()
    assert rows[0][f"{PDG}_mean_120s"] == 0.0
    assert rows[0][f"{PDG}_stddev_120s"] is None
    assert rows[0][f"{PDG}_change"] is None
    assert rows[0][f"{PDG}_count_120s"] == 1
    assert rows[119][f"{PDG}_mean_120s"] == 59.5
    assert rows[119][f"{PDG}_stddev_120s"] == pytest.approx(1210**0.5)
    assert rows[119][f"{PDG}_count_120s"] == 120
    assert rows[120][f"{PDG}_mean_120s"] == 60.5
    assert rows[120][f"{PDG}_count_120s"] == 120
    assert rows[120][f"{PDG}_change"] == 1.0
    assert (
        summarize_preparation(prepare_observations(source, load_rules()), load_rules())[f"{PDG}_complete_120s_rows"]
        == 3
    )


def test_rolling_features_count_only_usable_measurements_and_preserve_missing_changes(spark):
    values = [10.0, None, float("nan"), 20.0, float("inf"), 30.0, 40.0]
    flow = "gas_lift_flow_rate_m3_s"
    source = readings(spark, range(len(values)), {i: {PDG: value, flow: None} for i, value in enumerate(values)})
    rows = prepare_observations(source, load_rules()).orderBy("_row_in_file").collect()
    assert [r[f"{PDG}_mean_120s"] for r in rows] == [10.0, 10.0, 10.0, 15.0, 15.0, 20.0, 25.0]
    assert [r[f"{PDG}_count_120s"] for r in rows] == [1, 1, 1, 2, 2, 3, 4]
    assert [r[f"{PDG}_change"] for r in rows] == [None, None, None, None, None, None, 10.0]
    assert all(r[f"{flow}_mean_120s"] is None and r[f"{flow}_count_120s"] == 0 for r in rows)


def test_features_reset_at_time_breaks_and_recording_boundaries(spark):
    source = readings(spark, [0, 1, 3, 3, 4, None, 6, 5, 6], {i: {PDG: float(i * 10)} for i in range(9)})
    other = readings(spark, [0, 1], {0: {PDG: 1000.0}, 1: {PDG: 1002.0}}, path="b")
    rows = (
        prepare_observations(source.unionByName(other), load_rules()).orderBy("_source_path", "_row_in_file").collect()
    )
    for index in (2, 4, 6, 8):
        assert rows[index][f"{PDG}_count_120s"] == 1
        assert rows[index][f"{PDG}_change"] is None
    for index in (3, 5, 7):
        assert rows[index][f"{PDG}_mean_120s"] is None
        assert rows[index][f"{PDG}_count_120s"] is None
        assert rows[index][f"{CHOKE}_invalid_duration_seconds"] is None
    assert rows[-1][f"{PDG}_mean_120s"] == 1001.0
    assert rows[-1][f"{PDG}_change"] == 2.0


def test_invalid_choke_duration_uses_raw_values_and_resets_after_unknowns_and_breaks(spark):
    values = [40.0, 120.0, 120.0, 120.0, None, 120.0, 120.0, 42.0, float("nan"), 120.0, float("inf"), 120.0]
    source = readings(spark, range(len(values)), {i: {CHOKE: value} for i, value in enumerate(values)})
    rows = prepare_observations(source, load_rules()).orderBy("_row_in_file").collect()
    assert [r[f"{CHOKE}_invalid_duration_seconds"] for r in rows] == [0, 0, 1, 2, 0, 0, 1, 0, 0, 0, 0, 0]
    assert rows[3][f"{CHOKE}_clean"] == 40.0  # Repair does not hide the continuing raw fault.
    leading = readings(spark, [0, 1, 4, 5], {i: {CHOKE: 120.0} for i in range(4)}, path="b")
    rows = prepare_observations(leading, load_rules()).orderBy("_row_in_file").collect()
    assert [r[f"{CHOKE}_invalid_duration_seconds"] for r in rows] == [0, 1, 0, 1]


def test_future_readings_do_not_change_earlier_preparation(spark):
    rules = load_rules()
    source = readings(spark, range(160), {i: {PDG: float(i)} for i in range(160)})
    changed = source.withColumn(PDG, F.when(F.col("_row_in_file") > 120, F.lit(9999.0)).otherwise(F.col(PDG)))
    changed = changed.withColumn(CHOKE, F.when(F.col("_row_in_file") > 120, F.lit(-99.99)).otherwise(F.col(CHOKE)))
    left = prepare_observations(source, rules).filter("_row_in_file <= 120").orderBy("_row_in_file").collect()
    right = prepare_observations(changed, rules).filter("_row_in_file <= 120").orderBy("_row_in_file").collect()
    assert left == right


def test_configured_domains_agree_in_cleaning_issues_and_references(spark):
    from threew.silver.issues import build_quality_issues
    from threew.silver.reference import build_reference_tables

    rules = replace(load_rules(), opening_max_pct=80.0)
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


@pytest.mark.parametrize(
    "section,key,value",
    [
        ("sequence", "expected_interval_seconds", 0),
        ("cleaning", "missing_classes", "forward_fill"),
        ("domains", "opening_min_pct", float("nan")),
        ("sequence", "typo", 120),
        ("features", "window_seconds", 0),
        ("features", "window_seconds", 60),
        ("features", "sensors", [PDG]),
    ],
)
def test_rules_reject_invalid_or_unsupported_settings(tmp_path, section, key, value):
    raw = yaml.safe_load(DEFAULT_RULES_PATH.read_text())
    raw[section][key] = value
    path = tmp_path / "rules.yaml"
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError):
        load_rules(path)


def test_export_keeps_only_clean_values_and_describes_the_clean_class(spark):
    from threew.silver.output import OUTPUT_COLUMNS, export_observations
    from threew.silver.reference import SENSOR_FIELDS
    from threew.silver.transform import add_event_descriptions

    values = [40.0, 120.0, float("nan"), None]
    classes = [None, 0, 105, 5]
    source = readings(
        spark,
        range(4),
        {
            i: {
                CHOKE: values[i],
                "class_code": classes[i],
                "class_label_status": "unlabeled" if i == 0 else "consistent",
            }
            for i in range(4)
        },
    )
    for name, value in {
        "source_type": "REAL",
        "instance_label": 5,
        "state_code": 1,
        "_ingest_run_id": "bronze-source",
        "instance_id": "recording",
        "well_id": 1,
    }.items():
        source = source.withColumn(name, F.lit(value))
    source = source.withColumn("instance_start", F.col("timestamp"))
    source = add_event_descriptions(source)
    prepared = prepare_observations(source, load_rules())
    exported = export_observations(prepared)
    rows = exported.orderBy("_row_in_file").collect()
    assert len(exported.columns) == len(OUTPUT_COLUMNS) == 79
    assert not (set(SENSOR_FIELDS) | {"class_code", "_ingest_run_id"}) & set(exported.columns)
    assert [row[f"{CHOKE}_clean"] for row in rows] == [40.0, 40.0, None, None]
    assert rows[0].class_code_clean == 0 and rows[0].class_was_inferred
    assert rows[0].row_event_phase == "normal" and rows[0].class_label_status == "consistent"
    assert rows[0].row_event_name == "Normal Operation"
    assert summarize_preparation(exported, load_rules(), source) == summarize_preparation(prepared, load_rules())
