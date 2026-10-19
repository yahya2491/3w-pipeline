from datetime import datetime

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

from threew.silver.issues import build_quality_issues, quarantine_issues, summarize_quality
from threew.silver.quality import build_quality_summaries
from threew.silver.reconcile import BASE_OBSERVATION_TYPES
from threew.silver.reference import SENSOR_DEFINITIONS, build_reference_tables


@pytest.fixture
def observations(spark):
    rows = []
    times = ["00:00:00", "00:00:01", "00:00:01", "00:00:00.5", "00:00:03", None, "00:00:04", "00:00:04.25"]
    classes = [1, None, 2, 999, 1, 1, 1, 1]
    for position, (time, code) in enumerate(zip(times, classes, strict=True)):
        row = {
            "_source_path": "data/1/recording.parquet",
            "_file_sha256": "a" * 64,
            "_row_in_file": position,
            "_ingest_run_id": "bronze-run",
            "source_type": "REAL",
            "instance_label": 1,
            "instance_id": "recording",
            "well_id": 1,
            "instance_start": datetime.fromisoformat("2020-01-01T00:00:00"),
            "instance_event_name": "Abrupt Increase of BSW",
            "timestamp": datetime.fromisoformat("2020-01-01T" + time) if time else None,
            "class_code": code,
            "state_code": None,
            "row_event_phase": "unknown" if code is None else "unrecognized" if code == 999 else "steady",
            "class_label_status": "unlabeled" if code is None else "inconsistent" if code in (2, 999) else "consistent",
            **{definition["sensor_name"]: None for definition in SENSOR_DEFINITIONS},
            "downhole_pdg_pressure_pa": 10.0,
        }
        rows.append(row)
    rows[0]["downhole_safety_valve_state"] = 0.5
    rows[0]["gas_lift_choke_opening_pct"] = 0.0
    rows[4]["gas_lift_flow_rate_m3_s"] = float("inf")
    rows[4]["downhole_safety_valve_state"] = 0.25
    rows[4]["gas_lift_choke_opening_pct"] = 101.0
    rows[6]["downhole_pdg_pressure_pa"] = None
    return spark.createDataFrame(
        rows, StructType([StructField(name, kind, True) for name, kind in BASE_OBSERVATION_TYPES.items()])
    )


def test_issue_reasons_locate_problems_and_quarantine_is_use_specific(observations):
    issues = build_quality_issues(observations)
    found = {(r["_row_in_file"], r["field_name"], r["issue_code"]): r.asDict() for r in issues.collect()}
    expected = {
        (1, "class_code", "missing_class"),
        (1, "timestamp", "duplicate_timestamp"),
        (2, "class_code", "class_recording_mismatch"),
        (2, "timestamp", "duplicate_timestamp"),
        (3, "class_code", "unsupported_class"),
        (3, "timestamp", "backward_timestamp"),
        (4, "timestamp", "timestamp_gap"),
        (4, "gas_lift_flow_rate_m3_s", "nonfinite_sensor"),
        (4, "downhole_safety_valve_state", "unsupported_valve_state"),
        (4, "gas_lift_choke_opening_pct", "opening_out_of_range"),
        (5, "timestamp", "missing_timestamp"),
        (6, "sensor_readings", "no_finite_sensor_readings"),
        (7, "timestamp", "short_timestamp_interval"),
    }
    assert set(found) == expected
    assert all(row["reason"] and row["recommended_action"] for row in found.values())
    assert found[(1, "class_code", "missing_class")]["affected_use"] == "labeled_analysis"
    assert found[(4, "gas_lift_flow_rate_m3_s", "nonfinite_sensor")]["affected_use"] == "sensor_measurement"
    assert quarantine_issues(issues).count() == 10
    # No missing sensor-cell entries; zero and the intermediate valve state are valid.
    assert not any(key[0] == 0 for key in found)
    assert observations.count() == 8
    assert observations.filter("_row_in_file = 4").first()["gas_lift_choke_opening_pct"] == 101.0


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_values_are_flagged_once_and_never_clipped(observations, value):
    source = observations.filter("_row_in_file = 0").withColumn("downhole_safety_valve_state", F.lit(value))
    rows = build_quality_issues(source).collect()
    assert len(rows) == 1
    assert rows[0]["issue_code"] == "nonfinite_sensor"


def test_shared_references_match_documented_event_phases_and_sensor_domains(spark):
    tables = build_reference_tables(spark)
    events = {row["event_code"]: row for row in tables["event_reference"].collect()}
    assert set(events) == set(range(10))
    assert events[1]["transient_class_code"] == 101
    assert events[3]["supports_normal"] is False
    assert events[4]["supports_transient"] is False
    assert events[0]["supports_steady"] is False
    sensors = {row["source_name"]: row for row in tables["sensor_reference"].collect()}
    assert len(sensors) == 27
    assert len({row["sensor_name"] for row in sensors.values()}) == 27
    assert sensors["ESTADO-DHSV"]["allowed_values"] == [0.0, 0.5, 1.0]
    assert sensors["ABER-CKP"]["maximum"] == 100.0
    assert sensors["P-PDG"]["unit"] == "Pa"
    assert sensors["P-PDG"]["minimum"] is None
    assert sensors["T-TPT"]["minimum"] is None


def test_load_summary_counts_distinct_affected_rows_and_keeps_expected_missingness(observations):
    summaries = build_quality_summaries(observations)
    try:
        report = summarize_quality(
            build_quality_issues(observations),
            summaries.instance_quality,
            summaries.sensor_availability,
            observations.count(),
        )
        assert report["observation_rows"] == 8
        assert report["issue_entries"] == 13
        assert report["rows_with_issues"] == 7
        assert report["quarantine_issue_entries"] == 10
        assert report["rows_with_quarantine_issues"] == 6
        assert report["quarantine_row_percent"] == 75.0
        assert report["recording_totals"]["unlabeled_rows"] == 1
        assert report["recording_totals"]["null_state_rows"] == 8
        assert len(report["sensor_availability"]) == 27
    finally:
        summaries.unpersist()


def test_issue_free_rows_publish_an_empty_typed_quarantine(observations):
    source = observations.filter("_row_in_file = 0")
    issues = build_quality_issues(source)
    assert issues.count() == 0
    assert quarantine_issues(issues).schema == issues.schema
