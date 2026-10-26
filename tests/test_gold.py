import importlib.util
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from threew.silver.output import CONTRACT_VERSION
from threew.silver.rules import FEATURE_SENSORS

GOLD_DIR = Path(__file__).parents[1] / "gold"


def load(name):
    """gold/ is a plain folder of scripts, not a package, so load each one from its file."""
    spec = importlib.util.spec_from_file_location(f"gold_{name}", GOLD_DIR / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gold = load("build_gold")
query = load("query")

START = datetime(2017, 1, 1, tzinfo=UTC)


def observations(spark, recordings):
    """Silver-like rows, one second apart. recordings: {path: (source_type, well_id, event_id, [class per row])}."""
    rows = []
    for path, (source, well, event, classes) in recordings.items():
        for i, code in enumerate(classes):
            phase = {None: "unknown", 0: "normal"}.get(code, "transient" if code and code > 100 else "steady")
            rows.append(
                {
                    "_source_path": path,
                    "_row_in_file": i,
                    "timestamp": START + timedelta(seconds=i),
                    "instance_id": path,
                    "well_id": well,
                    "source_type": source,
                    "instance_label": event,
                    "instance_event_name": f"event {event}",
                    "class_code_clean": code,
                    "class_was_inferred": False,
                    "row_event_name": None if code is None else f"event {code % 100}",
                    "row_event_phase": phase,
                }
            )
    schema = (
        "_source_path string, _row_in_file long, timestamp timestamp, instance_id string, well_id int, "
        "source_type string, instance_label int, instance_event_name string, class_code_clean int, "
        "class_was_inferred boolean, row_event_name string, row_event_phase string"
    )
    return spark.createDataFrame(rows, schema)


def test_event_phases_split_each_recording_where_the_class_changes(spark):
    obs = observations(
        spark,
        {
            "a": ("REAL", 1, 1, [0, 0, 0, 101, 101, 1, 1, 1]),
            "b": ("SIMULATED", None, 2, [None, 0, 102, 102]),
        },
    )
    phases = {(p.recording_id, p.phase_seq): p for p in gold.build_event_phases(obs).collect()}

    assert sorted(phases) == [("a", 1), ("a", 2), ("a", 3), ("b", 1), ("b", 2), ("b", 3)]
    assert [phases["a", i].phase for i in (1, 2, 3)] == ["normal", "transient", "steady"]
    assert [phases["a", i].event_id for i in (1, 2, 3)] == [0, 1, 1]
    assert [(phases["a", i].start_offset_s, phases["a", i].n_rows) for i in (1, 2, 3)] == [(0, 3), (3, 2), (5, 3)]
    assert phases["a", 3].duration_s == 2  # rows 5..7
    # a missing class is its own phase, so no Silver row is lost
    assert (phases["b", 1].phase, phases["b", 1].class_code) == ("unknown", None)
    assert sum(p.n_rows for p in phases.values()) == obs.count()


def test_views_measure_lead_time_and_time_to_steady(spark):
    obs = observations(spark, {"a": ("REAL", 7, 1, [0] * 60 + [101] * 120 + [1] * 30)})
    query.register_views(
        spark, {"recordings": gold.build_recordings(obs), "event_phases": gold.build_event_phases(obs)}
    )

    timing = spark.table("recording_event_timing").first()
    assert (timing.normal_lead_s, timing.time_to_steady_s, timing.event_duration_s) == (60, 120, 149)
    summary = spark.table("well_event_summary").first()
    assert (summary.well_id, summary.recordings, summary.avg_normal_lead_min, summary.avg_time_to_steady_min) == (
        7,
        1,
        1.0,
        2.0,
    )


def test_checks_pass_on_a_correct_build_and_catch_a_lost_phase(spark):
    obs = observations(spark, {"a": ("REAL", 1, 1, [0, 0, 101, 1]), "b": ("REAL", 2, 0, [0, 0, 0])})
    recordings = gold.build_recordings(obs)
    phases = gold.build_event_phases(obs)
    windows = spark.createDataFrame(
        [("a", "train", False), ("b", "test", False)], "recording_id string, split string, class_was_inferred boolean"
    )
    assert all(c["passed"] for c in gold.run_checks(obs.count(), recordings, phases, windows))

    lost_phase = phases.filter("NOT (recording_id = 'a' AND phase_seq = 2)")
    leaky = windows.union(spark.createDataFrame([("a", "test", True)], windows.schema))
    failed = {c["name"] for c in gold.run_checks(obs.count(), recordings, lost_phase, leaky) if not c["passed"]}
    assert failed == {"phases_cover_every_silver_row", "each_recording_in_one_split", "no_inferred_labels_in_test"}


def test_windows_use_clean_targets_and_exclude_inferred_test_labels(spark):
    rows = []
    for recording in ("a", "b"):
        for position in (0, 60, 120):
            row = {
                "_source_path": recording,
                "source_type": "REAL",
                "well_id": 1,
                "_row_in_file": position,
                "class_code_clean": 105,
                "class_was_inferred": position == 0,
                "class_label_status": "consistent",
            }
            for sensor in FEATURE_SENSORS:
                row[f"{sensor}_count_120s"] = 120
                for stat in ("mean_120s", "stddev_120s", "change"):
                    row[f"{sensor}_{stat}"] = 1.0
            rows.append(row)
    rows.append({**rows[0], "_source_path": "bad", "class_code_clean": 999, "class_label_status": "inconsistent"})
    source = spark.createDataFrame(rows)
    result = gold.build_window_features(source).collect()

    assert len(result) == 5
    assert {row.label for row in result} == {5}
    assert {row.class_code for row in result} == {105}
    assert sum(row.class_was_inferred for row in result if row.split == "train") == 1
    assert all(not row.class_was_inferred for row in result if row.split == "test")
    train = {row.recording_id for row in result if row.split == "train"}
    test = {row.recording_id for row in result if row.split == "test"}
    assert not train & test


def test_gold_requires_one_verified_current_publication(tmp_path):
    table = tmp_path / "runs/current/silver_observations"
    table.mkdir(parents=True)
    marker = {
        "run_id": "current",
        "contract_version": CONTRACT_VERSION,
        "reconcile": {"passed": True},
        "tables": {"silver_observations": {"path": str(table)}},
    }
    ready = tmp_path / "silver_ready.json"
    ready.write_text(json.dumps(marker))
    assert gold.resolve_silver_input(tmp_path) == (marker, table)
    marker["contract_version"] = 2
    ready.write_text(json.dumps(marker))
    with pytest.raises(ValueError, match="verified current"):
        gold.resolve_silver_input(tmp_path)
