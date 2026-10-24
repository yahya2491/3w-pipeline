import importlib.util
import json
from pathlib import Path

import pytest

from threew.silver.output import CONTRACT_VERSION
from threew.silver.rules import FEATURE_SENSORS

spec = importlib.util.spec_from_file_location("gold_builder", Path(__file__).parents[1] / "gold/build_gold_table.py")
gold = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gold)


def test_gold_consumes_clean_targets_and_excludes_assumed_test_labels(spark):
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
    assert "class_code" not in source.columns
    prepared = gold.prepare_gold_rows(source).cache()
    try:
        result = gold.split_gold_rows(prepared).collect()
    finally:
        prepared.unpersist()
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
    assert gold.resolve_silver_input(tmp_path) == table
    marker["contract_version"] = 2
    ready.write_text(json.dumps(marker))
    with pytest.raises(ValueError, match="verified current"):
        gold.resolve_silver_input(tmp_path)
