import json
from contextlib import nullcontext
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql import DataFrameWriter
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

from threew.bronze.manifest import Manifest
from threew.config import StorageConfig
from threew.lock import LockHeld, run_lock
from threew.silver.input import read_bronze_ready_marker
from threew.silver.quality import QualitySummaries, build_quality_summaries
from threew.silver.reconcile import same_rows
from threew.silver.run import run_silver
from threew.silver.transform import BRONZE_TYPES, SENSOR_RENAMES, build_silver_observations


@pytest.fixture
def silver_input(spark, spark_cfg, tmp_path):
    cfg = replace(spark_cfg, storage=StorageConfig(root=tmp_path / "input"))
    start = datetime.fromisoformat("2020-01-01T00:00:00.500000")
    rows, manifest = [], []
    for index, kind in enumerate(("REAL", "SIMULATED", "DRAWN")):
        path, file_hash = f"data/1/{kind}-00001.parquet", str(index + 1) * 64
        for position in range(2):
            rows.append(
                {
                    "source_type": kind,
                    "label": 1,
                    "class": 1 if position else None,
                    "state": 1 if position else None,
                    "timestamp": start + timedelta(seconds=position),
                    "_source_path": path,
                    "_file_sha256": file_hash,
                    "_ingest_run_id": "bronze-test",
                    "_row_in_file": position,
                    **{
                        sensor: float(position * 100 + sensor_index)
                        for sensor_index, sensor in enumerate(SENSOR_RENAMES)
                    },
                }
            )
        manifest.append(
            {
                "run_id": "bronze-test",
                "path": path,
                "revision": cfg.source.revision,
                "label": 1,
                "source_type": kind,
                "well_id": 1 if kind == "REAL" else None,
                "instance_start": start if kind == "REAL" else None,
                "instance_id": f"{kind}-00001",
                "size": 100,
                "sha256": file_hash,
                "blob_id": "blob-test",
                "landing_path": "unused",
                "status": "LANDED",
                "error": None,
                "num_rows": 2,
                "schema_fingerprint": "test-schema",
                "schema_ok": True,
                "codec": "BROTLI",
                "recorded_at": datetime.now(UTC),
            }
        )
    rows[1]["QGL"] = None
    schema = StructType([StructField(name, data_type, True) for name, data_type in BRONZE_TYPES.items()])
    spark.createDataFrame(rows, schema).write.parquet(str(cfg.storage.bronze))
    Manifest(cfg.storage.state).append(manifest)
    marker = {
        "env": "dev",
        "bronze_table": str(cfg.storage.bronze),
        "run_id": "bronze-test",
        "source": {"repo_id": cfg.source.repo_id, "revision": cfg.source.revision},
        "files": 3,
        "rows": 6,
        "reconcile": {"passed": True},
    }
    cfg.storage.bronze_ready.write_text(json.dumps(marker))
    return cfg


def _run(cfg, spark, **kwargs):
    return run_silver(cfg, spark_factory=lambda _cfg: nullcontext(spark), **kwargs)


def _reports(root):
    return [json.loads(path.read_text()) for path in (root / "_runs").glob("*.json")]


def _prior_marker(cfg):
    cfg.storage.silver_ready.parent.mkdir(parents=True, exist_ok=True)
    previous = '{"run_id": "previous-success"}\n'
    cfg.storage.silver_ready.write_text(previous)
    return previous


def test_run_silver_reconciles_all_outputs_and_repeat_run_is_identical(spark, silver_input):
    cfg = silver_input
    first = _run(cfg, spark)
    second = _run(cfg, spark, batch_rows=2)
    assert first["run_id"] != second["run_id"]
    assert all(second["reconcile"]["checks"].values())
    assert second["reconcile"]["counts"] == {
        "bronze_rows": 6,
        "silver_observation_rows": 6,
        "bronze_recordings": 3,
        "silver_recordings": 3,
        "instance_quality_rows": 3,
        "sensor_availability_rows": 81,
        "quality_issues_rows": 60,
        "quality_quarantine_rows": 57,
        "event_reference_rows": 10,
        "sensor_reference_rows": 27,
    }
    summary = second["quality_summary"]
    assert second["quality_summary"] == first["quality_summary"]
    assert second["preparation_summary"] == first["preparation_summary"]
    with open(second["report"]) as saved_report:
        report = json.load(saved_report)
    assert report["execution"]["batch_count"] == 3
    assert [batch["status"] for batch in report["batches"]] == ["SUCCESS"] * 3
    assert second["contract_version"] == 4
    assert second["quality_rules"]["resolved"]["expected_interval_seconds"] == 1
    assert second["quality_rules"]["sha256"]
    assert second["preparation_summary"]["rows"] == 6
    assert second["preparation_summary"]["class_code_inferred_rows"] == 3
    assert second["preparation_summary"]["class_code_unresolved_rows"] == 0
    assert summary["rows_with_issues"] == 6
    assert summary["rows_with_quarantine_issues"] == 6
    assert summary["quarantine_row_percent"] == 100.0
    assert summary["issue_entries"] == 60
    assert summary["recording_totals"]["unlabeled_rows"] == 3
    assert len(summary["sensor_availability"]) == 27
    assert second["usage_policy"]["duplicate_identity"].startswith("Stop")
    for name in first["tables"]:
        left = spark.read.parquet(first["tables"][name]["path"])
        right = spark.read.parquet(second["tables"][name]["path"])
        assert same_rows(left, right)
    saved = spark.read.parquet(second["tables"]["silver_observations"]["path"])
    assert len(saved.columns) == 79
    assert saved.filter("class_was_inferred").count() == 3
    assert saved.filter("NOT class_was_inferred").count() == 3
    assert "class_code" not in saved.columns and "_ingest_run_id" not in saved.columns
    assert saved.filter("source_type = 'REAL' AND _row_in_file = 0").first()["timestamp"].microsecond == 500000
    assert saved.filter("source_type = 'REAL' AND _row_in_file = 0").first()["gas_lift_choke_opening_pct_clean"] == 0.0

    def no_spark(_cfg):
        raise AssertionError("Unchanged reuse must not start Spark")

    previous = cfg.storage.silver_ready.read_bytes()
    copied = cfg.storage.root / "ci-reports"
    reused = run_silver(cfg, spark_factory=no_spark, batch_rows=2, skip_unchanged=True, report_copy=copied)
    assert reused["run_id"] == second["run_id"]
    assert cfg.storage.silver_ready.read_bytes() == previous
    copied_reports = [json.loads(path.read_text()) for path in copied.glob("*.json")]
    assert len(copied_reports) == 1 and copied_reports[0]["status"] == "SKIPPED"
    assert copied_reports[0]["skip_reason"] == "unchanged"
    for report in _reports(cfg.storage.silver):
        if report["status"] == "SKIPPED":
            assert report["reused_run_id"] == second["run_id"]
            continue
        assert report["status"] == "SUCCESS"
        assert report["contract"]["version"] == 4
        assert report["contract"]["sha256"]
        assert report["config_sha256"]
        assert report["duration_seconds"] > 0
        assert report["phase_seconds"]["read_back_and_check"] > 0


@pytest.mark.parametrize(
    "corruption,failed_check",
    [
        ("duplicate_identity", "silver_observation_keys_are_unique_and_non_null"),
        ("changed_value", "silver_prepared_values_match_rules"),
        ("null_lineage", "silver_required_fields_are_non_null"),
        ("wrong_metadata", "silver_recording_and_event_details_match"),
        ("wrong_schema", "silver_schema_matches_contract"),
        ("null_key", "silver_observation_keys_are_unique_and_non_null"),
    ],
)
def test_corrupted_observations_cannot_be_published(spark, silver_input, monkeypatch, corruption, failed_check):
    cfg = silver_input
    previous = _prior_marker(cfg)

    def broken(bronze, manifest):
        df = build_silver_observations(bronze, manifest)
        if corruption == "duplicate_identity":
            return df.withColumn("_row_in_file", F.lit(0).cast("long"))
        if corruption == "changed_value":
            return df.withColumn("gas_lift_flow_rate_m3_s", F.lit(456.0))
        if corruption == "null_lineage":
            return df.withColumn("source_type", F.lit(None).cast("string"))
        if corruption == "wrong_schema":
            return df.withColumn("state_code", F.col("state_code").cast("string"))
        if corruption == "null_key":
            return df.withColumn("_row_in_file", F.lit(None).cast("long"))
        return df.withColumn("instance_id", F.lit("wrong-recording"))

    monkeypatch.setattr("threew.silver.run.build_silver_observations", broken)
    with pytest.raises(RuntimeError, match="reconciliation failed"):
        _run(cfg, spark)
    assert cfg.storage.silver_ready.read_text() == previous
    report = _reports(cfg.storage.silver)[0]
    assert report["status"] == "FAILED"
    assert report["reconcile"]["checks"][failed_check] is False
    assert report["error"]["phase"] == "read_back_and_check"


@pytest.mark.parametrize(
    "column", ["downhole_pdg_pressure_pa_clean", "downhole_pdg_pressure_pa_mean_120s", "class_code_clean"]
)
def test_corrupted_clean_reading_or_feature_prevents_publication(spark, silver_input, monkeypatch, column):
    from threew.silver.prepare import prepare_observations

    previous = _prior_marker(silver_input)

    def broken(observations, rules):
        prepared = prepare_observations(observations, rules)
        return prepared.withColumn(column, F.lit(999.0).cast(prepared.schema[column].dataType))

    monkeypatch.setattr("threew.silver.run.prepare_observations", broken)
    with pytest.raises(RuntimeError, match="silver_prepared_values_match_rules"):
        _run(silver_input, spark)
    assert silver_input.storage.silver_ready.read_text() == previous


@pytest.mark.parametrize(
    "corruption", ["wrong_recording", "wrong_value", "wrong_schema", "wrong_quality_recording", "wrong_quality_value"]
)
def test_corrupted_sensor_summary_cannot_be_published(spark, silver_input, monkeypatch, corruption):
    cfg = silver_input
    previous = _prior_marker(cfg)

    def broken(df):
        s = build_quality_summaries(df)
        if corruption == "wrong_quality_recording":
            return QualitySummaries(
                s.instance_quality.withColumn("_source_path", F.lit("wrong.parquet")),
                s.sensor_availability,
                s.recording_metrics,
            )
        if corruption == "wrong_quality_value":
            return QualitySummaries(
                s.instance_quality.withColumn("unlabeled_rows", F.lit(999).cast("long")),
                s.sensor_availability,
                s.recording_metrics,
            )
        if corruption == "wrong_recording":
            changed = s.sensor_availability.withColumn("_source_path", F.lit("wrong.parquet"))
        elif corruption == "wrong_value":
            changed = s.sensor_availability.withColumn("rows_with_finite_values", F.lit(0).cast("long"))
        else:
            changed = s.sensor_availability.withColumn(
                "rows_with_finite_values", F.col("rows_with_finite_values").cast("string")
            )
        return QualitySummaries(s.instance_quality, changed, s.recording_metrics)

    monkeypatch.setattr("threew.silver.run.build_quality_summaries", broken)
    with pytest.raises(RuntimeError, match="reconciliation failed"):
        _run(cfg, spark)
    assert cfg.storage.silver_ready.read_text() == previous
    assert _reports(cfg.storage.silver)[0]["status"] == "FAILED"


def test_missing_bronze_recording_is_rejected_before_outputs_are_written(spark, silver_input):
    cfg = silver_input
    original = spark.read.parquet(str(cfg.storage.bronze))
    subset = original.filter("source_type != 'DRAWN'").collect()
    spark.createDataFrame(subset, original.schema).write.mode("overwrite").parquet(str(cfg.storage.bronze))
    previous = _prior_marker(cfg)
    with pytest.raises(RuntimeError, match="bronze_recording_counts_match_manifest"):
        _run(cfg, spark)
    assert cfg.storage.silver_ready.read_text() == previous
    assert not (cfg.storage.silver / "runs").exists()
    assert _reports(cfg.storage.silver)[0]["error"]["phase"] == "input_checks"


def test_source_identity_comes_entirely_from_bronze_marker(spark, silver_input):
    cfg = replace(silver_input, source=replace(silver_input.source, repo_id="different/repository"))
    marker = _run(cfg, spark)
    assert marker["source"]["repo_id"] == "Yahya2491/3w-pipeline"
    assert marker["source"]["revision"] == read_bronze_ready_marker(cfg)["source"]["revision"]


@pytest.mark.parametrize("which", ["bronze", "silver"])
def test_overlapping_run_is_reported_and_previous_marker_preserved(spark, silver_input, which):
    cfg = silver_input
    previous = _prior_marker(cfg)
    path = cfg.storage.state / "ingest.lock" if which == "bronze" else cfg.storage.silver / "_state" / "silver.lock"
    with run_lock(path, {"run_id": "other-run"}), pytest.raises(LockHeld):
        _run(cfg, spark)
    assert cfg.storage.silver_ready.read_text() == previous
    assert _reports(cfg.storage.silver)[0]["status"] == "SKIPPED"


def test_input_lock_stays_held_during_transformation(spark, silver_input, monkeypatch):
    cfg = silver_input

    def inspect_lock(bronze, manifest):
        with pytest.raises(LockHeld), run_lock(cfg.storage.state / "ingest.lock"):
            pass
        return build_silver_observations(bronze, manifest)

    monkeypatch.setattr("threew.silver.run.build_silver_observations", inspect_lock)
    _run(cfg, spark)


def test_changed_ready_marker_prevents_publication(spark, silver_input, monkeypatch):
    cfg = silver_input
    previous = _prior_marker(cfg)

    def change_marker(bronze, manifest):
        marker = json.loads(cfg.storage.bronze_ready.read_text())
        marker["run_id"] = "other-bronze-run"
        cfg.storage.bronze_ready.write_text(json.dumps(marker))
        return build_silver_observations(bronze, manifest)

    monkeypatch.setattr("threew.silver.run.build_silver_observations", change_marker)
    with pytest.raises(RuntimeError, match="marker changed"):
        _run(cfg, spark)
    assert cfg.storage.silver_ready.read_text() == previous
    assert _reports(cfg.storage.silver)[0]["error"]["phase"] == "publish"


def test_write_failure_is_reported_and_leaves_previous_marker(spark, silver_input, monkeypatch):
    cfg = silver_input
    previous = _prior_marker(cfg)

    def fail(*args, **kwargs):
        raise OSError("injected write failure")

    monkeypatch.setattr(DataFrameWriter, "parquet", fail)
    with pytest.raises(OSError, match="injected write failure"):
        _run(cfg, spark)
    report = _reports(cfg.storage.silver)[0]
    assert report["status"] == "FAILED"
    assert report["error"]["phase"] == "transform_and_write"
    assert report["published"] is False
    assert cfg.storage.silver_ready.read_text() == previous


def test_later_batch_failure_keeps_previous_marker(spark, silver_input, monkeypatch):
    previous = _prior_marker(silver_input)
    write = DataFrameWriter.parquet
    observation_writes = 0

    def fail_second_batch(writer, path, *args, **kwargs):
        nonlocal observation_writes
        if path.endswith("/silver_observations"):
            observation_writes += 1
            if observation_writes == 2:
                raise OSError("injected later batch failure")
        return write(writer, path, *args, **kwargs)

    monkeypatch.setattr(DataFrameWriter, "parquet", fail_second_batch)
    with pytest.raises(OSError, match="later batch failure"):
        _run(silver_input, spark, batch_rows=2)
    assert silver_input.storage.silver_ready.read_text() == previous
    report = _reports(silver_input.storage.silver)[0]
    assert report["published"] is False
    assert report["status"] == "FAILED"
    assert [batch["status"] for batch in report["batches"]] == ["SUCCESS", "FAILED"]


def test_production_input_requires_isolated_destination(spark_cfg, tmp_path):
    cfg = replace(spark_cfg, env="prod", storage=StorageConfig(root=tmp_path / "input"))
    with pytest.raises(ValueError, match="explicit isolated"):
        run_silver(cfg)
    for path in (cfg.storage.silver, cfg.storage.silver / "nested", cfg.storage.bronze, cfg.storage.root, tmp_path):
        with pytest.raises(ValueError):
            run_silver(cfg, output_root=path)
    assert not cfg.storage.root.exists()


def test_production_input_can_write_an_isolated_test_run(spark, silver_input, tmp_path):
    cfg = replace(silver_input, env="prod")
    marker = json.loads(cfg.storage.bronze_ready.read_text())
    marker["env"] = "prod"
    cfg.storage.bronze_ready.write_text(json.dumps(marker))
    dest = tmp_path / "silver-test"
    result = _run(cfg, spark, output_root=dest)
    assert result["env"] == "prod"
    assert (dest / "silver_ready.json").is_file()
    assert not cfg.storage.silver.exists()


@pytest.mark.parametrize("field,value", [("rows", None), ("files", True), ("run_id", ""), ("source", {})])
def test_incomplete_ready_marker_is_rejected(silver_input, field, value):
    cfg = silver_input
    marker = json.loads(cfg.storage.bronze_ready.read_text())
    marker[field] = value
    cfg.storage.bronze_ready.write_text(json.dumps(marker))
    with pytest.raises(ValueError):
        read_bronze_ready_marker(cfg)


def test_ready_marker_write_failure_is_reported_and_keeps_previous_marker(spark, silver_input, monkeypatch):
    from threew.silver.run import _write_json_atomically

    cfg = silver_input
    previous = _prior_marker(cfg)

    def fail_marker(path, payload):
        if path == cfg.storage.silver_ready:
            raise OSError("injected publication failure")
        return _write_json_atomically(path, payload)

    monkeypatch.setattr("threew.silver.run._write_json_atomically", fail_marker)
    with pytest.raises(OSError, match="injected publication failure"):
        _run(cfg, spark)
    report = _reports(cfg.storage.silver)[0]
    assert report["status"] == "FAILED"
    assert report["error"]["phase"] == "publish"
    assert report["reconcile"]["passed"] is False
    assert report["published"] is False
    assert cfg.storage.silver_ready.read_text() == previous


@pytest.mark.parametrize(
    "table", ["event_reference", "sensor_reference", "quality_issues", "quality_quarantine", "quality_issues_schema"]
)
def test_milestone8_corrupted_additional_output_cannot_be_published(spark, silver_input, monkeypatch, table):
    from threew.silver.issues import build_quality_issues, quarantine_issues
    from threew.silver.reference import build_reference_tables

    cfg = silver_input
    previous = _prior_marker(cfg)
    if table in ("event_reference", "sensor_reference"):

        def broken_references(session, rules=None):
            tables = build_reference_tables(session, rules)
            field = "event_name" if table == "event_reference" else "unit"
            tables[table] = tables[table].withColumn(field, F.lit("wrong"))
            return tables

        monkeypatch.setattr("threew.silver.run.build_reference_tables", broken_references)
    elif table.startswith("quality_issues"):

        def broken_issues(observations, rules=None):
            issues = build_quality_issues(observations, rules)
            if table == "quality_issues_schema":
                return issues.drop("field_name")
            return issues.withColumn("reason", F.lit("wrong"))

        monkeypatch.setattr("threew.silver.run.build_quality_issues", broken_issues)
    else:

        def broken_quarantine(issues):
            return quarantine_issues(issues).filter("false")

        monkeypatch.setattr("threew.silver.run.quarantine_issues", broken_quarantine)
    failed_check = "quality_issues_schema_matches" if table == "quality_issues_schema" else f"{table}_values_match"
    with pytest.raises(RuntimeError, match=failed_check):
        _run(cfg, spark)
    assert cfg.storage.silver_ready.read_text() == previous


@pytest.mark.parametrize("conflicting", [False, True])
def test_milestone8_duplicate_source_identity_stops_before_writing(spark, silver_input, conflicting):
    cfg = silver_input
    original = spark.read.parquet(str(cfg.storage.bronze))
    rows = original.collect()
    duplicate = rows[0]
    copy = duplicate.asDict()
    if conflicting:
        copy["P-PDG"] = -123.0
    duplicate_rows = rows + [copy]
    spark.createDataFrame(duplicate_rows, original.schema).write.mode("overwrite").parquet(str(cfg.storage.bronze))
    marker = json.loads(cfg.storage.bronze_ready.read_text())
    marker["rows"] += 1
    cfg.storage.bronze_ready.write_text(json.dumps(marker))
    # Match the manifest count so the identity guard, rather than a count guard, is tested.
    records = Manifest(cfg.storage.state).current().to_pylist()
    changed = next(row for row in records if row["path"] == duplicate["_source_path"])
    changed["num_rows"] += 1
    changed["recorded_at"] = datetime.now(UTC)
    Manifest(cfg.storage.state).append([changed])
    with pytest.raises(RuntimeError, match="bronze_observation_keys_are_unique_and_non_null"):
        _run(cfg, spark)
    assert not (cfg.storage.silver / "runs").exists()
