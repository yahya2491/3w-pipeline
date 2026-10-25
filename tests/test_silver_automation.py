import json
from dataclasses import replace

from threew.config import load_config
from threew.silver.automation import TABLE_NAMES, build_signature, reusable_build, runtime_digest
from threew.silver.output import CONTRACT_VERSION
from threew.silver.rules import load_rules


def test_runtime_signature_ignores_gold_and_audit_files_but_tracks_pipeline_code(tmp_path):
    runtime = tmp_path / "src/threew"
    runtime.mkdir(parents=True)
    source = runtime / "engine.py"
    source.write_text("VERSION = 1")
    (tmp_path / "requirements.lock").write_text("pyspark==3.5.0")
    (tmp_path / "pyproject.toml").write_text("[project]")
    original = runtime_digest(tmp_path)
    (tmp_path / "gold").mkdir()
    (tmp_path / "gold/model.py").write_text("updated Gold")
    (tmp_path / "audit.json").write_text("{}")
    assert runtime_digest(tmp_path) == original
    source.write_text("VERSION = 2")
    assert runtime_digest(tmp_path) != original


def test_build_signature_tracks_content_rules_and_contract_not_bronze_run_id():
    cfg = load_config("dev")
    marker = {"source": {"repo_id": "test", "revision": "pinned"}, "files": 1, "rows": 2, "run_id": "one"}
    rows = [{"_source_path": "a", "_file_sha256": "v1", "instance_id": "a"}]
    counts = [{"_source_path": "a", "_file_sha256": "v1", "bronze_rows": 2}]
    rules = load_rules()

    def signature(marker=marker, rows=rows, rules=rules, contract="one"):
        return build_signature(cfg, marker, rows, counts, rules, 1000000, contract)

    original = signature()
    assert signature(marker={**marker, "run_id": "two"}) == original
    assert signature(rows=[{**rows[0], "_file_sha256": "v2"}]) != original
    assert signature(rules=replace(rules, choke_forward_fill_max_seconds=3)) != original
    assert signature(contract="two") != original


def test_only_matching_complete_verified_publications_can_be_reused(tmp_path):
    marker = {
        "run_id": "published",
        "build_signature": "same",
        "contract_version": CONTRACT_VERSION,
        "reconcile": {"passed": True},
        "tables": {},
    }
    for name in TABLE_NAMES:
        path = tmp_path / "runs/published" / name
        path.mkdir(parents=True)
        (path / "part.parquet").write_bytes(b"fixture")
        marker["tables"][name] = {"path": str(path), "parquet_bytes": 7}
    ready = tmp_path / "silver_ready.json"
    ready.write_text(json.dumps(marker))
    assert reusable_build(ready, "same")["run_id"] == "published"
    assert reusable_build(ready, "different") is None
    part = tmp_path / "runs/published/silver_observations/part.parquet"
    part.write_bytes(b"truncated")
    assert reusable_build(ready, "same") is None
    part.unlink()
    assert reusable_build(ready, "same") is None


def test_low_space_blocks_a_new_build_without_starting_spark_or_replacing_ready(tmp_path, monkeypatch):
    from types import SimpleNamespace

    import pytest

    from threew.config import StorageConfig
    from threew.silver.run import run_silver

    cfg = replace(load_config("dev"), storage=StorageConfig(root=tmp_path / "input"))
    output = tmp_path / "silver"
    output.mkdir()
    ready = output / "silver_ready.json"
    ready.write_text('{"run_id":"previous"}')
    marker = {"run_id": "bronze", "source": {"repo_id": "test", "revision": "pinned"}, "rows": 2, "files": 1}
    monkeypatch.setattr("threew.silver.run.read_bronze_ready_marker", lambda _cfg: marker)
    monkeypatch.setattr(
        "threew.silver.run.current_manifest_rows", lambda _cfg: [{"_source_path": "a", "_file_sha256": "one"}]
    )
    monkeypatch.setattr(
        "threew.silver.run.current_manifest_counts",
        lambda _cfg: [{"_source_path": "a", "_file_sha256": "one", "bronze_rows": 2}],
    )
    monkeypatch.setattr("threew.silver.run.shutil.disk_usage", lambda _path: SimpleNamespace(free=0))

    def no_spark(_cfg):
        raise AssertionError("Low-space failure must happen before Spark starts")

    with pytest.raises(OSError, match="requires 20 GiB"):
        run_silver(cfg, output_root=output, min_free_gib=20, spark_factory=no_spark)
    assert json.loads(ready.read_text())["run_id"] == "previous"
    report = json.loads(next((output / "_runs").glob("*.json")).read_text())
    assert report["status"] == "FAILED" and report["published"] is False
