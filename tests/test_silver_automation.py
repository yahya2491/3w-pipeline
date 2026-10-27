import json
from dataclasses import replace

import pytest

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


def test_runtime_signature_ignores_comments_formatting_and_docstrings(tmp_path):
    runtime = tmp_path / "src/threew"
    runtime.mkdir(parents=True)
    source = runtime / "engine.py"
    source.write_text(
        '"""Old module docs."""\n'
        'VALUE = "# important value"\n'
        'class Engine:\n    """Old class docs."""\n'
        '    def run(self):\n        """Old method docs."""\n        return VALUE\n'
        'async def fetch():\n    """Old async docs."""\n    return 1\n'
    )
    original = runtime_digest(tmp_path)
    source.write_text(
        '# New module comment\nVALUE="# important value"  # inline comment\n\n'
        "class Engine:\n    # Class explanation\n"
        '    def run(self):\n        """New method docs."""\n        return VALUE\n'
        "async def fetch():\n    return 1\n"
    )
    assert runtime_digest(tmp_path) == original
    source.write_text(source.read_text().replace("return 1", "return 2"))
    assert runtime_digest(tmp_path) != original


@pytest.mark.parametrize("definition", ["class Engine:", "def run():", "async def fetch():"])
def test_documentation_only_bodies_match_pass(tmp_path, definition):
    runtime = tmp_path / "src/threew"
    runtime.mkdir(parents=True)
    source = runtime / "engine.py"
    source.write_text(f'{definition}\n    """Documentation."""\n')
    original = runtime_digest(tmp_path)
    source.write_text(f"{definition}\n    pass\n")
    assert runtime_digest(tmp_path) == original


def test_runtime_signature_preserves_executable_strings(tmp_path):
    runtime = tmp_path / "src/threew"
    runtime.mkdir(parents=True)
    source = runtime / "engine.py"
    source.write_text('QUERY = "select 1 # literal"\n')
    original = runtime_digest(tmp_path)
    source.write_text('QUERY = "select 2 # literal"\n')
    assert runtime_digest(tmp_path) != original


def test_runtime_signature_ignores_lock_comments_but_preserves_pins_and_url_hashes(tmp_path):
    lock = tmp_path / "requirements.lock"
    dependency = "engine @ https://example.com/engine.whl#sha256=abc"
    lock.write_text(f"pyspark==3.5.0\n{dependency}\n")
    original = runtime_digest(tmp_path)
    lock.write_text(f"# Generated lock\npyspark==3.5.0  # Spark\n    # via threew\n\n{dependency}\n")
    assert runtime_digest(tmp_path) == original
    lock.write_text(f"pyspark==3.5.1\n{dependency}\n")
    assert runtime_digest(tmp_path) != original
    lock.write_text(f"pyspark==3.5.0\n{dependency.replace('abc', 'def')}\n")
    assert runtime_digest(tmp_path) != original


def test_runtime_signature_ignores_toml_comments_but_preserves_settings(tmp_path):
    project = tmp_path / "pyproject.toml"
    project.write_text('[project]\ndependencies = ["pyspark==3.5.0"]\n')
    original = runtime_digest(tmp_path)
    project.write_text('# Package docs\n[project] # metadata\ndependencies=[ "pyspark==3.5.0" ]\n')
    assert runtime_digest(tmp_path) == original
    project.write_text('[project]\ndependencies = ["pyspark==3.5.1"]\n')
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
