"""Full Bronze runs with the fake hub and a small Spark session."""

import dataclasses
import json
import subprocess
import sys
from contextlib import contextmanager

import pyarrow.parquet as pq
import pytest
from fakes import FakeHub, http_error

from threew.bronze import run as run_module
from threew.bronze.ingest import LOCK_FILE
from threew.bronze.report import SECTIONS
from threew.bronze.run import run_bronze
from threew.bronze.summary import summarize
from threew.config import REPO_ROOT, StorageConfig, load_config
from threew.lock import run_lock

REAL = "data/2/WELL-00002_20131104004101.parquet"
SIMULATED = "data/5/SIMULATED_00001.parquet"
DRAWN = "data/1/DRAWN_00001.parquet"


@pytest.fixture
def files(parquet_bytes) -> dict[str, bytes]:
    return {"README.md": b"card", REAL: parquet_bytes(5), SIMULATED: parquet_bytes(4), DRAWN: parquet_bytes(3)}


@pytest.fixture
def cfg(tmp_path, spark_cfg):
    base = load_config("dev")
    return dataclasses.replace(
        base,
        storage=StorageConfig(root=tmp_path),
        ingest=dataclasses.replace(base.ingest, sample_per_class=None, workers=2),
        spark=spark_cfg.spark,
    )


@pytest.fixture
def spark_factory(spark):
    """Hands out the test session, and records each time a run would have stopped it."""
    stops = []

    @contextmanager
    def factory(cfg):
        try:
            yield spark
        finally:
            stops.append(cfg.env)

    factory.stops = stops
    return factory


def run(cfg, hub, spark_factory, **kwargs) -> tuple[int, dict]:
    before = set(cfg.storage.runs.glob("*.json"))
    code = run_bronze(cfg, hub, sleep=lambda _: None, spark_factory=spark_factory, **kwargs)
    [new] = set(cfg.storage.runs.glob("*.json")) - before
    report = json.loads(new.read_text())
    assert set(report) == set(SECTIONS)
    assert report["run"]["exit_code"] == code
    return code, report


def outputs(cfg) -> list:
    return sorted(cfg.storage.bronze.rglob("*.parquet"))


def marker(cfg) -> dict | None:
    return json.loads(cfg.storage.bronze_ready.read_text()) if cfg.storage.bronze_ready.exists() else None


def reconcile_passed(report) -> bool:
    return all(c["passed"] for c in report["reconcile"]["checks"])


def test_first_run_then_rerun(cfg, files, spark_factory):
    hub = FakeHub(files)
    code, first = run(cfg, hub, spark_factory)
    assert (code, first["run"]["status"]) == (0, "SUCCESS")
    assert (first["convert"]["pending"], first["convert"]["converted"], first["convert"]["rows"]) == (3, 3, 12)
    assert reconcile_passed(first) and first["reconcile"]["mode"] == "fast"
    assert first["publish"]["written"] is True
    assert marker(cfg)["run_id"] == first["run"]["run_id"]
    assert (marker(cfg)["files"], marker(cfg)["rows"]) == (3, 12)
    assert first["totals"]["bronze_rows"] == first["totals"]["rows_total"] == 12
    assert len(outputs(cfg)) == 3
    assert spark_factory.stops == ["dev"]
    summary = summarize(first)
    assert summary.startswith(f"## ✅ SUCCESS · run `{first['run']['run_id']}` (dev)")
    assert "| reconcile | 6/6 checks passed · fast ·" in summary
    assert "| convert | 3 converted, 0 failed |" in summary
    assert "| publish | marker written: yes |" in summary
    assert "**Totals:** 3 files · 12 rows" in summary
    assert "No problems." in summary

    code, second = run(cfg, hub, spark_factory)
    assert (code, second["run"]["status"]) == (0, "SUCCESS")
    assert (second["fetch"]["files"], second["convert"]["converted"]) == (0, 0)
    assert reconcile_passed(second)
    assert marker(cfg)["run_id"] == second["run"]["run_id"] != first["run"]["run_id"]  # updated


def test_two_new_files_upstream(cfg, files, parquet_bytes, spark_factory):
    hub = FakeHub(files)
    run(cfg, hub, spark_factory)
    hub.files |= {
        "data/5/SIMULATED_00002.parquet": parquet_bytes(6),
        "data/9/SIMULATED_00001.parquet": parquet_bytes(2),
    }
    code, report = run(cfg, hub, spark_factory)
    assert code == 0
    assert (report["plan"]["new"], report["fetch"]["ok"], report["convert"]["converted"]) == (2, 2, 2)
    assert len(outputs(cfg)) == 5
    assert reconcile_passed(report)


def test_changed_file_replaces_its_old_output(cfg, files, parquet_bytes, spark_factory):
    hub = FakeHub(files)
    run(cfg, hub, spark_factory)
    old = {p.name for p in outputs(cfg)}
    hub.files[SIMULATED] = parquet_bytes(7)  # corrected upstream: new content, new checksum
    code, report = run(cfg, hub, spark_factory)
    assert (code, report["plan"]["changed"], report["convert"]["converted"]) == (0, 1, 1)
    [removed] = report["prune"]["removed"]
    assert removed.startswith("source_type=SIMULATED/label=5/")
    assert len(outputs(cfg)) == 3
    assert len({p.name for p in outputs(cfg)} - old) == 1  # exactly one new output name
    assert reconcile_passed(report)
    assert marker(cfg)["rows"] == 5 + 7 + 3


def test_output_deleted_by_hand_is_converted_again(cfg, files, spark_factory):
    hub = FakeHub(files)
    run(cfg, hub, spark_factory)
    deleted = outputs(cfg)[0]
    deleted.unlink()
    code, report = run(cfg, hub, spark_factory)
    assert (code, report["run"]["status"]) == (0, "SUCCESS")
    assert (report["fetch"]["files"], report["convert"]["converted"]) == (0, 1)  # self-healing
    assert deleted.exists()
    assert reconcile_passed(report)


def test_tampered_output_fails_and_withdraws_the_marker(cfg, files, spark_factory):
    hub = FakeHub(files)
    run(cfg, hub, spark_factory)
    assert marker(cfg) is not None
    tampered = next(p for p in outputs(cfg) if "SIMULATED" in p.as_posix())
    table = pq.read_table(tampered)
    pq.write_table(table.slice(1), tampered)  # first row removed
    code, report = run(cfg, hub, spark_factory)
    assert (code, report["run"]["status"]) == (2, "FAILED")
    assert not reconcile_passed(report)
    assert 3 in [c["number"] for c in report["reconcile"]["checks"] if not c["passed"]]
    assert report["publish"]["written"] is False
    assert "reconciliation failed" in report["publish"]["reason"]
    assert marker(cfg) is None
    summary = summarize(report)
    assert summary.startswith("## ❌ FAILED")
    assert "**Failed reconcile checks**" in summary
    assert "check 3 per file: " in summary
    assert "marker written: no (reconciliation failed" in summary


def test_conversion_error_is_partial_without_a_marker(cfg, files, parquet_bytes, spark_factory):
    hub = FakeHub(files)
    run(cfg, hub, spark_factory)
    bad = "data/9/SIMULATED_00002.parquet"
    hub.files[bad] = parquet_bytes(3, timestamp_step_ns=1)  # matches the contract, but sub-microsecond timestamps
    code, report = run(cfg, hub, spark_factory)
    assert (code, report["run"]["status"]) == (1, "PARTIAL")
    assert [f["path"] for f in report["convert"]["failures"]] == [bad]
    assert reconcile_passed(report)  # the rest of the table is checked, without the failed file
    assert report["reconcile"]["excluded"] == 1
    assert report["publish"]["written"] is False
    assert marker(cfg) is None
    summary = summarize(report)
    assert summary.startswith("## ⚠️ PARTIAL")
    assert f"**Conversion failures** (1):\n  - `{bad}`: timestamp can't be stored in microseconds" in summary


def test_download_failure_is_partial_and_listed(cfg, files, spark_factory):
    code, report = run(cfg, FakeHub(files, errors={DRAWN: http_error(404)}), spark_factory)
    assert (code, report["run"]["status"]) == (1, "PARTIAL")
    assert report["convert"]["converted"] == 2
    assert reconcile_passed(report)  # the two files that landed
    summary = summarize(report)
    assert summary.startswith("## ⚠️ PARTIAL")
    assert f"**Failed files** (1):\n  - `{DRAWN}`: download failed: HfHubHTTPError: 404" in summary


def test_exception_still_writes_the_report_releases_the_lock_and_stops_spark(
    cfg, files, spark_factory, monkeypatch, tmp_path
):
    def explode(*args, **kwargs):
        raise RuntimeError("reconcile exploded")

    monkeypatch.setattr(run_module, "reconcile", explode)
    out = tmp_path / "out"
    code, report = run(cfg, FakeHub(files), spark_factory, report_copy=out)
    assert (code, report["run"]["status"]) == (2, "FAILED")
    assert report["run"]["error"]["message"] == "reconcile exploded"
    assert spark_factory.stops == ["dev"]  # stopped even though reconcile raised
    with run_lock(cfg.storage.state / LOCK_FILE):  # released
        pass
    assert marker(cfg) is None
    run_id = report["run"]["run_id"]
    assert sorted(p.name for p in out.iterdir()) == [f"{run_id}.json", f"{run_id}.log"]  # copied despite the error
    assert "reconcile exploded" in (out / f"{run_id}.log").read_text()
    summary = summarize(report)
    assert summary.startswith("## ❌ FAILED")
    assert "- **Run error:** `RuntimeError: reconcile exploded`" in summary


def test_lock_held_skips_and_leaves_the_marker(cfg, files, spark_factory):
    hub = FakeHub(files)
    run(cfg, hub, spark_factory)
    with run_lock(cfg.storage.state / LOCK_FILE, {"run_id": "an-ingest-run"}):
        code, report = run(cfg, hub, spark_factory)
    assert (code, report["run"]["status"]) == (0, "SKIPPED")
    assert marker(cfg) is not None  # a skipped run touches nothing
    summary = summarize(report)
    assert summary.startswith("## ⏭️ SKIPPED")
    assert "an-ingest-run" in summary  # names the run that held the lock
    assert "| ingest | — |" in summary


def test_report_copy_has_the_report_log_and_plan(cfg, files, spark_factory, tmp_path):
    out = tmp_path / "out"
    _, report = run(cfg, FakeHub(files), spark_factory, report_copy=out)
    run_id = report["run"]["run_id"]
    assert sorted(p.name for p in out.iterdir()) == [
        f"{run_id}.json",
        f"{run_id}.log",
        f"{run_id}_reconcile_plan.txt",
    ]
    assert json.loads((out / f"{run_id}.json").read_text()) == report
    log = (out / f"{run_id}.log").read_text()
    assert f"run {run_id}: SUCCESS (exit 0)" in log
    assert "BroadcastHashJoin" in (out / f"{run_id}_reconcile_plan.txt").read_text()


def test_report_summary_script(cfg, files, spark_factory, tmp_path):
    out = tmp_path / "out"
    _, report = run(cfg, FakeHub(files), spark_factory, report_copy=out)
    script = REPO_ROOT / "scripts" / "report_summary.py"
    printed = subprocess.run(
        [sys.executable, script, *map(str, out.glob("*.json"))], capture_output=True, text=True, check=True
    ).stdout
    assert printed == summarize(report) + "\n"
    missing = str(out / "missing*.json")
    nothing = subprocess.run([sys.executable, script, missing], capture_output=True, text=True, check=False)
    assert nothing.returncode == 0
    assert "No run report was produced" in nothing.stdout
