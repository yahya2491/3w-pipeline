"""Run report tests: valid JSON, exit codes, errors, phase timing and the code version."""

import json
import re
from datetime import UTC, datetime
from pathlib import Path

import pytest

from threew.bronze import report as report_module
from threew.bronze.report import SECTIONS, RunReport, RunStatus, code_info
from threew.config import REPO_ROOT


def new_report() -> RunReport:
    return RunReport("20261009T120000Z-abc123", datetime(2026, 10, 9, 12, tzinfo=UTC), "dev")


def test_write_is_valid_json_with_every_section(tmp_path):
    report = new_report()
    report["plan"] = {"new": 3, "paths": (Path("/a"), Path("/b"))}
    report.finish(RunStatus.SUCCESS)
    path = report.write(tmp_path / "runs")
    data = json.loads(path.read_text())
    assert path.name == "20261009T120000Z-abc123.json"
    assert list(data) == list(SECTIONS)
    assert data["plan"] == {"new": 3, "paths": ["/a", "/b"]}
    assert data["run"]["started_at"] == "2026-10-09T12:00:00+00:00"
    assert list((tmp_path / "runs").iterdir()) == [path]  # no temporary file left


@pytest.mark.parametrize(
    "status, code",
    [(RunStatus.SUCCESS, 0), (RunStatus.PARTIAL, 1), (RunStatus.FAILED, 2), (RunStatus.SKIPPED, 0)],
)
def test_exit_codes(status, code):
    report = new_report()
    assert report.finish(status) == code
    assert (report["run"]["status"], report["run"]["exit_code"]) == (status.value, code)


def test_error_is_recorded_with_its_traceback():
    report = new_report()
    try:
        raise ValueError("bad thing")
    except ValueError as e:
        report.finish(RunStatus.FAILED, e)
    error = report["run"]["error"]
    assert (error["type"], error["message"]) == ("ValueError", "bad thing")
    assert "raise ValueError" in error["traceback"]


def test_phase_is_timed_even_when_it_raises():
    report = new_report()
    with pytest.raises(RuntimeError), report.phase("fetch"):
        raise RuntimeError
    assert report["phases"]["fetch"] >= 0


def test_unknown_section_is_refused():
    with pytest.raises(KeyError):
        new_report()["plna"] = {}


@pytest.mark.skipif(not (REPO_ROOT / ".git").exists(), reason="not running from a git checkout")
def test_code_info_in_a_checkout():
    info = code_info()
    assert re.fullmatch(r"[0-9a-f]{40}", info["commit"])
    assert info["packages"]["pyspark"] == "3.5.0"


def test_code_info_without_git(monkeypatch):
    def no_git(*args, **kwargs):
        raise FileNotFoundError("git")

    monkeypatch.setattr(report_module.subprocess, "run", no_git)
    info = code_info()
    assert info["commit"] is None
    assert "FileNotFoundError" in info["git_error"]
    assert info["python"]


def test_code_info_outside_a_repo(tmp_path):
    info = code_info(tmp_path)
    assert info["commit"] is None
    assert "CalledProcessError" in info["git_error"]  # the real git ran and refused: not a repository
