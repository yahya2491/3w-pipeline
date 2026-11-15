"""The run report: one JSON file per run in <storage.runs>/, written even when the run fails or is skipped."""

import json
import logging
import os
import platform
import socket
import subprocess
import time
import traceback
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from enum import StrEnum
from importlib import metadata
from pathlib import Path
from typing import Any

from threew.config import REPO_ROOT

log = logging.getLogger(__name__)

# Every report has all of these; a section the run never reached is null
SECTIONS = (
    "run",
    "code",
    "config",
    "source",
    "plan",
    "fetch",
    "landing",
    "schema",
    "failures",
    "orphans",
    "stale_staging",
    "convert",  # the rest only in full Bronze runs (threew.bronze.run)
    "prune",
    "reconcile",
    "publish",
    "phases",
    "totals",
)


class RunStatus(StrEnum):
    """How a run ended. EXIT_CODES below turns it into the process exit code."""

    SUCCESS = "SUCCESS"  # everything planned landed (drift is listed, and allowed unless fail_on_drift)
    PARTIAL = "PARTIAL"  # the run finished, but some files are FAILED
    FAILED = "FAILED"  # the run stopped on an error, or drifted files with fail_on_drift on
    SKIPPED = "SKIPPED"  # another run held the lock


# SKIPPED is 0: another run already doing the work is not an error. PARTIAL and FAILED turn the workflow red
EXIT_CODES = {RunStatus.SUCCESS: 0, RunStatus.PARTIAL: 1, RunStatus.FAILED: 2, RunStatus.SKIPPED: 0}


class RunReport:
    """Built up section by section during a run, then written once at the end."""

    def __init__(self, run_id: str, started_at: datetime, env: str) -> None:
        self.run_id = run_id
        self._start = time.monotonic()
        # every section starts as null, so a finished report shows exactly how far the run got
        self.sections: dict[str, Any] = dict.fromkeys(SECTIONS)
        self.sections["run"] = {
            "run_id": run_id,
            "env": env,
            "host": socket.gethostname(),
            "started_at": started_at,
            "finished_at": None,
            "duration_seconds": None,
            "status": None,
            "exit_code": None,
            "error": None,
            "note": None,
        }
        self.sections["phases"] = {}

    def __setitem__(self, section: str, value: Any) -> None:
        """report["plan"] = {...}. An unknown section name is a bug, so it raises instead of adding a new one."""
        if section not in SECTIONS:
            raise KeyError(f"unknown report section {section!r}")
        self.sections[section] = value

    def __getitem__(self, section: str) -> Any:
        return self.sections[section]

    @contextmanager
    def phase(self, name: str) -> Iterator[None]:
        """Time one phase of the run, in seconds, even if it raises."""
        start = time.monotonic()
        try:
            yield
        finally:
            self.sections["phases"][name] = round(time.monotonic() - start, 3)

    def finish(self, status: RunStatus, error: BaseException | None = None, note: str | None = None) -> int:
        """Record the final status, duration, note and error (with its traceback). Returns the exit code."""
        run = self.sections["run"]
        run["finished_at"] = datetime.now(UTC)
        run["duration_seconds"] = round(time.monotonic() - self._start, 3)
        run["status"] = status.value
        run["exit_code"] = EXIT_CODES[status]
        run["note"] = note
        if error is not None:
            run["error"] = {
                "type": type(error).__name__,
                "message": str(error),
                "traceback": "".join(traceback.format_exception(error)),
            }
        return EXIT_CODES[status]

    def to_dict(self) -> dict[str, Any]:
        """The report as JSON-ready data: datetimes, paths and enums become strings."""
        return _jsonable(self.sections)

    def write(self, runs_dir: Path) -> Path:
        """Write <runs_dir>/<run_id>.json atomically: a temporary file, then a rename."""
        runs_dir = Path(runs_dir)
        runs_dir.mkdir(parents=True, exist_ok=True)
        final = runs_dir / f"{self.run_id}.json"
        tmp = runs_dir / f".{final.name}.tmp"
        with open(tmp, "w") as f:
            json.dump(self.to_dict(), f, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, final)
        return final


def code_info(repo_root: Path = REPO_ROOT) -> dict[str, Any]:
    """Which code ran: git commit and whether the checkout had uncommitted changes, plus key versions."""
    info: dict[str, Any] = {
        "commit": None,
        "dirty": None,
        "python": platform.python_version(),
        "packages": {name: _version(name) for name in ("threew", "pyspark", "pyarrow", "huggingface_hub")},
    }
    try:
        info["commit"] = _git(repo_root, "rev-parse", "HEAD")
        info["dirty"] = bool(_git(repo_root, "status", "--porcelain", "--untracked-files=no"))
    except (OSError, subprocess.CalledProcessError) as exc:
        info["git_error"] = f"{type(exc).__name__}: {exc}"
    return info


def _git(repo_root: Path, *args: str) -> str:
    """Run one git command in the repo and return its output."""
    out = subprocess.run(["git", *args], cwd=repo_root, check=True, capture_output=True, text=True, timeout=10)
    return out.stdout.strip()


def _version(package: str) -> str | None:
    """The installed version of a package, or None if it isn't installed."""
    try:
        return metadata.version(package)
    except metadata.PackageNotFoundError:
        return None


def _jsonable(value: Any) -> Any:
    """Turn report values into JSON types, recursively. Anything unexpected is stored as text, with a warning."""
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, StrEnum):
        return value.value
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    log.warning("run report: storing a %s as text", type(value).__name__)
    return str(value)
