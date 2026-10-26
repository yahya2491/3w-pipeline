"""Move verified files from staging into landing, inspect their footers, and record every result in the manifest.

Landing layout, relative to storage.root (all dates and times are UTC):

    landing/3w/ingest_date=<YYYY-MM-DD>/run_id=<run_id>/label=<label>/<original file name>

Hive-style key=value folders, which Spark reads as partition columns. The label folder is `label=`, not `class=`:
the files have their own per-row `class` column, and Spark replaces a data column's values with a partition column
of the same name. run_id gives every run its own folder, so a run never writes where an earlier run wrote.

Each file's footer is inspected against the schema contract before it is moved. With fail_on_drift, a file that
doesn't match is not landed: it gets a FAILED row, so every later run plans it again and fails again until the
contract or the flag changes. Without it, the file lands with schema_ok=False.

Order matters for crash safety: a file is moved first and its row written after. A crash in between leaves a file
in landing with no row; the next plan sees it as NEW and lands it again in its own run folder.
"""

import logging
import os
import shutil
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import PurePosixPath
from typing import Any

from threew.bronze.fetch import FetchResult, FetchStatus, sha256_of
from threew.bronze.inspect import Inspection, inspect_file
from threew.bronze.manifest import Manifest, Status
from threew.bronze.naming import InstanceInfo
from threew.config import StorageConfig

log = logging.getLogger(__name__)

READ_ONLY = 0o444


@dataclass(frozen=True)
class Landing:
    rows: list[dict[str, Any]]  # one manifest row per result, in result order
    inspections: dict[str, Inspection]  # by repo path, for every inspected file: drift details for the run report


def landing_relpath(info: InstanceInfo, ingest_date: date, run_id: str) -> PurePosixPath:
    """Where a file lands, relative to storage.root (see the module docstring)."""
    return PurePosixPath(
        "landing",
        "3w",
        f"ingest_date={ingest_date:%Y-%m-%d}",
        f"run_id={run_id}",
        f"label={info.label}",
        PurePosixPath(info.path).name,
    )


def land_results(
    results: Sequence[FetchResult],
    run_id: str,
    ingest_date: date,
    revision: str,
    storage_cfg: StorageConfig,
    manifest: Manifest,
    contract: dict[str, str],
    chunk_size: int = 100,
    fail_on_drift: bool = False,
) -> Landing:
    """Inspect and land OK results, record a manifest row for every result, then delete <staging>/<run_id>/.

    Rows are written every chunk_size files, so a crash late in a big run loses at most chunk_size rows.
    """
    rows: list[dict[str, Any]] = []
    inspections: dict[str, Inspection] = {}
    pending: list[dict[str, Any]] = []
    for result in results:
        row, inspection = _land_one(result, run_id, ingest_date, revision, storage_cfg, contract, fail_on_drift)
        rows.append(row)
        if inspection is not None:
            inspections[result.candidate.path] = inspection
        pending.append(row)
        if len(pending) >= chunk_size:
            manifest.append(pending)
            pending = []
    manifest.append(pending)  # no-op when empty

    # Only once every row is written: after a crash the staging folder is left for inspection
    shutil.rmtree(storage_cfg.staging / run_id, ignore_errors=True)
    return Landing(rows, inspections)


def _land_one(
    result: FetchResult,
    run_id: str,
    ingest_date: date,
    revision: str,
    storage_cfg: StorageConfig,
    contract: dict[str, str],
    fail_on_drift: bool,
) -> tuple[dict[str, Any], Inspection | None]:
    if result.status is not FetchStatus.OK:
        return _row(result, run_id, revision, Status.FAILED, None, result.error), None

    relpath = landing_relpath(result.candidate.info, ingest_date, run_id)
    target = storage_cfg.root / relpath
    staged = result.staged_path

    inspection = inspect_file(staged, contract)  # footer only; the staged bytes are the verified ones
    if not inspection.schema_ok:
        log.warning("schema drift in %s: %s", result.candidate.path, drift_text(inspection))
        if fail_on_drift:
            error = f"schema drift: {drift_text(inspection)}"
            return _row(result, run_id, revision, Status.FAILED, None, error, inspection), inspection

    try:
        if target.exists():
            # With run_id in the path this practically never happens, but a file is never overwritten
            if sha256_of(target) != result.sha256_actual:
                error = f"collision: {relpath} already exists"
                return _row(result, run_id, revision, Status.FAILED, None, error), None
            log.info("adopting %s: already in landing with the same sha256", relpath)
            staged.unlink(missing_ok=True)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(staged, target)  # atomic: staging and landing are on the same filesystem
            os.chmod(target, READ_ONLY)
    except OSError as exc:
        error = f"landing failed: {type(exc).__name__}: {exc}"
        return _row(result, run_id, revision, Status.FAILED, None, error), None

    return _row(result, run_id, revision, Status.LANDED, relpath.as_posix(), None, inspection), inspection


def _row(
    result: FetchResult,
    run_id: str,
    revision: str,
    status: Status,
    landing_path: str | None,
    error: str | None,
    inspection: Inspection | None = None,
) -> dict[str, Any]:
    info, remote = result.candidate.info, result.candidate.remote
    return {
        "run_id": run_id,
        "path": remote.path,
        "revision": revision,
        "label": info.label,
        "source_type": info.source_type.value,
        "well_id": info.well_id,
        "instance_start": info.instance_start,
        "instance_id": info.instance_id,
        "size": remote.size,
        "sha256": remote.sha256,
        "blob_id": remote.blob_id,
        "landing_path": landing_path,
        "status": status.value,
        "error": error,
        "num_rows": inspection.num_rows if inspection else None,
        "schema_fingerprint": inspection.fingerprint if inspection else None,
        "schema_ok": inspection.schema_ok if inspection else None,
        "codec": inspection.codec if inspection else None,
        "recorded_at": datetime.now(UTC),
    }


def drift_text(inspection: Inspection) -> str:
    """What's wrong with a file's structure, in one line."""
    if not inspection.readable:
        return f"unreadable ({inspection.error})"
    parts = []
    if inspection.missing:
        parts.append(f"missing {list(inspection.missing)}")
    if inspection.extra:
        parts.append(f"extra {list(inspection.extra)}")
    if inspection.type_changes:
        parts.append("type changes " + ", ".join(f"{c}: {e} -> {a}" for c, e, a in inspection.type_changes))
    return "; ".join(parts)
