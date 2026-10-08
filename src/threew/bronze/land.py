"""Move verified files from staging into landing and record every result in the manifest.

Order matters for crash safety: a file is moved first and its row written after. A crash in between leaves a file
in landing with no row; the next plan sees it as NEW, and landing it again finds the identical file and adopts it.
"""

import logging
import os
import shutil
from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import PurePosixPath
from typing import Any

from threew.bronze.fetch import FetchResult, FetchStatus, sha256_of
from threew.bronze.manifest import Manifest, Status
from threew.config import StorageConfig

log = logging.getLogger(__name__)

READ_ONLY = 0o444


def landing_relpath(path: str, ingest_date: date) -> PurePosixPath:
    """Where a repo file lands, relative to storage.root: landing/3w/ingest_date=YYYY-MM-DD/<repo path>."""
    return PurePosixPath("landing", "3w", f"ingest_date={ingest_date:%Y-%m-%d}", path)


def land_results(
    results: Sequence[FetchResult],
    run_id: str,
    ingest_date: date,
    revision: str,
    storage_cfg: StorageConfig,
    manifest: Manifest,
    chunk_size: int = 100,
) -> list[dict[str, Any]]:
    """Land OK results, record a manifest row for every result, then delete <staging>/<run_id>/.

    Rows are written every chunk_size files, so a crash late in a big run loses at most chunk_size rows.
    """
    rows: list[dict[str, Any]] = []
    pending: list[dict[str, Any]] = []
    for result in results:
        row = _land_one(result, run_id, ingest_date, revision, storage_cfg)
        rows.append(row)
        pending.append(row)
        if len(pending) >= chunk_size:
            manifest.append(pending)
            pending = []
    manifest.append(pending)  # no-op when empty

    # Only once every row is written: after a crash the staging folder is left for inspection
    shutil.rmtree(storage_cfg.staging / run_id, ignore_errors=True)
    return rows


def _land_one(
    result: FetchResult, run_id: str, ingest_date: date, revision: str, storage_cfg: StorageConfig
) -> dict[str, Any]:
    if result.status is not FetchStatus.OK:
        return _row(result, run_id, revision, Status.FAILED, None, result.error)

    relpath = landing_relpath(result.candidate.path, ingest_date)
    target = storage_cfg.root / relpath
    staged = result.staged_path
    try:
        if target.exists():
            if sha256_of(target) != result.sha256_actual:
                # Different content: never overwrite, the existing file stays as it is
                return _row(result, run_id, revision, Status.FAILED, None, f"collision: {relpath} already exists")
            # Identical bytes, e.g. moved by a run that crashed before writing its row: adopt it
            log.info("adopting %s: already in landing with the same sha256", relpath)
            staged.unlink(missing_ok=True)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(staged, target)  # atomic: staging and landing are on the same filesystem
            os.chmod(target, READ_ONLY)
    except OSError as exc:
        return _row(result, run_id, revision, Status.FAILED, None, f"landing failed: {type(exc).__name__}: {exc}")
    return _row(result, run_id, revision, Status.LANDED, relpath.as_posix(), None)


def _row(
    result: FetchResult, run_id: str, revision: str, status: Status, landing_path: str | None, error: str | None
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
        "num_rows": None,  # filled in step 14
        "schema_fingerprint": None,
        "schema_ok": None,
        "recorded_at": datetime.now(UTC),
    }
