from datetime import UTC, datetime

import pytest

from threew.bronze.manifest import Manifest
from threew.bronze.orphans import find_orphans, find_stale_staging
from threew.config import StorageConfig

LANDED_PATH = "landing/3w/ingest_date=2026-10-09/run_id=r1/label=2/SIMULATED_00001.parquet"


def manifest_row(landing_path: str | None, status: str = "LANDED") -> dict:
    return {
        "run_id": "r1",
        "path": "data/2/SIMULATED_00001.parquet",
        "revision": "c1a7e8d0cfefc0875974ea079d11349a37fa0c56",
        "label": 2,
        "source_type": "SIMULATED",
        "well_id": None,
        "instance_start": None,
        "instance_id": "SIMULATED_00001",
        "size": 10,
        "sha256": "a" * 64,
        "blob_id": "b",
        "landing_path": landing_path,
        "status": status,
        "error": "boom" if status == "FAILED" else None,
        "num_rows": None,
        "schema_fingerprint": None,
        "schema_ok": None,
        "codec": None,
        "recorded_at": datetime(2026, 10, 9, tzinfo=UTC),
    }


@pytest.fixture
def storage(tmp_path):
    return StorageConfig(root=tmp_path)


def touch(storage: StorageConfig, relpath: str) -> None:
    path = storage.root / relpath
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x")


def test_no_landing_yet(storage):
    assert find_orphans(storage, Manifest(storage.state)) == []


def test_files_without_a_landed_row_are_orphans(storage):
    manifest = Manifest(storage.state)
    manifest.append([manifest_row(LANDED_PATH)])
    touch(storage, LANDED_PATH)
    stray = "landing/3w/ingest_date=2026-10-09/run_id=r0/label=2/SIMULATED_00001.parquet"  # crashed run's file
    note = "landing/3w/notes.txt"
    touch(storage, stray)
    touch(storage, note)
    assert find_orphans(storage, manifest) == sorted([stray, note])


def test_stale_staging(tmp_path):
    staging = tmp_path / "_staging"
    assert find_stale_staging(staging, "now") == []
    for name in ("now", "crashed-1", "smoke_20261008T231652Z"):
        (staging / name).mkdir(parents=True)
    assert find_stale_staging(staging, "now") == ["crashed-1", "smoke_20261008T231652Z"]
