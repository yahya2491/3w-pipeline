"""Landing tests: temporary folders, no network."""

import hashlib
import stat
from datetime import date
from pathlib import Path

import pytest

from threew.bronze.fetch import FetchResult, FetchStatus, VerifiedBy
from threew.bronze.land import land_results, landing_relpath
from threew.bronze.manifest import Manifest
from threew.bronze.naming import parse_path
from threew.bronze.plan import Candidate, Reason, build_plan
from threew.config import StorageConfig
from threew.source.hub import RemoteFile

RUN_ID = "20261009T120000Z-abc123"
INGEST_DATE = date(2026, 10, 9)
REVISION = "c1a7e8d0cfefc0875974ea079d11349a37fa0c56"


@pytest.fixture
def storage(tmp_path):
    return StorageConfig(root=tmp_path / "root")


@pytest.fixture
def manifest(storage):
    return Manifest(storage.state)


def remote_for(path: str, data: bytes) -> RemoteFile:
    return RemoteFile(path=path, size=len(data), sha256=hashlib.sha256(data).hexdigest(), blob_id="b")


def staged_ok(storage: StorageConfig, path: str, data: bytes, run_id: str = RUN_ID) -> FetchResult:
    """An OK fetch result whose file sits in <staging>/<run_id>/<path>, like fetch_all leaves it."""
    staged = storage.staging / run_id / path
    staged.parent.mkdir(parents=True, exist_ok=True)
    staged.write_bytes(data)
    remote = remote_for(path, data)
    candidate = Candidate(remote, parse_path(path))
    return FetchResult(candidate, Reason.NEW, FetchStatus.OK, staged, remote.sha256, VerifiedBy.SHA256, 1, 0.1, None)


def failed(path: str, error: str = "download failed: HfHubHTTPError: 404") -> FetchResult:
    candidate = Candidate(remote_for(path, b"never downloaded"), parse_path(path))
    return FetchResult(candidate, Reason.NEW, FetchStatus.FAILED, None, None, VerifiedBy.SHA256, 1, 0.1, error)


def land(results, storage, manifest, run_id=RUN_ID, **kwargs):
    return land_results(results, run_id, INGEST_DATE, REVISION, storage, manifest, **kwargs)


def landed_files(storage: StorageConfig) -> list[Path]:
    return sorted(p for p in storage.landing.rglob("*") if p.is_file()) if storage.landing.exists() else []


PATH = "data/2/WELL-00002_20131104004101.parquet"
DATA = b"PAR1 real file bytes PAR1"


def test_ok_files_land_unchanged_and_read_only(storage, manifest):
    land([staged_ok(storage, PATH, DATA)], storage, manifest)
    target = storage.root / "landing/3w/ingest_date=2026-10-09" / PATH
    assert landed_files(storage) == [target]
    assert hashlib.sha256(target.read_bytes()).hexdigest() == hashlib.sha256(DATA).hexdigest()
    assert stat.S_IMODE(target.stat().st_mode) == 0o444


def test_manifest_rows(storage, manifest):
    rows = land([staged_ok(storage, PATH, DATA)], storage, manifest)
    [row] = manifest.read().to_pylist()
    assert row == rows[0]
    assert row["landing_path"] == f"landing/3w/ingest_date=2026-10-09/{PATH}"
    assert not row["landing_path"].startswith("/")
    assert row["landing_path"] == landing_relpath(PATH, INGEST_DATE).as_posix()
    assert (row["run_id"], row["revision"], row["status"], row["error"]) == (RUN_ID, REVISION, "LANDED", None)
    assert (row["label"], row["source_type"], row["well_id"], row["instance_id"]) == (2, "REAL", 2, PATH[7:-8])
    assert (row["size"], row["sha256"]) == (len(DATA), hashlib.sha256(DATA).hexdigest())
    assert (row["num_rows"], row["schema_fingerprint"], row["schema_ok"]) == (None, None, None)
    assert row["recorded_at"].tzinfo is not None


def test_failed_results_give_failed_rows_and_nothing_in_landing(storage, manifest):
    land([failed(PATH)], storage, manifest)
    [row] = manifest.read().to_pylist()
    assert row["status"] == "FAILED"
    assert row["error"] == "download failed: HfHubHTTPError: 404"
    assert row["landing_path"] is None
    assert landed_files(storage) == []


def test_collision_with_different_content_fails_and_leaves_the_existing_file(storage, manifest):
    existing = storage.root / landing_relpath(PATH, INGEST_DATE)
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"someone else's bytes")
    existing.chmod(0o444)

    land([staged_ok(storage, PATH, DATA)], storage, manifest)
    [row] = manifest.read().to_pylist()
    assert row["status"] == "FAILED"
    assert row["error"].startswith("collision:")
    assert existing.read_bytes() == b"someone else's bytes"
    assert stat.S_IMODE(existing.stat().st_mode) == 0o444


def test_identical_file_already_in_landing_is_adopted(storage, manifest):
    existing = storage.root / landing_relpath(PATH, INGEST_DATE)
    existing.parent.mkdir(parents=True)
    existing.write_bytes(DATA)

    land([staged_ok(storage, PATH, DATA)], storage, manifest)
    assert manifest.read()["status"].to_pylist() == ["LANDED"]
    assert landed_files(storage) == [existing]


def test_rows_are_written_in_chunks(storage, manifest):
    results = [staged_ok(storage, f"data/{i % 10}/SIMULATED_{i:05d}.parquet", f"file {i}".encode()) for i in range(250)]
    land(results, storage, manifest, chunk_size=100)
    assert len(list(manifest.dir.glob("part-*.parquet"))) == 3  # 100 + 100 + 50
    assert manifest.read().num_rows == 250


def test_staging_run_folder_is_removed(storage, manifest):
    result = staged_ok(storage, PATH, DATA)
    cache = storage.staging / RUN_ID / ".cache" / "huggingface"  # what hf_hub_download leaves behind
    cache.mkdir(parents=True)
    (cache / "x.metadata").write_text("meta")
    land([result, failed("data/5/SIMULATED_00001.parquet")], storage, manifest)
    assert not (storage.staging / RUN_ID).exists()


def test_crash_after_the_move_is_recovered_by_the_next_run(storage, manifest, monkeypatch):
    def crash(rows):
        raise OSError("simulated crash before the manifest write")

    monkeypatch.setattr(manifest, "append", crash)
    with pytest.raises(OSError, match="simulated crash"):
        land([staged_ok(storage, PATH, DATA)], storage, manifest)
    monkeypatch.undo()

    # The file is in landing, but the manifest has no row for it...
    assert landed_files(storage) == [storage.root / landing_relpath(PATH, INGEST_DATE)]
    assert manifest.read().num_rows == 0
    # ...so the next plan lists it as NEW again
    plan = build_plan([remote_for(PATH, DATA)], manifest.current(), sample_per_class=None)
    assert [(c.path, reason) for c, reason in plan.to_download] == [(PATH, Reason.NEW)]

    # Landing it again on the same day adopts the identical file instead of failing on a collision
    land([staged_ok(storage, PATH, DATA, run_id="run-2")], storage, manifest, run_id="run-2")
    assert manifest.current()["landing_path"].to_pylist() == [landing_relpath(PATH, INGEST_DATE).as_posix()]
