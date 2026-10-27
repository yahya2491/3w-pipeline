"""Landing tests: temporary folders, no network."""

import hashlib
import stat
from datetime import date
from pathlib import Path

import pytest

from threew.bronze.fetch import FetchResult, FetchStatus, VerifiedBy
from threew.bronze.inspect import fingerprint
from threew.bronze.land import land_results, landing_relpath
from threew.bronze.manifest import Manifest
from threew.bronze.naming import parse_path
from threew.bronze.plan import Candidate, Reason, build_plan
from threew.config import StorageConfig
from threew.source.hub import RemoteFile

RUN_ID = "20261009T120000Z-abc123"
INGEST_DATE = date(2026, 10, 9)
REVISION = "c1a7e8d0cfefc0875974ea079d11349a37fa0c56"
PATH = "data/2/WELL-00002_20131104004101.parquet"


@pytest.fixture
def storage(tmp_path):
    return StorageConfig(root=tmp_path / "root")


@pytest.fixture
def manifest(storage):
    return Manifest(storage.state)


@pytest.fixture
def data(parquet_bytes) -> bytes:
    return parquet_bytes(num_rows=4)


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


def land(results, storage, manifest, contract, run_id=RUN_ID, **kwargs):
    return land_results(results, run_id, INGEST_DATE, REVISION, storage, manifest, contract, **kwargs)


def target_of(storage: StorageConfig, path: str = PATH, run_id: str = RUN_ID) -> Path:
    return storage.root / landing_relpath(parse_path(path), INGEST_DATE, run_id)


def landed_files(storage: StorageConfig) -> list[Path]:
    return sorted(p for p in storage.landing.rglob("*") if p.is_file()) if storage.landing.exists() else []


def test_landing_layout():
    relpath = landing_relpath(parse_path(PATH), INGEST_DATE, RUN_ID)
    assert relpath.as_posix() == (
        f"landing/3w/ingest_date=2026-10-09/run_id={RUN_ID}/label=2/WELL-00002_20131104004101.parquet"
    )


def test_ok_files_land_unchanged_and_read_only(storage, manifest, contract, data):
    land([staged_ok(storage, PATH, data)], storage, manifest, contract)
    target = target_of(storage)
    assert landed_files(storage) == [target]
    assert hashlib.sha256(target.read_bytes()).hexdigest() == hashlib.sha256(data).hexdigest()
    assert stat.S_IMODE(target.stat().st_mode) == 0o444


def test_manifest_rows(storage, manifest, contract, data):
    landing = land([staged_ok(storage, PATH, data)], storage, manifest, contract)
    [row] = manifest.read().to_pylist()
    assert row == landing.rows[0]
    assert row["landing_path"] == target_of(storage).relative_to(storage.root).as_posix()
    assert not row["landing_path"].startswith("/")
    assert (row["run_id"], row["revision"], row["status"], row["error"]) == (RUN_ID, REVISION, "LANDED", None)
    assert (row["label"], row["source_type"], row["well_id"]) == (2, "REAL", 2)
    assert row["instance_id"] == "WELL-00002_20131104004101"
    assert (row["size"], row["sha256"]) == (len(data), hashlib.sha256(data).hexdigest())
    assert row["recorded_at"].tzinfo is not None


def test_rows_have_the_step14_columns_filled_in(storage, manifest, contract, data):
    landing = land([staged_ok(storage, PATH, data)], storage, manifest, contract)
    [row] = manifest.read().to_pylist()
    assert (row["num_rows"], row["schema_fingerprint"], row["schema_ok"], row["codec"]) == (
        4,
        fingerprint(contract),
        True,
        "BROTLI",
    )
    assert landing.inspections[PATH].schema_ok


def test_a_drifted_or_unreadable_file_still_lands_but_is_flagged(storage, manifest, contract, parquet_bytes):
    drifted = parquet_bytes(columns={**contract, "NEW-SENSOR": "double"})
    other = "data/5/SIMULATED_00001.parquet"
    landing = land(
        [staged_ok(storage, PATH, drifted), staged_ok(storage, other, b"not parquet")], storage, manifest, contract
    )
    rows = {r["path"]: r for r in manifest.read().to_pylist()}
    assert (rows[PATH]["status"], rows[PATH]["schema_ok"]) == ("LANDED", False)
    assert landing.inspections[PATH].extra == ("NEW-SENSOR",)
    assert (rows[other]["status"], rows[other]["schema_ok"], rows[other]["num_rows"]) == ("LANDED", False, None)
    assert not landing.inspections[other].readable


def test_failed_results_give_failed_rows_and_nothing_in_landing(storage, manifest, contract):
    landing = land([failed(PATH)], storage, manifest, contract)
    [row] = manifest.read().to_pylist()
    assert row["status"] == "FAILED"
    assert row["error"] == "download failed: HfHubHTTPError: 404"
    assert row["landing_path"] is None
    assert (row["num_rows"], row["schema_ok"], row["codec"]) == (None, None, None)
    assert landed_files(storage) == []
    assert landing.inspections == {}


def test_collision_with_different_content_fails_and_leaves_the_existing_file(storage, manifest, contract, data):
    existing = target_of(storage)
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"someone else's bytes")
    existing.chmod(0o444)

    land([staged_ok(storage, PATH, data)], storage, manifest, contract)
    [row] = manifest.read().to_pylist()
    assert row["status"] == "FAILED"
    assert row["error"].startswith("collision:")
    assert existing.read_bytes() == b"someone else's bytes"
    assert stat.S_IMODE(existing.stat().st_mode) == 0o444


def test_identical_file_already_in_landing_is_adopted(storage, manifest, contract, data):
    existing = target_of(storage)
    existing.parent.mkdir(parents=True)
    existing.write_bytes(data)

    land([staged_ok(storage, PATH, data)], storage, manifest, contract)
    assert manifest.read()["status"].to_pylist() == ["LANDED"]
    assert landed_files(storage) == [existing]


def test_rows_are_written_in_chunks(storage, manifest, contract):
    results = [staged_ok(storage, f"data/{i % 10}/SIMULATED_{i:05d}.parquet", f"file {i}".encode()) for i in range(250)]
    land(results, storage, manifest, contract, chunk_size=100)
    assert len(list(manifest.dir.glob("part-*.parquet"))) == 3  # 100 + 100 + 50
    assert manifest.read().num_rows == 250


def test_staging_run_folder_is_removed(storage, manifest, contract, data):
    result = staged_ok(storage, PATH, data)
    cache = storage.staging / RUN_ID / ".cache" / "huggingface"  # what hf_hub_download leaves behind
    cache.mkdir(parents=True)
    (cache / "x.metadata").write_text("meta")
    land([result, failed("data/5/SIMULATED_00001.parquet")], storage, manifest, contract)
    assert not (storage.staging / RUN_ID).exists()


def test_crash_after_the_move_is_recovered_by_the_next_run(storage, manifest, contract, monkeypatch, data):
    def crash(rows):
        raise OSError("simulated crash before the manifest write")

    monkeypatch.setattr(manifest, "append", crash)
    with pytest.raises(OSError, match="simulated crash"):
        land([staged_ok(storage, PATH, data)], storage, manifest, contract)
    monkeypatch.undo()

    # The file is in landing, but the manifest has no row for it...
    orphan = target_of(storage)
    assert landed_files(storage) == [orphan]
    assert manifest.read().num_rows == 0
    # ...so the next plan lists it as NEW again
    plan = build_plan([remote_for(PATH, data)], manifest.current(), sample_per_class=None)
    assert [(c.path, reason) for c, reason in plan.to_download] == [(PATH, Reason.NEW)]

    # The next run lands it in its own run folder; the orphan stays for the leftover check to report
    land([staged_ok(storage, PATH, data, run_id="run-2")], storage, manifest, contract, run_id="run-2")
    recovered = target_of(storage, run_id="run-2")
    assert manifest.current()["landing_path"].to_pylist() == [recovered.relative_to(storage.root).as_posix()]
    assert landed_files(storage) == sorted([orphan, recovered])


def test_fail_on_drift_refuses_a_drifted_file(storage, manifest, contract, parquet_bytes, data):
    drifted = parquet_bytes(num_rows=2, columns={**contract, "NEW-SENSOR": "double"})
    other = "data/5/SIMULATED_00001.parquet"
    results = [staged_ok(storage, PATH, drifted), staged_ok(storage, other, data)]
    landing = land(results, storage, manifest, contract, fail_on_drift=True)

    rows = {r["path"]: r for r in manifest.read().to_pylist()}
    assert rows[PATH]["status"] == "FAILED"
    assert rows[PATH]["error"] == "schema drift: extra ['NEW-SENSOR']"
    assert rows[PATH]["landing_path"] is None
    # What the inspection found is still recorded
    assert (rows[PATH]["num_rows"], rows[PATH]["schema_ok"], rows[PATH]["codec"]) == (2, False, "BROTLI")
    assert landing.inspections[PATH].extra == ("NEW-SENSOR",)
    # The matching file lands as usual; the drifted one doesn't
    assert rows[other]["status"] == "LANDED"
    assert landed_files(storage) == [target_of(storage, other)]
    assert not (storage.staging / RUN_ID).exists()
    # FAILED rows don't count as landed, so the next run plans it again (and fails again)
    plan = build_plan([remote_for(PATH, drifted), remote_for(other, data)], manifest.current(), None)
    assert [(c.path, reason) for c, reason in plan.to_download] == [(PATH, Reason.NEW)]


def test_fail_on_drift_refuses_an_unreadable_file(storage, manifest, contract):
    land([staged_ok(storage, PATH, b"not parquet")], storage, manifest, contract, fail_on_drift=True)
    [row] = manifest.read().to_pylist()
    assert row["status"] == "FAILED"
    assert row["error"].startswith("schema drift: unreadable")
    assert landed_files(storage) == []
