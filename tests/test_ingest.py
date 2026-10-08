"""End-to-end ingest with a fake hub: temporary folders, no network, no Spark."""

import dataclasses
import hashlib
import json
import re
import shutil
from collections import defaultdict
from pathlib import Path

import httpx2
import pytest
from huggingface_hub.errors import HfHubHTTPError

from threew.bronze.ingest import LOCK_FILE, run_ingest
from threew.bronze.inspect import fingerprint
from threew.bronze.manifest import Manifest
from threew.bronze.report import SECTIONS
from threew.config import REPO_ROOT, StorageConfig, load_config
from threew.lock import run_lock
from threew.source.hub import RemoteFile, matches

REVISION = load_config("dev").source.revision
DRIFTED = "data/5/SIMULATED_00003.parquet"


@pytest.fixture
def files(parquet_bytes) -> dict[str, bytes]:
    return {
        "README.md": b"card",  # not matched by the include pattern
        "data/dataset.ini": b"ini",
        "data/0/WELL-00001_20170201010207.parquet": parquet_bytes(1),
        "data/0/WELL-00001_20170201060114.parquet": parquet_bytes(2),
        "data/2/SIMULATED_00001.parquet": parquet_bytes(3),
        "data/2/DRAWN_00001.parquet": parquet_bytes(4),
        "data/5/SIMULATED_00001.parquet": parquet_bytes(5),
        "data/5/SIMULATED_00002.parquet": parquet_bytes(6),
        "data/5/SIMULATED_00003.parquet": parquet_bytes(7),
        "data/5/SIMULATED_00004.parquet": parquet_bytes(8),  # dev samples 3 per class: not this one
    }


@pytest.fixture
def drifted_files(files, contract, parquet_bytes) -> dict[str, bytes]:
    return {**files, DRIFTED: parquet_bytes(7, columns={n: t for n, t in contract.items() if n != "P-PDG"})}


class FakeHub:
    def __init__(self, files: dict[str, bytes], errors: dict[str, Exception] | None = None) -> None:
        self.files = files
        self.errors = errors or {}  # path -> exception raised on every download of it
        self.list_error: Exception | None = None
        self.downloads: dict[str, int] = defaultdict(int)

    def resolve_revision(self, label: str) -> str:
        return REVISION

    def list_files(self, revision, include):
        if self.list_error:
            raise self.list_error
        return [
            RemoteFile(path, len(data), hashlib.sha256(data).hexdigest(), f"blob-{path}")
            for path, data in sorted(self.files.items())
            if matches(path, include)
        ]

    def download(self, path, revision, dest_dir):
        self.downloads[path] += 1
        if path in self.errors:
            raise self.errors[path]
        target = Path(dest_dir) / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(self.files[path])
        return target


def http_error(status: int) -> HfHubHTTPError:
    request = httpx2.Request("GET", "https://huggingface.co/datasets/x/resolve/abc/f.parquet")
    return HfHubHTTPError(f"{status} Client Error", response=httpx2.Response(status, request=request))


@pytest.fixture
def cfg(tmp_path):
    return dataclasses.replace(load_config("dev"), storage=StorageConfig(root=tmp_path))


def strict(cfg):
    return dataclasses.replace(cfg, schema=dataclasses.replace(cfg.schema, fail_on_drift=True))


def run(cfg, hub) -> tuple[int, dict]:
    """Run once; return the exit code and the run's report, parsed from its JSON file."""
    before = set(cfg.storage.runs.glob("*.json"))
    code = run_ingest(cfg, hub, sleep=lambda _: None)
    [new] = set(cfg.storage.runs.glob("*.json")) - before
    report = json.loads(new.read_text())  # valid JSON
    assert set(report) == set(SECTIONS)  # every section present, even if null
    assert report["run"]["exit_code"] == code
    return code, report


def landed(cfg) -> list[Path]:
    return sorted(cfg.storage.landing.rglob("*.parquet"))


def lock_is_free(cfg) -> bool:
    with run_lock(cfg.storage.state / LOCK_FILE):
        return True


def test_normal_run_then_nothing_to_do(cfg, files, contract):
    hub = FakeHub(files)
    code, report = run(cfg, hub)
    assert (code, report["run"]["status"]) == (0, "SUCCESS")
    assert all(report[section] is not None for section in SECTIONS)
    assert report["run"]["error"] is None
    assert set(report["phases"]) == {"pin", "list", "plan", "fetch", "land", "checks"}
    assert report["plan"]["new"] == 7
    assert report["landing"] == {"LANDED": 7}
    assert report["schema"]["fingerprints"] == {fingerprint(contract): 7}
    assert report["schema"]["codecs"] == {"BROTLI": 7}
    assert report["totals"]["files_landed"] == report["totals"]["landed_files_total"] == 7
    assert report["totals"]["rows_landed"] == 1 + 2 + 3 + 4 + 5 + 6 + 7
    assert (report["orphans"], report["stale_staging"], report["failures"]) == ([], [], [])
    assert report["config"]["env"] == "dev"
    assert len(landed(cfg)) == 7
    assert all(f"run_id={report['run']['run_id']}" in p.as_posix() for p in landed(cfg))

    code, second = run(cfg, hub)
    assert (code, second["run"]["status"]) == (0, "SUCCESS")
    assert (second["plan"]["new"], second["plan"]["unchanged"], second["fetch"]["files"]) == (0, 7, 0)
    assert sum(hub.downloads.values()) == 7  # nothing downloaded again
    assert Manifest(cfg.storage.state).read().num_rows == 7
    assert list(cfg.storage.staging.iterdir()) == []


def test_one_file_fails(cfg, files):
    bad = "data/2/DRAWN_00001.parquet"
    code, report = run(cfg, FakeHub(files, errors={bad: http_error(404)}))
    assert (code, report["run"]["status"]) == (1, "PARTIAL")
    [failure] = report["failures"]
    assert failure["path"] == bad
    assert "404" in failure["error"]
    assert failure["attempts"] == 1
    assert report["landing"] == {"LANDED": 6, "FAILED": 1}


def test_listing_raises_an_unexpected_error(cfg, files):
    hub = FakeHub(files)
    hub.list_error = RuntimeError("listing exploded")
    code, report = run(cfg, hub)
    assert (code, report["run"]["status"]) == (2, "FAILED")
    assert report["run"]["error"]["type"] == "RuntimeError"
    assert report["run"]["error"]["message"] == "listing exploded"
    assert "Traceback" in report["run"]["error"]["traceback"]
    assert report["plan"] is None  # never reached
    assert lock_is_free(cfg)  # released after a run that raised


def test_circuit_breaker_trips(cfg, parquet_bytes):
    many = {f"data/{label}/SIMULATED_{n:05d}.parquet": parquet_bytes(1) for label in range(4) for n in range(1, 4)}
    code, report = run(cfg, FakeHub(many, errors={path: http_error(401) for path in many}))
    assert (code, report["run"]["status"]) == (2, "FAILED")
    assert report["run"]["error"]["type"] == "TooManyFailuresError"
    assert "consecutive failures" in report["run"]["error"]["message"]
    assert lock_is_free(cfg)


def test_drift_without_fail_on_drift_succeeds_and_lists_it(cfg, drifted_files):
    code, report = run(cfg, FakeHub(drifted_files))
    assert (code, report["run"]["status"]) == (0, "SUCCESS")
    assert [(d["path"], d["missing"]) for d in report["schema"]["drifted"]] == [(DRIFTED, ["P-PDG"])]
    assert report["landing"] == {"LANDED": 7}


def test_drift_with_fail_on_drift_fails(cfg, drifted_files):
    hub = FakeHub(drifted_files)
    code, report = run(strict(cfg), hub)
    assert (code, report["run"]["status"]) == (2, "FAILED")
    assert report["schema"]["fail_on_drift"] is True
    assert [d["path"] for d in report["schema"]["drifted"]] == [DRIFTED]
    assert [f["path"] for f in report["failures"]] == [DRIFTED]
    assert report["landing"] == {"LANDED": 6, "FAILED": 1}
    assert DRIFTED.rsplit("/", 1)[1] not in {p.name for p in landed(cfg)}

    # The drifted file is planned again and the run fails again: the drift can't slip through
    code, again = run(strict(cfg), hub)
    assert (code, again["plan"]["new"], again["plan"]["unchanged"]) == (2, 1, 6)


def test_lock_already_held_skips(cfg, files):
    hub = FakeHub(files)
    with run_lock(cfg.storage.state / LOCK_FILE, {"run_id": "the-other-run"}):
        code, report = run(cfg, hub)
    assert (code, report["run"]["status"]) == (0, "SKIPPED")
    assert "the-other-run" in report["run"]["note"]
    assert report["plan"] is None
    assert sum(hub.downloads.values()) == 0
    assert lock_is_free(cfg)


def test_extra_file_in_landing_is_listed_as_an_orphan_not_deleted(cfg, files):
    hub = FakeHub(files)
    run(cfg, hub)
    copy = landed(cfg)[0].with_name("COPY_00001.parquet")
    shutil.copy(landed(cfg)[0], copy)

    _, report = run(cfg, hub)
    assert report["orphans"] == [copy.relative_to(cfg.storage.root).as_posix()]
    assert report["run"]["status"] == "SUCCESS"
    assert copy.exists()


def test_leftover_staging_folder_is_listed(cfg, files):
    leftover = cfg.storage.staging / "20261001T000000Z-dead00"
    (leftover / "data/0").mkdir(parents=True)
    (leftover / "data/0/partial.parquet").write_bytes(b"half a download")

    _, report = run(cfg, FakeHub(files))
    assert report["stale_staging"] == ["20261001T000000Z-dead00"]
    assert leftover.exists()  # reported, not deleted


@pytest.mark.skipif(not (REPO_ROOT / ".git").exists(), reason="not running from a git checkout")
def test_code_section_has_the_commit(cfg, files):
    _, report = run(cfg, FakeHub(files))
    assert re.fullmatch(r"[0-9a-f]{40}", report["code"]["commit"])
    assert isinstance(report["code"]["dirty"], bool)
    assert report["code"]["packages"]["pyspark"] == "3.5.0"
