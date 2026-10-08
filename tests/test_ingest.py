"""End-to-end ingest with a fake hub: temporary folders, no network, no Spark."""

import dataclasses
import hashlib
from collections import defaultdict
from pathlib import Path

import pytest

from threew.bronze.ingest import run_ingest
from threew.bronze.manifest import Manifest
from threew.config import StorageConfig, load_config
from threew.source.hub import RemoteFile, matches

REVISION = load_config("dev").source.revision
FILES = {
    "README.md": b"card",  # not matched by the include pattern
    "data/dataset.ini": b"ini",
    "data/0/WELL-00001_20170201010207.parquet": b"real 1",
    "data/0/WELL-00001_20170201060114.parquet": b"real 2",
    "data/2/SIMULATED_00001.parquet": b"sim 2",
    "data/2/DRAWN_00001.parquet": b"drawn 2",
    "data/5/SIMULATED_00001.parquet": b"sim 5-1",
    "data/5/SIMULATED_00002.parquet": b"sim 5-2",
    "data/5/SIMULATED_00003.parquet": b"sim 5-3",
    "data/5/SIMULATED_00004.parquet": b"sim 5-4",  # dev samples 3 per class: not this one
}


class FakeHub:
    def __init__(self, files: dict[str, bytes]) -> None:
        self.files = files
        self.downloads: dict[str, int] = defaultdict(int)

    def resolve_revision(self, label: str) -> str:
        return REVISION

    def list_files(self, revision, include):
        return [
            RemoteFile(path, len(data), hashlib.sha256(data).hexdigest(), f"blob-{path}")
            for path, data in sorted(self.files.items())
            if matches(path, include)
        ]

    def download(self, path, revision, dest_dir):
        self.downloads[path] += 1
        target = Path(dest_dir) / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(self.files[path])
        return target


@pytest.fixture
def cfg(tmp_path):
    return dataclasses.replace(load_config("dev"), storage=StorageConfig(root=tmp_path))


def landed(cfg) -> list[Path]:
    return sorted(cfg.storage.landing.rglob("*.parquet"))


def test_two_runs(cfg):
    hub = FakeHub(FILES)

    first = run_ingest(cfg, hub, sleep=lambda _: None)
    assert first["plan"]["new"] == 7
    assert first["landed"] == {"LANDED": 7}
    assert len(landed(cfg)) == 7
    assert {p.read_bytes() for p in landed(cfg)} == {FILES[p] for p in hub.downloads}
    assert Manifest(cfg.storage.state).read().num_rows == 7
    assert sum(hub.downloads.values()) == 7

    second = run_ingest(cfg, hub, sleep=lambda _: None)
    assert second["run_id"] != first["run_id"]
    assert (second["plan"]["new"], second["plan"]["changed"], second["plan"]["unchanged"]) == (0, 0, 7)
    assert second["fetch"]["files"] == 0
    assert sum(hub.downloads.values()) == 7  # nothing downloaded again
    assert Manifest(cfg.storage.state).read().num_rows == 7
    assert list(cfg.storage.staging.iterdir()) == []  # both runs cleaned up their staging folders
