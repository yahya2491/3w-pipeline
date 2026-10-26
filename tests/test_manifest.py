"""Manifest tests: a temporary folder, no internet, no Spark."""

from datetime import UTC, datetime

import pytest

from threew.bronze import manifest as manifest_module
from threew.bronze.manifest import MANIFEST_SCHEMA, Manifest, ManifestError

SHA_A = "a" * 64
SHA_B = "b" * 64
T1 = datetime(2026, 10, 9, 12, 0, 0, tzinfo=UTC)
T2 = datetime(2026, 10, 10, 12, 0, 0, tzinfo=UTC)


def row(path="data/2/WELL-00002_20131104004101.parquet", status="LANDED", sha256=SHA_A, recorded_at=T1, **overrides):
    real = "WELL-" in path
    values = {
        "run_id": "run-1",
        "path": path,
        "revision": "c1a7e8d0cfefc0875974ea079d11349a37fa0c56",
        "label": int(path.split("/")[1]),
        "source_type": "REAL" if real else "SIMULATED",
        "well_id": 2 if real else None,
        "instance_start": datetime(2013, 11, 4, 0, 41, 1) if real else None,  # noqa: DTZ001  naive by design
        "instance_id": path.rsplit("/", 1)[1].removesuffix(".parquet"),
        "size": 407562,
        "sha256": sha256,
        "blob_id": "1b92da14d6e14116e663c44adaa49079fe926e6c",
        "landing_path": f"landing/3w/{path}" if status == "LANDED" else None,  # relative to storage.root
        "status": status,
        "error": "HTTP 503" if status == "FAILED" else None,
        "num_rows": None,  # step-14 columns: null until then
        "schema_fingerprint": None,
        "schema_ok": None,
        "codec": None,
        "recorded_at": recorded_at,
    }
    return values | overrides


@pytest.fixture
def manifest(tmp_path):
    return Manifest(tmp_path / "state")


def test_empty_folder(manifest):
    table = manifest.read()
    assert table.num_rows == 0
    assert table.schema.equals(MANIFEST_SCHEMA)
    assert manifest.landed_keys() == set()
    assert manifest.current().num_rows == 0


def test_append_then_read_gives_the_same_values_and_types(manifest):
    rows = [row(), row("data/5/SIMULATED_00042.parquet", sha256=SHA_B)]
    manifest.append(rows)
    table = manifest.read()
    assert table.schema.equals(MANIFEST_SCHEMA)
    assert table.to_pylist() == rows


def test_two_runs_appending(manifest):
    manifest.append([row(run_id="run-1")])
    manifest.append([row("data/5/SIMULATED_00042.parquet", run_id="run-2")])
    assert manifest.read()["run_id"].to_pylist() == ["run-1", "run-2"]
    assert len(list(manifest.dir.glob("part-*.parquet"))) == 2


def test_failed_rows_are_not_landed(manifest):
    manifest.append([row(), row("data/5/SIMULATED_00042.parquet", status="FAILED", sha256=SHA_B)])
    assert manifest.landed_keys() == {("data/2/WELL-00002_20131104004101.parquet", SHA_A)}


def test_current_returns_the_newest_landing_of_a_path(manifest):
    # Newer chunk written first: current() goes by recorded_at, not by write order
    manifest.append([row(sha256=SHA_B, recorded_at=T2, run_id="run-2")])
    manifest.append([row(sha256=SHA_A, recorded_at=T1, run_id="run-1")])
    manifest.append([row(sha256="c" * 64, recorded_at=T2 + (T2 - T1), status="FAILED", run_id="run-3")])
    current = manifest.current()
    assert current.num_rows == 1
    assert current["sha256"].to_pylist() == [SHA_B]
    assert manifest.landed_keys() == {(row()["path"], SHA_A), (row()["path"], SHA_B)}


def test_crash_mid_write_leaves_nothing_readable(manifest, monkeypatch):
    manifest.append([row()])

    def crash(src, dst):
        raise OSError("simulated crash before the rename")

    monkeypatch.setattr(manifest_module.os, "replace", crash)
    with pytest.raises(OSError, match="simulated crash"):
        manifest.append([row("data/5/SIMULATED_00042.parquet")])

    assert len(list(manifest.dir.glob(".*.tmp"))) == 1  # the leftover temp file
    assert manifest.read()["path"].to_pylist() == [row()["path"]]


@pytest.mark.parametrize(
    "bad, match",
    [
        ({"status": "DOWNLOADED"}, "status must be LANDED or FAILED"),
        ({"label": "2"}, "don't fit the manifest schema"),  # wrong type
        ({"label": 40000}, "don't fit the manifest schema"),  # out of int16 range
        ({"run_id": None}, "'run_id' must not be null"),
        ({"sha265": SHA_A}, "unknown columns"),  # typo'd column
        ({"num_rows": "1000"}, "don't fit the manifest schema"),
        ({"landing_path": "/home/ubuntu/3w-data/landing/3w/x.parquet"}, "relative to storage.root"),
        ({"landing_path": "../outside/x.parquet"}, "relative to storage.root"),
        ({"landing_path": None}, "LANDED row needs a landing_path"),
        ({"error": "boom"}, "LANDED row needs a landing_path and no error"),
        ({"status": "FAILED", "error": None}, "FAILED row needs an error"),
        ({"status": "FAILED", "landing_path": "landing/3w/x.parquet"}, "FAILED row needs an error and no landing_path"),
    ],
)
def test_invalid_rows_are_refused(manifest, bad, match):
    with pytest.raises(ManifestError, match=match):
        manifest.append([row(**bad)])
    assert manifest.read().num_rows == 0


def test_landing_path_is_stored_relative(manifest):
    manifest.append([row(), row("data/5/SIMULATED_00042.parquet", sha256=SHA_B)])
    for landing_path in manifest.read()["landing_path"].to_pylist():
        assert not landing_path.startswith("/")


def test_step14_columns_are_nullable_and_round_trip(manifest):
    before = row()  # written before footer inspection existed: nulls
    after = row(
        "data/5/SIMULATED_00042.parquet", num_rows=61000, schema_fingerprint="f" * 16, schema_ok=True, codec="BROTLI"
    )
    manifest.append([before, after])
    assert manifest.read().to_pylist() == [before, after]


def test_missing_column_is_refused(manifest):
    incomplete = row()
    del incomplete["revision"]
    with pytest.raises(ManifestError, match="missing columns"):
        manifest.append([incomplete])
