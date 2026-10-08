"""Plan tests: pure inputs, no network, no Spark."""

import random
from collections import Counter, defaultdict
from datetime import UTC, datetime

import pyarrow as pa
import pytest

from threew.bronze.manifest import MANIFEST_SCHEMA, Manifest
from threew.bronze.naming import parse_path
from threew.bronze.plan import Reason, build_plan, content_key
from threew.source.hub import RemoteFile

REVISION = "c1a7e8d0cfefc0875974ea079d11349a37fa0c56"


def remote(path, sha256="default", blob_id="blob", size=100):
    return RemoteFile(path=path, size=size, sha256=f"sha-{path}" if sha256 == "default" else sha256, blob_id=blob_id)


def manifest_row(file: RemoteFile, status="LANDED", **overrides):
    info = parse_path(file.path)
    values = {
        "run_id": "run-1",
        "path": file.path,
        "revision": REVISION,
        "label": info.label,
        "source_type": info.source_type,
        "well_id": info.well_id,
        "instance_start": info.instance_start,
        "instance_id": info.instance_id,
        "size": file.size,
        "sha256": file.sha256,
        "blob_id": file.blob_id,
        "landing_path": f"landing/3w/{file.path}" if status == "LANDED" else None,
        "status": status,
        "error": "HTTP 503" if status == "FAILED" else None,
        "num_rows": None,
        "schema_fingerprint": None,
        "schema_ok": None,
        "codec": None,
        "recorded_at": datetime(2026, 10, 9, tzinfo=UTC),
    }
    return values | overrides


def manifest_of(*rows):
    return pa.Table.from_pylist(list(rows), schema=MANIFEST_SCHEMA)


EMPTY = MANIFEST_SCHEMA.empty_table()
LISTING = [
    remote("data/0/WELL-00001_20170201010207.parquet"),
    remote("data/2/SIMULATED_00001.parquet"),
    remote("data/9/DRAWN_00001.parquet"),
]


def reasons(plan):
    return {candidate.path: reason for candidate, reason in plan.to_download}


def test_empty_manifest_everything_is_new():
    plan = build_plan(LISTING, EMPTY, sample_per_class=None)
    assert reasons(plan) == {f.path: Reason.NEW for f in LISTING}
    assert plan.unchanged == 0


def test_everything_landed_with_same_checksums_nothing_to_download():
    plan = build_plan(LISTING, manifest_of(*(manifest_row(f) for f in LISTING)), sample_per_class=None)
    assert plan.to_download == ()
    assert plan.unchanged == 3


def test_one_changed_checksum():
    landed = [manifest_row(f) for f in LISTING]
    landed[1]["sha256"] = "an-older-sha256"
    plan = build_plan(LISTING, manifest_of(*landed), sample_per_class=None)
    assert reasons(plan) == {LISTING[1].path: Reason.CHANGED}
    assert plan.unchanged == 2


def test_a_file_that_failed_before_is_new(tmp_path):
    manifest = Manifest(tmp_path)
    manifest.append([manifest_row(LISTING[0]), manifest_row(LISTING[1], status="FAILED")])
    plan = build_plan(LISTING, manifest.current(), sample_per_class=None)
    assert reasons(plan) == {LISTING[1].path: Reason.NEW, LISTING[2].path: Reason.NEW}
    # Same answer if FAILED rows are passed in directly
    direct = build_plan(LISTING, manifest_of(manifest_row(LISTING[1], status="FAILED")), sample_per_class=None)
    assert reasons(direct)[LISTING[1].path] == Reason.NEW


def test_manifest_path_missing_from_listing_is_removed_upstream():
    gone = remote("data/5/SIMULATED_00099.parquet")
    plan = build_plan(LISTING, manifest_of(manifest_row(gone)), sample_per_class=None)
    assert plan.removed_upstream == (gone.path,)
    assert gone.path not in reasons(plan)
    assert plan.summary()["removed_upstream"] == 1


def test_file_without_sha256_is_compared_by_blob_id():
    file = remote("data/2/SIMULATED_00001.parquet", sha256=None, blob_id="abc")
    assert content_key(file.sha256, file.blob_id) == "blob:abc"
    same = build_plan([file], manifest_of(manifest_row(file)), sample_per_class=None)
    assert same.unchanged == 1
    other_blob = build_plan([file], manifest_of(manifest_row(file, blob_id="def")), sample_per_class=None)
    assert reasons(other_blob) == {file.path: Reason.CHANGED}


def test_rejected_names_are_reported_not_downloaded():
    plan = build_plan([*LISTING, remote("data/2/TEST_00001.parquet")], EMPTY, sample_per_class=None)
    assert plan.rejected == (("data/2/TEST_00001.parquet", "unknown prefix, expected WELL-, SIMULATED_ or DRAWN_"),)
    assert len(plan.to_download) == 3


def test_empty_listing_raises():
    with pytest.raises(ValueError, match="listing is empty"):
        build_plan([], EMPTY, sample_per_class=None)


def test_read_instead_of_current_is_refused():
    twice = manifest_of(manifest_row(LISTING[0]), manifest_row(LISTING[0], sha256="newer"))
    with pytest.raises(ValueError, match="Manifest.current"):
        build_plan(LISTING, twice, sample_per_class=None)


# Sampling. Class 0: 3 REAL + 3 SIMULATED; class 1: 4 SIMULATED + 1 DRAWN; class 2: only 2 files;
# class 3: 2 of each type
SAMPLING_LISTING = [
    remote("data/0/SIMULATED_00001.parquet"),
    remote("data/0/SIMULATED_00002.parquet"),
    remote("data/0/SIMULATED_00003.parquet"),
    remote("data/0/WELL-00009_20180101000000.parquet"),
    remote("data/0/WELL-00007_20170201010207.parquet"),
    remote("data/0/WELL-00001_20140101000000.parquet"),
    remote("data/1/DRAWN_00001.parquet"),
    remote("data/1/SIMULATED_00004.parquet"),
    remote("data/1/SIMULATED_00003.parquet"),
    remote("data/1/SIMULATED_00002.parquet"),
    remote("data/1/SIMULATED_00001.parquet"),
    remote("data/2/SIMULATED_00001.parquet"),
    remote("data/2/DRAWN_00001.parquet"),
    remote("data/3/DRAWN_00002.parquet"),
    remote("data/3/DRAWN_00001.parquet"),
    remote("data/3/SIMULATED_00002.parquet"),
    remote("data/3/SIMULATED_00001.parquet"),
    remote("data/3/WELL-00004_20160101000000.parquet"),
    remote("data/3/WELL-00003_20150101000000.parquet"),
]


def full_listing():
    """10 classes like the real data: REAL and SIMULATED everywhere, DRAWN in the even classes only."""
    files = []
    for label in range(10):
        files += [remote(f"data/{label}/WELL-{label + 1:05d}_201702{day:02d}010207.parquet") for day in range(1, 5)]
        files += [remote(f"data/{label}/SIMULATED_{n:05d}.parquet") for n in range(1, 5)]
        if label % 2 == 0:
            files += [remote(f"data/{label}/DRAWN_{n:05d}.parquet") for n in range(1, 3)]
    return files


def sample(listing, per_class=3):
    return [candidate for candidate, _ in build_plan(listing, EMPTY, per_class).to_download]


def sampled_paths(listing, per_class=3):
    return [candidate.path for candidate in sample(listing, per_class)]


def types_per_class(candidates):
    types = defaultdict(set)
    for c in candidates:
        types[c.info.label].add(c.info.source_type)
    return types


def test_sampling_is_deterministic():
    for listing in (SAMPLING_LISTING, full_listing()):
        expected = sampled_paths(listing)
        for seed in range(5):
            shuffled = list(listing)
            random.Random(seed).shuffle(shuffled)
            assert sampled_paths(shuffled) == expected


def test_sampling_cycles_through_the_types():
    assert build_plan(SAMPLING_LISTING, EMPTY, 3).summary()["files_per_class"] == {0: 3, 1: 3, 2: 2, 3: 3}
    assert sampled_paths(SAMPLING_LISTING) == [
        # Class 0, rounds: REAL, SIMULATED | REAL. The old "REAL first" rule would have taken all 3 REAL files
        "data/0/SIMULATED_00001.parquet",
        "data/0/WELL-00001_20140101000000.parquet",
        "data/0/WELL-00007_20170201010207.parquet",
        # Class 1, no REAL: SIMULATED, DRAWN | SIMULATED
        "data/1/DRAWN_00001.parquet",
        "data/1/SIMULATED_00001.parquet",
        "data/1/SIMULATED_00002.parquet",
        # Class 2 only has 2 files: both
        "data/2/DRAWN_00001.parquet",
        "data/2/SIMULATED_00001.parquet",
        # Class 3: one of each type, the first by path
        "data/3/DRAWN_00001.parquet",
        "data/3/SIMULATED_00001.parquet",
        "data/3/WELL-00003_20150101000000.parquet",
    ]


def test_sample_covers_every_type_a_class_has():
    for listing in (SAMPLING_LISTING, full_listing()):
        assert types_per_class(sample(listing)) == types_per_class(sample(listing, per_class=None))


def test_full_listing_samples_30_files():
    picked = sample(full_listing())
    assert len(picked) == 30
    assert Counter(c.info.label for c in picked) == {label: 3 for label in range(10)}


def test_sampled_out_files_are_not_removed_upstream():
    # Landed in an earlier full run; this dev run samples 1 per class, but the files still exist upstream
    landed = manifest_of(*(manifest_row(f) for f in SAMPLING_LISTING))
    plan = build_plan(SAMPLING_LISTING, landed, 1)
    assert plan.removed_upstream == ()
    assert plan.unchanged == 4  # one per class


def test_summary():
    landed = manifest_of(manifest_row(LISTING[0]), manifest_row(LISTING[1], sha256="old"))
    s = build_plan(LISTING, landed, sample_per_class=None).summary()
    assert s == {
        "new": 1,
        "changed": 1,
        "unchanged": 1,
        "removed_upstream": 0,
        "rejected": 0,
        "files_to_download": 2,
        "bytes_to_download": 200,
        "files_per_class": {2: 1, 9: 1},
    }
