"""Conversion tests, with small files written like the real ones: Brotli, nanosecond timestamps, some nulls."""

import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from threew.bronze import convert as convert_module
from threew.bronze.convert import LINEAGE_COLUMNS, ConvertStatus, convert_all, convert_file
from threew.bronze.manifest import Manifest
from threew.config import StorageConfig

NS_PER_S = 1_000_000_000
START_NS = 1_383_525_661 * NS_PER_S  # 2013-11-04 00:41:01, one row per second like 3W
PATH = "data/2/WELL-00002_20131104004101.parquet"


def real_like_table(contract: dict[str, str], n: int = 5, sub_microsecond: bool = False) -> pa.Table:
    columns = {}
    for name, type_ in contract.items():
        if type_ == "double":
            values = [float(i) * 1.5 for i in range(n)]
            if name == "P-PDG":
                values[1] = None  # sensors have gaps
            columns[name] = pa.array(values, pa.float64())
        elif type_ == "int16":
            columns[name] = pa.array([None if i == 0 else 2 for i in range(n)], pa.int16())  # unlabeled first row
        else:
            nanos = [START_NS + i * NS_PER_S + (1 if sub_microsecond and i == 2 else 0) for i in range(n)]
            columns[name] = pa.array(nanos, pa.timestamp("ns"))
    return pa.table(columns)


def write_landed(tmp_path: Path, table: pa.Table) -> tuple[Path, str]:
    path = tmp_path / "landing" / "WELL-00002_20131104004101.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, compression="brotli")
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def manifest_row(sha256: str, path: str = PATH, schema_ok: bool | None = True, **overrides) -> dict:
    label = int(path.split("/")[1])
    real = "WELL-" in path
    values = {
        "run_id": "20261008T235040Z-664edd",
        "path": path,
        "revision": "c1a7e8d0cfefc0875974ea079d11349a37fa0c56",
        "label": label,
        "source_type": "REAL" if real else "SIMULATED",
        "well_id": 2 if real else None,
        "instance_start": datetime(2013, 11, 4, 0, 41, 1) if real else None,  # noqa: DTZ001  naive by design
        "instance_id": path.rsplit("/", 1)[1].removesuffix(".parquet"),
        "size": 100,
        "sha256": sha256,
        "blob_id": "b",
        "landing_path": f"landing/3w/ingest_date=2026-10-08/run_id=r1/label={label}/{path.rsplit('/', 1)[1]}",
        "status": "LANDED",
        "error": None,
        "num_rows": 5,
        "schema_fingerprint": "f59f09e498a1352e",
        "schema_ok": schema_ok,
        "codec": "BROTLI",
        "recorded_at": datetime(2026, 10, 8, 23, 51, tzinfo=UTC),
    }
    return values | overrides


def test_one_file_converts(tmp_path, contract):
    source = real_like_table(contract)
    landed, sha = write_landed(tmp_path, source)
    result = convert_file(landed, manifest_row(sha), tmp_path / "bronze")

    assert result.status is ConvertStatus.OK, result.error
    assert result.rows_in == result.rows_out == 5
    assert result.output_bytes == result.output_path.stat().st_size
    assert result.input_bytes == landed.stat().st_size
    out = pq.read_table(result.output_path)

    assert out.column_names == [*contract, *LINEAGE_COLUMNS]  # original names and order, then lineage
    for name in contract:
        if name != "timestamp":
            assert out[name].type == source[name].type
            assert out[name].to_pylist() == source[name].to_pylist()  # values and nulls
    assert out["timestamp"].type == pa.timestamp("us")
    assert out["timestamp"].cast(pa.int64()).to_pylist() == [
        v // 1000 for v in source["timestamp"].cast(pa.int64()).to_pylist()
    ]
    assert out["timestamp"].to_pylist() == source["timestamp"].cast(pa.timestamp("us")).to_pylist()  # same instants

    assert out["_source_path"].to_pylist() == [PATH] * 5
    assert out["_file_sha256"].to_pylist() == [sha] * 5
    assert out["_ingest_run_id"].to_pylist() == ["20261008T235040Z-664edd"] * 5
    assert out["_row_in_file"].to_pylist() == [0, 1, 2, 3, 4]
    assert out["_row_in_file"].type == pa.int64()
    assert out.schema.metadata is None  # the stale pandas metadata is gone


def test_output_path_has_partition_folders_and_no_partition_columns(tmp_path, contract):
    landed, sha = write_landed(tmp_path, real_like_table(contract))
    out_dir = tmp_path / "bronze"
    result = convert_file(landed, manifest_row(sha), out_dir)
    assert result.output_path == out_dir / "source_type=REAL" / "label=2" / f"{sha[:16]}.parquet"
    names = pq.read_schema(result.output_path).names
    assert "source_type" not in names
    assert "label" not in names


def test_converting_twice_gives_the_same_file(tmp_path, contract):
    landed, sha = write_landed(tmp_path, real_like_table(contract))
    first = convert_file(landed, manifest_row(sha), tmp_path / "bronze")
    first_bytes = first.output_path.read_bytes()
    second = convert_file(landed, manifest_row(sha), tmp_path / "bronze")
    assert second.output_path == first.output_path
    assert second.output_path.read_bytes() == first_bytes
    assert len(list((tmp_path / "bronze").rglob("*"))) == 3  # two partition folders and one file


def test_sub_microsecond_timestamp_fails_and_writes_nothing(tmp_path, contract):
    landed, sha = write_landed(tmp_path, real_like_table(contract, sub_microsecond=True))
    result = convert_file(landed, manifest_row(sha), tmp_path / "bronze")
    assert result.status is ConvertStatus.FAILED
    assert "microseconds without losing precision" in result.error
    assert result.output_path is None
    assert not (tmp_path / "bronze").exists() or not any(p.is_file() for p in (tmp_path / "bronze").rglob("*"))


def test_crash_while_writing_leaves_no_half_file(tmp_path, contract, monkeypatch):
    landed, sha = write_landed(tmp_path, real_like_table(contract))

    def crash(table, where, **kwargs):
        where.write(b"PAR1 half a file")  # some bytes reach the disk, then the write dies
        raise OSError("disk full")

    monkeypatch.setattr(convert_module.pq, "write_table", crash)
    result = convert_file(landed, manifest_row(sha), tmp_path / "bronze")
    assert result.status is ConvertStatus.FAILED
    assert "disk full" in result.error
    files = [p for p in (tmp_path / "bronze").rglob("*") if p.is_file()]
    assert all(p.name.startswith(".") and p.name.endswith(".tmp") for p in files)  # at most a temporary file
    assert not (tmp_path / "bronze" / "source_type=REAL" / "label=2" / f"{sha[:16]}.parquet").exists()


def test_without_a_manifest_sha256_the_file_is_hashed(tmp_path, contract):
    landed, sha = write_landed(tmp_path, real_like_table(contract))
    result = convert_file(landed, manifest_row(None), tmp_path / "bronze")
    assert result.output_path.name == f"{sha[:16]}.parquet"
    assert set(pq.read_table(result.output_path)["_file_sha256"].to_pylist()) == {sha}


def test_convert_all_skips_files_that_dont_match_the_contract(tmp_path, contract):
    storage = StorageConfig(root=tmp_path)
    manifest = Manifest(storage.state)
    rows = []
    for path, schema_ok in [
        (PATH, True),
        ("data/5/SIMULATED_00001.parquet", True),
        ("data/5/SIMULATED_00002.parquet", False),
    ]:
        row = manifest_row("0" * 64, path=path, schema_ok=schema_ok)
        landed = storage.root / row["landing_path"]
        landed.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(real_like_table(contract), landed, compression="brotli")
        row["sha256"] = hashlib.sha256(landed.read_bytes()).hexdigest()
        rows.append(row)
    manifest.append(rows)

    results = {r.source_path: r for r in convert_all(manifest, storage, workers=2)}
    assert results[PATH].status is ConvertStatus.OK
    assert results["data/5/SIMULATED_00001.parquet"].status is ConvertStatus.OK
    skipped = results["data/5/SIMULATED_00002.parquet"]
    assert (skipped.status, skipped.error) == (ConvertStatus.SKIPPED, "schema_ok is false")
    assert sorted(p.relative_to(storage.bronze).parts[:2] for p in storage.bronze.rglob("*.parquet")) == [
        ("source_type=REAL", "label=2"),
        ("source_type=SIMULATED", "label=5"),
    ]


def test_spark_reads_the_bronze_table_with_partition_columns(tmp_path, contract, spark):
    landed, sha = write_landed(tmp_path, real_like_table(contract))
    out_dir = tmp_path / "bronze"
    convert_file(landed, manifest_row(sha), out_dir)
    df = spark.read.parquet(str(out_dir))
    assert df.columns[-2:] == ["source_type", "label"]  # from the folders, no duplicate column
    assert df.count() == 5
    row = df.orderBy("_row_in_file").collect()[1]
    assert (row["source_type"], row["label"], row["class"], row["P-PDG"]) == ("REAL", 2, 2, None)
