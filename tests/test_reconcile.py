"""Reconciliation tests: a tiny Bronze table built with convert_file, Spark on one core."""

import dataclasses
import hashlib
import shutil
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql.types import LongType, StructField, StructType

from threew.bronze.convert import convert_file
from threew.bronze.manifest import Manifest
from threew.bronze.reconcile import expected_schema, reconcile, schema_differences
from threew.config import StorageConfig, load_config

START_NS = 1_383_525_661 * 1_000_000_000
FILES = {  # path -> rows
    "data/2/WELL-00002_20131104004101.parquet": 5,
    "data/5/SIMULATED_00001.parquet": 4,
    "data/1/DRAWN_00001.parquet": 3,
}


def source_table(contract: dict[str, str], n: int) -> pa.Table:
    columns = {}
    for name, type_ in contract.items():
        if type_ == "double":
            values = [float(i) + 0.25 for i in range(n)]
            if name == "P-PDG":
                values[1] = None  # a sensor gap
            if name == "QGL":
                values[0] = float("nan")  # NaN is a value, not a null
            columns[name] = pa.array(values, pa.float64())
        elif type_ == "int16":
            columns[name] = pa.array([None] + [2] * (n - 1), pa.int16())  # first row unlabeled
        else:
            columns[name] = pa.array([START_NS + i * 1_000_000_000 for i in range(n)], pa.timestamp("ns"))
    return pa.table(columns)


def manifest_row(path: str, sha256: str, num_rows: int, recorded_at=None) -> dict:
    label, name = int(path.split("/")[1]), path.rsplit("/", 1)[1]
    source_type = "REAL" if name.startswith("WELL-") else name.split("_")[0]
    return {
        "run_id": "r1",
        "path": path,
        "revision": "c1a7e8d0cfefc0875974ea079d11349a37fa0c56",
        "label": label,
        "source_type": source_type,
        "well_id": 2 if source_type == "REAL" else None,
        "instance_start": datetime(2013, 11, 4, 0, 41, 1) if source_type == "REAL" else None,  # noqa: DTZ001
        "instance_id": name.removesuffix(".parquet"),
        "size": 100,
        "sha256": sha256,
        "blob_id": "b",
        "landing_path": f"landing/3w/ingest_date=2026-10-08/run_id=r1/label={label}/{name}",
        "status": "LANDED",
        "error": None,
        "num_rows": num_rows,
        "schema_fingerprint": "f59f09e498a1352e",
        "schema_ok": True,
        "codec": "BROTLI",
        "recorded_at": recorded_at or datetime(2026, 10, 8, 23, 51, tzinfo=UTC),
    }


def land_and_convert(cfg, contract, path: str, n: int) -> tuple[dict, object]:
    row = manifest_row(path, "", n)
    landed = cfg.storage.root / row["landing_path"]
    landed.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(source_table(contract, n), landed, compression="brotli")
    row["sha256"] = hashlib.sha256(landed.read_bytes()).hexdigest()
    result = convert_file(landed, row, cfg.storage.bronze)
    assert result.error is None
    return row, result.output_path


@pytest.fixture
def env(tmp_path, contract):
    cfg = dataclasses.replace(load_config("dev"), storage=StorageConfig(root=tmp_path))
    manifest = Manifest(cfg.storage.state)
    rows, outputs = {}, {}
    for path, n in FILES.items():
        rows[path], outputs[path] = land_and_convert(cfg, contract, path, n)
    manifest.append(list(rows.values()))
    return cfg, manifest, rows, outputs


def failed_checks(result) -> set[int]:
    return {c.number for c in result.checks if not c.passed}


def check(result, number: int):
    return next(c for c in result.checks if c.number == number)


REAL, SIMULATED, DRAWN = FILES


def test_clean_table_passes_every_check(spark, env):
    cfg, manifest, _, _ = env
    result = reconcile(spark, cfg, manifest)
    assert failed_checks(result) == set(), [c for c in result.checks if not c.passed]
    assert check(result, 2).details == {"spark": 12, "manifest": 12}
    assert {f["path"] for f in check(result, 6).details["files"]} == set(FILES)  # one of each source type
    assert "BroadcastHashJoin" in result.plan
    assert result.to_dict()["plan_has_broadcast_hash_join"] is True


def test_deleted_output_file_is_missing(spark, env):
    cfg, manifest, rows, outputs = env
    outputs[REAL].unlink()
    result = reconcile(spark, cfg, manifest)
    assert 3 in failed_checks(result)
    assert check(result, 3).details["missing_files"] == [[REAL, rows[REAL]["sha256"]]]


def test_output_file_not_in_the_manifest_is_extra(spark, env, contract):
    cfg, manifest, _, _ = env
    stray, _ = land_and_convert(cfg, contract, "data/5/SIMULATED_00002.parquet", 2)  # converted, never recorded
    result = reconcile(spark, cfg, manifest)
    assert 3 in failed_checks(result)
    assert check(result, 3).details["extra_files"] == [[stray["path"], stray["sha256"]]]


def test_manifest_row_count_mismatch(spark, env):
    cfg, manifest, rows, _ = env
    corrected = rows[REAL] | {"num_rows": 6, "recorded_at": datetime(2026, 10, 9, tzinfo=UTC)}
    manifest.append([corrected])  # current() now says 6 rows, Bronze has 5
    result = reconcile(spark, cfg, manifest)
    assert 3 in failed_checks(result)
    assert check(result, 3).details["mismatched_files"] == [
        {"file": [REAL, rows[REAL]["sha256"]], "spark": 5, "manifest": 6}
    ]


def test_file_in_the_wrong_label_folder(spark, env):
    cfg, manifest, _, outputs = env
    wrong = cfg.storage.bronze / "source_type=REAL" / "label=5" / outputs[REAL].name
    wrong.parent.mkdir(parents=True)
    shutil.move(outputs[REAL], wrong)
    result = reconcile(spark, cfg, manifest)
    assert 4 in failed_checks(result)
    [problem] = check(result, 4).details["files"]
    assert (problem["found"], problem["manifest"]) == ([("REAL", 5)], ("REAL", 2))


def test_duplicated_row(spark, env):
    cfg, manifest, _, outputs = env
    table = pq.read_table(outputs[SIMULATED])
    pq.write_table(pa.concat_tables([table, table.slice(1, 1)]), outputs[SIMULATED], compression="snappy")
    result = reconcile(spark, cfg, manifest)
    assert 5 in failed_checks(result)
    [problem] = check(result, 5).details["files"]
    assert (problem["rows"], problem["distinct"], problem["max"]) == (5, 4, 3)


def test_changed_value(spark, env):
    cfg, manifest, _, outputs = env
    table = pq.read_table(outputs[DRAWN])
    values = table["P-ANULAR"].to_pylist()
    values[1] = 999.0
    table = table.set_column(table.schema.get_field_index("P-ANULAR"), "P-ANULAR", pa.array(values, pa.float64()))
    pq.write_table(table, outputs[DRAWN], compression="snappy")
    result = reconcile(spark, cfg, manifest)
    assert failed_checks(result) == {6}  # counts, partitions and row numbers are all still right
    [difference] = check(result, 6).details["differences"]
    assert (difference["path"], difference["column"], difference["row"], difference["bronze"]) == (
        DRAWN,
        "P-ANULAR",
        1,
        "999.0",
    )


def test_schema_with_class_as_bigint_fails(contract):
    expected = expected_schema(contract)
    actual = StructType([StructField(f.name, LongType()) if f.name == "class" else f for f in expected])
    assert schema_differences(actual, expected) == ["class: expected smallint, got bigint"]
    assert schema_differences(expected, expected) == []
