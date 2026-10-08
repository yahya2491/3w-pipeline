"""Footer inspection against the schema contract, with small Parquet files written by the tests."""

import configparser
from pathlib import Path

import pytest

from threew.bronze.inspect import fingerprint, inspect_file, load_contract

FIXTURES = Path(__file__).parent / "fixtures"


def write(tmp_path: Path, data: bytes, name: str = "f.parquet") -> Path:
    path = tmp_path / name
    path.write_bytes(data)
    return path


def test_file_matching_the_contract(tmp_path, contract, parquet_bytes):
    inspection = inspect_file(write(tmp_path, parquet_bytes(num_rows=5)), contract)
    assert inspection.readable
    assert inspection.schema_ok
    assert inspection.num_rows == 5
    assert inspection.codec == "BROTLI"
    assert inspection.fingerprint == fingerprint(contract)
    assert (inspection.missing, inspection.extra, inspection.type_changes) == ((), (), ())


def test_codec_is_recorded_as_written(tmp_path, contract, parquet_bytes):
    assert inspect_file(write(tmp_path, parquet_bytes(compression="snappy")), contract).codec == "SNAPPY"


@pytest.mark.parametrize(
    "change, field, expected",
    [
        ("drop", "missing", ("P-PDG",)),
        ("add", "extra", ("NEW-SENSOR",)),
        ("retype", "type_changes", (("class", "int16", "int64"),)),
    ],
)
def test_drift_is_reported_in_the_right_list(tmp_path, contract, parquet_bytes, change, field, expected):
    columns = dict(contract)
    if change == "drop":
        del columns["P-PDG"]
    elif change == "add":
        columns["NEW-SENSOR"] = "double"
    else:
        columns["class"] = "int64"
    inspection = inspect_file(write(tmp_path, parquet_bytes(columns=columns)), contract)
    assert inspection.readable
    assert not inspection.schema_ok
    assert getattr(inspection, field) == expected
    assert inspection.fingerprint != fingerprint(contract)
    others = {"missing", "extra", "type_changes"} - {field}
    assert all(getattr(inspection, other) == () for other in others)


def test_column_order_does_not_change_the_fingerprint(tmp_path, contract, parquet_bytes):
    reordered = dict(reversed(list(contract.items())))
    a = inspect_file(write(tmp_path, parquet_bytes(), "a.parquet"), contract)
    b = inspect_file(write(tmp_path, parquet_bytes(columns=reordered), "b.parquet"), contract)
    assert a.fingerprint == b.fingerprint
    assert b.schema_ok


@pytest.mark.parametrize("data", [b"this is not parquet", b""], ids=["text", "empty"])
def test_non_parquet_file(tmp_path, contract, data):
    inspection = inspect_file(write(tmp_path, data), contract)  # no exception
    assert not inspection.readable
    assert not inspection.schema_ok
    assert inspection.num_rows is None
    assert inspection.fingerprint is None
    assert inspection.codec is None
    assert inspection.error


def test_contract_has_exactly_the_columns_of_dataset_ini(contract):
    # tests/fixtures/dataset.ini is upstream's data/dataset.ini; its [PARQUET_FILE_PROPERTIES] lists every column
    ini = configparser.ConfigParser(interpolation=None)  # values contain "%%"
    ini.optionxform = str  # keep the column names' case
    ini.read(FIXTURES / "dataset.ini")
    columns = list(ini["PARQUET_FILE_PROPERTIES"])
    assert len(columns) == 30
    assert set(contract) == set(columns)


def test_load_contract_refuses_a_file_without_columns(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text("name: nothing here\n")
    with pytest.raises(ValueError, match="columns"):
        load_contract(path)
