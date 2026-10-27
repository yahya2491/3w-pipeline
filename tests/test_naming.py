"""File-name parsing tests: real, simulated and drawn names, rejected names, and the path as the key."""

from datetime import datetime

import pytest

from threew.bronze.naming import InstanceInfo, NameParseError, SourceType, parse_all, parse_path


def test_real_file():
    info = parse_path("data/2/WELL-00002_20131104004101.parquet")
    assert info == InstanceInfo(
        path="data/2/WELL-00002_20131104004101.parquet",
        label=2,
        source_type=SourceType.REAL,
        well_id=2,
        instance_start=datetime(2013, 11, 4, 0, 41, 1),  # noqa: DTZ001  naive, like the parser's output
        instance_id="WELL-00002_20131104004101",
    )
    assert info.instance_start.tzinfo is None  # 3W doesn't say which timezone, so none is assumed


@pytest.mark.parametrize(
    "path, source_type, instance_id",
    [
        ("data/5/SIMULATED_00042.parquet", SourceType.SIMULATED, "SIMULATED_00042"),
        ("data/1/DRAWN_00003.parquet", SourceType.DRAWN, "DRAWN_00003"),
    ],
)
def test_simulated_and_drawn_files(path, source_type, instance_id):
    info = parse_path(path)
    assert info.source_type == source_type
    assert info.instance_id == instance_id
    assert info.well_id is None
    assert info.instance_start is None


@pytest.mark.parametrize(
    "path, reason",
    [
        ("dataset/2/WELL-00002_20131104004101.parquet", "not under data/"),
        ("data/10/WELL-00002_20131104004101.parquet", "not a single digit"),
        ("data/2/x/WELL-00002_20131104004101.parquet", "nested path"),
        ("data/2/WELL-00002_20131104004101.csv", "not a .parquet file"),
        ("data/2/TEST_00001.parquet", "unknown prefix"),
        ("data/2/WELL-00002_2013110400410.parquet", "malformed real name"),  # 13-digit timestamp
        ("data/2/WELL-00002_20131304004101.parquet", "invalid timestamp"),  # month 13
        ("data/2/WELL-00002_20130230004101.parquet", "invalid timestamp"),  # 30 February
        ("data/2/SIMULATED_0001.parquet", "malformed simulated name"),
        ("data/٢/SIMULATED_00001.parquet", "not a single digit"),  # non-ASCII digit
        ("data/2/SIMULATED_00001.parquet\n", "not a .parquet file"),  # "$" would have let this through
    ],
)
def test_rejected_names(path, reason):
    with pytest.raises(NameParseError, match=reason) as exc_info:
        parse_path(path)
    assert exc_info.value.path == path


def test_path_is_the_unique_key():
    a = parse_path("data/1/SIMULATED_00001.parquet")
    b = parse_path("data/2/SIMULATED_00001.parquet")
    assert a.instance_id == b.instance_id
    assert a.path != b.path


def test_parse_all_keeps_going_past_bad_names():
    paths = [
        "data/0/WELL-00001_20170201010207.parquet",
        "data/2/TEST_00001.parquet",
        "data/3/SIMULATED_00001.parquet",
        "data/dataset.ini",
        "data/9/DRAWN_00001.parquet",
    ]
    parsed, rejected = parse_all(paths)
    assert [p.path for p in parsed] == [paths[0], paths[2], paths[4]]
    assert [path for path, _ in rejected] == [paths[1], paths[3]]
    assert "unknown prefix" in rejected[0][1]
