"""Shared fixtures: one small Spark session, the schema contract and Parquet files built like the real ones."""

import dataclasses
import io

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from threew.bronze.inspect import load_contract
from threew.config import load_config
from threew.spark import get_spark

START_NS = 1_383_525_661 * 1_000_000_000  # 2013-11-04 00:41:01
ARROW_TYPES = {
    "double": pa.float64(),
    "int16": pa.int16(),
    "int64": pa.int64(),
    "timestamp[ns]": pa.timestamp("ns"),
}


@pytest.fixture(scope="session")
def spark_cfg():
    """The dev config with a small Spark: one core, two shuffle partitions."""
    cfg = load_config("dev")
    return dataclasses.replace(cfg, spark=dataclasses.replace(cfg.spark, master="local[1]", shuffle_partitions=2))


@pytest.fixture(scope="session")
def spark(spark_cfg):
    """One Spark session for the whole test run, built the same way the jobs build theirs."""
    session = get_spark(spark_cfg)
    yield session
    session.stop()


@pytest.fixture(scope="session")
def contract() -> dict[str, str]:
    return load_contract(load_config("dev").schema.contract)


@pytest.fixture
def parquet_bytes(contract):
    """make(num_rows, columns, compression) -> bytes of a small Parquet file; columns default to the contract's."""

    def make(
        num_rows: int = 3,
        columns: dict[str, str] | None = None,
        compression: str = "brotli",
        timestamp_step_ns: int = 1_000_000_000,
    ) -> bytes:
        """Timestamps one second apart like 3W's; a step below 1000 ns makes them unconvertible to microseconds."""
        columns = dict(contract) if columns is None else columns
        arrays = {}
        for name, t in columns.items():
            values = range(num_rows)
            if t == "timestamp[ns]":
                values = [START_NS + i * timestamp_step_ns for i in values]
            arrays[name] = pa.array(values, type=ARROW_TYPES[t])
        table = pa.table(arrays)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression=compression)
        return buffer.getvalue()

    return make
