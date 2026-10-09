import dataclasses
import io

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from threew.bronze.inspect import load_contract
from threew.config import load_config
from threew.spark import get_spark

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

    def make(num_rows: int = 3, columns: dict[str, str] | None = None, compression: str = "brotli") -> bytes:
        columns = dict(contract) if columns is None else columns
        table = pa.table({name: pa.array(range(num_rows), type=ARROW_TYPES[t]) for name, t in columns.items()})
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression=compression)
        return buffer.getvalue()

    return make
