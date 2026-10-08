"""Smoke test: the environment is the pinned one and Spark can run a job."""

import os
import sys

import pyspark
import pytest
from pyspark.sql import SparkSession


def test_python_version():
    assert sys.version_info[:2] == (3, 11)


def test_pyspark_version():
    assert pyspark.__version__ == "3.5.0"


def test_imports():
    import huggingface_hub  # noqa: F401
    import pyarrow  # noqa: F401
    import yaml  # noqa: F401

    import threew  # noqa: F401  (src/ layout: only importable once the package is installed)


@pytest.fixture(scope="module")
def spark():
    # Spark starts Python workers with `python3` from PATH unless told otherwise; outside an activated
    # venv that is the system Python (3.12), which fails with PYTHON_VERSION_MISMATCH.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = SparkSession.builder.master("local[1]").appName("smoke").getOrCreate()
    yield session
    session.stop()


def test_spark_job(spark):
    df = spark.createDataFrame([(0, "normal"), (1, "event"), (101, "transient")], ["class", "name"])
    assert df.filter(df["class"] > 0).count() == 2
