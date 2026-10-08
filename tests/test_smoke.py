"""Smoke test: the environment is the pinned one and Spark can run a job."""

import sys

import pyspark


def test_python_version():
    assert sys.version_info[:2] == (3, 11)


def test_pyspark_version():
    assert pyspark.__version__ == "3.5.0"


def test_imports():
    import huggingface_hub  # noqa: F401
    import pyarrow  # noqa: F401
    import yaml  # noqa: F401

    import threew  # noqa: F401  (src/ layout: only importable once the package is installed)


def test_spark_job(spark):  # session from conftest.py, built by threew.spark.get_spark
    df = spark.createDataFrame([(0, "normal"), (1, "event"), (101, "transient")], ["class", "name"])
    assert df.filter(df["class"] > 0).count() == 2
