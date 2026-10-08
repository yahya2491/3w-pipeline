import os
import sys

from threew.config import load_config
from threew.spark import get_spark


def test_session_uses_config(spark):
    cfg = load_config("dev")
    assert spark.sparkContext.master == cfg.spark.master
    assert spark.sparkContext.appName == cfg.spark.app_name
    assert spark.conf.get("spark.driver.memory") == cfg.spark.driver_memory
    assert spark.conf.get("spark.sql.shuffle.partitions") == str(cfg.spark.shuffle_partitions)


def test_workers_run_this_python(spark):
    assert spark.conf.get("spark.pyspark.python") == sys.executable
    # The lambda runs in a Python worker process, which must be the same interpreter as the driver
    worker_python = spark.sparkContext.parallelize([0], 1).map(lambda _: __import__("sys").executable).first()
    assert worker_python == sys.executable


def test_overrides_a_wrong_pyspark_python(spark, monkeypatch):
    monkeypatch.setenv("PYSPARK_PYTHON", "/usr/bin/python3")
    get_spark(load_config("dev"))
    assert os.environ["PYSPARK_PYTHON"] == sys.executable
