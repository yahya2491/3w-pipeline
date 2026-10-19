import os
import sys

import pytest

from threew.spark import cleanup_spark, get_spark


def test_cleanup_failure_propagates_without_original_error():
    def fail():
        raise RuntimeError("cleanup failed")

    with pytest.raises(RuntimeError, match="cleanup failed"):
        cleanup_spark(fail, "test")


def test_cleanup_preserves_original_failure(caplog):
    def fail():
        raise RuntimeError("cleanup failed")

    with pytest.raises(ValueError, match="original failure"):
        try:
            raise ValueError("original failure")
        finally:
            cleanup_spark(fail, "test")
    assert "preserving original error" in caplog.text


def test_session_uses_config(spark, spark_cfg):
    cfg = spark_cfg
    assert spark.sparkContext.master == cfg.spark.master
    assert spark.sparkContext.appName == cfg.spark.app_name
    assert spark.conf.get("spark.driver.memory") == cfg.spark.driver_memory
    assert spark.conf.get("spark.sql.shuffle.partitions") == str(cfg.spark.shuffle_partitions)
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


def test_workers_run_this_python(spark):
    assert spark.conf.get("spark.pyspark.python") == sys.executable
    # The lambda runs in a Python worker process, which must be the same interpreter as the driver
    worker_python = spark.sparkContext.parallelize([0], 1).map(lambda _: __import__("sys").executable).first()
    assert worker_python == sys.executable


def test_overrides_a_wrong_pyspark_python(spark, spark_cfg, monkeypatch):
    monkeypatch.setenv("PYSPARK_PYTHON", "/usr/bin/python3")
    get_spark(spark_cfg)
    assert os.environ["PYSPARK_PYTHON"] == sys.executable
