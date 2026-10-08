"""The one place Spark sessions are created, so every job gets the same, correct setup."""

import os
import sys

from pyspark.sql import SparkSession

from threew.config import Config


def get_spark(cfg: Config) -> SparkSession:
    """Spark session configured from cfg.spark, with Python workers running this interpreter.

    Settings like driver memory only take effect if this starts the first session in the process;
    getOrCreate() returns a session that is already running.
    """
    # PySpark 3.5 starts Python workers with $PYSPARK_PYTHON (default: `python3` on PATH) and ignores
    # spark.pyspark.python for sessions built from Python, so the environment variable is what counts.
    # Without it, running the venv's python without activating the venv gives workers the system Python.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    return (
        SparkSession.builder.appName(cfg.spark.app_name)
        .master(cfg.spark.master)
        .config("spark.driver.memory", cfg.spark.driver_memory)
        .config("spark.sql.shuffle.partitions", cfg.spark.shuffle_partitions)
        .config("spark.pyspark.python", sys.executable)  # used by spark-submit, shown in the Spark UI
        .getOrCreate()
    )
