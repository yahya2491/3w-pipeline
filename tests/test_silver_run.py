import json
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql.types import StructField, StructType

from threew.bronze.manifest import Manifest
from threew.config import StorageConfig
from threew.silver.run import run_silver
from threew.silver.transform import BRONZE_TYPES, SENSOR_RENAMES


@contextmanager
def _existing_spark_session(spark):
    yield spark


def test_run_silver_writes_and_reconciles_all_three_outputs(spark, spark_cfg, tmp_path):
    cfg = replace(spark_cfg, storage=StorageConfig(root=tmp_path))
    source_path = "data/1/WELL-00001.parquet"
    file_hash = "a" * 64
    start = datetime.fromisoformat("2020-01-01T00:00:00")
    rows = [
        {
            "source_type": "REAL",
            "label": 1,
            "class": 1,
            "state": 1,
            "timestamp": start + timedelta(seconds=index),
            "_source_path": source_path,
            "_file_sha256": file_hash,
            "_ingest_run_id": "bronze-test",
            "_row_in_file": index,
            **{sensor: float(index) for sensor in SENSOR_RENAMES},
        }
        for index in range(2)
    ]
    schema = StructType([StructField(name, data_type, nullable=True) for name, data_type in BRONZE_TYPES.items()])
    spark.createDataFrame(rows, schema).write.parquet(str(cfg.storage.bronze))

    Manifest(cfg.storage.state).append(
        [
            {
                "run_id": "bronze-test",
                "path": source_path,
                "revision": cfg.source.revision,
                "label": 1,
                "source_type": "REAL",
                "well_id": 1,
                "instance_start": start,
                "instance_id": "WELL-00001",
                "size": 100,
                "sha256": file_hash,
                "blob_id": "blob-test",
                "landing_path": "landing/data/1/WELL-00001.parquet",
                "status": "LANDED",
                "error": None,
                "num_rows": 2,
                "schema_fingerprint": "test-schema",
                "schema_ok": True,
                "codec": "BROTLI",
                "recorded_at": datetime.now(UTC),
            }
        ]
    )
    cfg.storage.bronze_ready.parent.mkdir(parents=True, exist_ok=True)
    cfg.storage.bronze_ready.write_text(
        json.dumps(
            {
                "env": "dev",
                "bronze_table": str(cfg.storage.bronze),
                "run_id": "bronze-test",
                "source": {"repo_id": cfg.source.repo_id, "revision": cfg.source.revision},
                "reconcile": {"passed": True},
            }
        ),
        encoding="utf-8",
    )

    marker = run_silver(cfg, spark_factory=lambda _cfg: _existing_spark_session(spark))
    checks = marker["reconcile"]["checks"]

    assert all(checks.values())
    assert marker["reconcile"]["counts"] == {
        "bronze_rows": 2,
        "silver_observation_rows": 2,
        "bronze_recordings": 1,
        "silver_recordings": 1,
        "instance_quality_rows": 1,
        "sensor_availability_rows": 27,
    }
    assert cfg.storage.silver_ready.is_file()
    report = json.loads(cfg.storage.silver_runs.joinpath(f"{marker['run_id']}.json").read_text(encoding="utf-8"))
    assert report["reconcile"]["passed"] is True
    tables = marker["tables"]
    saved_observations = spark.read.parquet(tables["silver_observations"]["path"])
    assert saved_observations.select("gas_lift_choke_opening_pct").first()[0] == 0.0
    assert spark.read.parquet(tables["instance_quality"]["path"]).count() == 1
    assert spark.read.parquet(tables["sensor_availability"]["path"]).count() == 27


def test_run_silver_refuses_production_before_reading_or_writing(spark_cfg, tmp_path):
    cfg = replace(spark_cfg, env="prod", storage=StorageConfig(root=tmp_path))

    with pytest.raises(ValueError, match="limited to --env dev"):
        run_silver(cfg)

    assert not cfg.storage.silver.exists()
