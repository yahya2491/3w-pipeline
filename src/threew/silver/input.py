"""Read the current, ready Bronze input and its small recording manifest."""

import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from threew.bronze.fetch import sha256_of
from threew.bronze.manifest import Manifest
from threew.config import Config


def read_bronze_ready_marker(cfg: Config) -> dict[str, Any]:
    """Return the ready marker only when it describes this environment's reconciled Bronze table."""
    path = cfg.storage.bronze_ready
    if not path.is_file():
        raise FileNotFoundError(f"Bronze is not marked ready at {path}; run the Bronze pipeline first.")

    marker = json.loads(path.read_text(encoding="utf-8"))
    if marker.get("env") != cfg.env:
        raise ValueError(f"Bronze marker environment {marker.get('env')!r} does not match {cfg.env!r}.")
    marker_table = marker.get("bronze_table")
    if not isinstance(marker_table, str) or Path(marker_table).resolve() != cfg.storage.bronze:
        raise ValueError("Bronze marker points to a different table than the current environment config.")
    reconcile = marker.get("reconcile")
    if not isinstance(reconcile, dict) or reconcile.get("passed") is not True:
        raise ValueError("Bronze ready marker does not report a successful reconciliation.")
    return marker


def _silver_rows_from_manifest_records(
    records: Iterable[Mapping[str, Any]], storage_root: Path
) -> list[dict[str, Any]]:
    """Keep current schema-approved files and use the exact hash written into Bronze."""
    silver_rows = []
    for record in records:
        if record.get("schema_ok") is not True:
            continue

        source_path = record.get("path")
        source_type = record.get("source_type")
        label = record.get("label")
        instance_id = record.get("instance_id")
        if not isinstance(source_path, str) or not source_path:
            raise ValueError("A current manifest record has no source path.")
        if not isinstance(source_type, str) or not source_type:
            raise ValueError(f"Manifest record {source_path!r} has no source type.")
        if label is None or not isinstance(instance_id, str) or not instance_id:
            raise ValueError(f"Manifest record {source_path!r} is missing its label or instance ID.")

        file_sha256 = record.get("sha256")
        if not file_sha256:
            landing_path = record.get("landing_path")
            if not isinstance(landing_path, str) or not landing_path:
                raise ValueError(f"Manifest record {source_path!r} has no hash or landing path to calculate one.")
            file_sha256 = sha256_of(storage_root / landing_path)
        if not isinstance(file_sha256, str) or not file_sha256:
            raise ValueError(f"Manifest record {source_path!r} has no usable file hash.")

        well_id = record.get("well_id")
        silver_rows.append(
            {
                "_source_path": source_path,
                "_file_sha256": file_sha256,
                "_manifest_source_type": source_type,
                "_manifest_label": int(label),
                "instance_id": instance_id,
                "well_id": int(well_id) if well_id is not None else None,
                "instance_start": record.get("instance_start"),
            }
        )

    if not silver_rows:
        raise ValueError("The current Bronze manifest has no schema-approved recordings.")
    return silver_rows


def current_manifest_rows(cfg: Config) -> list[dict[str, Any]]:
    """Load the newest schema-approved manifest row for each file after checking Bronze is ready."""
    read_bronze_ready_marker(cfg)
    current = Manifest(cfg.storage.state).current().to_pylist()
    return _silver_rows_from_manifest_records(current, cfg.storage.root)
