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
    path = cfg.storage.bronze_ready  # Read the publication marker before trusting the table.
    if not path.is_file():
        raise FileNotFoundError(f"Bronze is not marked ready at {path}; run the Bronze pipeline first.")

    marker = json.loads(path.read_text(encoding="utf-8"))
    if marker.get("env") != cfg.env:  # Do not accidentally use a prod publication for a dev run.
        raise ValueError(f"Bronze marker environment {marker.get('env')!r} does not match {cfg.env!r}.")
    marker_table = marker.get("bronze_table")  # The marker must refer to this configured Bronze table.
    if not isinstance(marker_table, str) or Path(marker_table).resolve() != cfg.storage.bronze:
        raise ValueError("Bronze marker points to a different table than the current environment config.")
    reconcile = marker.get("reconcile")  # A marker alone is not enough; its checks must have passed.
    if not isinstance(reconcile, dict) or reconcile.get("passed") is not True:
        raise ValueError("Bronze ready marker does not report a successful reconciliation.")
    if not isinstance(marker.get("run_id"), str) or not marker["run_id"]:
        raise ValueError("Bronze marker has no run ID.")
    source = marker.get("source")
    if not isinstance(source, dict) or any(
        not isinstance(source.get(name), str) or not source[name] for name in ("repo_id", "revision")
    ):
        raise ValueError("Bronze marker has no source repository or revision.")
    for name in ("files", "rows"):  # These counts become our expected Silver input totals.
        if type(marker.get(name)) is not int or marker[name] < 0:
            raise ValueError(f"Bronze marker has no valid {name} count.")
    return marker


def _silver_rows_from_manifest_records(
    records: Iterable[Mapping[str, Any]], storage_root: Path
) -> list[dict[str, Any]]:
    """Keep current schema-approved files and use the exact hash written into Bronze."""
    silver_rows = []
    for record in records:
        if record.get("schema_ok") is not True:  # Leave schema-rejected files out of the Silver catalogue.
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

        file_sha256 = record.get("sha256")  # The hash distinguishes different versions of the same path.
        if not file_sha256:  # Older manifest rows may need their hash calculated from landing.
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
                "_manifest_label": int(label),  # Keep the recording label separate from each observation class.
                "instance_id": instance_id,
                "well_id": int(well_id) if well_id is not None else None,  # Synthetic recordings have no real well ID.
                "instance_start": record.get("instance_start"),
            }
        )

    if not silver_rows:
        raise ValueError("The current Bronze manifest has no schema-approved recordings.")
    return silver_rows


def current_manifest_rows(cfg: Config) -> list[dict[str, Any]]:
    """Load the newest schema-approved manifest row for each file after checking Bronze is ready."""
    read_bronze_ready_marker(cfg)
    current = Manifest(cfg.storage.state).current().to_pylist()  # Load current versions, not the full ingest history.
    return _silver_rows_from_manifest_records(current, cfg.storage.root)


def current_manifest_counts(cfg: Config) -> list[dict[str, Any]]:
    """Expected row counts for every current schema-approved source-file version."""
    records = [r for r in Manifest(cfg.storage.state).current().to_pylist() if r.get("schema_ok") is True]
    metadata = _silver_rows_from_manifest_records(records, cfg.storage.root)
    counts = []
    for record, row in zip(records, metadata, strict=True):  # Keep counts paired with their exact file version.
        num_rows = record.get("num_rows")
        if type(num_rows) is not int or num_rows < 0:
            raise ValueError(f"Manifest record {row['_source_path']!r} has no valid observation count.")
        counts.append(
            {"_source_path": row["_source_path"], "_file_sha256": row["_file_sha256"], "bronze_rows": num_rows}
        )
    return counts
