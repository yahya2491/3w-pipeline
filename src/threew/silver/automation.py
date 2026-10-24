"""Content signatures for reusing a successfully published Silver build."""

import hashlib
import json
from pathlib import Path

from threew.config import REPO_ROOT
from threew.silver.output import CONTRACT_VERSION

TABLE_NAMES = (
    "silver_observations",
    "instance_quality",
    "sensor_availability",
    "quality_issues",
    "quality_quarantine",
    "event_reference",
    "sensor_reference",
)


def runtime_digest(root=REPO_ROOT):
    """Hash runtime sources and locked dependencies, ignoring unrelated Gold work."""
    root = Path(root)
    files = [*root.joinpath("src/threew").rglob("*.py")]
    files += [root / name for name in ("requirements.lock", "pyproject.toml")]
    digest = hashlib.sha256()
    for path in sorted(files):
        if path.is_file():
            digest.update(path.relative_to(root).as_posix().encode() + b"\0")
            digest.update(path.read_bytes() + b"\0")
    return digest.hexdigest()


def build_signature(cfg, bronze_marker, manifest_rows, manifest_counts, rules, batch_rows, contract_sha256):
    """Ignore ingestion run IDs when the exact source versions and settings match."""
    payload = {
        "format": 1,
        "config": cfg.to_dict(),
        "source": bronze_marker["source"],
        "input_rows": bronze_marker["rows"],
        "input_files": bronze_marker["files"],
        "recordings": sorted(manifest_rows, key=lambda row: (row["_source_path"], row["_file_sha256"])),
        "counts": sorted(manifest_counts, key=lambda row: (row["_source_path"], row["_file_sha256"])),
        "rules": rules.snapshot()["sha256"],
        "batch_rows": batch_rows,
        "contract_version": CONTRACT_VERSION,
        "contract_sha256": contract_sha256,
        "runtime_sha256": runtime_digest(),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


def reusable_build(ready_path, signature):
    """Trust a verified marker only while all seven recorded Parquet outputs remain."""
    ready_path = Path(ready_path)
    try:
        marker = json.loads(ready_path.read_text())
        if (
            marker.get("build_signature") != signature
            or marker.get("contract_version") != CONTRACT_VERSION
            or marker.get("reconcile", {}).get("passed") is not True
            or not isinstance(marker.get("run_id"), str)
            or not marker["run_id"]
        ):
            return None
        if set(marker["tables"]) != set(TABLE_NAMES):
            return None
        run_root = ready_path.parent.resolve() / "runs" / marker["run_id"]
        for name in TABLE_NAMES:
            table = marker["tables"][name]
            path = Path(table["path"])
            if path.resolve() != run_root / name or not path.is_dir():
                return None
            parts = list(path.glob("*.parquet"))
            if not parts or sum(part.stat().st_size for part in parts) != table["parquet_bytes"]:
                return None
        return marker
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        return None
