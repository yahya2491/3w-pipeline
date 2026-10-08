"""Checks for leftovers. They are reported, never deleted: deciding what to do with them is a human's call."""

from pathlib import Path

from threew.bronze.manifest import Manifest, Status
from threew.config import StorageConfig


def find_orphans(storage: StorageConfig, manifest: Manifest) -> list[str]:
    """Files in landing that no LANDED manifest row points to, e.g. moved by a run that crashed before its row.

    Paths are relative to storage.root, like the manifest's landing_path.
    """
    if not storage.landing.exists():
        return []
    table = manifest.read()
    status, landing_paths = table["status"].to_pylist(), table["landing_path"].to_pylist()
    recorded = {p for s, p in zip(status, landing_paths, strict=True) if s == Status.LANDED}
    on_disk = (p.relative_to(storage.root).as_posix() for p in storage.landing.rglob("*") if p.is_file())
    return sorted(p for p in on_disk if p not in recorded)


def find_stale_staging(staging_dir: Path, current_run_id: str) -> list[str]:
    """Run folders in staging other than this run's: left by a run that crashed or was killed."""
    if not staging_dir.exists():
        return []
    return sorted(p.name for p in staging_dir.iterdir() if p.name != current_run_id)
