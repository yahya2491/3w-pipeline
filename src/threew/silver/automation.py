"""Content signatures for reusing a successfully published Silver build."""

import ast
import hashlib
import json
import re
import tomllib
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


class _WithoutDocstrings(ast.NodeTransformer):
    """Remove documentation strings, preserving executable string constants."""

    def _visit_body(self, node):
        self.generic_visit(node)  # Handle nested functions and classes before removing their docstrings.
        if (
            node.body
            and isinstance(node.body[0], ast.Expr)
            and isinstance(node.body[0].value, ast.Constant)
            and isinstance(node.body[0].value.value, str)
        ):
            node.body.pop(0)  # Only the first standalone string is a docstring; keep other string constants.
        if not node.body and not isinstance(node, ast.Module):
            node.body.append(ast.Pass())  # Keep a valid body if the docstring was its only statement.
        return node

    visit_Module = _visit_body
    visit_ClassDef = _visit_body
    visit_FunctionDef = _visit_body
    visit_AsyncFunctionDef = _visit_body


def _runtime_content(path):
    if path.suffix == ".py":
        tree = _WithoutDocstrings().visit(ast.parse(path.read_bytes(), filename=str(path)))
        return ast.dump(tree, include_attributes=False).encode()  # Ignore line numbers and formatting in the signature.
    if path.name == "pyproject.toml":
        with path.open("rb") as source:
            return json.dumps(tomllib.load(source), sort_keys=True, default=str).encode()
    lines = (re.sub(r"(^|\s+)#.*$", "", line).strip() for line in path.read_text(encoding="utf-8").splitlines())
    return "\n".join(line for line in lines if line).encode()  # Strip pip comments but keep URL fragments.


def runtime_digest(root=REPO_ROOT):
    """Hash runtime structure and settings, excluding comments and docstrings."""
    root = Path(root)
    files = [*root.joinpath("src/threew").rglob("*.py")]
    files += [root / name for name in ("requirements.lock", "pyproject.toml")]
    digest = hashlib.sha256(b"silver-runtime-v2\0")  # Comment edits should not force a new build.
    for path in sorted(files):
        if path.is_file():
            digest.update(path.relative_to(root).as_posix().encode() + b"\0")
            digest.update(_runtime_content(path) + b"\0")
    return digest.hexdigest()


def build_signature(cfg, bronze_marker, manifest_rows, manifest_counts, rules, batch_rows, contract_sha256):
    """Ignore ingestion run IDs when the exact source versions and settings match."""
    payload = {
        "format": 2,
        "config": cfg.to_dict(),
        "source": bronze_marker["source"],  # Use source content, so a new ingest run alone does not force a rebuild.
        "input_rows": bronze_marker["rows"],
        "input_files": bronze_marker["files"],
        "recordings": sorted(manifest_rows, key=lambda row: (row["_source_path"], row["_file_sha256"])),
        "counts": sorted(manifest_counts, key=lambda row: (row["_source_path"], row["_file_sha256"])),
        "rules": rules.snapshot()["sha256"],  # A change to the resolved cleaning rules invalidates reuse.
        "batch_rows": batch_rows,
        "contract_version": CONTRACT_VERSION,
        "contract_sha256": contract_sha256,  # Contract wording is also part of the recorded build signature.
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
        if set(marker["tables"]) != set(TABLE_NAMES):  # Reuse requires all seven tables, not just observations.
            return None
        run_root = ready_path.parent.resolve() / "runs" / marker["run_id"]
        for name in TABLE_NAMES:
            table = marker["tables"][name]
            path = Path(table["path"])
            if path.resolve() != run_root / name or not path.is_dir():  # Reject paths outside the selected run.
                return None
            parts = list(path.glob("*.parquet"))  # The marker is reusable only while its output files still exist.
            if not parts or sum(part.stat().st_size for part in parts) != table["parquet_bytes"]:
                return None
        return marker
    except (OSError, ValueError, TypeError, KeyError, AttributeError):  # A damaged marker means rebuild, not reuse.
        return None
