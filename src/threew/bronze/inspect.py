"""Check a landed file's structure against the schema contract, reading only the Parquet footer.

fetch.py proves the bytes are the published ones; this proves they have the columns and types Bronze expects.
"""

import hashlib
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import yaml


@dataclass(frozen=True)
class Inspection:
    readable: bool
    num_rows: int | None
    fingerprint: str | None  # same columns and types -> same fingerprint, whatever the column order
    schema_ok: bool
    missing: tuple[str, ...]  # in the contract, not in the file
    extra: tuple[str, ...]  # in the file, not in the contract
    type_changes: tuple[tuple[str, str, str], ...]  # (column, expected type, actual type)
    codec: str | None  # of the first column chunk, e.g. BROTLI
    error: str | None = None  # why the file couldn't be read


def load_contract(path: Path) -> dict[str, str]:
    """Column name -> Arrow type string, as pyarrow prints it (double, int16, timestamp[ns], ...)."""
    data = yaml.safe_load(Path(path).read_text()) or {}
    columns = data.get("columns")
    if not isinstance(columns, dict) or not columns:
        raise ValueError(f"{path}: expected a non-empty 'columns' mapping")
    if not all(isinstance(k, str) and isinstance(v, str) for k, v in columns.items()):
        raise ValueError(f"{path}: every column must map a name to a type string")
    return dict(columns)


def fingerprint(columns: dict[str, str]) -> str:
    """First 16 hex characters of the sha256 of the sorted name:type pairs."""
    text = ",".join(sorted(f"{name}:{type_}" for name, type_ in columns.items()))
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def inspect_file(path: Path, contract: dict[str, str]) -> Inspection:
    """Inspect one Parquet file's footer. Never raises for a bad file: it comes back readable=False."""
    try:
        metadata = pq.read_metadata(path)
        schema = pq.read_schema(path)
    except (pa.ArrowException, OSError) as exc:
        return Inspection(False, None, None, False, (), (), (), None, f"{type(exc).__name__}: {exc}")

    actual = {field.name: str(field.type) for field in schema}
    missing = tuple(name for name in contract if name not in actual)
    extra = tuple(name for name in actual if name not in contract)
    type_changes = tuple(
        (name, expected, actual[name])
        for name, expected in contract.items()
        if name in actual and actual[name] != expected
    )
    codec = metadata.row_group(0).column(0).compression if metadata.num_row_groups and metadata.num_columns else None
    return Inspection(
        readable=True,
        num_rows=metadata.num_rows,
        fingerprint=fingerprint(actual),
        schema_ok=not (missing or extra or type_changes),
        missing=missing,
        extra=extra,
        type_changes=type_changes,
        codec=codec,
    )
