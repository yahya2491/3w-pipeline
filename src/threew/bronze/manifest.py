"""The Bronze manifest: one row per file per ingest attempt, the pipeline's memory of what it has.

Stored as Parquet chunk files under <state>/manifest/, one chunk per append (normally one per run). A chunk is
written under a temporary name and renamed into place, so a crash mid-write leaves a temp file that read() ignores,
never a half-written chunk.
"""

import os
import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# Every append is checked against this schema: a wrong type or a missing value fails instead of being stored.
MANIFEST_SCHEMA = pa.schema(
    [
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("path", pa.string(), nullable=False),  # path on Hugging Face, the unique key
        pa.field("revision", pa.string(), nullable=False),  # HF commit SHA the file was read at
        pa.field("label", pa.int16(), nullable=False),  # from the folder; int16 like the data's `class` column
        pa.field("source_type", pa.string(), nullable=False),  # REAL, SIMULATED or DRAWN
        pa.field("well_id", pa.int32()),  # REAL only
        pa.field("instance_start", pa.timestamp("us")),  # REAL only; naive, 3W gives no timezone
        pa.field("instance_id", pa.string(), nullable=False),
        pa.field("size", pa.int64(), nullable=False),  # bytes
        pa.field("sha256", pa.string()),  # null only if Hugging Face gives no LFS info
        pa.field("blob_id", pa.string(), nullable=False),
        pa.field("landing_path", pa.string()),  # relative to storage.root; null when FAILED
        pa.field("status", pa.string(), nullable=False),  # LANDED or FAILED
        pa.field("error", pa.string()),  # only when FAILED
        # Filled in step 14; nullable so rows written before then stay valid
        pa.field("num_rows", pa.int64()),  # from the Parquet footer
        pa.field("schema_fingerprint", pa.string()),  # hash of column names and types
        pa.field("schema_ok", pa.bool_()),  # matches the contract
        pa.field("recorded_at", pa.timestamp("us", tz="UTC"), nullable=False),  # when this attempt finished
    ]
)

CHUNK_GLOB = "part-*.parquet"


class Status(StrEnum):
    LANDED = "LANDED"
    FAILED = "FAILED"


class ManifestError(ValueError):
    """Rows that don't fit the manifest schema, or a manifest file that doesn't."""


class Manifest:
    def __init__(self, state_dir: Path) -> None:
        self.dir = Path(state_dir) / "manifest"

    def read(self) -> pa.Table:
        """All rows in write order; an empty table with the manifest columns on the first run."""
        chunks = sorted(self.dir.glob(CHUNK_GLOB))  # temp files from a crashed write don't match
        if not chunks:
            return MANIFEST_SCHEMA.empty_table()
        tables = []
        for chunk in chunks:
            table = pq.read_table(chunk)
            if not table.schema.equals(MANIFEST_SCHEMA, check_metadata=False):
                raise ManifestError(f"{chunk} does not have the manifest schema:\n{table.schema}")
            tables.append(table)
        return pa.concat_tables(tables)

    def append(self, rows: Sequence[Mapping[str, Any]]) -> Path | None:
        """Validate rows and write them as one new chunk. Returns the chunk's path, or None for no rows."""
        if not rows:
            return None
        table = _to_table(rows)
        self.dir.mkdir(parents=True, exist_ok=True)
        # The UTC timestamp makes names sort in write order; the uuid keeps concurrent writers apart
        name = f"part-{datetime.now(UTC):%Y%m%dT%H%M%S%fZ}-{uuid.uuid4().hex[:8]}.parquet"
        tmp = self.dir / f".{name}.tmp"
        with open(tmp, "wb") as f:
            pq.write_table(table, f)
            f.flush()
            os.fsync(f.fileno())
        final = self.dir / name
        os.replace(tmp, final)  # atomic: readers see the whole chunk or none of it
        return final

    def landed_keys(self) -> set[tuple[str, str | None]]:
        """(path, sha256) of every file that has landed: what doesn't need downloading again."""
        landed = self._landed()
        return set(zip(landed["path"].to_pylist(), landed["sha256"].to_pylist(), strict=True))

    def current(self) -> pa.Table:
        """For each path, its newest LANDED row (e.g. after a file was corrected upstream), sorted by path."""
        newest: dict[str, dict[str, Any]] = {}
        for row in self._landed().to_pylist():  # write order, so on equal recorded_at the later write wins
            previous = newest.get(row["path"])
            if previous is None or row["recorded_at"] >= previous["recorded_at"]:
                newest[row["path"]] = row
        return pa.Table.from_pylist([newest[p] for p in sorted(newest)], schema=MANIFEST_SCHEMA)

    def _landed(self) -> pa.Table:
        table = self.read()
        return table.filter(pc.equal(table["status"], Status.LANDED.value))


def _to_table(rows: Sequence[Mapping[str, Any]]) -> pa.Table:
    expected = set(MANIFEST_SCHEMA.names)
    for i, row in enumerate(rows):
        missing, extra = expected - row.keys(), row.keys() - expected
        if missing or extra:
            raise ManifestError(f"row {i}: missing columns {sorted(missing)}, unknown columns {sorted(extra)}")
        _check_row(i, row)
    try:
        table = pa.Table.from_pylist(list(rows), schema=MANIFEST_SCHEMA)
    except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
        raise ManifestError(f"rows don't fit the manifest schema: {e}") from e
    for field in MANIFEST_SCHEMA:  # from_pylist doesn't enforce nullable=False
        if not field.nullable and table[field.name].null_count:
            raise ManifestError(f"column {field.name!r} must not be null")
    return table


def _check_row(i: int, row: Mapping[str, Any]) -> None:
    status, landing_path, error = row["status"], row["landing_path"], row["error"]
    if status == Status.LANDED:
        if landing_path is None or error is not None:
            raise ManifestError(f"row {i}: a LANDED row needs a landing_path and no error")
        # Relative, so the manifest stays valid if storage.root moves (another disk, hdfs://, ...)
        if (
            not isinstance(landing_path, str)
            or landing_path.startswith("/")
            or ".." in PurePosixPath(landing_path).parts
        ):
            raise ManifestError(f"row {i}: landing_path must be relative to storage.root, got {landing_path!r}")
    elif status == Status.FAILED:
        if error is None or landing_path is not None:
            raise ManifestError(f"row {i}: a FAILED row needs an error and no landing_path")
    else:
        raise ManifestError(f"row {i}: status must be LANDED or FAILED, got {status!r}")
