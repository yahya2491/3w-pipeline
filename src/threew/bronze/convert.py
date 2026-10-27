"""Convert landed files into the Bronze table: the source values unchanged, plus four lineage columns.

Layout, under storage.bronze (= <root>/bronze/3w_observations):

    source_type=<REAL|SIMULATED|DRAWN>/label=<label>/<first 16 hex of the file's sha256>.parquet

One output file per landed file, named by its checksum: converting again overwrites it with an identical file, and
a file corrected upstream gets a new name instead of overwriting the old version. source_type and label live only
in the folder names; a column of the same name inside the file would clash with Spark's partition column.

Each output file has the 30 source columns with the same names, values and nulls (timestamp as timestamp[us]
instead of [ns]), then the lineage columns _source_path, _file_sha256, _ingest_run_id and _row_in_file.
"""

import logging
import os
import time
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from threew.bronze.fetch import sha256_of
from threew.bronze.manifest import Manifest
from threew.config import StorageConfig

log = logging.getLogger(__name__)

TIMESTAMP = "timestamp"
LINEAGE_COLUMNS = ("_source_path", "_file_sha256", "_ingest_run_id", "_row_in_file")
COMPRESSION = "snappy"


class ConvertStatus(StrEnum):
    OK = "OK"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"  # not converted on purpose, e.g. the file didn't match the schema contract


@dataclass(frozen=True)
class ConvertResult:
    source_path: str  # path on Hugging Face, the instance's unique key
    status: ConvertStatus
    rows_in: int | None
    rows_out: int | None  # from the written file's footer; equals rows_in for OK
    output_path: Path | None
    input_bytes: int | None
    output_bytes: int | None
    seconds: float
    error: str | None = None


def output_relpath(manifest_row: Mapping[str, Any], file_sha256: str) -> Path:
    """Where a landed file's Bronze version goes, relative to storage.bronze."""
    return (
        Path(f"source_type={manifest_row['source_type']}")
        / f"label={manifest_row['label']}"
        / f"{file_sha256[:16]}.parquet"
    )


def convert_file(landed_path: Path, manifest_row: Mapping[str, Any], out_dir: Path) -> ConvertResult:
    """Convert one landed file into out_dir. Never raises for a bad file: the error is in the result."""
    start = time.monotonic()
    source_path = manifest_row["path"]
    rows_in = None
    tmp = None

    def failed(error: str) -> ConvertResult:
        if tmp is not None:
            tmp.unlink(missing_ok=True)
        return ConvertResult(source_path, ConvertStatus.FAILED, rows_in, None, None, None, None, _since(start), error)

    try:
        table = pq.read_table(landed_path)
        rows_in = table.num_rows
        input_bytes = landed_path.stat().st_size
        file_sha256 = manifest_row["sha256"] or sha256_of(landed_path)  # sha256 is only null without LFS info

        try:  # safe: if any timestamp has sub-microsecond digits, fail instead of rounding them away
            micros = pc.cast(table[TIMESTAMP], pa.timestamp("us"), safe=True)
        except pa.ArrowInvalid as exc:
            return failed(f"timestamp can't be stored in microseconds without losing precision: {exc}")
        table = table.set_column(table.schema.get_field_index(TIMESTAMP), TIMESTAMP, micros)

        # Built in C++ (pa.repeat, cumulative_sum) rather than from Python lists, which hold Python's global lock
        # for every row and stop the threads from running in parallel
        n = table.num_rows
        lineage = {
            "_source_path": pa.repeat(pa.scalar(source_path, pa.string()), n),
            "_file_sha256": pa.repeat(pa.scalar(file_sha256, pa.string()), n),
            "_ingest_run_id": pa.repeat(pa.scalar(manifest_row["run_id"], pa.string()), n),
            "_row_in_file": pc.subtract(pc.cumulative_sum(pa.repeat(pa.scalar(1, pa.int64()), n)), 1),
        }
        for name, column in lineage.items():
            table = table.append_column(name, column)
        # The source files' pandas metadata describes a datetime64[ns] index, which is no longer true
        table = table.replace_schema_metadata(None)

        final = out_dir / output_relpath(manifest_row, file_sha256)
        final.parent.mkdir(parents=True, exist_ok=True)
        tmp = final.with_name(f".{final.name}.tmp")  # a dot-file: Spark never reads it
        with open(tmp, "wb") as f:
            pq.write_table(table, f, compression=COMPRESSION)
            f.flush()
            os.fsync(f.fileno())
        rows_out = pq.read_metadata(tmp).num_rows
        if rows_out != rows_in:
            return failed(f"wrote {rows_out} rows, read {rows_in}")
        os.replace(tmp, final)  # atomic: the final name only ever holds a complete file
        tmp = None
        return ConvertResult(
            source_path, ConvertStatus.OK, rows_in, rows_out, final, input_bytes, final.stat().st_size, _since(start)
        )
    except (OSError, pa.ArrowException, KeyError) as exc:
        return failed(f"{type(exc).__name__}: {exc}")


def convert_all(
    manifest: Manifest, storage_cfg: StorageConfig, workers: int, *, only_pending: bool = False
) -> list[ConvertResult]:
    """Convert current LANDED files with schema_ok into storage.bronze; others are SKIPPED. In path order.

    only_pending converts only files whose Bronze output doesn't exist yet, and leaves the others out of the
    result. Output names are deterministic and written atomically, so an existing output is a finished one.
    """
    rows = manifest.current().to_pylist()  # newest LANDED row per path
    results: dict[str, ConvertResult] = {}
    todo = []
    for row in rows:
        if row["schema_ok"] is not True:
            reason = "not inspected" if row["schema_ok"] is None else "schema_ok is false"
            results[row["path"]] = ConvertResult(
                row["path"], ConvertStatus.SKIPPED, None, None, None, None, None, 0.0, reason
            )
        elif not (only_pending and expected_output(row, storage_cfg).exists()):
            todo.append(row)

    def convert(row):
        return convert_file(storage_cfg.root / row["landing_path"], row, storage_cfg.bronze)

    # Threads: pyarrow decodes and encodes outside Python's global lock, so this uses all the cores
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="convert") as pool:
        for done, result in enumerate(pool.map(convert, todo), start=1):
            results[result.source_path] = result
            if result.status is ConvertStatus.FAILED:
                log.warning("convert failed: %s: %s", result.source_path, result.error)
            if done % 250 == 0:
                log.info("converted %d/%d", done, len(todo))
    return [results[row["path"]] for row in rows if row["path"] in results]


def expected_output(row: Mapping[str, Any], storage_cfg: StorageConfig) -> Path:
    """Where a manifest row's Bronze output is (or will be)."""
    file_sha256 = row["sha256"] or sha256_of(storage_cfg.root / row["landing_path"])
    return storage_cfg.bronze / output_relpath(row, file_sha256)


class PruneError(RuntimeError):
    """Pruning would remove the whole table: the manifest is empty or unreadable, not the table stale."""


def prune_outputs(manifest: Manifest, storage_cfg: StorageConfig) -> dict[str, list[str]]:
    """Delete Bronze outputs that no current, contract-matching manifest row points to, e.g. the old version of a
    file that changed upstream, and temporary files left by a crashed write. Paths are relative to storage.bronze.
    """
    bronze = storage_cfg.bronze
    if not bronze.exists():
        return {"removed": [], "temporary_removed": []}
    keep = {expected_output(r, storage_cfg) for r in manifest.current().to_pylist() if r["schema_ok"] is True}
    outputs = sorted(bronze.rglob("*.parquet"))
    if outputs and not keep:
        raise PruneError(f"the manifest lists no convertible files, but {bronze} has {len(outputs)}: refusing")
    removed = [p for p in outputs if p not in keep and not p.name.startswith(".")]
    temporary = sorted(p for p in bronze.rglob(".*.tmp") if p.is_file())
    for path in removed + temporary:
        path.unlink()
    for folder in sorted({p.parent for p in removed + temporary}, key=lambda d: len(d.parts), reverse=True):
        for empty in (folder, folder.parent):  # label=, then source_type=, if nothing is left in them
            if empty != bronze and empty.exists() and not any(empty.iterdir()):
                empty.rmdir()
    return {
        "removed": [p.relative_to(bronze).as_posix() for p in removed],
        "temporary_removed": [p.relative_to(bronze).as_posix() for p in temporary],
    }


def _since(start: float) -> float:
    return round(time.monotonic() - start, 3)
