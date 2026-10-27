"""Convert every landed file into the Bronze table, on its own.

The full Bronze run (python -m threew.bronze.run) does the same as its convert phase.

Usage:
    python scripts/build_bronze.py --env dev
"""

import argparse
import logging
import sys
import time
from collections import Counter

from threew.bronze.convert import ConvertStatus, convert_all
from threew.bronze.manifest import Manifest
from threew.config import load_config


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", choices=["dev", "prod"], required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    cfg = load_config(args.env)
    manifest = Manifest(cfg.storage.state)
    current = manifest.current()
    convertible = zip(current["num_rows"].to_pylist(), current["schema_ok"].to_pylist(), strict=True)
    expected_rows = sum(n for n, ok in convertible if ok)  # only files that match the contract are converted

    start = time.monotonic()
    results = convert_all(manifest, cfg.storage, cfg.ingest.workers)
    seconds = time.monotonic() - start

    by_status = Counter(r.status for r in results)
    ok = [r for r in results if r.status is ConvertStatus.OK]
    rows_out = sum(r.rows_out for r in ok)
    in_bytes, out_bytes = sum(r.input_bytes for r in ok), sum(r.output_bytes for r in ok)
    print(f"Env: {cfg.env} | Bronze table: {cfg.storage.bronze} | {cfg.ingest.workers} workers")
    print(
        f"Converted: {by_status[ConvertStatus.OK]:,} | failed: {by_status[ConvertStatus.FAILED]:,} | "
        f"skipped: {by_status[ConvertStatus.SKIPPED]:,}"
    )
    match = "matches" if rows_out == expected_rows else f"DOES NOT MATCH the manifest's {expected_rows:,}"
    print(f"sum(rows_out): {rows_out:,} ({match} the manifest's num_rows total)")
    print(f"Input: {in_bytes:,} bytes (landing, Brotli) -> output: {out_bytes:,} bytes (Bronze, Snappy)")
    print(f"Duration: {seconds:.1f} s ({rows_out / seconds:,.0f} rows/s)" if seconds else "Duration: 0 s")

    partitions = Counter((r.output_path.parent.parent.name, r.output_path.parent.name) for r in ok)
    per_type = Counter()
    for (source_type, _), n in partitions.items():
        per_type[source_type] += n
    print("Files per source_type: " + ", ".join(f"{t}: {n:,}" for t, n in sorted(per_type.items())))
    print("Files per partition:")
    for (source_type, label), n in sorted(partitions.items()):
        print(f"  {source_type}/{label}: {n}")
    for r in results:
        if r.status is not ConvertStatus.OK:
            print(f"  {r.status} {r.source_path}: {r.error}")
    return 0 if by_status[ConvertStatus.FAILED] == 0 and rows_out == expected_rows else 1


if __name__ == "__main__":
    sys.exit(main())
