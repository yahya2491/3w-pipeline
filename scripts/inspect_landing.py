"""Summarize the structure of everything in landing from the manifest alone; the files are never read.

Usage:
    python scripts/inspect_landing.py --env dev
"""

import argparse
from collections import Counter

from threew.bronze.manifest import Manifest
from threew.config import load_config


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", choices=["dev", "prod"], required=True)
    args = parser.parse_args()

    cfg = load_config(args.env)
    manifest = Manifest(cfg.storage.state)
    rows = manifest.current().to_pylist()  # the newest LANDED row per path
    print(f"Env: {cfg.env} | {len(rows):,} landed files in {manifest.dir}")
    if not rows:
        return

    def counts(column: str) -> str:
        return ", ".join(f"{value}: {n:,}" for value, n in Counter(r[column] for r in rows).most_common())

    print(f"Fingerprints: {counts('schema_fingerprint')}")
    print(f"schema_ok: {counts('schema_ok')}")
    print(f"Codecs: {counts('codec')}")
    print(f"Total num_rows: {sum(r['num_rows'] or 0 for r in rows):,}")
    not_inspected = sum(r["schema_ok"] is None for r in rows)
    if not_inspected:
        print(f"Not inspected (landed before footer inspection existed): {not_inspected:,}")
    for r in rows:
        if r["schema_ok"] is False:
            print(f"  DRIFT {r['path']} (fingerprint {r['schema_fingerprint']}, {r['landing_path']})")


if __name__ == "__main__":
    main()
