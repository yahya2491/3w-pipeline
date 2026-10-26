"""Download and verify the first N planned files into a throwaway staging folder, then delete it.

Measures download speed and estimates how long a full run would take. Never writes to landing or the manifest.

Usage:
    python scripts/fetch_smoke.py --env dev --limit 3
"""

import argparse
import logging
import shutil
import sys
import time
from datetime import UTC, datetime

from threew.bronze.fetch import fetch_all, fetch_stats
from threew.bronze.manifest import Manifest
from threew.bronze.plan import build_plan
from threew.config import load_config
from threew.source.hub import HfHubClient, verify_pin
from threew.source.retry import with_retry


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", choices=["dev", "prod"], required=True)
    parser.add_argument("--limit", type=int, default=3, help="number of planned files to fetch (default 3)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx2").setLevel(logging.WARNING)  # one INFO line per HTTP request otherwise

    cfg = load_config(args.env)
    source = cfg.source
    client = HfHubClient(source.repo_id, source.repo_type)

    def retry(fn):
        return with_retry(fn, cfg.ingest.max_retries, cfg.ingest.retry_backoff_seconds)

    pin = retry(lambda: verify_pin(client, source))
    listing = retry(lambda: client.list_files(source.revision, source.include))
    plan = build_plan(listing, Manifest(cfg.storage.state).current(), cfg.ingest.sample_per_class)
    items = plan.to_download[: args.limit]

    smoke_dir = cfg.storage.staging / f"smoke_{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
    print(
        f"Env: {cfg.env} | {source.repo_id} @ {source.revision} ({pin.label}: {'TAG MOVED' if pin.tag_moved else 'ok'})"
    )
    print(f"Fetching {len(items)} of {len(plan.to_download):,} planned files with {cfg.ingest.workers} workers")
    print(f"Staging: {smoke_dir}")

    try:
        start = time.monotonic()
        results = fetch_all(
            client,
            items,
            source.revision,
            smoke_dir,
            cfg.ingest.workers,
            cfg.ingest.max_retries,
            cfg.ingest.retry_backoff_seconds,
        )
        elapsed = time.monotonic() - start

        for r in results:
            line = (
                f"  {r.candidate.path}  {r.status}  attempts={r.attempts}  {r.verified_by}  "
                f"{r.candidate.remote.size:,} bytes  {r.seconds:.2f} s"
            )
            print(line + (f"  {r.error}" if r.error else ""))

        s = fetch_stats(results, elapsed)
        print(
            f"OK: {s['ok']} | FAILED: {s['failed']} | {s['bytes']:,} bytes in {s['seconds']:.2f} s ({s['mb_per_s']} MB/s)"
        )
        print(
            f"Download calls: {s['download_calls']} | network retries: {s['network_retries']} | "
            f"checksum re-downloads: {s['checksum_redownloads']}"
        )
        if s["bytes"]:
            full_bytes = sum(f.size for f in listing)
            estimate = full_bytes / (s["bytes"] / elapsed)
            print(
                f"Estimated full run: {full_bytes:,} bytes / {s['mb_per_s']} MB/s = {estimate:,.0f} s "
                f"({estimate / 60:.1f} min)"
            )
        return 1 if s["failed"] else 0
    finally:
        shutil.rmtree(smoke_dir, ignore_errors=True)
        print(f"Removed {smoke_dir}: {'yes' if not smoke_dir.exists() else 'NO, still there'}")


if __name__ == "__main__":
    sys.exit(main())
