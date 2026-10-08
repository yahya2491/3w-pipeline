"""Show what an ingest run would download, without downloading anything.

Usage:
    python scripts/plan_preview.py --env prod
"""

import argparse
import logging

from threew.bronze.manifest import Manifest
from threew.bronze.plan import build_plan
from threew.config import load_config
from threew.source.hub import HfHubClient, verify_pin
from threew.source.retry import with_retry


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", choices=["dev", "prod"], required=True)
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
    manifest = Manifest(cfg.storage.state)
    current = manifest.current()
    plan = build_plan(listing, current, cfg.ingest.sample_per_class)
    s = plan.summary()

    print(f"Env: {cfg.env} (sample_per_class: {cfg.ingest.sample_per_class})")
    print(f"Source: {source.repo_id} @ {source.revision} ({pin.label}: {'TAG MOVED' if pin.tag_moved else 'ok'})")
    print(f"Listing: {len(listing):,} files | manifest: {current.num_rows:,} current rows in {manifest.dir}")
    print(
        f"NEW: {s['new']:,} | CHANGED: {s['changed']:,} | unchanged: {s['unchanged']:,} | "
        f"removed upstream: {s['removed_upstream']:,} | rejected: {s['rejected']:,}"
    )
    print(f"To download: {s['files_to_download']:,} files, {s['bytes_to_download']:,} bytes")
    print("Per class: " + ", ".join(f"{label}: {n}" for label, n in s["files_per_class"].items()))
    for candidate, reason in plan.to_download[:5]:
        print(f"  {reason:<7} {candidate.path} ({candidate.remote.size:,} bytes)")
    if len(plan.to_download) > 5:
        print(f"  ... and {len(plan.to_download) - 5:,} more")
    for path, reason in plan.rejected:
        print(f"  REJECTED {path}: {reason}")
    for path in plan.removed_upstream:
        print(f"  REMOVED_UPSTREAM {path}")


if __name__ == "__main__":
    main()
