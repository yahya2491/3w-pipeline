"""Print what the pinned Hugging Face source holds, before anything is ingested.

Usage:
    python scripts/hub_probe.py --env prod
"""

import argparse
import logging
from collections import Counter

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

    print(f"Source: {source.repo_id} ({source.repo_type}) @ {source.revision}, include {list(source.include)}")

    pin = retry(lambda: verify_pin(client, source))
    status = "TAG MOVED, using the pinned revision" if pin.tag_moved else "matches the pinned revision"
    print(f"Pin check: {pin.label} -> {pin.resolved} ({status})")

    files = retry(lambda: client.list_files(source.revision, source.include))
    total = sum(f.size for f in files)
    print(f"Files: {len(files):,}")
    print(f"Total size: {total:,} bytes ({total / 1e9:.2f} GB, {total / 2**30:.2f} GiB)")

    per_class = Counter(f.path.split("/")[-2] for f in files)
    print("Files per class: " + ", ".join(f"{label}: {n}" for label, n in sorted(per_class.items())))

    with_sha = sum(f.sha256 is not None for f in files)
    print(f"With sha256: {with_sha:,} of {len(files):,}")

    print("First 3:")
    for f in files[:3]:
        print(f"  {f}")


if __name__ == "__main__":
    main()
