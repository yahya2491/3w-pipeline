"""Print what the pinned Hugging Face source holds, before anything is ingested.

Usage:
    python scripts/hub_probe.py --env prod
"""

import argparse
import logging
from collections import Counter

from threew.bronze.naming import SourceType, parse_all
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

    parsed, rejected = parse_all(f.path for f in files)
    print(f"Name parsing: parsed {len(parsed):,}, rejected: {len(rejected)}")
    for path, reason in rejected:
        print(f"  rejected {path}: {reason}")

    by_type = Counter(p.source_type for p in parsed)
    print("By source type: " + ", ".join(f"{t}: {by_type[t]:,}" for t in SourceType))

    real = [p for p in parsed if p.source_type is SourceType.REAL]
    if real:
        wells = sorted({p.well_id for p in real})
        missing = sorted(set(range(wells[0], wells[-1] + 1)) - set(wells))
        print(f"Distinct wells: {len(wells)} (wells {wells[0]} to {wells[-1]}, missing: {missing})")
        starts = [p.instance_start for p in real]
        print(f"Real instance_start: earliest {min(starts)}, latest {max(starts)} (no timezone)")

    shared = sum(n > 1 for n in Counter(p.instance_id for p in parsed).values())
    print(f"Instance IDs found in more than one label folder: {shared} (so the path, not the ID, is the key)")


if __name__ == "__main__":
    main()
