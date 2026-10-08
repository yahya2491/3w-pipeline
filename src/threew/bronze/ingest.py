"""Bronze ingestion run: pin check, listing, plan, download and verify, land and inspect, manifest.

Usage:
    python -m threew.bronze.ingest --env dev
"""

import argparse
import logging
import sys
import time
from collections import Counter
from collections.abc import Callable
from typing import Any

from threew.bronze.fetch import fetch_all, fetch_stats
from threew.bronze.inspect import Inspection, load_contract
from threew.bronze.land import land_results
from threew.bronze.manifest import Manifest
from threew.bronze.plan import build_plan
from threew.config import Config, load_config
from threew.runs import new_run_id
from threew.source.hub import HfHubClient, HubClient, verify_pin
from threew.source.retry import with_retry

log = logging.getLogger(__name__)


class SchemaDriftError(RuntimeError):
    """schema.fail_on_drift is on and files didn't match the contract. They were recorded as FAILED, not landed."""

    def __init__(self, summary: dict[str, Any]) -> None:
        paths = [d["path"] for d in summary["schema"]["drifted"]]
        listed = ", ".join(paths[:5]) + (f" and {len(paths) - 5} more" if len(paths) > 5 else "")
        super().__init__(f"{len(paths)} file(s) don't match the schema contract and were not landed: {listed}")
        self.summary = summary


def run_ingest(cfg: Config, client: HubClient, *, sleep: Callable[[float], None] = time.sleep) -> dict[str, Any]:
    """One ingestion run. Returns a summary of what happened.

    Raises SchemaDriftError, after every manifest row is written, if schema.fail_on_drift is on and a file drifted.
    """
    run_id, started_at = new_run_id()
    source, ingest = cfg.source, cfg.ingest

    def retry(fn):
        return with_retry(fn, ingest.max_retries, ingest.retry_backoff_seconds, sleep=sleep)

    log.info("run %s (%s): %s @ %s", run_id, cfg.env, source.repo_id, source.revision)
    pin = retry(lambda: verify_pin(client, source))
    listing = retry(lambda: client.list_files(source.revision, source.include))
    manifest = Manifest(cfg.storage.state)
    plan = build_plan(listing, manifest.current(), ingest.sample_per_class)
    log.info("plan: %s", plan.summary())

    start = time.monotonic()
    results = fetch_all(
        client,
        plan.to_download,
        source.revision,
        cfg.storage.staging / run_id,
        ingest.workers,
        ingest.max_retries,
        ingest.retry_backoff_seconds,
        sleep=sleep,
    )
    fetch_seconds = time.monotonic() - start
    contract = load_contract(cfg.schema.contract)
    landing = land_results(
        results,
        run_id,
        started_at.date(),
        source.revision,
        cfg.storage,
        manifest,
        contract,
        fail_on_drift=cfg.schema.fail_on_drift,
    )

    summary = {
        "run_id": run_id,
        "env": cfg.env,
        "started_at": started_at.isoformat(),
        "revision": source.revision,
        "tag_moved": pin.tag_moved,
        "plan": plan.summary(),
        "fetch": fetch_stats(results, fetch_seconds),
        "landed": dict(Counter(row["status"] for row in landing.rows)),
        "schema": schema_summary(landing.inspections),
        "manifest_rows": manifest.read().num_rows,
    }
    if cfg.schema.fail_on_drift and summary["schema"]["drifted"]:
        raise SchemaDriftError(summary)
    return summary


def schema_summary(inspections: dict[str, Inspection]) -> dict[str, Any]:
    """What this run's landed files look like: fingerprints, codecs and every file that drifted from the contract."""
    drifted = [
        {
            "path": path,
            "readable": i.readable,
            "missing": list(i.missing),
            "extra": list(i.extra),
            "type_changes": [list(change) for change in i.type_changes],
            "error": i.error,
        }
        for path, i in sorted(inspections.items())
        if not i.schema_ok
    ]
    return {
        "files": len(inspections),
        "schema_ok": sum(i.schema_ok for i in inspections.values()),
        "fingerprints": dict(Counter(i.fingerprint for i in inspections.values())),
        "codecs": dict(Counter(i.codec for i in inspections.values())),
        "num_rows": sum(i.num_rows or 0 for i in inspections.values()),
        "drifted": drifted,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", choices=["dev", "prod"], required=True)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx2").setLevel(logging.WARNING)  # one INFO line per HTTP request otherwise

    cfg = load_config(args.env)
    try:
        s = run_ingest(cfg, HfHubClient(cfg.source.repo_id, cfg.source.repo_type))
    except SchemaDriftError as e:
        print_summary(e.summary, cfg)
        print(f"FAILED (schema.fail_on_drift): {e}", file=sys.stderr)
        return 2
    print_summary(s, cfg)
    return 1 if s["landed"].get("FAILED") else 0


def print_summary(s: dict[str, Any], cfg: Config) -> None:
    plan, fetch = s["plan"], s["fetch"]
    print(f"Run {s['run_id']} ({s['env']}) at {s['revision']}{' (TAG MOVED)' if s['tag_moved'] else ''}")
    print(
        f"Plan: NEW {plan['new']}, CHANGED {plan['changed']}, unchanged {plan['unchanged']}, "
        f"removed upstream {plan['removed_upstream']}, rejected {plan['rejected']}"
    )
    print(
        f"Fetch: {fetch['ok']} OK, {fetch['failed']} FAILED, {fetch['bytes']:,} bytes "
        f"in {fetch['seconds']:.1f} s ({fetch['mb_per_s']} MB/s)"
    )
    print(f"Landed: {s['landed'].get('LANDED', 0)}, failed: {s['landed'].get('FAILED', 0)}")
    schema = s["schema"]
    if schema["files"]:
        print(
            f"Schema: {schema['schema_ok']}/{schema['files']} match the contract, {schema['num_rows']:,} rows | "
            f"fingerprints {schema['fingerprints']} | codecs {schema['codecs']}"
        )
        for d in schema["drifted"]:
            print(f"  DRIFT {d['path']}: missing {d['missing']}, extra {d['extra']}, types {d['type_changes']}")
    print(f"Manifest: {s['manifest_rows']} rows in {cfg.storage.state / 'manifest'}")


if __name__ == "__main__":
    sys.exit(main())
