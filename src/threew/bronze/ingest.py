"""Bronze ingestion run: lock, pin check, listing, plan, download and verify, land and inspect, manifest, report.

Usage:
    python -m threew.bronze.ingest --env dev

Exit code: 0 SUCCESS or SKIPPED (another run held the lock), 1 PARTIAL (some files FAILED), 2 FAILED.
Every run, whatever happens, writes a report to <storage.runs>/<run_id>.json.
"""

import argparse
import logging
import sys
import time
from collections import Counter
from collections.abc import Callable
from contextlib import ExitStack
from typing import Any

from threew.bronze.fetch import fetch_all, fetch_stats
from threew.bronze.inspect import Inspection, fingerprint, load_contract
from threew.bronze.land import land_results
from threew.bronze.manifest import Manifest, Status
from threew.bronze.orphans import find_orphans, find_stale_staging
from threew.bronze.plan import build_plan
from threew.bronze.report import RunReport, RunStatus, code_info
from threew.config import Config, load_config
from threew.lock import LockHeld, run_lock
from threew.runs import new_run_id
from threew.source.hub import HfHubClient, HubClient, verify_pin
from threew.source.retry import with_retry

log = logging.getLogger(__name__)

LOCK_FILE = "ingest.lock"  # in storage.state: one run at a time per storage root


def run_ingest(cfg: Config, client: HubClient, *, sleep: Callable[[float], None] = time.sleep) -> int:
    """One ingestion run. Returns the exit code; the details are in the run report."""
    run_id, started_at = new_run_id()
    report = RunReport(run_id, started_at, cfg.env)
    report["code"] = code_info()
    report["config"] = cfg.to_dict()
    status, error, note = RunStatus.FAILED, None, None

    with ExitStack() as held:  # releases the lock only after the report is written
        try:
            owner = {"run_id": run_id, "started_at": started_at.isoformat(), "host": report["run"]["host"]}
            held.enter_context(run_lock(cfg.storage.state / LOCK_FILE, owner))
            status = _run_phases(cfg, client, report, run_id, started_at, sleep)
        except LockHeld as e:
            status, note = RunStatus.SKIPPED, str(e)
        except Exception as e:
            status, error = RunStatus.FAILED, e
            log.exception("run %s failed", run_id)
        except BaseException as e:  # e.g. Ctrl-C: still record it, then stop
            status, error = RunStatus.FAILED, e
            raise
        finally:
            exit_code = report.finish(status, error, note)
            path = report.write(cfg.storage.runs)
            _log_summary(report, path)
    return exit_code


def _run_phases(cfg: Config, client: HubClient, report: RunReport, run_id, started_at, sleep) -> RunStatus:
    source, ingest = cfg.source, cfg.ingest

    def retry(fn):
        return with_retry(fn, ingest.max_retries, ingest.retry_backoff_seconds, sleep=sleep)

    log.info("run %s (%s): %s @ %s", run_id, cfg.env, source.repo_id, source.revision)
    with report.phase("pin"):
        pin = retry(lambda: verify_pin(client, source))
    report["source"] = {
        "repo_id": source.repo_id,
        "repo_type": source.repo_type,
        "revision": source.revision,
        "revision_label": pin.label,
        "label_resolves_to": pin.resolved,
        "tag_moved": pin.tag_moved,
    }

    with report.phase("list"):
        listing = retry(lambda: client.list_files(source.revision, source.include))
    manifest = Manifest(cfg.storage.state)
    with report.phase("plan"):
        plan = build_plan(listing, manifest.current(), ingest.sample_per_class)
    report["plan"] = {
        "files_listed": len(listing),
        **plan.summary(),
        "removed_upstream_paths": list(plan.removed_upstream),
        "rejected_names": [{"path": p, "reason": r} for p, r in plan.rejected],
    }

    with report.phase("fetch"):
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
        report["fetch"] = fetch_stats(results, time.monotonic() - start)

    contract = load_contract(cfg.schema.contract)
    with report.phase("land"):
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
    rows = landing.rows
    report["landing"] = dict(Counter(row["status"] for row in rows))
    report["schema"] = {
        "contract": str(cfg.schema.contract),
        "contract_fingerprint": fingerprint(contract),
        "fail_on_drift": cfg.schema.fail_on_drift,
        **schema_summary(landing.inspections),
    }
    attempts = {r.candidate.path: r.attempts for r in results}
    failures = [
        {"path": row["path"], "error": row["error"], "attempts": attempts.get(row["path"])}
        for row in rows
        if row["status"] == Status.FAILED
    ]
    report["failures"] = failures

    with report.phase("checks"):
        report["orphans"] = find_orphans(cfg.storage, manifest)
        report["stale_staging"] = find_stale_staging(cfg.storage.staging, run_id)

    landed = [row for row in rows if row["status"] == Status.LANDED]
    current = manifest.current()
    report["totals"] = {
        "files_listed": len(listing),
        "files_planned": len(plan.to_download),
        "files_landed": len(landed),
        "files_failed": len(failures),
        "bytes_landed": sum(row["size"] for row in landed),
        "rows_landed": sum(row["num_rows"] or 0 for row in landed),
        "manifest_rows": manifest.read().num_rows,
        "landed_files_total": current.num_rows,
        "rows_total": sum(n or 0 for n in current["num_rows"].to_pylist()),
    }

    if cfg.schema.fail_on_drift and report["schema"]["drifted"]:
        return RunStatus.FAILED
    if failures:
        return RunStatus.PARTIAL
    return RunStatus.SUCCESS


def schema_summary(inspections: dict[str, Inspection]) -> dict[str, Any]:
    """What this run's files look like: fingerprints, codecs and every file that drifted from the contract."""
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


def _log_summary(report: RunReport, path) -> None:
    run, plan, fetch, totals = report["run"], report["plan"], report["fetch"], report["totals"]
    log.info("run %s: %s (exit %s) in %ss", run["run_id"], run["status"], run["exit_code"], run["duration_seconds"])
    if run["note"]:
        log.info("  %s", run["note"])
    if run["error"]:
        log.error("  %s: %s", run["error"]["type"], run["error"]["message"])
    if plan:
        log.info("  plan: NEW %s, CHANGED %s, unchanged %s", plan["new"], plan["changed"], plan["unchanged"])
    if fetch:
        log.info(
            "  fetch: %s OK, %s FAILED, %s bytes, %s MB/s, %s network retries, %s checksum re-downloads",
            fetch["ok"],
            fetch["failed"],
            fetch["bytes"],
            fetch["mb_per_s"],
            fetch["network_retries"],
            fetch["checksum_redownloads"],
        )
    if totals:
        log.info(
            "  landed %s, failed %s; %s files in landing",
            totals["files_landed"],
            totals["files_failed"],
            totals["landed_files_total"],
        )
    for key in ("orphans", "stale_staging"):
        if report[key]:
            log.warning("  %s: %s", key, report[key])
    log.info("  report: %s", path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", choices=["dev", "prod"], required=True)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx2").setLevel(logging.WARNING)  # one INFO line per HTTP request otherwise

    cfg = load_config(args.env)
    return run_ingest(cfg, HfHubClient(cfg.source.repo_id, cfg.source.repo_type))


if __name__ == "__main__":
    sys.exit(main())
