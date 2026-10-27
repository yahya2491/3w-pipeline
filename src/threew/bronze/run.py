"""Full Bronze run: ingest, convert, prune, reconcile and publish, under one lock and one run report.

Usage:
    python -m threew.bronze.run --env dev [--deep] [--report-copy DIR]

Phases, in order:
    ingest     download, verify, land and inspect new and changed files (PARTIAL or FAILED, as in ingest.py)
    convert    convert landed files whose Bronze output doesn't exist yet; a failure makes the run PARTIAL
    prune      delete Bronze outputs that no current manifest row points to; an error makes the run FAILED
    reconcile  the six checks in reconcile.py, fast (--deep adds countDistinct); any failed check makes the run FAILED
    publish    write <root>/bronze/bronze_ready.json, only if everything above was SUCCESS

The ready marker is withdrawn as soon as a run holds the lock and written again only at the end of a fully
successful run, so a reader never sees it while the table is changing, or after a run that didn't succeed.

Exit code: 0 SUCCESS or SKIPPED, 1 PARTIAL, 2 FAILED. `python -m threew.bronze.ingest` still runs the ingest phase
on its own.
"""

import argparse
import json
import logging
import os
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from threew.bronze.convert import ConvertStatus, convert_all, prune_outputs
from threew.bronze.ingest import execute, ingest_phases
from threew.bronze.manifest import Manifest
from threew.bronze.reconcile import ReconcileResult, reconcile
from threew.bronze.report import RunReport, RunStatus
from threew.config import Config, load_config
from threew.source.hub import HfHubClient, HubClient
from threew.spark import get_spark

log = logging.getLogger(__name__)

SparkFactory = Callable[[Config], AbstractContextManager[SparkSession]]


@contextmanager
def spark_session(cfg: Config) -> Iterator[SparkSession]:
    """A Spark session for the reconcile phase only, stopped afterwards whatever happens."""
    session = get_spark(cfg)
    session.sparkContext.setLogLevel("WARN")
    try:
        yield session
    finally:
        session.stop()


def run_bronze(
    cfg: Config,
    client: HubClient,
    *,
    deep: bool = False,
    sleep: Callable[[float], None] = time.sleep,
    spark_factory: SparkFactory = spark_session,
    report_copy: Path | None = None,
) -> int:
    """One full Bronze run. Returns the exit code; the details are in the run report.

    With report_copy, the run's report, log and reconcile plan are copied into that folder at the very end.
    """

    def body(report: RunReport, run_id: str, started_at: datetime) -> RunStatus:
        return _bronze_phases(cfg, client, report, run_id, started_at, sleep, deep, spark_factory)

    return execute(cfg, body, report_copy=report_copy)


def _bronze_phases(cfg, client, report: RunReport, run_id, started_at, sleep, deep, spark_factory) -> RunStatus:
    withdraw_marker(cfg)  # the table is about to change: not ready until this run says so
    status = ingest_phases(cfg, client, report, run_id, started_at, sleep)
    if status is RunStatus.FAILED:
        report["publish"] = _not_published(cfg, "the ingest phase FAILED")
        return status
    manifest = Manifest(cfg.storage.state)

    with report.phase("convert"):
        start = time.monotonic()
        results = convert_all(manifest, cfg.storage, cfg.ingest.workers, only_pending=True)
        seconds = round(time.monotonic() - start, 3)
    converted = [r for r in results if r.status is ConvertStatus.OK]
    failed = [r for r in results if r.status is ConvertStatus.FAILED]
    skipped = [r for r in results if r.status is ConvertStatus.SKIPPED]
    report["convert"] = {
        "pending": len(converted) + len(failed),
        "converted": len(converted),
        "failed": len(failed),
        "skipped": len(skipped),
        "rows": sum(r.rows_out for r in converted),
        "bytes_in": sum(r.input_bytes for r in converted),
        "bytes_out": sum(r.output_bytes for r in converted),
        "seconds": seconds,
        "failures": [{"path": r.source_path, "error": r.error} for r in failed],
        "skipped_files": [{"path": r.source_path, "reason": r.error} for r in skipped],
    }
    if failed:
        status = RunStatus.PARTIAL

    with report.phase("prune"):
        report["prune"] = prune_outputs(manifest, cfg.storage)

    # Files whose conversion failed are missing on purpose: check the rest of the table, and stay PARTIAL
    sha_by_path = dict(zip(*(manifest.current()[c].to_pylist() for c in ("path", "sha256")), strict=True))
    exclude = frozenset((r.source_path, sha_by_path[r.source_path]) for r in failed)
    with report.phase("reconcile"), spark_factory(cfg) as spark:
        result = reconcile(spark, cfg, manifest, deep=deep, exclude=exclude)
    report["reconcile"] = result.to_dict()
    (cfg.storage.runs / f"{run_id}_reconcile_plan.txt").parent.mkdir(parents=True, exist_ok=True)
    (cfg.storage.runs / f"{run_id}_reconcile_plan.txt").write_text(result.plan)
    if report["totals"] is not None:
        checks = {c.number: c for c in result.checks}
        report["totals"] |= {
            "bronze_files": checks[3].details["files_in_bronze"],
            "bronze_rows": checks[2].details["spark"],
        }

    if not result.passed:
        withdraw_marker(cfg)
        failed_checks = [c.number for c in result.checks if not c.passed]
        report["publish"] = _not_published(cfg, f"reconciliation failed: checks {failed_checks}")
        return RunStatus.FAILED
    if status is not RunStatus.SUCCESS:
        report["publish"] = _not_published(cfg, f"the run is {status.value}")
        return status

    with report.phase("publish"):
        marker = publish_marker(cfg, report, result, run_id)
    report["publish"] = {"written": True, "path": str(cfg.storage.bronze_ready), "reason": None, "marker": marker}
    return RunStatus.SUCCESS


def publish_marker(cfg: Config, report: RunReport, result: ReconcileResult, run_id: str) -> dict[str, Any]:
    """Write bronze_ready.json atomically: what a reader of the Bronze table can rely on, and since when."""
    checks = {c.number: c for c in result.checks}
    marker = {
        "run_id": run_id,
        "published_at": datetime.now(UTC).isoformat(),
        "env": cfg.env,
        "bronze_table": str(cfg.storage.bronze),
        "source": {"repo_id": cfg.source.repo_id, "revision": cfg.source.revision},
        "files": checks[3].details["files_in_bronze"],
        "rows": checks[2].details["spark"],
        "reconcile": {"mode": result.mode, "passed": True, "checks": {str(c.number): c.passed for c in result.checks}},
        "code_commit": report["code"]["commit"],
        "report": str(cfg.storage.runs / f"{run_id}.json"),
    }
    path = cfg.storage.bronze_ready
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(marker, indent=2) + "\n")
    os.replace(tmp, path)
    return marker


def withdraw_marker(cfg: Config) -> None:
    cfg.storage.bronze_ready.unlink(missing_ok=True)


def _not_published(cfg: Config, reason: str) -> dict[str, Any]:
    return {"written": False, "path": str(cfg.storage.bronze_ready), "reason": reason, "marker": None}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", choices=["dev", "prod"], required=True)
    parser.add_argument("--deep", action="store_true", help="reconcile with countDistinct (a strict row-order proof)")
    parser.add_argument("--report-copy", type=Path, metavar="DIR", help="copy the run's report, log and plan into DIR")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx2").setLevel(logging.WARNING)  # one INFO line per HTTP request otherwise
    logging.getLogger("py4j").setLevel(logging.WARNING)

    cfg = load_config(args.env)
    client = HfHubClient(cfg.source.repo_id, cfg.source.repo_type)
    return run_bronze(cfg, client, deep=args.deep, report_copy=args.report_copy)


if __name__ == "__main__":
    sys.exit(main())
