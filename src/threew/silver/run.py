"""Build and verify Silver. Production input requires an explicit isolated output root.

Usage:
    python -m threew.silver.run --env dev
    python -m threew.silver.run --env prod --output-root /mnt/silver-data/silver-schedule
"""

import argparse
import hashlib
import json
import logging
import math
import os
import shutil
import sys
import time
import uuid
from collections import Counter
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, ExitStack, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from threew.bronze.ingest import LOCK_FILE
from threew.bronze.report import code_info
from threew.config import REPO_ROOT, Config, load_config
from threew.lock import LockHeld, run_lock
from threew.silver.automation import build_signature, reusable_build
from threew.silver.batch import DEFAULT_BATCH_ROWS, merge_quality_summaries, plan_batches
from threew.silver.input import current_manifest_counts, current_manifest_rows, read_bronze_ready_marker
from threew.silver.issues import USAGE_POLICY, build_quality_issues, quarantine_issues, summarize_quality
from threew.silver.output import CONTRACT_VERSION, export_observations
from threew.silver.prepare import prepare_observations, summarize_preparation
from threew.silver.quality import build_quality_summaries
from threew.silver.reconcile import input_checks, output_checks, unique_non_null_keys
from threew.silver.reference import OBSERVATION_KEY, RECORDING_KEY, REFERENCE_VERSION, build_reference_tables
from threew.silver.rules import DEFAULT_RULES_PATH, load_rules
from threew.silver.transform import build_silver_observations
from threew.spark import cleanup_spark, get_spark

SparkFactory = Callable[[Config], AbstractContextManager[SparkSession]]


@contextmanager
def spark_session(cfg: Config) -> Iterator[SparkSession]:
    session = get_spark(cfg)
    session.sparkContext.setLogLevel("WARN")
    try:
        yield session
    finally:
        cleanup_spark(session.stop, "stop session")


def _write_json_atomically(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _overlap(a: Path, b: Path) -> bool:
    return a == b or a.is_relative_to(b) or b.is_relative_to(a)


def _output_root(cfg: Config, output_root: Path | None) -> Path:
    if cfg.env == "prod" and output_root is None:
        raise ValueError(
            "Production input requires an explicit isolated --output-root; canonical production Silver is protected."
        )
    if output_root is None:
        return cfg.storage.silver
    path = Path(output_root).expanduser().resolve()
    protected = (cfg.storage.bronze.parent, cfg.storage.landing.parent, cfg.storage.staging)
    if path == cfg.storage.root or cfg.storage.root.is_relative_to(path) or any(_overlap(path, p) for p in protected):
        raise ValueError("Silver output must be separate from Bronze, landing, staging and their ancestors.")
    if cfg.env == "prod" and _overlap(path, cfg.storage.silver):
        raise ValueError("Canonical production Silver is protected; choose a separate --output-root.")
    return path


@contextmanager
def _phase(report: dict[str, Any], name: str) -> Iterator[None]:
    report["phase"] = name
    started = time.monotonic()
    try:
        yield
    finally:
        previous = report["phase_seconds"].get(name, 0)
        report["phase_seconds"][name] = round(previous + time.monotonic() - started, 3)


def _require_checks(report: dict[str, Any], checks: dict[str, bool]) -> None:
    report["reconcile"]["checks"].update(checks)
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise RuntimeError(f"Silver reconciliation failed: {', '.join(failed)}")


def _run_batch(spark, cfg, batch, table_paths, rules, report, batch_report, *, first):
    """Append one disjoint batch and verify only its newly written Parquet files."""
    keys = spark.createDataFrame(batch.counts, "_source_path string, _file_sha256 string, bronze_rows long")
    bronze = (
        spark.read.parquet(str(cfg.storage.bronze))
        .filter(
            F.col("source_type").isin(sorted({row["_manifest_source_type"] for row in batch.metadata}))
            & F.col("label").isin(sorted({row["_manifest_label"] for row in batch.metadata}))
            & F.col("_source_path").isin([row["_source_path"] for row in batch.counts])
        )
        .join(F.broadcast(keys.select(*RECORDING_KEY)), list(RECORDING_KEY), "left_semi")
    )
    bronze_counts = bronze.groupBy(*RECORDING_KEY).count().withColumnRenamed("count", "bronze_rows")
    bronze_counts.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        with _phase(report, "input_checks"):
            checks = input_checks(spark, bronze_counts, {"rows": batch.rows, "files": len(batch.counts)}, batch.counts)
            checks.update(
                {
                    "bronze_observation_keys_are_unique_and_non_null": unique_non_null_keys(bronze, OBSERVATION_KEY),
                    "bronze_row_positions_are_non_negative": bronze.filter("_row_in_file < 0").limit(1).count() == 0,
                }
            )
            batch_report["checks"].update(checks)
            _require_checks(report, checks)
        with _phase(report, "transform_and_write"):
            source_silver = build_silver_observations(bronze, batch.metadata)
            # Never persist the wide observation table: Parquet is the saved copy.
            silver = export_observations(prepare_observations(source_silver, rules))
            summaries = build_quality_summaries(source_silver)
            issues = build_quality_issues(source_silver, rules).persist(StorageLevel.MEMORY_AND_DISK)
            tables = {
                "silver_observations": silver,
                "instance_quality": summaries.instance_quality,
                "sensor_availability": summaries.sensor_availability,
                "quality_issues": issues,
                "quality_quarantine": quarantine_issues(issues),
            }
            if first:
                tables.update(build_reference_tables(spark, rules))
            batch_files = {}
            try:
                for name, table in tables.items():
                    path = table_paths[name]
                    before = set(path.glob("*.parquet"))
                    writer = table.write.mode("errorifexists" if first else "append")
                    if name == "silver_observations":
                        writer = writer.option("parquet.block.size", 32 * 1024 * 1024).option(
                            "parquet.page.size", 256 * 1024
                        )
                    writer.parquet(str(path))
                    batch_files[name] = sorted(set(path.glob("*.parquet")) - before)
                    if not batch_files[name]:
                        raise RuntimeError(f"No Parquet file was written for batch table {name}.")
            finally:
                cleanup_spark(issues.unpersist, "release issues")
                cleanup_spark(summaries.unpersist, "release summaries")
        with _phase(report, "read_back_and_check"):
            saved = {
                name: spark.read.parquet(*[str(path) for path in batch_files[name]])
                if name in batch_files
                else spark.read.parquet(str(path))
                for name, path in table_paths.items()
            }
            checks = output_checks(
                bronze,
                bronze_counts,
                batch.metadata,
                saved["silver_observations"],
                saved["instance_quality"],
                saved["sensor_availability"],
                {
                    name: saved[name]
                    for name in ("quality_issues", "quality_quarantine", "event_reference", "sensor_reference")
                },
                rules=rules,
            )
            batch_report["checks"].update(checks)
            _require_checks(report, checks)
            counts = {name: table.count() for name, table in saved.items()}
            batch_report["table_rows"] = counts
            batch_report["parquet_bytes"] = {
                name: sum(path.stat().st_size for path in paths) for name, paths in batch_files.items()
            }
            preparation = summarize_preparation(saved["silver_observations"], rules, source_silver)
            quality = summarize_quality(
                saved["quality_issues"],
                saved["instance_quality"],
                saved["sensor_availability"],
                counts["silver_observations"],
            )
            return counts, preparation, quality
    finally:
        cleanup_spark(bronze_counts.unpersist, "release Bronze counts")


def run_silver(
    cfg: Config,
    *,
    spark_factory: SparkFactory = spark_session,
    output_root: Path | None = None,
    rules_path: Path = DEFAULT_RULES_PATH,
    batch_rows: int = DEFAULT_BATCH_ROWS,
    skip_unchanged: bool = False,
    report_copy: Path | None = None,
    min_free_gib: float = 0,
) -> dict[str, Any]:
    """Hold the writer's input lock and the output lock until verified publication."""
    if type(batch_rows) is not int or batch_rows <= 0:
        raise ValueError("batch_rows must be a positive integer.")
    if not math.isfinite(min_free_gib) or min_free_gib < 0:
        raise ValueError("Minimum free GiB must be finite and non-negative.")
    silver_root = _output_root(cfg, output_root)
    rules = load_rules(rules_path)
    now, started = datetime.now(UTC), time.monotonic()
    run_id = f"{now:%Y%m%dT%H%M%S%fZ}-{uuid.uuid4().hex[:8]}"
    run_root = silver_root / "runs" / run_id
    report_path = silver_root / "_runs" / f"{run_id}.json"
    ready_path = silver_root / "silver_ready.json"
    table_paths = {
        name: run_root / name
        for name in (
            "silver_observations",
            "instance_quality",
            "sensor_availability",
            "quality_issues",
            "quality_quarantine",
            "event_reference",
            "sensor_reference",
        )
    }
    contract = REPO_ROOT / "docs" / "silver" / "SILVER_CONTRACT.md"
    config = cfg.to_dict()
    report = {
        "run_id": run_id,
        "env": cfg.env,
        "started_at": now.isoformat(),
        "status": "RUNNING",
        "phase": "lock",
        "phase_seconds": {},
        "error": None,
        "code": code_info(),
        "config": config,
        "config_sha256": hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest(),
        "contract": {"version": CONTRACT_VERSION, "sha256": hashlib.sha256(contract.read_bytes()).hexdigest()},
        "execution": {"batch_rows": batch_rows, "complete_recordings": True},
        "batches": [],
        "quality_rules": rules.snapshot(),
        "preparation_summary": None,
        "output_root": str(silver_root),
        "source": None,
        "quality_summary": None,
        "usage_policy": USAGE_POLICY,
        "reference_version": REFERENCE_VERSION,
        "tables": {name: {"path": str(path), "rows": None} for name, path in table_paths.items()},
        "reconcile": {"passed": False, "checks": {}, "counts": {}},
        "published": False,
    }
    with ExitStack() as held:
        try:
            owner = {"run_id": run_id, "started_at": now.isoformat(), "layer": "silver"}
            with _phase(report, "lock"):
                held.enter_context(run_lock(cfg.storage.state / LOCK_FILE, owner))
                held.enter_context(run_lock(silver_root / "_state" / "silver.lock", owner))
            with _phase(report, "input"):
                bronze_marker = read_bronze_ready_marker(cfg)
                report["source"] = {"bronze_run_id": bronze_marker["run_id"], **bronze_marker["source"]}
                manifest_rows = current_manifest_rows(cfg)
                manifest_counts = current_manifest_counts(cfg)
                if run_root.exists():
                    raise FileExistsError(f"Silver run folder already exists: {run_root}")
                signature = build_signature(
                    cfg, bronze_marker, manifest_rows, manifest_counts, rules, batch_rows, report["contract"]["sha256"]
                )
                report["build_signature"] = signature
                previous = reusable_build(ready_path, signature) if skip_unchanged else None
                if previous is not None:
                    report["status"] = "SKIPPED"
                    report["skip_reason"] = "unchanged"
                    report["reused_run_id"] = previous["run_id"]
                    print(f"Silver unchanged; reusing verified run {previous['run_id']}", flush=True)
                    return previous
                if min_free_gib:
                    free = shutil.disk_usage(silver_root).free
                    report["disk_preflight"] = {"free_bytes": free, "minimum_gib": min_free_gib}
                    if free < min_free_gib * 1024**3:
                        raise OSError(
                            f"Silver requires {min_free_gib:g} GiB free for a new build; found {free / 1024**3:.1f} GiB"
                        )
            batches = plan_batches(manifest_rows, manifest_counts, batch_rows)
            report["execution"]["batch_count"] = len(batches)
            # Audit the whole input before batch filters can hide missing/extra recordings.
            # Only the small per-recording counts are cached here.
            with spark_factory(cfg) as spark, _phase(report, "input_checks"):
                bronze_counts = (
                    spark.read.parquet(str(cfg.storage.bronze))
                    .groupBy(*RECORDING_KEY)
                    .count()
                    .withColumnRenamed("count", "bronze_rows")
                    .persist(StorageLevel.MEMORY_AND_DISK)
                )
                try:
                    _require_checks(report, input_checks(spark, bronze_counts, bronze_marker, manifest_counts))
                finally:
                    cleanup_spark(bronze_counts.unpersist, "release input audit")
            table_totals, preparation_totals, quality_parts = Counter(), Counter(), []
            for index, batch in enumerate(batches, 1):
                batch_started = time.monotonic()
                batch_report = {
                    "batch": index,
                    "expected_rows": batch.rows,
                    "recordings": len(batch.counts),
                    "recording_keys": [{name: row[name] for name in RECORDING_KEY} for row in batch.counts],
                    "status": "RUNNING",
                    "checks": {},
                }
                report["batches"].append(batch_report)
                _write_json_atomically(report_path, report)
                print(
                    f"Silver batch {index}/{len(batches)}: {len(batch.counts)} complete recordings, "
                    f"{batch.rows:,} observations"
                    + (" (one recording exceeds batch target)" if batch.rows > batch_rows else ""),
                    flush=True,
                )
                try:
                    # Stop each SparkContext to release its cache and shuffle directories.
                    with spark_factory(cfg) as spark:
                        batch_counts, preparation, quality = _run_batch(
                            spark, cfg, batch, table_paths, rules, report, batch_report, first=index == 1
                        )
                    table_totals.update(
                        {
                            name: count
                            for name, count in batch_counts.items()
                            if index == 1 or name not in ("event_reference", "sensor_reference")
                        }
                    )
                    preparation_totals.update(preparation)
                    quality_parts.append(quality)
                    batch_report["status"] = "SUCCESS"
                except BaseException:
                    batch_report["status"] = "FAILED"
                    raise
                finally:
                    batch_report["duration_seconds"] = round(time.monotonic() - batch_started, 3)
                    _write_json_atomically(report_path, report)
                print(f"Silver batch {index}/{len(batches)} verified; Spark temporary data released", flush=True)
            counts = {
                "bronze_rows": bronze_marker["rows"],
                "bronze_recordings": bronze_marker["files"],
                "silver_observation_rows": table_totals["silver_observations"],
                # Batches have complete, disjoint recordings and exact per-recording checks.
                "silver_recordings": sum(len(batch.counts) for batch in batches),
                **{f"{name}_rows": value for name, value in table_totals.items() if name != "silver_observations"},
            }
            report["reconcile"]["counts"] = counts
            _require_checks(
                report,
                {
                    "all_batches_verified": all(batch["status"] == "SUCCESS" for batch in report["batches"]),
                    "batch_totals_match_bronze": counts["silver_observation_rows"] == bronze_marker["rows"]
                    and counts["silver_recordings"] == bronze_marker["files"],
                },
            )
            for name, path in table_paths.items():
                report["tables"][name]["rows"] = table_totals[name]
                report["tables"][name]["parquet_bytes"] = sum(p.stat().st_size for p in path.glob("*.parquet"))
            report["preparation_summary"] = dict(preparation_totals)
            report["quality_summary"] = merge_quality_summaries(quality_parts)
            with _phase(report, "publish"):
                if read_bronze_ready_marker(cfg) != bronze_marker:
                    raise RuntimeError("Bronze ready marker changed during the Silver run.")
                report["reconcile"]["checks"]["bronze_marker_unchanged"] = True
                report["reconcile"]["passed"] = True
                report["status"], report["published"] = "SUCCESS", True
                report["finished_at"] = datetime.now(UTC).isoformat()
                report["duration_seconds"] = round(time.monotonic() - started, 3)
                _write_json_atomically(report_path, report)
                marker = {
                    "run_id": run_id,
                    "published_at": datetime.now(UTC).isoformat(),
                    "env": cfg.env,
                    "source": report["source"],
                    "tables": report["tables"],
                    "reconcile": report["reconcile"],
                    "report": str(report_path),
                    "quality_summary": report["quality_summary"],
                    "usage_policy": report["usage_policy"],
                    "reference_version": report["reference_version"],
                    "quality_rules": report["quality_rules"],
                    "preparation_summary": report["preparation_summary"],
                    "contract_version": CONTRACT_VERSION,
                    "build_signature": report["build_signature"],
                    "execution": report["execution"],
                }
                _write_json_atomically(ready_path, marker)
        except BaseException as exc:
            report["status"] = "SKIPPED" if isinstance(exc, LockHeld) else "FAILED"
            report["published"] = False
            report["reconcile"]["passed"] = False
            report["error"] = {"type": type(exc).__name__, "message": str(exc), "phase": report["phase"]}
            raise
        finally:
            report["finished_at"] = datetime.now(UTC).isoformat()
            report["duration_seconds"] = round(time.monotonic() - started, 3)
            if report["status"] != "SUCCESS":
                _write_json_atomically(report_path, report)
            if report_copy is not None:

                def copy_report():
                    destination = Path(report_copy)
                    destination.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(report_path, destination / report_path.name)

                cleanup_spark(copy_report, "copy the Silver job report")
    print(
        f"Silver run: {run_id}; {counts['silver_observation_rows']:,} observations, {counts['instance_quality_rows']:,} recording summaries, {counts['sensor_availability_rows']:,} sensor summaries"
    )
    print(f"Reconciliation: all {len(report['reconcile']['checks'])} checks passed")
    print(f"Ready marker: {ready_path}")
    return marker


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", choices=("dev", "prod"), required=True)
    parser.add_argument("--output-root", type=Path, help="isolated Silver destination (required for production input)")
    parser.add_argument(
        "--rules", type=Path, default=DEFAULT_RULES_PATH, help="versioned Silver quality/cleaning rules"
    )
    parser.add_argument(
        "--batch-rows",
        type=int,
        default=DEFAULT_BATCH_ROWS,
        help="target observations per batch; recordings are never split (default: 1000000)",
    )
    parser.add_argument(
        "--skip-unchanged", action="store_true", help="reuse matching verified output without starting Spark"
    )
    parser.add_argument("--report-copy", type=Path, help="copy this attempt's JSON report for CI artifacts")
    parser.add_argument(
        "--min-free-gib", type=float, default=0, help="free-space floor for new builds; reuse is allowed below it"
    )
    args = parser.parse_args(argv)
    try:
        run_silver(
            load_config(args.env),
            output_root=args.output_root,
            rules_path=args.rules,
            batch_rows=args.batch_rows,
            skip_unchanged=args.skip_unchanged,
            report_copy=args.report_copy,
            min_free_gib=args.min_free_gib,
        )
    except LockHeld as exc:
        print(f"Silver skipped: {exc}", file=sys.stderr)
        return 0
    except Exception:
        logging.getLogger(__name__).exception("Silver failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
