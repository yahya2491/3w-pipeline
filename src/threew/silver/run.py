"""Build and verify Silver. Production input requires an explicit isolated output root.

Usage:
    python -m threew.silver.run --env dev
    python -m threew.silver.run --env prod --output-root /home/ubuntu/3w-data/silver-oh
"""

import argparse
import hashlib
import json
import logging
import os
import sys
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, ExitStack, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import SparkSession

from threew.bronze.ingest import LOCK_FILE
from threew.bronze.report import code_info
from threew.config import REPO_ROOT, Config, load_config
from threew.lock import LockHeld, run_lock
from threew.silver.input import current_manifest_counts, current_manifest_rows, read_bronze_ready_marker
from threew.silver.issues import USAGE_POLICY, build_quality_issues, quarantine_issues, summarize_quality
from threew.silver.prepare import prepare_observations, summarize_preparation
from threew.silver.quality import build_quality_summaries
from threew.silver.reconcile import OBSERVATION_KEY, RECORDING_KEY, input_checks, output_checks, unique_non_null_keys
from threew.silver.reference import REFERENCE_VERSION, build_reference_tables
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
    protected = [cfg.storage.bronze.parent, cfg.storage.landing.parent, cfg.storage.staging, cfg.storage.root]
    # Siblings such as <root>/silver-oh are allowed; the input root itself and
    # any ancestors are not, and neither are Bronze/landing/staging directories.
    if (
        path == cfg.storage.root
        or cfg.storage.root.is_relative_to(path)
        or any(_overlap(path, p) for p in protected[:-1])
    ):
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
        report["phase_seconds"][name] = round(time.monotonic() - started, 3)


def _require_checks(report: dict[str, Any], checks: dict[str, bool]) -> None:
    report["reconcile"]["checks"].update(checks)
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise RuntimeError(f"Silver reconciliation failed: {', '.join(failed)}")


def run_silver(
    cfg: Config,
    *,
    spark_factory: SparkFactory = spark_session,
    output_root: Path | None = None,
    rules_path: Path = DEFAULT_RULES_PATH,
) -> dict[str, Any]:
    """Hold the writer's input lock and the output lock until verified publication."""
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
        "contract": {"version": 2, "sha256": hashlib.sha256(contract.read_bytes()).hexdigest()},
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
            with spark_factory(cfg) as spark:
                bronze = spark.read.parquet(str(cfg.storage.bronze))
                bronze_counts = (
                    bronze.groupBy(*RECORDING_KEY)
                    .count()
                    .withColumnRenamed("count", "bronze_rows")
                    .persist(StorageLevel.MEMORY_AND_DISK)
                )
                silver = None
                try:
                    with _phase(report, "input_checks"):
                        _require_checks(report, input_checks(spark, bronze_counts, bronze_marker, manifest_counts))
                        # Reject duplicate identities before windows, joins or writes can hide ambiguity.
                        _require_checks(
                            report,
                            {
                                "bronze_observation_keys_are_unique_and_non_null": unique_non_null_keys(
                                    bronze, OBSERVATION_KEY
                                ),
                                "bronze_row_positions_are_non_negative": bronze.filter("_row_in_file < 0")
                                .limit(1)
                                .count()
                                == 0,
                            },
                        )
                    with _phase(report, "transform_and_write"):
                        source_silver = build_silver_observations(bronze, manifest_rows)
                        # Wide prepared rows must not compete with windows/writers for heap.
                        # Disk reuse still avoids recomputing temporal features during checks.
                        silver = prepare_observations(source_silver, rules).persist(StorageLevel.DISK_ONLY)
                        summaries = build_quality_summaries(source_silver)
                        issues = build_quality_issues(source_silver, rules).persist(StorageLevel.MEMORY_AND_DISK)
                        additional = {
                            "quality_issues": issues,
                            "quality_quarantine": quarantine_issues(issues),
                            **build_reference_tables(spark, rules),
                        }
                        try:
                            (
                                silver.write.mode("errorifexists")
                                .option("parquet.block.size", 32 * 1024 * 1024)
                                .option("parquet.page.size", 256 * 1024)
                                .parquet(str(table_paths["silver_observations"]))
                            )
                            summaries.instance_quality.write.mode("errorifexists").parquet(
                                str(table_paths["instance_quality"])
                            )
                            summaries.sensor_availability.write.mode("errorifexists").parquet(
                                str(table_paths["sensor_availability"])
                            )
                            for name, table in additional.items():
                                table.write.mode("errorifexists").parquet(str(table_paths[name]))
                        finally:
                            cleanup_spark(issues.unpersist, "release issues")
                            cleanup_spark(summaries.unpersist, "release summaries")
                    with _phase(report, "read_back_and_check"):
                        saved = {name: spark.read.parquet(str(path)) for name, path in table_paths.items()}
                        counts = {
                            "bronze_rows": bronze_marker["rows"],
                            "bronze_recordings": bronze_marker["files"],
                            "silver_observation_rows": saved["silver_observations"].count(),
                            "silver_recordings": saved["silver_observations"].select(*RECORDING_KEY).distinct().count(),
                            "instance_quality_rows": saved["instance_quality"].count(),
                            "sensor_availability_rows": saved["sensor_availability"].count(),
                        }
                        for name in ("quality_issues", "quality_quarantine", "event_reference", "sensor_reference"):
                            counts[f"{name}_rows"] = saved[name].count()
                        report["reconcile"]["counts"] = counts
                        for name, key in (
                            ("silver_observations", "silver_observation_rows"),
                            ("instance_quality", "instance_quality_rows"),
                            ("sensor_availability", "sensor_availability_rows"),
                            *[
                                (name, f"{name}_rows")
                                for name in (
                                    "quality_issues",
                                    "quality_quarantine",
                                    "event_reference",
                                    "sensor_reference",
                                )
                            ],
                        ):
                            report["tables"][name]["rows"] = counts[key]
                            report["tables"][name]["parquet_bytes"] = sum(
                                path.stat().st_size for path in table_paths[name].rglob("*.parquet")
                            )
                        _require_checks(
                            report,
                            output_checks(
                                bronze,
                                bronze_counts,
                                manifest_rows,
                                saved["silver_observations"],
                                saved["instance_quality"],
                                saved["sensor_availability"],
                                {
                                    name: saved[name]
                                    for name in (
                                        "quality_issues",
                                        "quality_quarantine",
                                        "event_reference",
                                        "sensor_reference",
                                    )
                                },
                                rules=rules,
                                preparation_source=source_silver,
                            ),
                        )
                        report["preparation_summary"] = summarize_preparation(saved["silver_observations"], rules)
                        report["quality_summary"] = summarize_quality(
                            saved["quality_issues"],
                            saved["instance_quality"],
                            saved["sensor_availability"],
                            counts["silver_observation_rows"],
                        )
                finally:
                    if silver is not None:
                        cleanup_spark(silver.unpersist, "release prepared observations")
                    cleanup_spark(bronze_counts.unpersist, "release Bronze counts")
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
                    "contract_version": 2,
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
    parser.add_argument("--rules", type=Path, default=DEFAULT_RULES_PATH, help="versioned Silver quality/feature rules")
    args = parser.parse_args(argv)
    try:
        run_silver(load_config(args.env), output_root=args.output_root, rules_path=args.rules)
    except LockHeld as exc:
        print(f"Silver skipped: {exc}", file=sys.stderr)
        return 0
    except Exception:
        logging.getLogger(__name__).exception("Silver failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
