"""Reconcile the Bronze table against the manifest and the landing files with Spark.

Prints each check, writes the result to <storage.runs>/reconcile_<timestamp>.json and the physical plan of the
per-file query to docs/evidence/bronze/reconcile_plan_<env>.txt. Exits 0 if every check passes, 1 otherwise.

Usage:
    python scripts/reconcile_bronze.py --env dev
"""

import argparse
import json
import logging
import os
import sys
from datetime import UTC, datetime

from threew.bronze.manifest import Manifest
from threew.bronze.reconcile import reconcile
from threew.config import REPO_ROOT, load_config
from threew.spark import get_spark

EVIDENCE_DIR = REPO_ROOT / "docs" / "evidence" / "bronze"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", choices=["dev", "prod"], required=True)
    parser.add_argument("--profile", action="store_true", help="also time the per-file pass without countDistinct")
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    cfg = load_config(args.env)
    spark = get_spark(cfg)
    spark.sparkContext.setLogLevel("WARN")
    try:
        result = reconcile(spark, cfg, Manifest(cfg.storage.state), profile=args.profile)
    finally:
        spark.stop()

    print(f"Env: {cfg.env} | {result.bronze_path} | Spark {cfg.spark.master}, {cfg.spark.driver_memory}")
    for c in result.checks:
        summary = {k: v for k, v in c.details.items() if not isinstance(v, list) and k != "shared_pass"}
        shared = " (one shared pass)" if c.details.get("shared_pass") else ""
        print(f"  {c.number} {c.name:<11} {'PASS' if c.passed else 'FAIL'}  {c.seconds:7.2f} s{shared}  {summary}")
        for k, v in c.details.items():
            if isinstance(v, list) and v and k != "files":
                print(f"      {k}: {v[:5]}")
        if c.number == 6:
            for f in c.details["files"]:
                print(f"      compared {f['path']}: {f['rows']:,} rows, {f['values']:,} values")
    print(
        f"Total Spark time: {result.spark_seconds:.2f} s | plan has BroadcastHashJoin: {'BroadcastHashJoin' in result.plan}"
    )
    if result.profile:
        print(f"Profile: {result.profile}")
    print("RESULT:", "ALL PASS" if result.passed else "FAILED")

    stamp = f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
    out = cfg.storage.runs / f"reconcile_{stamp}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(f".{out.name}.tmp")
    tmp.write_text(json.dumps({"finished_at": stamp, **result.to_dict()}, indent=2) + "\n")
    os.replace(tmp, out)
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    plan_path = EVIDENCE_DIR / f"reconcile_plan_{cfg.env}.txt"
    plan_path.write_text(result.plan)
    print(f"Result: {out}\nPlan:   {plan_path}")
    return 0 if result.passed else 1


if __name__ == "__main__":
    sys.exit(main())
