"""Serve the Gold views: load the published tables and run gold/views.sql with Spark SQL.

Reads only what gold_ready.json points to, so it never sees a build that failed its checks.

    python gold/query.py --env dev                          # print both summaries
    python gold/query.py --env prod --csv docs/evidence/gold  # also save them as CSV
"""

import argparse
import csv
import dataclasses
import json
from pathlib import Path

from threew.config import load_config
from threew.spark import cleanup_spark, get_spark

PROD_GOLD_ROOT = Path("/mnt/silver-data/gold")
VIEWS_SQL = Path(__file__).with_name("views.sql")
SUMMARIES = ("well_event_summary", "event_type_summary")


def register_views(spark, tables):
    """Register the Gold tables as temp views, then create the views defined in views.sql."""
    for name, df in tables.items():
        df.createOrReplaceTempView(name)
    for statement in VIEWS_SQL.read_text().split(";"):
        # drop comment lines so the empty piece after the last semicolon is skipped
        sql = "\n".join(line for line in statement.splitlines() if not line.strip().startswith("--")).strip()
        if sql:
            spark.sql(sql)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", choices=["dev", "prod"], default="dev")
    parser.add_argument("--gold-root", type=Path, help="folder holding gold_ready.json")
    parser.add_argument("--csv", type=Path, help="also save each summary to <folder>/<view>.csv")
    args = parser.parse_args(argv)

    cfg = load_config(args.env)
    gold_root = args.gold_root or (PROD_GOLD_ROOT if args.env == "prod" else cfg.storage.root / "gold")
    marker = json.loads((gold_root / "gold_ready.json").read_text())
    print(f"Gold run {marker['run_id']} (built {marker['built_at']}, Silver run {marker['silver']['run_id']})")

    spark = get_spark(dataclasses.replace(cfg, spark=dataclasses.replace(cfg.spark, app_name="3w-gold-query")))
    try:
        tables = {name: spark.read.parquet(marker["tables"][name]["path"]) for name in ("recordings", "event_phases")}
        register_views(spark, tables)
        for view in SUMMARIES:
            df = spark.table(view)
            print(f"\n{view}")
            df.show(200, truncate=False)
            if args.csv:
                args.csv.mkdir(parents=True, exist_ok=True)
                with open(args.csv / f"{view}.csv", "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(df.columns)
                    writer.writerows(df.collect())
    finally:
        cleanup_spark(spark.stop, "stop Spark")


if __name__ == "__main__":
    main()
