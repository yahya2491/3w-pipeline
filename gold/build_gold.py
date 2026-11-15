"""Build the Gold well-event product from one validated Silver publication.

Gold serves two consumers (see docs/gold/GOLD_CONTRACT.md):
    recordings, event_phases -> operations engineers, through the SQL views in gold/views.sql
    window_features          -> the event-classifier demo in consumers/event_classifier/

Each build writes to <gold root>/runs/<run_id>/, checks the tables it wrote, and only then
replaces <gold root>/gold_ready.json. If a check fails, the previous marker stays in place,
so consumers keep reading the last good build.

    python gold/build_gold.py --env dev
    python gold/build_gold.py --env prod --silver-root /mnt/silver-data/3w-data/silver-v3
"""

import argparse
import dataclasses
import json
import os
import secrets
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

from threew.config import REPO_ROOT, load_config
from threew.silver.output import CONTRACT_VERSION
from threew.spark import cleanup_spark, get_spark

GOLD_CONTRACT_VERSION = 1  # bump when a table's columns or meaning change; the classifier refuses other versions
PROD_SILVER_ROOT = Path("/mnt/silver-data/silver-schedule")  # where the scheduled Silver run publishes on the VM
PROD_GOLD_ROOT = Path("/mnt/silver-data/gold")  # Gold's runs/ folder and gold_ready.json on the VM

STRIDE = 60  # one classifier window per minute of recording
SEED = 42  # fixes the train/test split, so every build puts the same recordings in test
TEST_FRACTION = 0.2  # share of recordings in each (event, source type) group that go to test
VALUE_LIMIT = 1e12  # Silver keeps corrupt readings such as -1.18e42 Pa; beyond this a feature counts as missing


def resolve_silver_input(root):
    """Read one ready marker; never mix publications or guess an output folder."""
    root = Path(root)
    marker = json.loads((root / "silver_ready.json").read_text())
    # only a publication Silver itself verified, with the column layout this code expects
    # (CONTRACT_VERSION is imported from Silver's own code, so a Silver change can't be misread silently)
    if marker.get("contract_version") != CONTRACT_VERSION or marker.get("reconcile", {}).get("passed") is not True:
        raise ValueError("Gold requires a verified current Silver publication; rebuild Silver first")
    # the table path in the marker must be inside that same run's folder, and must exist
    path = Path(marker["tables"]["silver_observations"]["path"])
    if path.resolve() != root.resolve() / "runs" / marker["run_id"] / "silver_observations" or not path.is_dir():
        raise ValueError("Silver observations do not belong to the selected ready publication")
    return marker, path


def seconds_between(start, end):
    """Whole seconds from start to end; works on Silver's time-zone-free timestamps."""
    return F.expr(f"timestampdiff(SECOND, {start}, {end})")


# ---------------------------------------------------------------- operations tables


def build_recordings(obs):
    """One row per recording: well, source, event, time span and how much of its labelling is inferred."""
    # _source_path (Bronze's lineage column) identifies the recording: one source file is one recording
    return (
        obs.groupBy(F.col("_source_path").alias("recording_id"))
        .agg(
            # these are the same on every row of a recording
            F.first("instance_id").alias("instance_id"),
            F.first("well_id").alias("well_id"),
            F.first("source_type").alias("source_type"),
            F.first("instance_label").alias("event_id"),
            F.first("instance_event_name").alias("event_name"),
            F.min("timestamp").alias("start_ts"),
            F.max("timestamp").alias("end_ts"),
            F.count(F.lit(1)).alias("n_rows"),  # one row per second
            F.sum(F.col("class_was_inferred").cast("int")).alias("n_inferred"),  # true = 1, so the sum counts them
        )
        .withColumn("duration_s", seconds_between("start_ts", "end_ts"))
        .withColumn("inferred_label_share", F.round(F.col("n_inferred") / F.col("n_rows"), 4))  # 0 to 1
    )


def build_event_phases(obs):
    """One row per continuous stretch of the same row class inside a recording.

    Rows are taken in their original order; a new phase starts at the first row and wherever the
    class differs from the previous row. A running count of those starts numbers the phases 1, 2, ...
    """
    # one window per recording, with the rows in their original order (_row_in_file from Bronze)
    in_order = Window.partitionBy("_source_path").orderBy("_row_in_file")
    previous_class = F.lag("class_code_clean").over(in_order)  # the class of the row just before
    # _row_in_file is never null, so its lag is null only on the first row (the class itself can be null)
    is_first_row = F.lag("_row_in_file").over(in_order).isNull()
    rows = (
        # only the columns this table needs, so less data goes through the sort
        obs.select(
            "_source_path",
            "_row_in_file",
            "timestamp",
            "class_code_clean",
            "class_was_inferred",
            "row_event_name",
            "row_event_phase",
        )
        # no orderBy: the earliest timestamp of the whole recording, used for the offsets below
        .withColumn("recording_start", F.min("timestamp").over(Window.partitionBy("_source_path")))
        # 1 where a new phase starts: the first row, or a class different from the previous row's.
        # eqNullSafe treats null as a value, so a change into or out of a null class still counts
        .withColumn("starts_phase", (is_first_row | ~F.col("class_code_clean").eqNullSafe(previous_class)).cast("int"))
        # running sum of the starts, from the first row up to this one: 1 in the first phase, 2 in the second, ...
        .withColumn("phase_seq", F.sum("starts_phase").over(in_order.rowsBetween(Window.unboundedPreceding, 0)))
    )
    # collapse each numbered stretch of rows into one phase row
    return (
        rows.groupBy(F.col("_source_path").alias("recording_id"), "phase_seq")
        .agg(
            # one class per phase, so its code, name and phase are the same on every row
            F.first("class_code_clean").alias("class_code"),
            F.first("row_event_name").alias("event_name"),
            F.first("row_event_phase").alias("phase"),
            F.min("_row_in_file").alias("start_row"),
            F.max("_row_in_file").alias("end_row"),
            F.min("timestamp").alias("start_ts"),
            F.max("timestamp").alias("end_ts"),
            F.first("recording_start").alias("recording_start"),
            F.count(F.lit(1)).alias("n_rows"),
            F.sum(F.col("class_was_inferred").cast("int")).alias("n_inferred"),
        )
        .withColumn("event_id", (F.col("class_code") % 100).cast("int"))  # 101-109 and 1-9 are events 1-9
        # seconds since the recording started; the SQL views compute their timings from these
        .withColumn("start_offset_s", seconds_between("recording_start", "start_ts"))
        .withColumn("end_offset_s", seconds_between("recording_start", "end_ts"))
        .withColumn("duration_s", F.col("end_offset_s") - F.col("start_offset_s"))
        .drop("recording_start")
    )


# ---------------------------------------------------------------- classifier table


def prepare_windows(obs):
    """Labelled rows every STRIDE seconds, keeping only complete, plausible 120-second sensor features."""
    # the sensors are found from Silver's column names, so a sensor Silver adds needs no change here
    sensors = sorted(c[: -len("_count_120s")] for c in obs.columns if c.endswith("_count_120s"))
    if not sensors:
        raise ValueError("Silver has no temporal sensor features")
    # only rows with a class that fits the recording's own event ("consistent" in Silver)
    df = obs.filter(F.col("class_code_clean").isNotNull() & (F.col("class_label_status") == "consistent"))
    df = df.withColumn("class_code", F.col("class_code_clean"))
    # the event 0-9 to predict: an event's transient (101-109) and steady (1-9) rows share one label
    df = df.withColumn("label", (F.col("class_code") % 100).cast("int"))
    # each sensor's 3 features are kept only if its 120-second history is complete and no value is absurd;
    # otherwise all 3 become null (missing), and the row itself stays
    feature_cols = []
    updates = {}
    for sensor in sensors:
        names = [f"{sensor}_{stat}" for stat in ("mean_120s", "stddev_120s", "change")]
        ok = F.col(f"{sensor}_count_120s") == 120
        for name in names:
            # a null value counts as fine here, otherwise one null would wipe the other two
            ok = ok & F.coalesce(F.abs(F.col(name)) <= VALUE_LIMIT, F.lit(True))
        updates.update({name: F.when(ok, F.col(name)) for name in names})  # when() without otherwise() = null
        feature_cols += names
    # withColumns replaces all of them at once, so every check sees the original values
    df = df.withColumns(updates)
    df = df.filter(F.coalesce(*[F.col(c) for c in feature_cols]).isNotNull())  # drop rows with no feature left
    df = df.filter(F.col("_row_in_file") % STRIDE == 0)  # one row per minute: neighbouring seconds are near-copies
    # ids and labels first (not features), then the 21 features
    keep = ["_source_path", "source_type", "well_id", "_row_in_file", "class_code", "class_was_inferred", "label"]
    return df.select(*keep, *feature_cols).withColumnRenamed("_source_path", "recording_id")


def split_windows(df):
    """Put whole recordings in train or test; inferred labels are used for training only."""
    # the split is decided per recording, never per row. One row per recording here;
    # max(label) is the recording's event, since an event recording has labels 0 (normal) and its event number
    rec = df.groupBy("recording_id").agg(
        F.max("label").alias("recording_event"), F.first("source_type").alias("rec_source")
    )
    # within each (event, source) group, order recordings by a seeded hash; the first 20% go to test
    ordered = Window.partitionBy("recording_event", "rec_source").orderBy(F.xxhash64("recording_id", F.lit(SEED)))
    rec = rec.withColumn("rank", F.percent_rank().over(ordered))  # 0 for the first recording, up to 1 for the last
    rec = rec.withColumn("split", F.when(F.col("rank") < TEST_FRACTION, "test").otherwise("train"))
    # broadcast: the per-recording table is tiny, so the window rows aren't shuffled for this join
    df = df.join(F.broadcast(rec.select("recording_id", "recording_event", "split")), on="recording_id")
    # keep every train row, but only test rows whose class wasn't inferred (test is graded on supplied labels)
    return df.filter((F.col("split") == "train") | ~F.col("class_was_inferred"))


def build_window_features(obs):
    """The classifier table: one feature row per minute of each recording, with its label and train/test split."""
    # cached because the split reads the prepared rows twice (once per recording, once in the join)
    return split_windows(prepare_windows(obs).cache())


# ---------------------------------------------------------------- publish checks


def check(name, passed, **numbers):
    """One check for the report: its name, pass or fail, and the numbers behind it (the evidence, not just a flag)."""
    return {"name": name, "passed": bool(passed), **numbers}


def run_checks(silver_rows, recordings, event_phases, window_features):
    """The four checks from the Gold contract. Gold is published only if all of them pass."""
    # check 1: per recording, the phases' row counts add up to the recording's row count, and the total equals
    # Silver's. The full join also catches a recording with no phases, or phases with no recording
    phase_rows = event_phases.groupBy("recording_id").agg(F.sum("n_rows").alias("phase_rows"))
    mismatched = (
        recordings.join(phase_rows, "recording_id", "full")
        .filter(~F.col("n_rows").eqNullSafe(F.col("phase_rows")))
        .count()
    )
    gold_rows = recordings.agg(F.sum("n_rows")).first()[0] or 0

    # check 2: keys are unique, and every phase and window points to a known recording.
    # A left_anti join keeps only the rows with no match, so it lists the orphans directly
    n_recordings = recordings.count()
    distinct_recordings = recordings.select("recording_id").distinct().count()
    duplicate_phases = event_phases.groupBy("recording_id", "phase_seq").count().filter("count > 1").count()
    orphan_phases = event_phases.join(recordings, "recording_id", "left_anti").count()
    orphan_windows = window_features.join(recordings, "recording_id", "left_anti").count()

    # check 3: no recording has windows on both sides of the split (no leakage between train and test)
    # check 4: the test set has no inferred labels
    in_both_splits = (
        window_features.groupBy("recording_id").agg(F.countDistinct("split").alias("n")).filter("n > 1").count()
    )
    inferred_in_test = window_features.filter((F.col("split") == "test") & F.col("class_was_inferred")).count()

    return [
        check(
            "phases_cover_every_silver_row",
            mismatched == 0 and gold_rows == silver_rows,
            silver_rows=silver_rows,
            gold_rows=gold_rows,
            mismatched_recordings=mismatched,
        ),
        check(
            "keys_unique_and_resolve",
            n_recordings == distinct_recordings and duplicate_phases == orphan_phases == orphan_windows == 0,
            duplicate_recordings=n_recordings - distinct_recordings,
            duplicate_phases=duplicate_phases,
            orphan_phases=orphan_phases,
            orphan_windows=orphan_windows,
        ),
        check("each_recording_in_one_split", in_both_splits == 0, recordings_in_both=in_both_splits),
        check("no_inferred_labels_in_test", inferred_in_test == 0, inferred_test_rows=inferred_in_test),
    ]


# ---------------------------------------------------------------- run


def new_run_id():
    """A run ID like 20261026T194903Z-e2521f: the UTC start time (so IDs sort by time) plus a random suffix."""
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(3)


def code_version():
    """The commit this build ran from, and whether tracked files had uncommitted changes."""
    # check=False and None on OSError: a build without git still runs, it just records no commit
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, capture_output=True, text=True, check=False
        ).stdout
        status = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=no"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        ).stdout
    except OSError:
        return None
    return {"commit": commit.strip() or None, "dirty": bool(status.strip())}


def write_json(path, payload):
    """Write to a temporary file and rename it, so readers never see a half-written file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, default=str) + "\n")
    os.replace(tmp, path)


def main(argv=None):
    """Build, check and, if every check passes, publish one Gold run. Returns 0 if published, 1 if not."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env", choices=["dev", "prod"], default="dev")
    parser.add_argument("--silver-root", type=Path, help="folder holding silver_ready.json")
    parser.add_argument("--gold-root", type=Path, help="folder for runs/ and gold_ready.json")
    args = parser.parse_args(argv)

    # which Silver to read and where to write Gold: the flags win, otherwise the VM paths (prod) or data/dev (dev)
    cfg = load_config(args.env)
    prod = args.env == "prod"
    silver_root = args.silver_root or (PROD_SILVER_ROOT if prod else cfg.storage.silver)
    gold_root = args.gold_root or (PROD_GOLD_ROOT if prod else cfg.storage.root / "gold")

    silver_marker, source = resolve_silver_input(silver_root)  # stops here unless Silver's marker is valid
    # every build gets its own folder, so an earlier build is never touched
    run_id = new_run_id()
    run_dir = gold_root / "runs" / run_id
    print(f"Gold run {run_id}: Silver run {silver_marker['run_id']} -> {run_dir}")

    # the shared Spark setup (UTC, the venv's Python) with Gold's own app name; the config is frozen,
    # so dataclasses.replace makes a copy with the new name
    spark = get_spark(dataclasses.replace(cfg, spark=dataclasses.replace(cfg.spark, app_name="3w-gold")))
    try:
        obs = spark.read.parquet(str(source))  # lazy: a plan to read Silver, run by each write below
        # every builder takes Silver as a DataFrame and returns a DataFrame: no I/O inside, so tests can call them
        builders = {
            "recordings": build_recordings,
            "event_phases": build_event_phases,
            "window_features": build_window_features,
        }
        for name, build in builders.items():
            # each table is small enough for one file; repartition keeps the work before it parallel
            build(obs).repartition(1).write.parquet(str(run_dir / name))
            print(f"  wrote {name}")

        # check the tables as written, not the plans that produced them
        tables = {name: spark.read.parquet(str(run_dir / name)) for name in builders}
        checks = run_checks(obs.count(), **tables)
        passed = all(c["passed"] for c in checks)
        # the report is also the ready marker: lineage (code commit, Silver run), table paths and rows, the checks
        report = {
            "run_id": run_id,
            "built_at": datetime.now(UTC).isoformat(),
            "passed": passed,
            "contract_version": GOLD_CONTRACT_VERSION,
            "env": args.env,
            "code_version": code_version(),
            "silver": {
                "root": str(silver_root),
                "run_id": silver_marker["run_id"],
                "contract_version": silver_marker["contract_version"],
                "published_at": silver_marker.get("published_at"),
            },
            "tables": {name: {"path": str(run_dir / name), "rows": df.count()} for name, df in tables.items()},
            "checks": checks,
        }
    finally:
        # stop Spark even if a build or check raised; if stopping fails too, the original error is kept
        cleanup_spark(spark.stop, "stop Spark")

    write_json(run_dir / "_report.json", report)  # always written, pass or fail, so a failed build can be looked at
    for c in checks:
        print(f"  {'PASS' if c['passed'] else 'FAIL'} {c['name']}")
    # a failed check: leave the previous gold_ready.json in place, so readers keep the last good build
    if not passed:
        print(f"Gold NOT published; the previous gold_ready.json is unchanged. See {run_dir / '_report.json'}")
        return 1
    # publish: replace gold_ready.json atomically; query.py and the classifier only ever follow this file
    write_json(gold_root / "gold_ready.json", report)
    print(f"Gold published: {gold_root / 'gold_ready.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
