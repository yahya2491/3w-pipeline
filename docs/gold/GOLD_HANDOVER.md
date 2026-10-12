# Gold layer handover

This is what you need to start building Gold from our Silver layer. Read this first; [the Silver reference](../silver/SILVER_REFERENCE.md) has the details.

## Where we are

Silver is implemented and has completed two successful full-dataset runs. Both passed all 29 build checks. Their full comparison also passed: all seven tables have matching schemas and contents, including missing values and repeated rows.

The compared runs are `20261011T182624613993Z-1a810002` and `20261011T201028142426Z-7a99e5c0`. The saved [comparison log](../evidence/silver/comparison.log) records the same code commit for both runs, seven `MATCH` results, and `All seven tables match.` The original VM log is under `/home/ubuntu/3w-data/silver-oh/comparisons/`; a copy is now kept in the repo with the run evidence.

The quality inspection on 12 October confirmed missing labels, rows without finite sensors, and out-of-range openings. We kept the existing rules and original values; the findings and usage decisions are below. The source meaning of `-99.99` has not been established, so we have not automatically converted it to missing.

You can start developing Gold against a successful Silver result. The current VM output remains in the separate `silver-oh` test destination; the Silver PR is the next review step.

## What Silver gives you

Bronze prepares the original files. Silver gives the columns clearer names, adds recording/event details, and records quality problems. It keeps the original readings and missing values. **Gold should read Silver; it does not need to download from Hugging Face or join the Bronze catalogue again.**

One observation row contains the timestamp, row/recording labels, all 27 sensor readings, and source details. Its key is source path + file fingerprint + original row position. Recording summaries use source path + file fingerprint. Do not join using `instance_id` alone.

Each of the seven folders holds a different table. **All events 0–9 remain together in `silver_observations`; the seven folders do not represent seven events.** Gold can select the tables its question needs.

| Folder/table | What it contains | How Gold can use it |
|---|---|---|
| `silver_observations` | Every original observation, all 27 renamed sensor columns, labels, recording details, and source keys. | Read measurements, choose source/event/phase, and calculate results after applying the relevant quality rules. |
| `instance_quality` | One quality summary per recording. | Check recording length, missing labels, time problems, and sensor coverage. Join on source path + file fingerprint. |
| `sensor_availability` | One row per recording and sensor, counting finite readings. | Choose sensors with enough coverage for the question. Join on recording key + sensor name; do not join all 27 entries to every observation before counting rows. |
| `quality_issues` | Every detected issue, including warnings, with its field, reason, affected use, and suggested action. | Explain exclusions, identify missing classes, and report quality limitations. Use distinct observation keys when selecting affected rows. |
| `quality_quarantine` | The error-level subset of the issue list. | Exclude a sensor's flagged measurements or review timing/label errors. Match on the observation key and restrict to the relevant field/use. |
| `event_reference` | The 10 event definitions and supported phases. | Give outputs consistent event names and explain valid phases. Join recording labels to `event_code`; a transient row class needs its event code derived before joining. |
| `sensor_reference` | The 27 sensor definitions, units, names, and documented bounds/codes. | Label results correctly and use one shared sensor vocabulary. Join a result's `sensor_name` to the reference's `sensor_name`. |

The main observation table contains flagged values as well as ordinary readings. Silver's reusable output is the observations **and** their quality information; reading the main table alone does not automatically exclude errors.

The full result has 76,587,318 observations, 2,228 recording summaries, and 60,156 sensor summaries. The [contract](../silver/SILVER_CONTRACT.md) lists the observation fields and all renamed sensors.

## Where to read it

On the VM, the current test output is:

```text
/home/ubuntu/3w-data/silver-oh/
├── runs/<run_id>/<table_name>/   # Actual Parquet data
├── _runs/<run_id>.json          # Full report for each attempt
├── comparisons/                # Logs from the comparison command
└── silver_ready.json           # Pointer to the latest successful result
```

For local development, the default output root is `data/dev/silver/`. The marker gives the table paths. Read it once at the start and use all tables from that same run, even if a newer run finishes later.

**`cfg.storage.silver` still means the default `silver/` folder.** For the current VM result, explicitly select `cfg.storage.root / "silver-oh"`. Do not read the whole `runs/` folder: that would mix repeated builds of the same data.

## Read Silver with Spark

Run this from your project checkout with its `.venv` active. Set `env = "dev"` for the laptop sample or `"prod"` for the VM. This is an example reader, not an existing Gold command:

```python
import json
from pathlib import Path

from threew.config import load_config
from threew.silver.run import spark_session

env = "dev"  # Change to "prod" on the VM.
cfg = load_config(env)
silver_root = cfg.storage.silver if env == "dev" else cfg.storage.root / "silver-oh"
ready = json.loads((silver_root / "silver_ready.json").read_text())
if ready["env"] != env or ready["reconcile"]["passed"] is not True:
    raise RuntimeError("Silver is not ready for this environment.")

report = json.loads(Path(ready["report"]).read_text())
if (
    report["run_id"] != ready["run_id"]
    or report["env"] != env
    or report["status"] != "SUCCESS"
    or report["published"] is not True
    or report["reconcile"]["passed"] is not True
):
    raise RuntimeError("Silver report does not confirm the selected result.")

print("Silver input run:", ready["run_id"])
print("Source:", ready["source"])
print("Quality:", ready["quality_summary"]["by_rule"])
with spark_session(cfg) as spark:
    tables = {name: spark.read.parquet(details["path"]) for name, details in ready["tables"].items()}
    observations = tables["silver_observations"]
    observations.select(
        "instance_id",
        "timestamp",
        "instance_label",
        "class_code",
        "row_event_phase",
        "downhole_pdg_pressure_pa",
    ).show(5, truncate=False)
    # Build your Gold calculations here, while the Spark session is open.
```

This reads the saved tables and shows five examples. It does not rebuild Silver. Production Gold should also record the selected Silver run ID, source, reference version, and the rules it used.

## Quality rules you need to use

**A successful build does not mean every reading is valid for every task.** Both full-run reports show:

- 4,028,400 missing class labels, about 5.26% of observations. These readings can still be used for work that does not need a row class.
- 5,193 observations with no finite sensor readings.
- 161,872 production-choke opening errors: 161,538 readings below 0 and 334 above 100, with an overall flagged range of −99.99 to 100.4375. Repeated −99.99 values were observed, but their source meaning is unconfirmed. Keep the current flags and exclude those opening measurements from calculations; do not clip or guess replacements.
- No missing/duplicate timestamps, backward intervals, gaps over one second, or class/recording mismatches under the implemented checks.

Four sensors have no finite readings anywhere: `service_pump_downstream_pressure_pa`, `production_shutdown_valve_upstream_pressure_pa`, `subsea_production_line_pressure_pa`, and `service_pump_flow_rate_m3_s`. Other sensors have different coverage across recordings. Check `sensor_availability` before choosing inputs.

For a Gold result:

- If it requires a row class, use `class_label_status = "consistent"`. `instance_label` describes the whole recording and does not fill a missing row class.
- If it requires a sensor, require a finite reading and check for errors affecting that field. Finite alone is not enough; an opening of 120 is finite but fails its range rule.
- For time-based work, check timestamp issues and preserve `_row_in_file`. Source times have no stated time zone. Simulated/drawn timestamps are not real calendar dates.
- Do not discard a complete observation simply because another sensor or its class has an issue. Filter according to the fields your result actually needs, and record how many observations were excluded.
- Do not join observations directly to every issue and then aggregate: several issues for one observation can multiply its contribution. Select distinct affected observation keys first, or use an existence/anti join.

For example, to calculate with production-choke openings while excluding their error entries, use this **inside the Spark block above**. It reads the saved quarantine; it does not repeat Silver's detection rules:

```python
from pyspark.sql import functions as F

key = ["_source_path", "_file_sha256", "_row_in_file"]
sensor = "production_choke_opening_pct"
bad_sensor = (
    tables["quality_quarantine"]
    .filter((F.col("affected_use") == "sensor_measurement") & (F.col("field_name") == sensor))
    .select(*key)
    .distinct()
)
value = F.col(sensor)
sensor_rows = observations.filter(
    value.isNotNull() & ~F.isnan(value) & (value != float("inf")) & (value != float("-inf"))
).join(bad_sensor, on=key, how="left_anti")
```

This keeps other rows even if they have an unrelated problem. Add label or time checks only when the Gold calculation needs them. There is no built-in reader that automatically applies all usage rules for you.

To summarise that sensor for each recording, continue inside the same Spark block:

```python
recording_key = ["_source_path", "_file_sha256"]
recording_openings = (
    sensor_rows.groupBy(*recording_key, "source_type", "instance_label")
    .agg(
        F.count("*").alias("included_readings"),
        F.avg(sensor).alias("mean_opening_pct"),
        F.min(sensor).alias("minimum_opening_pct"),
        F.max(sensor).alias("maximum_opening_pct"),
    )
    .withColumn("silver_run_id", F.lit(ready["run_id"]))
)
recording_openings.show(5, truncate=False)
```

This is a reading/aggregation example, not a Gold writer. Recordings with no eligible readings will be absent from this result. For a complete recording catalogue, left-join the aggregates to `instance_quality`, show zero included readings, and leave measurements missing rather than inventing zero means.

### What to exclude for each kind of question

| Gold question | Required checks | What can still be kept |
|---|---|---|
| Average pressure in each recording | Finite pressure and no error for that pressure field. | Rows with missing classes or errors in an unrelated opening sensor. |
| Compare openings by event phase | Finite, unflagged opening plus `class_label_status = "consistent"`. | Other valid fields from observations whose opening was excluded can support separate results. |
| Calculate a time-based rate or window | Relevant sensor checks and review of timestamp issues/spacing. | Original row positions and actual gaps; do not infer regular timing just from row counts. |
| Summarise missing labels and sensor coverage | Existing summaries and issue counts with the right denominator. | Missing/flagged records must remain visible in the quality result. |

Use `instance_label` for a recording's event category and `class_code`/`row_event_phase` for its condition at an observation. A normal period within an Event 1 recording is not automatically a recording of Event 0.

**Never replace every missing or bad measurement with zero.** If several sensors are needed, define eligibility for those sensors. Keep counts of total, included, and excluded observations so the report shows how much data supported the answer.

## Recommended first Gold outputs

Start with these two outputs, then add a phase comparison if it answers the project question. These are recommendations for the next layer; they have not been implemented.

| Proposed output | What one row means | Inputs and purpose |
|---|---|---|
| `recording_sensor_statistics` | One recording and sensor. | Use observations, sensor-specific quarantine filters, and availability/reference definitions. Save total/finite/included counts plus mean, minimum, maximum, and units. Answer which sensors are available and what their readings look like. |
| `event_quality_overview` | One source type and recording event. | Aggregate `instance_quality`, sensor coverage, and distinct affected observation keys from issues. Show recording/observation totals, missing-label rate, and error-affected row rate. Make the limitations visible in the project report. |
| `event_phase_sensor_statistics` | One source type, recording event, row phase, and sensor. | Use class-consistent observations and eligible sensor readings. Compare normal, transient, and steady phases where supported by `event_reference`. |

For averages across recordings, choose whether each observation or each recording gets equal weight. Longer recordings dominate an average over all observations. Report that choice rather than silently mixing the two meanings.

Recommended working order:

1. Pick the questions and a small set of sensors with useful coverage. Avoid the four completely unavailable sensors for measurement results.
2. Build the recording-level statistics on the 30-file dev sample. Apply quality filters for each selected sensor and record exclusion counts.
3. Build the event quality overview so readers can see what was missing or excluded. Do not sum issue entries and call them distinct bad observations.
4. Add a phase comparison if needed, keeping `REAL`, `SIMULATED`, and `DRAWN` separate by default.
5. Check tiny examples with known expected results, run on the VM into a separate Gold output root, and compare repeated outputs.

If preparing machine-learning data later, split by recording rather than individual observation; consider well groups for real recordings. Decide separately whether synthetic scenarios belong in training or evaluation. Do not use synthetic clock dates as real calendar dates.

Save the selected Silver run ID, reference version, chosen sensors, quality/inclusion rules, and output counts in the Gold report. No extra dependencies are needed for the Spark examples here.

## What to decide before writing Gold

Gold answers questions. First write down the question, required fields, inclusion rules, and what one output row will mean. Possible outputs include a recording-level sensor summary, an event/phase comparison, or a dataset prepared for a particular analysis. These are suggestions, not implemented outputs or a fixed Gold plan.

Choose deliberately whether to include `REAL`, `SIMULATED`, and `DRAWN` together; they have different coverage and meanings. Decide whether to group by the recording event or the observation's event/phase. If building a training dataset, keep observations from the same recording together when choosing train/test groups, and consider grouping real recordings by well.

Save Gold to its own destination. Record the input Silver run and your filtering/aggregation rules; check row counts, output keys, calculations, and rerun behaviour before marking it ready. Check the same-output result using complete table contents, not just equal counts.

**There is no `threew.gold.run` command or `cfg.storage.gold` setting yet.** Those need to be implemented when the Gold design is agreed.

## Local setup and checks

Use Python 3.11 and the repo's PySpark 3.5.0 environment, with Java 8 or 17. Use your own VM checkout because the VM account is shared. The full Silver runs used their checkout's `.venv`; the separate `spark-env` previously had incompatible PySpark 4.2.0.

For a new checkout, after creating/activating the environment:

```bash
pip install -r requirements-dev.lock
pip install -e . --no-deps
```

If the dev data is not already prepared:

```bash
python -m threew.bronze.run --env dev
python -m threew.silver.run --env dev
```

Bronze needs internet for the initial 30-file download. Silver reuses that prepared input. Do not manually rerun production Bronze to start Gold; use the existing successful Silver result.

Dev and prod use the same code with different configurations. Work on a short-lived branch, then a PR to `main`. CI runs Ruff and tests with Java 8 and 17. Before your PR:

```bash
ruff check .
ruff format --check .
HF_HUB_OFFLINE=1 pytest -q
```

Use small offline test inputs. Keep the full Parquet outputs on the VM; save compact run reports and comparison evidence in the repo. See [the Silver run guide](../silver/SILVER_RUN_GUIDE.md) for build/comparison commands and [the Silver reference](../silver/SILVER_REFERENCE.md) for schemas, quality rules, and run evidence.
