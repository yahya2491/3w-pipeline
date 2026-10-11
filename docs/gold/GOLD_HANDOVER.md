# Gold layer handover

This is what you need to start building Gold from our Silver layer. Read this first; [the Silver reference](../silver/SILVER_REFERENCE.md) has the details.

## Where we are

Silver is implemented and has completed two successful full-dataset runs. Both passed all 29 build checks. Their full comparison also passed: all seven tables have matching schemas and contents, including missing values and repeated rows.

The compared runs are `20261011T182624613993Z-1a810002` and `20261011T201028142426Z-7a99e5c0`. The saved [comparison log](../evidence/silver/comparison.log) records the same code commit for both runs, seven `MATCH` results, and `All seven tables match.` The original VM log is under `/home/ubuntu/3w-data/silver-oh/comparisons/`; a copy is now kept in the repo with the run evidence.

The Milestone 11 quality/design review is still pending, including review of the opening-range errors.

You can start developing Gold against a successful Silver result. Treat the current output as a test result until the Milestone 11 review and the Silver PR are finished.

## What Silver gives you

Bronze prepares the original files. Silver gives the columns clearer names, adds recording/event details, and records quality problems. It keeps the original readings and missing values. **Gold should read Silver; it does not need to download from Hugging Face or join the Bronze catalogue again.**

One observation row contains the timestamp, row/recording labels, all 27 sensor readings, and source details. Its key is source path + file fingerprint + original row position. Recording summaries use source path + file fingerprint. Do not join using `instance_id` alone.

| Table | Use it for |
|---|---|
| `silver_observations` | The actual observations, renamed sensors, recording details, and event/phase fields. |
| `instance_quality` | Quality and coverage for each recording. |
| `sensor_availability` | Checking which sensors have readings in each recording. |
| `quality_issues` | Understanding problems and their effect on your intended analysis. |
| `quality_quarantine` | Finding error-level fields that need review. It contains issue entries, not full observation copies. |
| `event_reference` | Reusing the 10 event names and supported phases. |
| `sensor_reference` | Reusing the 27 sensor names, meanings, units, and value rules. |

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
    tables = {
        name: spark.read.parquet(details["path"])
        for name, details in ready["tables"].items()
    }
    observations = tables["silver_observations"]
    observations.select(
        "instance_id", "timestamp", "instance_label", "class_code",
        "row_event_phase", "downhole_pdg_pressure_pa",
    ).show(5, truncate=False)
    # Build your Gold calculations here, while the Spark session is open.
```

This reads the saved tables and shows five examples. It does not rebuild Silver. Production Gold should also record the selected Silver run ID, source, reference version, and the rules it used.

## Quality rules you need to use

**A successful build does not mean every reading is valid for every task.** Both full-run reports show:

- 4,028,400 missing class labels, about 5.26% of observations. These readings can still be used for work that does not need a row class.
- 5,193 observations with no finite sensor readings.
- 161,872 opening-range errors under the current 0–100 percent rule. These still need review against the source.
- No missing/duplicate timestamps, backward intervals, gaps over one second, or class/recording mismatches under the implemented checks.

Four sensors have no finite readings anywhere: `service_pump_downstream_pressure_pa`, `production_shutdown_valve_upstream_pressure_pa`, `subsea_production_line_pressure_pa`, and `service_pump_flow_rate_m3_s`. Other sensors have different coverage across recordings. Check `sensor_availability` before choosing inputs.

For a Gold result:

- If it requires a row class, use `class_label_status = "consistent"`. `instance_label` describes the whole recording and does not fill a missing row class.
- If it requires a sensor, require a finite reading and check for errors affecting that field. Finite alone is not enough; an opening of 120 is finite but fails its range rule.
- For time-based work, check timestamp issues and preserve `_row_in_file`. Source times have no stated time zone. Simulated/drawn timestamps are not real calendar dates.
- Do not discard a complete observation simply because another sensor or its class has an issue. Filter according to the fields your result actually needs, and record how many observations were excluded.
- Do not join observations directly to every issue and then aggregate: several issues for one observation can multiply its contribution. Select distinct affected observation keys first, or use an existence/anti join.

For example, to exclude errors affecting a particular sensor, use this **inside the Spark block above**:

```python
from pyspark.sql import functions as F

key = ["_source_path", "_file_sha256", "_row_in_file"]
sensor = "downhole_pdg_pressure_pa"
bad_sensor = (
    tables["quality_quarantine"]
    .filter((F.col("affected_use") == "sensor_measurement") & (F.col("field_name") == sensor))
    .select(*key).distinct()
)
value = F.col(sensor)
sensor_rows = observations.filter(
    value.isNotNull() & ~F.isnan(value)
    & (value != float("inf")) & (value != float("-inf"))
).join(bad_sensor, on=key, how="left_anti")
```

This keeps other rows even if they have an unrelated problem. Add label or time checks only when the Gold calculation needs them. There is no built-in reader that automatically applies all usage rules for you.

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
