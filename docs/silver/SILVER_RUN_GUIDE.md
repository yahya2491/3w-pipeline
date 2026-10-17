# Silver run guide

## Prepare the 30-file dev sample

If the dev Bronze sample is not already there, run:

```bash
python -m threew.bronze.run --env dev
```

Bronze downloads 30 original Parquet files, checks them, and prepares a Spark-readable table under `data/dev/bronze/3w_observations/`.

Prepare the sample once and reuse it while developing Silver to compare changes against the same input.

To get a read-only overview of that Bronze data, run:

```bash
python -m threew.silver.profile --env dev
```

This prints a summary of the recordings, sensors, labels, and timestamp consistency. It does not build or change Silver.

## Build Silver from the dev sample

Run:

```bash
python -m threew.silver.run --env dev
```

Milestone 12 is included automatically. The command loads `conf/quality/silver_rules.yaml`,
adds clean sensor readings, limited three-second choke repairs and 120/300-second features
to `silver_observations`, and saves resolved rules plus a preparation summary. See
[Silver assumptions](SILVER_ASSUMPTIONS.md) before selecting predictor columns. There are
still seven tables. Larger output and additional window processing mean older timings
and storage measurements are not estimates for this version.

The development configuration uses two local cores and a 4 GB Spark driver. Prepared
observations are reused from disk through verification. The wide observation writer uses
32 MiB Parquet row groups and 256 KiB pages to limit simultaneous column buffers. Allow
several minutes for the 30-file sample; the rolling features are substantially heavier
than the original rename-and-profile pipeline. A 2 GB driver exhausted its Java heap
while writing this sample during validation, so use the updated configuration.

The earlier Milestone 12 run took about 17 minutes on this development setup and passed
all 30 saved-output checks before the feature-column reductions. Its [evidence record](../evidence/silver/milestone12/README.md)
does not validate the current schema. Rerun the dev sample to record current checks and
storage size; runtime varies by machine.

Use `--rules /absolute/path/to/rules.yaml` only for a deliberately documented alternative.

After a successful v2 run, assess short choke estimates on deliberately hidden valid readings:

```bash
python scripts/evaluate_choke_repair.py --env dev --output output/choke_repair.json
```

This reports errors for one-, two- and three-second masked stretches by sensor/source type.
It does not establish the true values of actual faulty readings or tune settings on held-out
test data. For a separate Silver destination, add `--silver-root /absolute/path/to/silver-root`.

This command:

1. Checks that Bronze finished successfully and that its rows match the recording catalogue.
2. Matches each observation to its recording and adds recording details where available.
3. Renames sensor and label columns and adds readable event descriptions.
4. Preserves the original readings, timestamps, missing values, and row identities.
5. Builds separate summaries for each recording and each sensor in each recording.
6. Checks labels, timestamps, and sensor values, recording quality problems and reasons for reviewing them.
7. Creates shared event and sensor reference tables.
8. Adds cleaned readings, repair indicators, observed/prepared counts, causal temporal features and choke pressure differences.
9. Saves all seven tables, reads them back, and checks source preservation and derived contents before marking the result ready.

For the dev sample, expect 1,931,052 observations, 30 recording summaries, 810 sensor summaries, 10 event references, and 27 sensor references. Quality issue counts depend on the input and rules.

Each attempt gets a new folder under `data/dev/silver/runs/<run_id>/`. The actual data is saved as Parquet files inside the seven table folders:

| Table | What it contains |
|---|---|
| `silver_observations` | Every original observation, with clearer column names and added recording/event details. |
| `instance_quality` | One row per recording, summarising missing labels, timestamp problems, and sensor coverage. |
| `sensor_availability` | One row per recording and sensor, counting finite readings and missing or non-finite readings. |
| `quality_issues` | Problems found, with the affected observation, field, reason, severity, and recommended action. |
| `quality_quarantine` | The error-level entries from `quality_issues`, identifying readings that need review for the stated use. |
| `event_reference` | The 10 event definitions and their supported class/phase combinations. |
| `sensor_reference` | The 27 sensor definitions, source names, Silver names, units, and documented value rules. |

`quality_quarantine` is a list of issues, not a second copy of the full observations. The complete readings remain in `silver_observations`; use the source path, file fingerprint, and original row position to find the affected row. A problem with one sensor does not automatically make the other sensors unusable.

`data/dev/silver/silver_ready.json` points to the latest successful run. It does not contain the sensor data. If a new run fails, the previous marker stays in place. An incomplete attempt may leave a folder behind, so use the table paths in the ready marker.

Each attempt also writes a report under `data/dev/silver/_runs/` when it finishes normally or handles a failure. A process killed abruptly can leave no final report. Reports contain counts, checks, timings, output sizes, source/code/configuration details, and a quality summary.

To inspect the new preparation counts without starting Spark:

```bash
python - <<'PY'
import json
from pathlib import Path

ready = json.loads(Path("data/dev/silver/silver_ready.json").read_text())
print("Contract:", ready.get("contract_version", 1))
print(json.dumps(ready.get("preparation_summary"), indent=2))
print("Rules:", ready.get("quality_rules", {}).get("sha256"))
PY
```

An old marker has no preparation summary; rebuild Silver to produce contract v2.

## Check the code changes

The Silver run above checks the whole pipeline against the 30-file sample. The automated tests use smaller inputs and do not need to download the full dataset.

From the project folder, with the environment active, run:

```bash
ruff check .
ruff format --check .
HF_HUB_OFFLINE=1 pytest -q
```

For input and column/label transformation tests:

```bash
pytest -q tests/test_silver_input.py tests/test_silver_transform.py
```

For recording/sensor summaries and timestamp checks:

```bash
pytest -q tests/test_silver_quality.py
```

For quality issues, quarantine, and safe-run checks:

```bash
pytest -q tests/test_silver_issues.py tests/test_silver_run.py
```

For cleaning, elapsed-time features, shared rules and reconstruction evaluation:

```bash
pytest -q tests/test_silver_prepare.py
```

The [milestone report](SILVER_MILESTONE_REPORT.md) explains the development steps. The [Silver contract](SILVER_CONTRACT.md) and [assumptions](SILVER_ASSUMPTIONS.md) describe the original observation fields and the Milestone 12 preparation; all seven table names remain stable.

## Run on the full dataset on the VM

After the dev checks pass, run from the VM checkout, activate its `.venv`, and reuse the existing production Bronze table. This test does not require a new production Bronze run.

Run:

```bash
source .venv/bin/activate
python -m threew.silver.run --env prod --output-root /home/ubuntu/3w-data/silver-oh
```

This reads production Bronze and saves the test result under `silver-oh`.

## Read and save the full-run report

On the VM, run this to print the report for the latest successful result:

```bash
python - <<'PY'
import json
from pathlib import Path

root = Path("/home/ubuntu/3w-data/silver-oh")
ready = json.loads((root / "silver_ready.json").read_text())
report_path = Path(ready["report"])
print("Report:", report_path)
print(report_path.read_text())
PY
```

This only reads the report; it does not start Spark or rebuild Silver. Keep the original JSON report from `_runs/` as evidence for each successful run.

## Compare the two successful runs

After Silver has finished successfully twice, check that both runs produced the same data. Equal row counts alone do not prove this: one reading could have changed while the number of rows stayed the same.

Run the command below from the VM project folder with `.venv` active. It:

1. Finds the two most recent successful run reports in `silver-oh/_runs/`.
2. Checks that both reports describe the same Bronze input, configuration, contract, and reference version. It stops if these differ.
3. Reads each of the seven saved tables and compares its columns, data types, and every row.
4. Prints `MATCH` or `DIFFERENT` for each table.

This does not rebuild Silver or change its saved tables. It starts Spark to read and compare both results, so the full-dataset comparison can take time. File names, row order, and run-folder names can differ while the table contents still match.

Like the Silver run command, this uses `tee` to show the output and save it to a dated log under `/home/ubuntu/3w-data/silver-oh/comparisons/`, alongside the saved runs and their reports. `set -o pipefail` makes a comparison failure count as a failed command even when its output is piped to `tee`.

```bash
set -o pipefail
mkdir -p /home/ubuntu/3w-data/silver-oh/comparisons/
python -u - <<'PY' 2>&1 | tee /home/ubuntu/3w-data/silver-oh/comparisons/compare-$(date +%Y%m%d-%H%M%S).log
import json
from pathlib import Path

from threew.config import load_config
from threew.silver.reconcile import same_rows
from threew.silver.run import spark_session

root = Path("/home/ubuntu/3w-data/silver-oh")
successful = []
for path in sorted((root / "_runs").glob("*.json")):
    report = json.loads(path.read_text())
    if report.get("status") == "SUCCESS" and report.get("published") is True:
        successful.append(report)
if len(successful) < 2:
    raise SystemExit("Two successful runs are needed before comparing.")

first, second = successful[-2:]
print("Comparing:", first["run_id"], "and", second["run_id"], flush=True)
print("Code commits:", first["code"]["commit"], second["code"]["commit"], flush=True)
for field in ("source", "config_sha256", "contract", "reference_version", "quality_rules"):
    if first[field] != second[field]:
        raise SystemExit(f"Runs differ in {field}; review the inputs/settings first.")

tables = {
    "silver_observations", "instance_quality", "sensor_availability",
    "quality_issues", "quality_quarantine", "event_reference", "sensor_reference",
}
if set(first["tables"]) != tables or set(second["tables"]) != tables:
    raise SystemExit("Both runs must contain the current seven tables.")

matched = True
with spark_session(load_config("prod")) as spark:
    for name in sorted(tables):
        left = spark.read.parquet(first["tables"][name]["path"])
        right = spark.read.parquet(second["tables"][name]["path"])
        equal = left.schema == right.schema and same_rows(left, right)
        matched = matched and equal
        print(name, "MATCH" if equal else "DIFFERENT", flush=True)
if not matched:
    raise SystemExit("The two results differ; investigate before claiming reproducibility.")
print("All seven tables match.")
PY
```

`same_rows` checks the actual values, including missing values and repeated rows. For example, two copies of a row in one result must also appear twice in the other result. Row order does not affect the comparison.

A passing comparison prints seven `MATCH` results followed by `All seven tables match.` Keep the comparison log alongside the two original run reports as evidence for Milestone 10.

The script prints both code commits; it does not automatically check that they are equal. If they differ, review what changed before describing this as a repeat using unchanged code. Matching tables still shows that those two runs produced the same output.

Two successful Silver runs alone are not enough to claim reproducibility. The comparison must also pass.

## Review the quality issues

```bash
cd ~/3w-pipeline
source .venv/bin/activate
set -o pipefail
mkdir -p /home/ubuntu/3w-data/silver-oh/reviews/

python -u - <<'PY' 2>&1 | tee /home/ubuntu/3w-data/silver-oh/reviews/quality-$(date +%Y%m%d-%H%M%S).log
import json
from pathlib import Path

from pyspark.sql import functions as F
from threew.config import load_config
from threew.silver.run import spark_session

root = Path("/home/ubuntu/3w-data/silver-oh")
ready = json.loads((root / "silver_ready.json").read_text())
print("Silver run:", ready["run_id"], flush=True)

with spark_session(load_config("prod")) as spark:
    issues = spark.read.parquet(ready["tables"]["quality_issues"]["path"])

    counts = (
        issues.groupBy("issue_code", "severity")
        .count()
        .orderBy("issue_code")
        .collect()
    )

    print("\nISSUE TOTALS", flush=True)
    for row in counts:
        print(f"{row['issue_code']}: {row['count']:,} ({row['severity']})")

    for row in counts:
        code = row["issue_code"]
        print(f"\nFIVE EXAMPLES: {code}", flush=True)
        issues.filter(F.col("issue_code") == code).select(
            "_source_path",
            "_row_in_file",
            "field_name",
            "observed_value",
            "reason",
            "recommended_action",
        ).show(5, truncate=False)

    print("\nFLAGGED OPENING VALUES: COUNTS AND RANGES", flush=True)
    openings = issues.filter(
        F.col("issue_code") == "opening_out_of_range"
    ).withColumn("value", F.col("observed_value").cast("double"))

    openings.groupBy("field_name").agg(
        F.count("*").alias("flagged_readings"),
        F.min("value").alias("lowest_value"),
        F.max("value").alias("highest_value"),
        F.sum((F.col("value") < 0).cast("long")).alias("below_zero"),
        F.sum((F.col("value") > 100).cast("long")).alias("above_100"),
    ).show(truncate=False)
PY
```
