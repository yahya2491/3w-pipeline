# Silver run guide

Use the locked Linux environment described in the [README](../../README.md). The commands below build the current **106-column Silver v2**. Field definitions are in the [contract](SILVER_CONTRACT.md); measured results are in the [milestone report](SILVER_MILESTONE_REPORT.md).

## Run the dev sample

Reuse the existing dev Bronze table. If it has not been prepared, run Bronze once:

```bash
python -m threew.bronze.run --env dev
```

Then build Silver:

```bash
python -u -m threew.silver.run --env dev
```

The sample contains 30 recordings and 1,931,052 observations. To inspect Bronze without rebuilding it, use `python -m threew.silver.profile --env dev`.

## Run on the full dataset on the VM

The [recorded 106-column VM run](../evidence/silver/v2/prod_run1/silver_run_1_,metrics.json) passed all 32 checks over 2,228 recordings and 76,587,318 observations. It published 10.53 GB of Parquet across seven tables in 6 hours 10 minutes. Each new run retains its own complete output copy; allow additional space for Spark temporary data.

The attached volume is mounted at `/mnt/silver-data`; [VM storage](VM_STORAGE.md) describes the layout. Run one job at a time and reuse the existing production Bronze table. For a reproducibility pair, keep the same commit, clean working tree, dependencies, input, rules and configuration; do not pull or change batch size between the two runs. Production deployment remains restricted to `main`; development changes belong on `dev`.

```bash
cd ~/3w-pipeline
source .venv/bin/activate
mountpoint /mnt/silver-data
df -h / /mnt/silver-data

SPARK_LOCAL_DIRS=/mnt/silver-data/spark-tmp \
TMPDIR=/mnt/silver-data/spark-tmp \
python -u -m threew.silver.run \
  --env prod \
  --output-root /mnt/silver-data/3w-data/silver-v2
```

Run the build only after `mountpoint` confirms the volume is mounted. This foreground command requires the SSH session to stay connected. The original baseline remains under `/home/ubuntu/3w-data/silver-v2/`; each new attempt gets its own run directory on the volume. The `--output-root` override leaves the configured Bronze input path unchanged.

## What batches mean

A batch is a group of complete recordings, targeting 1,000,000 observations. A recording larger than the target runs alone. Silver writes and checks each batch, stops its Spark context to release temporary files, and then continues. All batches contribute to the same seven tables.

The full prepared-observation table is not cached. Only smaller recording counts, summaries, and issue data are cached during a batch. The writer uses 32 MiB Parquet row groups and 256 KiB pages to limit column buffers.

To reduce the batch target, add `--batch-rows 500000`. This cannot split an oversized recording. The dev configuration uses two local cores; production uses four. Both use a 4 GB Spark driver.

The 120-second feature history is separate from batching. Complete recordings preserve the sequence needed for repairs and features.

## Find the output and reports

Under the selected Silver root:

| Path | Contents |
|---|---|
| `runs/<run_id>/<table_name>/` | The seven Parquet tables for that attempt. |
| `_runs/<run_id>.json` | Full report: code/configuration, batch progress, checks, counts, sizes, timings, and quality results. |
| `silver_ready.json` | Summary and table paths for the latest successful run. Also points to the full report. |


To display the full report for the latest successful VM run without starting Spark:

```bash
python - <<'PY'
import json
from pathlib import Path

root = Path("/mnt/silver-data/3w-data/silver-v2")
ready = json.loads((root / "silver_ready.json").read_text())
report_path = Path(ready["report"])
print("Run:", ready["run_id"])
print("Report:", report_path)
print(report_path.read_text())
PY
```

## Check code changes

```bash
ruff check .
ruff format --check .
HF_HUB_OFFLINE=1 python -m pytest -q
```

## Compare two saved runs

Equal row counts alone do not establish equal contents. Compare the seven tables only when both runs' outputs are still present. This separate comparison reads whole tables and needs extra shuffle space; the runner's batch limit does not apply. Do not run it on the constrained VM without enough room for both outputs and the comparison.

From the VM checkout with `.venv` active, compare the explicit original baseline with the latest published mounted-volume run. The guard rejects a copied baseline marker if no new successful run has been published. The two run reports retain their actual table paths, even when the report itself was copied.

```bash
export SPARK_LOCAL_DIRS=/mnt/silver-data/spark-tmp
export TMPDIR=/mnt/silver-data/spark-tmp

python -u - <<'PY'
import json
from pathlib import Path

from threew.config import load_config
from threew.silver.reconcile import same_rows
from threew.silver.run import spark_session

baseline_id = "20261017T232322182032Z-ba450086"
first_path = Path("/home/ubuntu/3w-data/silver-v2/_runs") / f"{baseline_id}.json"
root = Path("/mnt/silver-data/3w-data/silver-v2")
ready = json.loads((root / "silver_ready.json").read_text())
first = json.loads(first_path.read_text())
second = json.loads(Path(ready["report"]).read_text())
if first["run_id"] == second["run_id"]:
    raise SystemExit("The marker still selects the copied baseline; a new successful run is needed.")
for report in (first, second):
    if report.get("status") != "SUCCESS" or report.get("published") is not True:
        raise SystemExit("Both reports must describe successful published runs.")
    if report["reconcile"].get("passed") is not True or not all(report["reconcile"]["checks"].values()):
        raise SystemExit("Both runs must have passed all reconciliation checks.")
if ready["run_id"] != second["run_id"] or ready["tables"] != second["tables"]:
    raise SystemExit("The ready marker and selected report disagree.")
print("Runs:", first["run_id"], second["run_id"])
print("Code:", first["code"], second["code"])
for field in ("code", "source", "config_sha256", "contract", "reference_version", "quality_rules", "execution"):
    if first[field] != second[field]:
        raise SystemExit(f"Runs differ in {field}; review before comparing.")
if first["code"].get("dirty") is not False or second["code"].get("dirty") is not False:
    raise SystemExit("Use clean checkouts for the identical-code claim.")
if set(first["tables"]) != set(second["tables"]) or len(first["tables"]) != 7:
    raise SystemExit("Runs must contain the same seven tables.")
with spark_session(load_config("prod")) as spark:
    for name in sorted(first["tables"]):
        left = spark.read.parquet(first["tables"][name]["path"])
        right = spark.read.parquet(second["tables"][name]["path"])
        if left.schema != right.schema or not same_rows(left, right):
            raise SystemExit(f"{name}: DIFFERENT")
        print(name, "MATCH")
print("All seven tables match.")
PY
```

Save the execution and comparison logs, both original reports, and separately named snapshots of both ready markers under `/mnt/silver-data/evidence/silver-v2/`. Small reviewed evidence files can be committed under `docs/evidence/silver/v2/` on `dev`; datasets, Spark scratch files and virtual environments stay outside Git. `same_rows` compares duplicate multiplicities and nulls while ignoring row order. The [historical v1 comparison](../evidence/silver/comparison.log) demonstrates this check for those earlier outputs; it does not establish equality for later v2 runs.
