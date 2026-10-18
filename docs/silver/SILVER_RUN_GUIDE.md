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

Commit and push local changes before updating the VM checkout. Run one job at a time and reuse the existing production Bronze table:

```bash
cd ~/3w-pipeline
git pull --ff-only
source .venv/bin/activate
df -h /home/ubuntu/3w-data

python -u -m threew.silver.run \
  --env prod \
  --output-root /home/ubuntu/3w-data/silver-v2
```


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

root = Path("/home/ubuntu/3w-data/silver-v2")
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

From the VM checkout with `.venv` active:

```bash
python -u - <<'PY'
import json
from pathlib import Path

from threew.config import load_config
from threew.silver.reconcile import same_rows
from threew.silver.run import spark_session

root = Path("/home/ubuntu/3w-data/silver-v2")
reports = [json.loads(path.read_text()) for path in sorted((root / "_runs").glob("*.json"))]
successful = [r for r in reports if r.get("status") == "SUCCESS" and r.get("published") is True]
if len(successful) < 2:
    raise SystemExit("Two successful runs are needed.")
first, second = successful[-2:]
print("Runs:", first["run_id"], second["run_id"])
print("Code:", first["code"], second["code"])
for field in ("source", "config_sha256", "contract", "reference_version", "quality_rules"):
    if first[field] != second[field]:
        raise SystemExit(f"Runs differ in {field}; review before comparing.")
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

Save the output alongside the two original reports. `same_rows` compares duplicate multiplicities and nulls while ignoring row order. Review the recorded commits and dirty states separately before claiming the code was unchanged. The [historical v1 comparison](../evidence/silver/comparison.log) demonstrates this check for those earlier outputs.
