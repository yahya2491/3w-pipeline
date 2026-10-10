# Silver layer handover

This is what you need to start on the Silver layer. A second, much longer file (`docs/BRONZE_REFERENCE.md`) has every detail, and is written so your AI agent can look things up in it. Read this one first.

## Background in short

- We mirrored the Petrobras 3W dataset (version 2.0.0, CC BY 4.0) to our own Hugging Face dataset repo, `Yahya2491/3w-pipeline`, pinned to commit `c1a7e8d0…`. It holds 2,228 Parquet files, one per instance.
- The Bronze pipeline downloads those files over the Hugging Face API onto our VM (`group20`). It verifies every file by sha256 and keeps the original bytes untouched in a landing zone.
- The raw files can't be read by Spark 3.5: they use Brotli compression and nanosecond timestamps. So Bronze also writes a Spark-readable copy, the **Bronze table**. It has the same rows and values, Snappy compression and microsecond timestamps.
- Spark then reconciles the Bronze table against the manifest: row counts per file, partitions, row order, and a value-by-value comparison for sample files. If anything is off, Bronze refuses to publish.
- All of this runs from one command and on a weekly GitHub Actions schedule on the VM. Every run writes a JSON report.

**You only need the Bronze table.** You never need to touch Hugging Face or the landing zone.

## Where the data is (on the VM)

| What | Path |
|---|---|
| Bronze table (read this) | `/home/ubuntu/3w-data/bronze/3w_observations/` |
| Ready marker (check this first) | `/home/ubuntu/3w-data/bronze/bronze_ready.json` |
| Manifest (one row per file, metadata) | `/home/ubuntu/3w-data/bronze/_state/manifest/` |
| Run reports | `/home/ubuntu/3w-data/bronze/_runs/` |

The table is partitioned by `source_type` (REAL, SIMULATED, DRAWN) and `label` (0 to 9). There's one Parquet file per original instance.

**Size:** 2,228 files, 76,587,318 rows, about 3.2 GB.

## What one row is

**One sensor reading, taken once per second, from one instance.** An instance is one original 3W file: a continuous time series from one real well, or one simulated or hand-drawn scenario. It belongs to one event type, the folder `label`.

The key for an instance is **`_source_path`**, for example `data/2/WELL-00002_20131104004101.parquet`. Don't use the file name or `instance_id` alone: 225 simulated and drawn names like `SIMULATED_00001` appear in several label folders, and they're different data.

## Columns in the Bronze table

| Column(s) | Type | Notes |
|---|---|---|
| 27 sensor columns, e.g. `P-PDG`, `T-TPT`, `QGL`, `ESTADO-DHSV` | double | the original names, hyphens included |
| `class` | smallint | label per row: 0 = normal, N = event N steady state, 100+N = event N transient |
| `state` | smallint | the well's operational status |
| `timestamp` | timestamp_ntz | no time zone, see below |
| `_source_path` | string | instance key |
| `_file_sha256` | string | version of the source file |
| `_ingest_run_id` | string | the Bronze run that landed it |
| `_row_in_file` | bigint | original row order, 0 to n−1 |
| `source_type` | string (partition) | REAL / SIMULATED / DRAWN |
| `label` | int (partition) | event type of the instance, 0 to 9 |

**Units:** pressures in Pa, temperatures in °C, flows in m³/s, choke openings in %, and valve states (`ESTADO-*`) coded 0, 0.5 or 1. The full list with meanings is in `BRONZE_REFERENCE.md` §6.

**Per-instance attributes** (`well_id`, `instance_start`, `instance_id`) aren't in the table. They're in the manifest, so join it on `_source_path`. It's about 2,228 rows, so broadcast it.

## Reading it

```python
import json
from pyspark.sql import functions as F
from threew.config import load_config
from threew.spark import get_spark  # sets the venv Python, UTC session time zone, config values

cfg = load_config("prod")  # "dev" on your laptop
spark = get_spark(cfg)

# 1. Refuse to run if Bronze isn't ready (raises FileNotFoundError while the marker is absent)
marker = json.loads(cfg.storage.bronze_ready.read_text())  # <root>/bronze/bronze_ready.json
print(marker["run_id"], marker["source"]["revision"])

# 2. Read the table (partition pruning works on source_type and label)
bronze = spark.read.parquet(str(cfg.storage.bronze))  # <root>/bronze/3w_observations
real = bronze.where(F.col("source_type") == "REAL")

# 3. Hyphenated names: F.col("P-PDG") works; in Spark SQL use backticks: `P-PDG`
```

Take paths from `cfg.storage` (`bronze`, `bronze_ready`, `state`, `runs`, …; see `src/threew/config.py`) rather than hard-coding them, so the same code runs in dev and prod.

## Data facts that matter for Silver

These are measured, not guessed.

1. **Three kinds of data.**
   - REAL: 1,119 files from 40 wells. Wells 1 to 42 exist except 17 and 18.
   - SIMULATED: 1,089 files.
   - DRAWN: 20 files.

   Simulated and drawn timestamps are synthetic, and they have no `well_id`.
2. **Not every event type has every kind.**
   - Classes 0 and 4 are real only.
   - Class 7 has no simulated files.
   - Drawn files exist only in classes 1 and 7 (10 each).

   So a real-versus-simulated comparison can't include classes 0, 4 and 7.
3. **Empty labels in real data.** `class` and `state` are null in 12.25% of real rows, and never in simulated or drawn rows. You need a rule for these: reject, flag, or fill.
4. **Many sensors are entirely null within a file,** and which ones varies by file and by source type. On the 30-file dev sample, real files had 7 to 23 of the 27 sensors entirely null (typically 9), simulated files 20 to 22 and drawn files 22: they have only a handful of sensors filled in. So "which sensors does this instance have" is a real modelling question.
5. **Timestamps have no time zone.** 3W doesn't say which zone they're in. Bronze keeps them as written (`timestamp_ntz`), and nothing is shifted. Real instances start between 2011-08-30 and 2023-09-18 (the start time in each file name, which equals the file's first `timestamp`).
   A profile of all 2,228 recordings found no null or duplicate timestamps, repeated adjacent times, backward intervals, or gaps over one second; every adjacent interval was one second. Silver keeps these source timestamps unchanged and checks sequence order using `_row_in_file`.
6. **Column names have hyphens and Portuguese abbreviations.** Renaming them to snake_case English is a natural Silver standardisation step.
7. **Schema is identical across all 2,228 files.** That's 30 columns with one fingerprint, checked on every Bronze run against `conf/contracts/3w_v2.yaml`.
8. **No byte-identical duplicate files exist.** There's nothing to deduplicate at the file level.
9. **Row labels match the folder.** Every row's `class` is 0, the folder `label`, `100 + label`, or null, checked on all 2,228 files. Folders 3 and 4 hold only their own label, never 0.

**Not measured yet,** and good first profiling tasks for Silver:
- frozen (stuck) sensor values
- out-of-range or negative values
- the full range of `state` values
- all-null sensors across the full dataset, not just the dev sample

## What Bronze guarantees you

- Every row in the table is exactly what Petrobras published. Only the format changed, and that's proven by the reconcile checks.
- Total rows = 76,587,318, and the per-file counts match the manifest.
- If `bronze_ready.json` exists, the last Bronze run fully succeeded and reconciled. While a run is in progress or has failed, the marker is absent.

## How we work in the repo

- **Repo:** `github.com/yahya2491/3w-pipeline` (private). Python 3.11, pyspark 3.5.0, Java 8 on the VM.
- **Install:** `pip install -r requirements-dev.lock && pip install -e . --no-deps`. Don't add packages without re-locking with `pip-compile`.
- **`main` is protected.** Work on a branch, open a pull request, and wait for CI (ruff, plus pytest on Java 8 and 17).
- **Tests run offline** (`HF_HUB_OFFLINE=1` in CI). Use small fixture files, not the real data.
- **Config, not hard-coding:** `conf/base.yaml` plus `dev.yaml` or `prod.yaml`.
- **All pipeline times are UTC.**
- **Dev data on your laptop:** `python -m threew.bronze.run --env dev` builds a 30-file sample (3 per class, all types represented, 1,931,052 rows) in `./data/dev/` in about a minute. It needs internet access to Hugging Face. Develop Silver on that, then run on prod on the VM.
- **On the VM:** our venv is `~/venvs/3w`, and the manual checkout is `~/3w-pipeline`. Don't use the course's `spark-env`: it has the wrong pyspark version.

## Suggested way to build Silver

The same patterns as Bronze will make the report consistent:
- **Read the marker first,** and record which Bronze `run_id` and revision you processed.
- **Write output under `/home/ubuntu/3w-data/silver/`,** with its own run report, lock and ready marker.
- **Measure quality before and after** (missing, invalid, rejected, accepted counts). The guideline weights Silver most heavily, and wants that evidence.
- **Profile before tuning.** From Bronze we know the VM is disk-bound (about 24% I/O wait) and the table is 2,228 small files. Compacting files, or zstd instead of Snappy, are hypotheses to test, not decisions.
- **Reach for Bronze evidence and design notes** in `docs/evidence/bronze/README.md` and `docs/dataset_notes.md`.
