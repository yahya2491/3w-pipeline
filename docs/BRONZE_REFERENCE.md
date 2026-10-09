# Bronze layer reference (for agents and lookup)

Detailed reference for the 3W data engineering pipeline as of the end of the Bronze layer (October 2026). It's written for AI agents and for looking things up. For a readable overview, see `docs/SILVER_HANDOVER.md`.

**Source of truth:** the repository code and the run reports on the VM. If this file disagrees with the code, trust the code, and update this file in the same pull request. Facts marked **[measured]** were checked on the real prod data. **[design]** means a decision. **[unverified]** means believed but not checked.

---

## 0. Quick facts

| Item | Value |
|---|---|
| Course | DAT535 Data Engineering, UiS, Medallion architecture project (shared dataset with DAT540) |
| Dataset | Petrobras 3W, dataset version 2.0.0, data license CC BY 4.0 |
| Upstream | `github.com/petrobras/3W`, commit `d57c058af6baf65f1b285f3e56f46e4f018a07d9` |
| Our source system | Hugging Face dataset `Yahya2491/3w-pipeline` |
| Pinned HF revision | `c1a7e8d0cfefc0875974ea079d11349a37fa0c56` (tag `v2.0.0`; the tag *object* is `23b9c64…`, never pin that) |
| Code repo | `github.com/yahya2491/3w-pipeline` (private), branch `main` protected |
| Prod host | OpenStack VM `group20`, Ubuntu 24.04, 4 vCPU, 7.8 GiB RAM (8.3 GB), no swap |
| Prod storage root | `/home/ubuntu/3w-data` (local disk; no HDFS installed) |
| Files / rows | 2,228 files / 76,587,318 rows **[measured]** |
| Raw size | 1,873,131,359 bytes (Brotli Parquet) **[measured]** |
| Bronze table size | 3,246,119,002 bytes (Snappy Parquet) **[measured]** |
| Python / Spark / Java | 3.11 / pyspark 3.5.0 / Java 8 on VM (CI tests Java 8 and 17) |
| Time convention | All pipeline timestamps (run IDs, dates, `recorded_at`) are UTC **[design]** |

---

## 1. Architecture

```
petrobras/3W (GitHub, upstream)
   │  one-time publish: scripts/publish_to_hf.py (byte-identical upload + dataset card + tag)
   ▼
Hugging Face dataset Yahya2491/3w-pipeline @ c1a7e8d0  (data/0..9/*.parquet, dataset.ini, LICENSE-CC-BY)
   │  Bronze ingest via huggingface_hub API (list, download, sha256 verify)
   ▼
VM /home/ubuntu/3w-data
   ├── landing/3w/...               original bytes, read-only (0444), one folder per run
   ├── bronze/_state/manifest/      ledger of every landed/failed file
   ├── bronze/3w_observations/      Bronze table (Spark-readable copy, partitioned)
   ├── bronze/bronze_ready.json     published only after a fully successful, reconciled run
   └── bronze/_runs/                JSON report + log per run
   ▼
Silver (to be built) → Gold (to be built)
```

**Bronze has two zones [design]:**

| | Landing | Bronze table |
|---|---|---|
| Purpose | proof of exactly what the source published | what the pipeline reads |
| Format | original bytes (Brotli, timestamp[ns]) | Snappy, timestamp[us] (read as timestamp_ntz) |
| Read by | only the converter | Silver |
| Deleted? | never | derived; outputs not in `manifest.current()` are pruned |

**Why convert at all [measured]:** Spark 3.5 can't read the raw files, for two separate reasons:
- the nanosecond timestamps fail as soon as Spark reads the schema: `AnalysisException: Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))`
- Brotli-compressed values fail when they're decoded: `BadConfigurationException: Class org.apache.hadoop.io.compress.BrotliCodec was not found`. Footer-only operations such as `count()` still work on Brotli files, which can hide this.

pyarrow reads them fine, so pyarrow is used as a format adapter. Everything from the Bronze table onward is Spark.

---

## 2. Storage layout (prod)

All paths are relative to `storage.root` = `/home/ubuntu/3w-data`. In dev, it's `./data/dev`, relative to the repo.

| Path | Content |
|---|---|
| `landing/3w/ingest_date=<YYYY-MM-DD>/run_id=<run_id>/label=<0-9>/<original filename>` | raw files, chmod 444 |
| `_staging/<run_id>/` | download area. Same filesystem as landing, so `os.replace` is atomic. Deleted after a successful landing. |
| `bronze/_state/manifest/part-<UTC timestamp>-<8 hex>.parquet` | manifest chunks, append-only: one per append (normally every 100 files), written under a dot-file temp name, then renamed |
| `bronze/_state/ingest.lock` | `fcntl.flock` lock file. Contents (pid, run_id) are informational only. |
| `bronze/3w_observations/source_type=<REAL\|SIMULATED\|DRAWN>/label=<0-9>/<sha256[:16]>.parquet` | Bronze table, one output file per landed file |
| `bronze/bronze_ready.json` | ready marker |
| `bronze/_runs/<run_id>.json`, `<run_id>.log`, `<run_id>_reconcile_plan.txt` | run report, log and reconcile plan, per run. `scripts/reconcile_bronze.py` adds `reconcile_<UTC timestamp>.json` and its `_plan.txt`. |

Notes:
- **The partition folder is `label=`, not `class=` [design, measured].** The files have their own per-row `class` column, and Spark doesn't fail on the clash: it silently replaces every row's `class` with the folder's value. Tested with Spark 3.5.0: per-row values [0, 102, 2] came back as [2, 2, 2], with no error.
- **Bronze table file names are content-addressed** (first 16 hex characters of the source file's sha256). Re-converting produces a byte-identical file at the same path **[measured: a full prod rebuild gave 2,228 identical sha256]**.
- **The table has 19 partitions [measured]:**
  - REAL in labels 0 to 9
  - SIMULATED in 1, 2, 3, 5, 6, 8, 9
  - DRAWN in 1 and 7

---

## 3. Bronze table schema (as Spark reads it)

**[measured]** by reconcile check 1.

| Column | Spark type | Origin |
|---|---|---|
| `ABER-CKGL` … `T-TPT` (27 sensors, §6) | double | source, unchanged |
| `class` | smallint | source, unchanged (nullable) |
| `state` | smallint | source, unchanged (nullable) |
| `timestamp` | timestamp_ntz | source; cast from ns to µs with pyarrow `safe=True`, which is lossless because values are whole seconds |
| `_source_path` | string | lineage: HF repo path of the instance, e.g. `data/2/WELL-00002_20131104004101.parquet` |
| `_file_sha256` | string | lineage: sha256 of the source file |
| `_ingest_run_id` | string | lineage: the run that landed the file |
| `_row_in_file` | bigint | lineage: 0-based row position in the source file |
| `source_type` | string | partition column (from the folder name) |
| `label` | int | partition column (from the folder name) |

- **Partition columns aren't stored inside the files.**
- **The pandas metadata from the source files is dropped:** it described `timestamp` as a nanosecond index, which is no longer true.
- **Key:** `(_source_path, _row_in_file)` uniquely identifies a row in the current table. `(_source_path, _file_sha256)` identifies an instance version.
- **Spark session settings** (via `threew.spark.get_spark(cfg)`):
  - `spark.sql.session.timeZone=UTC`
  - `spark.pyspark.python=sys.executable`, which fixes Spark launching the wrong Python
  - master, driver memory and shuffle partitions from the config

---

## 4. Manifest schema

Location: `bronze/_state/manifest/`. One row per file per run that touched it. **This schema is final for Bronze.**

| Column | Type | Null | Notes |
|---|---|---|---|
| `run_id` | string | no | e.g. `20261008T235040Z-664edd` (UTC start + 6 hex characters; sorts by time) |
| `path` | string | no | HF repo path, the instance key (= `_source_path`) |
| `revision` | string | no | HF commit SHA |
| `label` | int16 | no | from the folder |
| `source_type` | string | no | REAL / SIMULATED / DRAWN |
| `well_id` | int32 | yes | REAL only |
| `instance_start` | timestamp[us], no tz | yes | REAL only, parsed from the file name |
| `instance_id` | string | no | file name without `.parquet`; **not unique across labels** |
| `size` | int64 | no | bytes |
| `sha256` | string | yes | null only if HF gave no LFS info (never the case for v2.0.0) |
| `blob_id` | string | no | git OID |
| `landing_path` | string | yes | relative to the storage root; null when FAILED |
| `status` | string | no | LANDED or FAILED |
| `error` | string | yes | FAILED only |
| `num_rows` | int64 | yes | from the Parquet footer |
| `schema_fingerprint` | string | yes | 16 hex characters of the sha256 of the sorted `name:type` list |
| `schema_ok` | bool | yes | matches the contract |
| `codec` | string | yes | e.g. BROTLI |
| `recorded_at` | timestamp[us, UTC] | no | |

- **`Manifest.current()`** returns the newest LANDED row per `path`, by `recorded_at`. The Bronze table mirrors `current()` exactly.
- **Reading it in Spark without the Python class:** `spark.read.parquet(<state>/manifest)`, filter `status = 'LANDED'`, then keep the row with the max `recorded_at` per `path`.
- **Prod state [measured]:** 2,228 LANDED rows, one fingerprint `f59f09e498a1352e`, all `schema_ok`, codec BROTLI for all.

---

## 5. File naming and instance identity

Parser: `src/threew/bronze/naming.py`. The outer pattern `data/(\d)/([^/]+)\.parquet` is applied with `fullmatch` (not `^…$`, whose `$` would also accept a trailing newline) and `re.ASCII` (so only 0-9 count as digits). Then the file name is `fullmatch`ed against one of:

| Pattern | source_type | well_id | instance_start |
|---|---|---|---|
| `WELL-(\d{5})_(\d{14})` | REAL | int | `%Y%m%d%H%M%S`, no tz |
| `SIMULATED_(\d{5})` | SIMULATED | null | null |
| `DRAWN_(\d{5})` | DRAWN | null | null |

**[measured]:**
- 0 rejected names
- 225 instance IDs appear in more than one label folder:
  - SIMULATED: 215 IDs covering 865 files
  - DRAWN: 10 IDs covering 20 files
  - REAL: 0
- 0 groups of byte-identical files

**Consequence:** always key on `path` / `_source_path`, never on `instance_id` alone.

---

## 6. Source columns (from upstream `dataset.ini`)

| Column | Meaning | Unit / domain |
|---|---|---|
| `ABER-CKGL` | Opening of the GLCK (gas lift choke) | % |
| `ABER-CKP` | Opening of the PCK (production choke) | % |
| `ESTADO-DHSV` | State of the DHSV (downhole safety valve) | 0, 0.5, 1 |
| `ESTADO-M1` | State of the PMV (production master valve) | 0, 0.5, 1 |
| `ESTADO-M2` | State of the AMV (annulus master valve) | 0, 0.5, 1 |
| `ESTADO-PXO` | State of the PXO (pig-crossover) valve | 0, 0.5, 1 |
| `ESTADO-SDV-GL` | State of the gas lift SDV (shutdown valve) | 0, 0.5, 1 |
| `ESTADO-SDV-P` | State of the production SDV | 0, 0.5, 1 |
| `ESTADO-W1` | State of the PWV (production wing valve) | 0, 0.5, 1 |
| `ESTADO-W2` | State of the AWV (annulus wing valve) | 0, 0.5, 1 |
| `ESTADO-XO` | State of the XO (crossover) valve | 0, 0.5, 1 |
| `P-ANULAR` | Pressure in the well annulus | Pa |
| `P-JUS-BS` | Downstream pressure of the SP (service pump) | Pa |
| `P-JUS-CKGL` | Downstream pressure of the GLCK | Pa |
| `P-JUS-CKP` | Downstream pressure of the PCK | Pa |
| `P-MON-CKGL` | Upstream pressure of the GLCK | Pa |
| `P-MON-CKP` | Upstream pressure of the PCK | Pa |
| `P-MON-SDV-P` | Upstream pressure of the production SDV | Pa |
| `P-PDG` | Downhole pressure at the PDG (permanent downhole gauge) | Pa |
| `PT-P` | Subsea Xmas-tree pressure downstream of the PWV in the production line | Pa |
| `P-TPT` | Subsea Xmas-tree pressure at the TPT (temperature and pressure transducer) | Pa |
| `QBS` | Flow rate at the SP (service pump) | m³/s |
| `QGL` | Gas lift flow rate | m³/s |
| `T-JUS-CKP` | Downstream temperature of the PCK | °C |
| `T-MON-CKP` | Upstream temperature of the PCK | °C |
| `T-PDG` | Downhole temperature at the PDG | °C |
| `T-TPT` | Subsea Xmas-tree temperature at the TPT | °C |
| `timestamp` | Instant at which the observation was generated | no tz |
| `class` | Label of the observation | see §7 |
| `state` | Well operational status | domain not profiled |

`P-PDG` = Pressão no PDG; "MON" = montante (upstream); "JUS" = jusante (downstream); "ABER" = abertura (opening); "ESTADO" = state.

**Type note [measured]:** upstream docs say labels are `Int64`, but the files store `class` and `state` as int16 (Spark smallint).

---

## 7. Labels and event types

| Label | Event | Has transient (class = 100 + label)? |
|---|---|---|
| 0 | Normal Operation | n/a |
| 1 | Abrupt Increase of BSW | yes |
| 2 | Spurious Closure of DHSV | yes |
| 3 | Severe Slugging | no |
| 4 | Flow Instability | no |
| 5 | Rapid Productivity Loss | yes |
| 6 | Quick Restriction in PCK | yes |
| 7 | Scaling in PCK | yes |
| 8 | Hydrate in Production Line | yes |
| 9 | Hydrate in Service Line | yes |

- **Folder `label`** = the event type of the whole instance.
- **Row `class`** = the per-observation label: 0 (normal period), `label` (steady state of the event), `100 + label` (transient), or null (unlabeled; real files only).
- **[measured on all 2,228 files]:** every row's `class` is in `{0, label, 100 + label}` or null for its folder. Folder 0 holds only 0; folders 3 and 4 hold only their own label, never 0; folders 1, 2 and 5 to 9 hold all three. Still a good Silver validation rule, since it guards future revisions.
- `TRANSIENT_OFFSET = 100` in `dataset.ini`.

---

## 8. Dataset profile

All **[measured]** on v2.0.0 at the pinned revision.

**Files per label:**

| Label | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 |
|---|---|---|---|---|---|---|---|---|---|---|
| Files | 594 | 128 | 38 | 106 | 343 | 450 | 221 | 46 | 95 | 207 |

**By source type:**
- REAL: 1,119 files
- SIMULATED: 1,089 files
- DRAWN: 20 files

**Coverage constraints:**
- Labels 0 and 4 are REAL only.
- Label 7 has no SIMULATED files.
- DRAWN exists only in labels 1 and 7 (10 each).

**Wells and time range:**
- 40 distinct real wells: IDs 1 to 42, with 17 and 18 missing.
- Real `instance_start` ranges from 2011-08-30 23:53:21 to 2023-09-18 17:01:06 (no tz).
- Real `instance_start` (from the file name) equals the first `timestamp` in the file for all 1,119 real files.

**Rows:**
- 76,587,318 in total: 40,748,658 SIMULATED, 32,872,250 REAL and 2,966,410 DRAWN (measured on the upstream files; see `docs/dataset_notes.md`).

**Labels and sensors:**
- **Null labels:** `class` and `state` are null in 12.25% of REAL rows, and 0% of SIMULATED and DRAWN rows.
- **Sampling:** observations at 1-second intervals; a 6-hour real file has ~21.5k rows.
- **All-null sensor columns per file are common, and vary by file** [measured on the 30-file dev sample]:
  - REAL (21 files): 7 to 23 of the 27 sensors entirely null, typically 9
  - SIMULATED (7 files): 20 to 22
  - DRAWN (2 files): 22
- **Simulated and drawn timestamps** are synthetic, not real calendar time.

**Not yet measured (Silver candidates):**
- duplicate or missing timestamps within an instance
- frozen or stuck sensor values
- physically implausible values (negative absolute pressures, out-of-range openings)
- the `state` value domain
- all-null sensors across the full dataset, not just the dev sample

---

## 9. Code map

Package `threew` under `src/`.

| Module | Responsibility |
|---|---|
| `threew/config.py` | `load_config(env)`: merges `conf/base.yaml` + `conf/<env>.yaml` into frozen dataclasses. Unknown keys raise errors, and the revision must be a 40-character SHA. Derived paths (landing, bronze, state, runs, staging, bronze_ready) are absolute and can't be set in YAML. `to_dict()`. |
| `threew/spark.py` | `get_spark(cfg)`: the shared session builder (see §3) |
| `threew/runs.py` | `new_run_id()` → `(run_id, started_at_utc)` |
| `threew/lock.py` | `run_lock(path, owner)`: `fcntl.flock` (exclusive, non-blocking). Released by the OS on process death. Raises `LockHeld`. |
| `threew/source/hub.py` | `RemoteFile`, `HubClient` protocol, `HfHubClient`, `verify_pin`. Only full commit SHAs are accepted for list and download. |
| `threew/source/retry.py` | `with_retry`: retries timeouts, connection resets, 408, 429, 5xx (5 s, 10 s, 20 s); fails fast on 401 and 404 |
| `threew/bronze/naming.py` | `InstanceInfo`, `SourceType`, `parse_path`, `parse_all` |
| `threew/bronze/manifest.py` | `MANIFEST_SCHEMA`, `Manifest` (`read`, `append`, `landed_keys`, `current`) |
| `threew/bronze/plan.py` | `Candidate`, `Reason` (NEW / CHANGED), `Plan`, `build_plan` (pure). Deterministic dev sampling: one per type per round (REAL, SIMULATED, DRAWN), up to `sample_per_class`. |
| `threew/bronze/fetch.py` | `FetchResult`, `sha256_of`, `fetch_all`: thread pool, size + sha256 verification, one re-download on mismatch, circuit breaker after 10 consecutive initial failures. `attempts` and `checksum_retries`. |
| `threew/bronze/land.py` | `land_results`: inspect, `os.replace` into landing, chmod 444, manifest rows in chunks of 100. Order is file first, then record. |
| `threew/bronze/inspect.py` | `load_contract`, `inspect_file` (footer only), fingerprint, drift lists |
| `threew/bronze/convert.py` | `convert_file`, `convert_all` (`only_pending=True` in runs): landing → Bronze table (pyarrow, safe ts cast, lineage columns, atomic write, footer row check). Also `expected_output` and `prune_outputs`. |
| `threew/bronze/reconcile.py` | `expected_schema`, `reconcile`: 6 checks, fast and deep modes |
| `threew/bronze/orphans.py` | `find_orphans`, `find_stale_staging` (report only; never delete) |
| `threew/bronze/report.py` | `RunReport` (atomic JSON), `code_info()` (git SHA, dirty flag, package versions) |
| `threew/bronze/ingest.py` | ingest-only command: `python -m threew.bronze.ingest --env … [--report-copy DIR]`. Also `execute()`, shared with `run.py`: the lock, the run report, the per-run log (`<runs>/<run_id>.log`, the `threew.*` loggers at INFO; the CLIs also log to the terminal) and `--report-copy`. |
| `threew/bronze/summary.py` | `summarize(report)`: a run report as Markdown (status, source, one row per phase, totals, problems) for the GitHub job summary |
| `threew/bronze/run.py` | full Bronze: `python -m threew.bronze.run --env … [--deep] [--report-copy DIR]` |

**Scripts (`scripts/`):**
- `publish_to_hf.py`
- `hub_probe.py`: source-side evidence (counts, naming, duplicates)
- `plan_preview.py`
- `fetch_smoke.py`
- `inspect_landing.py`
- `build_bronze.py`
- `reconcile_bronze.py`: deep by default; `--fast`, `--profile` (also times the other mode), `--evidence` (also saves the plan to `docs/evidence/bronze/`)
- `report_summary.py`: Markdown for the GitHub job summary

**Config:** `conf/base.yaml`, `conf/dev.yaml`, `conf/prod.yaml`, and the schema contract `conf/contracts/3w_v2.yaml` (30 columns with types, generated from a real footer).

**Base values:**

| Key | Value |
|---|---|
| `ingest.workers` | 4 |
| `ingest.max_retries` | 3 |
| `ingest.retry_backoff_seconds` | 5 |
| `spark.master` | `local[4]` |
| `spark.driver_memory` | 4g |
| `spark.shuffle_partitions` | 8 |
| `schema.fail_on_drift` | false (base); **true in prod** |

Dev overrides: `sample_per_class: 3`, `local[2]`, 2g.

---

## 10. Run phases, statuses and exit codes

`python -m threew.bronze.run` takes the lock, withdraws the ready marker, then runs:

| Phase | Does | Failure effect |
|---|---|---|
| ingest | pin check → list → parse → plan vs `manifest.current()` → fetch → verify → inspect → land → manifest | file failures give PARTIAL; crash or circuit breaker gives FAILED |
| convert | converts LANDED + schema_ok files whose output doesn't exist (the table is the ledger) | PARTIAL |
| prune | removes outputs whose `(path, sha256)` isn't in `current()`, plus leftover temp files and empty folders; refuses to empty the table | FAILED |
| reconcile | 6 checks (§11); fast by default, `--deep` adds `countDistinct` | FAILED, marker stays absent |
| publish | writes `bronze_ready.json` | only if everything above succeeded |

| Status | Exit | Meaning |
|---|---|---|
| SUCCESS | 0 | all good, marker published |
| SKIPPED | 0 | another run holds the lock; marker untouched |
| PARTIAL | 1 | finished, but some files failed (download, landing or convert); no marker |
| FAILED | 2 | crash, circuit breaker, prune or reconcile failure, or schema drift with `fail_on_drift: true`; no marker |

**Drift policy [design]:**
- **`fail_on_drift: false` (dev):** a drifted file lands with `schema_ok = false`.
- **`fail_on_drift: true` (prod):** a drifted file is *not* landed. It gets a FAILED row with the details, so every later run plans it again and fails visibly until the contract is updated.

**Run report sections:**
- `run`, `code`, `config`, `source`, `plan`, `fetch`, `landing`, `schema`, `failures`, `orphans`, `stale_staging`, `phases`, `totals`
- plus `convert`, `prune`, `reconcile`, `publish`

**Fetch counters:**
- `download_calls`
- `checksum_redownloads`
- `network_retries` = `download_calls` − files − `checksum_redownloads`

**Ready marker fields [measured]:** `run_id`, `published_at`, `env`, `bronze_table`, `source{repo_id, revision}`, `files`, `rows`, `reconcile{mode, passed, checks}`, `code_commit`, `report`.

---

## 11. Reconcile checks

| # | Check | How |
|---|---|---|
| 1 | Schema | Spark schema == `expected_schema()` |
| 2 | Total rows | `count()` == sum of `num_rows` over `current()` LANDED + schema_ok, minus files whose conversion failed in this run (left out, and the run is PARTIAL) |
| 3 | Per file | per `(_source_path, _file_sha256)` count == manifest `num_rows`; 0 missing, 0 extra |
| 4 | Partitions | `source_type` and `label` partition values == manifest |
| 5 | Row order | fast: min = 0, max = n−1, sum = n(n−1)/2 and sum of squares = (n−1)n(2n−1)/6, with n the file's row count; deep adds `countDistinct(_row_in_file)` = n, a strict proof |
| 6 | Fidelity | 3 files (one per source type): every value read by Spark from Bronze == pyarrow read of the landing file (NaN == NaN) |

- Checks 3 to 5 run as **one `groupBy`**, joined to a **broadcast** manifest DataFrame. The saved plan shows `BroadcastHashJoin` with `BroadcastExchange` on the manifest side.

**Prod timings [measured]:**

| | Deep (step 18) | Fast |
|---|---|---|
| Checks 3–5 | 66.88 s | 11.39 s |
| All Spark checks | 93.97 s | ~30 s |

- `countDistinct` costs ~59 s, 8.6× the rest of the pass.
- A deep workflow run reconciles in ~84.5 s, versus ~30 s fast.
- Fidelity: 7,196,190 values in 239,873 rows identical.

---

## 12. Performance evidence

All **[measured]**.

**Full fresh ingest:**
- 720.8 s in total
- 718 s downloading at 2.61 MB/s, with 4 workers and an authenticated read token
- 0 failures, 0 network retries

**Steady-state Bronze run (nothing new):** ~36 s in total, most of it the Spark reconcile (~30 s fast).

**Conversion of 2,228 files:** 87.6 s at first, then 42.8 s after vectorizing the lineage columns (`pa.repeat`, `cumulative_sum`) instead of Python lists, which hold the GIL. That's 1.79 M rows/s, with byte-identical output.

**`vmstat` during conversion:**
- us 48%, sy 10%, id 18%, wa 24%
- ~2 processes blocked on I/O
- CPU ~225%

The VM disk is the bottleneck.

**Bronze table shape:**
- 2,228 files, average ~1.46 MB, largest source file ~768k rows
- a small-file layout: compaction is a candidate optimisation, to be justified by Silver profiling
- Snappy output is ~1.7× the Brotli size; zstd is an untested hypothesis for reducing I/O

**CI:** lint ~8 s; tests ~90 s per Java version (pytest ~45 s).

---

## 13. Environments and operations

**VM (`group20`):**
- Reached via the UiS gateway (`ssh -J uis-gw ubuntu@<vm>`) with a password plus verification code. Not reachable from GitHub-hosted runners.
- Python venv: `~/venvs/3w` (Python 3.11.15). Activating it also sources `~/.3w.env`, which holds `HF_TOKEN` (read-only token, mode 600).
- Manual checkout: `~/3w-pipeline`, read-only deploy key: pulls only, no pushes.
- **Don't use the course's `~/spark-env`:** it has pyspark 4.2.0 against Spark 3.5.0 and Java 8.
- `/opt/spark` is Spark 3.5.0 with Scala 2.12. Our pip pyspark is 3.5.0 too, so `SPARK_HOME` doesn't matter.

**Self-hosted runner:**
- service `actions.runner.yahya2491-3w-pipeline.group20` in `~/actions-runner-3w`
- labels `self-hosted, Linux, X64, uis-openstack, prod`
- jobs check out to `~/actions-runner-3w/_work/3w-pipeline/3w-pipeline/` and build a fresh venv from `requirements.lock` each run (pip cache in `~/.cache/pip`)
- a different runner for another repo lives in `~/actions-runner`; don't touch it

**GitHub:**
- **`ci.yml`:** `lint` (ruff check + format check) and `test` (matrix: Java 8 and 17), on pushes to any branch, pull requests to `main` and manual dispatch. A new push cancels the outdated run. `HF_HUB_OFFLINE=1`, no secrets.
- **`bronze.yml`:**
  - schedule `17 3 * * 1` (Mondays 03:17 UTC), plus `workflow_dispatch` (inputs `env`, `deep`)
  - concurrency group per env, with no cancel-in-progress
  - runs on the self-hosted runner, `environment: production` (`main` only, secret `HF_TOKEN`)
  - writes a job summary and uploads the report, log and plan as an artifact for 90 days
- **`runner-check.yml`:** a manual environment sanity check.
- **Branch protection on `main`:** requires `lint`, `test (8)` and `test (17)`, for admins too (`enforce_admins`). All changes go through pull requests.

**Overlap protection [measured]:** GitHub concurrency queues overlapping workflow runs, and `flock` makes a workflow run SKIPPED if a manual SSH run holds the lock (and vice versa).

**Dev:**
- `python -m threew.bronze.run --env dev` builds 30 files (3 per label, all source types where present), 1,931,052 rows, under `./data/dev/` in ~1 minute.
- Needs network access to Hugging Face; anonymous works for this public dataset.

**Dependencies:**
- `pyproject.toml` lists the direct deps: pyspark==3.5.0, pyarrow, huggingface_hub, pyyaml.
- `requirements.lock` (prod) and `requirements-dev.lock` (+ pytest, ruff) are made with `pip-compile` under Python 3.11. Re-lock when adding deps.
- No pandas dependency; pyspark 3.5 has issues with pandas 3.

---

## 14. Invariants Silver can rely on

1. If `bronze/bronze_ready.json` exists, the table at `bronze/3w_observations` equals `manifest.current()` (LANDED + schema_ok), and all 6 reconcile checks passed in that run.
2. Every row's original 30 columns equal the published source values. Only the compression and the timestamp unit changed.
3. `(_source_path, _row_in_file)` is unique, and `_row_in_file` is contiguous from 0 to n−1 per file.
4. There's exactly one version of each `_source_path` in the table.
5. The schema is identical across all files (contract `3w_v2`, fingerprint `f59f09e498a1352e`).
6. No timezone conversion was applied anywhere: `timestamp` is wall-clock as published.

## 15. Things Bronze deliberately does not do

- **No value cleaning, renaming, casting (other than the timestamp unit), filtering or deduplication of rows.** That's Silver.
- **No joining of manifest attributes into rows.** Silver joins `well_id`, `instance_start` and `instance_id` from the manifest on `_source_path`.
- **No handling of removed-upstream files** beyond reporting them. None exist at the pinned revision.
- **No deletion of landing files or orphans.**

## 16. Conventions for new layers

- **Same patterns as Bronze:**
  - config-driven paths under `storage.root` (e.g. `silver/`)
  - a lock, and a JSON run report with phases and timings
  - a ready marker published only after quality checks pass
  - exit codes 0 / 1 / 2
  - a GitHub job summary
- **Record the upstream marker's `run_id` and revision** in the Silver report and marker, for lineage.
- **Tests:** offline, using small fixture Parquet files (`tests/conftest.py`, `tests/fakes.py`) and Spark `local[1]`.
- **Evidence:** keep measured numbers and saved plans in `docs/evidence/<layer>/`. Use `--evidence`-style flags so runs don't dirty the git checkout.
- **Tuning:** only evidence-based (profile first, then change, then re-measure). Record before and after numbers.

## 17. Evidence and related docs

- `docs/source.md`: upstream commit, HF commit, tag object vs commit, hash check
- `docs/dataset_notes.md`: measured dataset facts, with where each was measured
- `docs/evidence/bronze/README.md`: run reports, demos (self-healing, tamper refusal, lock skip), timing tables, reconcile plans
- `hf/README.md`: the HF dataset card
- `tests/fixtures/README.md`: the committed `dataset.ini` copy (CC BY 4.0)
