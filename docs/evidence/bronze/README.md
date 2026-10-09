# Bronze evidence: first full prod run

Run reports from the VM (`group20`, 4 CPUs, 7 GB RAM, Java 8), copied from `/home/ubuntu/3w-data/bronze/_runs/`.
Source: `Yahya2491/3w-pipeline` @ `c1a7e8d0cfefc0875974ea079d11349a37fa0c56` (`v2.0.0`). All times UTC.

| Report | What | Status | Duration | Downloaded |
|---|---|---|---|---|
| [`20261008T235040Z-664edd.json`](20261008T235040Z-664edd.json) | first full run | SUCCESS, exit 0 | 720.8 s | 2,228 files, 1,873,131,359 bytes at 2.609 MB/s |
| [`20261009T000331Z-be539f.json`](20261009T000331Z-be539f.json) | rerun right after | SUCCESS, exit 0 | 1.3 s | 0 files; 2,228 unchanged |
| [`20261009T000412Z-f9ac61.json`](20261009T000412Z-f9ac61.json) | rerun with `fail_on_drift: true` | SUCCESS, exit 0 | 1.2 s | 0 files; 2,228 unchanged |

## Numbers to carry forward

- **rows_total = 76,587,318.** Part D must reconcile against this exactly. It also equals the sum measured directly
  on the upstream files (40,748,658 simulated + 32,872,250 real + 2,966,410 drawn, see
  [`dataset_notes.md`](../../dataset_notes.md)).
- **Duration:** 720.8 s for the whole run, 717.96 s of it downloading (4 workers); landing and inspecting
  2,228 files took 1.8 s.
- **Throughput:** 2.609 MB/s overall, with a read-only token.

## First run (`20261008T235040Z-664edd`)

- Fetch: 2,228 OK, 0 FAILED, 1,873,131,359 bytes; every file verified by sha256 against the listing.
- Retries: 0 network retries. These reports predate the fetch section's `download_calls`, `network_retries` and
  `checksum_redownloads` fields (per file: `attempts` and `checksum_retries`); the count comes from the run's log,
  where `with_retry` logs every retry as a warning, and it has no warnings at all. Checksum re-downloads weren't
  logged then, so that count isn't known for this run.
- Schema: one fingerprint, `f59f09e498a1352e`, for all 2,228 files; 0 drifted; codec BROTLI for all 2,228.
- Totals: 2,228 landed, 0 failed, 2,228 manifest rows; no orphans, no stale staging, no failures.
- Code: commit `0bd0e9f2`, clean checkout.
- The token never appears in a report: `grep -c 'hf_'` prints 0 for every report.

## Checks from outside the pipeline

Run on the VM after the first run:

| Check | Result |
|---|---|
| `find ~/3w-data/landing -name '*.parquet' \| wc -l` | 2,228 |
| `find ~/3w-data/landing -name '*.parquet' ! -perm 444 \| wc -l` | 0 |
| `du -sh ~/3w-data/landing` | 1.8G (1,873,131,359 bytes) |
| `~/3w-data/_staging` after the run | empty |
| sha256sum of 10 random LANDED files vs the manifest's sha256 | 10/10 match |

## Idempotency and fail_on_drift

The rerun right after planned 0 NEW, 0 CHANGED and 2,228 unchanged, downloaded nothing and finished in 1.3 s.
With zero drift shown, `schema.fail_on_drift` was turned on in `conf/prod.yaml` (commit `bb2afb11`); the run after
that was SUCCESS with 0 downloads, so the flag doesn't break a clean run.

## Bronze table build (step 17)

`python scripts/build_bronze.py --env prod` on the VM, 4 worker threads, converting all 2,228 landed files into
`bronze/3w_observations/source_type=<type>/label=<label>/<sha256[:16]>.parquet`:

| | First build | After building lineage columns in C++ (`d726e33`) |
|---|---|---|
| Files | 2,228 converted, 0 failed, 0 skipped | same |
| sum(rows_out) | 76,587,318 (= manifest rows_total) | same |
| Duration | 87.6 s, 874,690 rows/s | 42.8 s, 1,788,565 rows/s |
| CPU (`/usr/bin/time`) | 203%, max RSS 1.7 GB | 225%, max RSS 1.6 GB |
| Output | 3,246,119,002 bytes | byte-identical: same sha256 for all 2,228 files |

- Sizes: landing 1.8G (Brotli) -> Bronze 3.1G (Snappy); Snappy is about 1.7x larger but much faster to decode.
- Files per source_type: REAL 1,119, SIMULATED 1,089, DRAWN 20; 19 partitions (DRAWN only under label 1 and 7).
- The lineage columns had been built from Python lists, which hold Python's global lock for every row: 125 ms
  for the largest file (768,314 rows) against 12 ms with `pa.repeat` and `cumulative_sum`.
- Converting the whole table a second time produced byte-identical files, so a rerun is safe.

### What limits the build: vmstat

`vmstat 2` during a third prod build (42.3 s, output again byte-identical), averaged over the 22 samples while it ran:

| us | sy | id | wa (I/O wait) | blocked processes | disk read | disk write |
|---|---|---|---|---|---|---|
| 48% | 10% | 18% | 24% | 1.9 | 30 MB/s | 74 MB/s |

About 2.3 of the 4 cores are busy (matching the 225% CPU), and a quarter of the time goes to waiting on the disk:
the build writes 3.2 GB at about 75 MB/s. The VM's disk, not Python's global lock, is what keeps it from 4 cores.

## Bronze reconciliation (step 18)

`python scripts/reconcile_bronze.py --env prod --profile` on the VM (Spark `local[4]`, 4 GB driver): all six checks
pass. Result: [`reconcile_20261009T003336Z.json`](reconcile_20261009T003336Z.json); physical plan:
[`reconcile_plan_prod.txt`](reconcile_plan_prod.txt) (dev: [`reconcile_plan_dev.txt`](reconcile_plan_dev.txt)).

| # | Check | Result | Seconds |
|---|---|---|---|
| 1 | Schema | expected columns and types: 27 sensors double, class/state smallint, timestamp timestamp_ntz, 4 lineage columns, source_type string and label int as partitions | 0.04 |
| 2 | Total rows | Spark count() 76,587,318 = manifest 76,587,318 | 3.92 |
| 3 | Per file | 2,228 files in Bronze, 2,228 in the manifest; 0 missing, 0 extra, 0 row-count mismatches | 66.88 (one pass for 3-5) |
| 4 | Partitions | every file in the source_type= and label= folders the manifest says | (same pass) |
| 5 | Row order | per file, _row_in_file is exactly 0 .. n-1 | (same pass) |
| 6 | Fidelity | 7,196,190 values in 239,873 rows (one REAL, one SIMULATED, one DRAWN file) identical between Spark's read of Bronze and pyarrow's read of the landing file, nulls and NaN included | 11.15 |

Total Spark time 93.97 s.

- **Broadcast join:** checks 3-5 are one `groupBy` over the 76.6 million rows, joined with the manifest (2,228
  rows) as a broadcast. The final plan shows `BroadcastHashJoin LeftOuter BuildRight` with a `BroadcastExchange`
  on the manifest side, so the big side is never shuffled for the join.
- **countDistinct is the expensive part:** the same pass without `countDistinct(_row_in_file)` takes 7.76 s
  instead of 66.88 s, so the distinct count costs about 59 s, 8.6x the rest. It needs a second aggregation stage
  over (file, row number) pairs, visible as the extra pair of `HashAggregate` steps in the plan.

## One Bronze run: self-healing and refusing to publish (step 19)

`python -m threew.bronze.run` runs ingest, convert, prune, reconcile (fast mode) and publish under one lock and
one report. Reports in [`bronze_run/`](bronze_run/); all on the VM.

| Demo (dev) | What was done before the run | Result |
|---|---|---|
| A [`dev_20261009T004922Z-8b11b4`](bronze_run/dev_20261009T004922Z-8b11b4.json) | nothing | SUCCESS, exit 0: 0 downloaded, 0 converted, reconcile passes, marker written (30 files, 1,931,052 rows) |
| B [`dev_20261009T004943Z-235779`](bronze_run/dev_20261009T004943Z-235779.json) | one Bronze output deleted by hand | SUCCESS, exit 0: exactly 1 file converted again, reconcile passes, marker written. **Self-healing** |
| C [`dev_20261009T005004Z-18a69f`](bronze_run/dev_20261009T005004Z-18a69f.json) | one output replaced by a copy with its first row removed (45,599 -> 45,598 rows) | **FAILED, exit 2**: checks 2, 3, 5 and 6 fail (check 3: `data/1/SIMULATED_00001.parquet` Spark 45,598 vs manifest 45,599); marker withdrawn and not written. **Refuses to publish a bad table** |
| D [`dev_20261009T005025Z-bb5ed3`](bronze_run/dev_20261009T005025Z-bb5ed3.json) | the tampered file deleted | SUCCESS, exit 0: 1 file converted again, reconcile passes, marker back |

Prod ([`prod_20261009T005103Z-3252a6`](bronze_run/prod_20261009T005103Z-3252a6.json)): SUCCESS, exit 0, in 36.0 s;
0 downloaded, 0 converted, 0 pruned; fast reconcile passes with 2,228 files, 0 missing, 0 extra, 0 mismatches and
76,587,318 rows; marker written.

| Reconcile on prod | Deep (step 18, with countDistinct) | Fast (step 19) |
|---|---|---|
| Checks 3-5, one pass | 66.88 s | 11.39 s |
| Spark time for all six checks | 93.97 s | 29.81 s |

The reconcile phase took 34.0 s in all, including about 4 s to start and stop the Spark session, which runs for
that phase only.
