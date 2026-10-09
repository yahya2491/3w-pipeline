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
