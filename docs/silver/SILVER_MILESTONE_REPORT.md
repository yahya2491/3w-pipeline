# Silver validation report

## Validated delivery

Silver exports **79 observation columns** under internal contract version 4,
with one row for every Bronze observation. The [contract](SILVER_CONTRACT.md)
defines all fields and seven output tables. The [quality rules](QUALITY_RULES.md)
and [assumptions](SILVER_ASSUMPTIONS.md) distinguish preserved source facts from
cleaning decisions and inferred targets.

Two full VM builds on 24-25 October 2026 passed all 32 reconciliation checks.
Their [reports, ready markers and comparison log](../evidence/silver/v3/README.md)
are the current grading evidence. Older design iterations and superseded run
copies are available in Git history.

| Measurement | First build | Second build |
|---|---:|---:|
| Recordings | 2,228 | 2,228 |
| Observations | 76,587,318 | 76,587,318 |
| Complete-recording batches | 82 | 82 |
| Target rows per batch | 1,000,000 | 1,000,000 |
| Checks passed | 32/32 | 32/32 |
| Saved Parquet, all tables | 8,549,212,642 bytes | 8,549,212,642 bytes |
| Elapsed time | 6 h 19 min 21 s | 5 h 51 min 44 s |

These are observed execution times and saved output sizes, not peak temporary
disk measurements or a controlled performance benchmark. Both builds used Git
revision `af3168b`, PySpark 3.5.0, four local cores and a 4 GB Spark driver.

## What was checked

Reconciliation verifies source/recording counts, exact observation identities,
retained source context, prepared values under the resolved rules, and the
schemas and contents of all reference, availability and quality tables. All
batches must pass before a ready marker is published. A failed Silver build
preserves the previous successful marker.

The separate [comparison](../evidence/silver/v3/comparison.log) reports identical
schemas and complete contents for all seven tables, including nulls and duplicate
multiplicities, independently of physical file layout and row order. This
establishes repeatability for the recorded inputs and settings. Reconciliation
shares transformation functions with preparation; regression tests with explicit
expected values provide additional checks. Neither comparison nor reconciliation
establishes that an imputed value is the true measurement.

## Cleaning results

Both full reports record 4,028,400 inferred class labels and no unresolved clean
class labels. Leading unknowns inherit the first consistent supplied class;
internal gaps require matching consistent original labels on both sides.
`class_was_inferred` distinguishes assumptions from supplied annotations.

The 360-second choke rule repairs 334 production-choke readings; 161,538 invalid
readings remain unresolved. Missing/non-finite sensor readings are not imputed.
There are no gas-lift choke repairs. Original-source warnings and errors remain
in the quality tables even when clean values are available.

The source contains 5,193 observations with no finite sensor readings, and four
sensors have no finite readings anywhere. These limits remain visible in
availability summaries. Zero is a valid reading, not a missing-value code.

## Bounded processing and lineage

Silver processes complete recordings in batches, keeping repair anchors and
120-second histories inside their recording and sequence segment. A recording
larger than the target is processed alone. The full prepared observation table
is not cached. Spark is stopped between batches; Parquet row groups/pages are
bounded to reduce write buffers. Batching limits intermediate work but does not
make the independent full-table comparison a bounded-memory job.

The export contains one clean field per sensor and one clean class target;
duplicate originals remain in Bronze. `_source_path`, `_file_sha256` and
`_row_in_file` recover exact source observations. Source-quality tables continue
to describe those originals.

## Operation and downstream use

The saved manual builds are under `/mnt/silver-data/3w-data/silver-v3/`.
The [Silver workflow](../../.github/workflows/silver.yml) is configured to fetch
latest main, check the mount, build the locked environment and publish to
`/mnt/silver-data/silver-schedule/` after successful Bronze or CI workflows.
It also supports manual dispatch. Workflow activation requires merge to main;
the manual evidence does not demonstrate a scheduled execution.

Bronze remains under `/home/ubuntu/3w-data/`. Unchanged verified Silver output
can be reused; new or forced builds retain complete additional output copies.
Only the scheduled job's own scratch is automatically removed. See the
[run guide](SILVER_RUN_GUIDE.md) and [storage guide](SILVER_RUN_GUIDE.md#vm-storage).

Gold reads one successful current-contract ready marker, uses clean targets,
keeps inferred labels out of held-out evaluation, and splits whole recordings.
Existing model results are not evidence of retraining on these new builds.
The [handover](../gold/GOLD_HANDOVER.md) documents the current consumer and limits.
