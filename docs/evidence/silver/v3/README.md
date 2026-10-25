# Validated Silver builds

The two full VM builds below export the current **79-column observation schema**
(internal contract version 4). Each processes all 2,228 recordings and 76,587,318
observations in 82 complete-recording batches targeting 1,000,000 rows.
Both reports record `SUCCESS`, `published: true`, and all 32 checks passing.

| Evidence | First build | Second build |
|---|---|---|
| Run ID | `20261024T175806357259Z-675ce602` | `20261025T010710680886Z-216d5afd` |
| Started, UTC | 24 October 2026, 17:58:06 | 25 October 2026, 01:07:10 |
| Full report | [First report](prod_run1/20261024T175806357259Z-675ce602.json) | [Second report](prod_run2/20261025T010710680886Z-216d5afd.json) |
| Published marker | [First marker](prod_run1/silver_ready.json) | [Second marker](prod_run2/silver_ready.json) |
| Elapsed time | 6 h 19 min 21 s | 5 h 51 min 44 s |
| Saved Parquet, all seven tables | 8,549,212,642 bytes | 8,549,212,642 bytes |

Both used clean Git revision `af3168be64eaf045215ea546a381920dcca39b54`,
the same pinned source, configuration, resolved rules and build signature.
The reports retain the contract-document hash recorded at execution; later
wording changes to that document do not rewrite historical evidence.

## Saved output and comparison

The manual-build root is `/mnt/silver-data/3w-data/silver-v3/`.
Each dataset is under `runs/<run_id>/<table_name>/`; its full report is under
`_runs/<run_id>.json`. The scheduled output root is separately configured as
`/mnt/silver-data/silver-schedule/`. These saved manual builds do not prove
that the scheduled workflow has executed.

The [comparison log](comparison.log) records seven `MATCH` results and:

```text
RESULT: All seven tables have identical schemas and complete contents.
```

The comparison includes duplicate multiplicities and nulls, ignoring physical
file layout and row order. It does not compare run metadata. The repeatable
command is in the [run guide](../../../silver/SILVER_RUN_GUIDE.md#compare-two-saved-runs).

## Cleaning and remaining limitations

Both reports record:

| Preparation result | Rows |
|---|---:|
| Inferred class labels | 4,028,400 |
| Unresolved clean class labels | 0 |
| Repaired production-choke openings | 334 |
| Unresolved invalid production-choke openings | 161,538 |
| Gas-lift choke repairs | 0 |
| Rows without an observed valid sensor | 5,193 |
| Rows without usable time | 0 |

The label policy is `backfill_leading_and_matching`, with a 360-second maximum
choke-anchor age. Inferred labels and choke estimates implement the documented
[assumptions](../../../silver/SILVER_ASSUMPTIONS.md); successful validation does
not prove their physical truth or model benefit.

Source-quality tables intentionally retain 4,028,400 missing-class warnings,
161,872 original opening-range errors and 5,193 no-finite-sensor errors.
Thus `has_quality_errors: true` is compatible with a successful build.
Original observations are preserved in Bronze; no Parquet datasets are stored
in this repository. Earlier Silver evidence is recoverable through Git history.
