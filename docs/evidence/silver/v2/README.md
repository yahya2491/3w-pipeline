# Silver v2 full-run evidence

The two final 106-column builds completed on the VM and published all seven tables. The saved [comparison log](comparison.log) reports matching schemas and complete contents for every table, including duplicate multiplicities and nulls. Physical Parquet layout and row order are not part of this comparison.

## Evidence files

| Evidence | File |
|---|---|
| First full run report | [prod_run1/silver_run_1_,metrics.json](prod_run1/silver_run_1_,metrics.json) |
| First run ready-marker snapshot | [prod_run1/silver_ready.json](prod_run1/silver_ready.json) |
| Second full run report | [prod_run2/silver_run_2_,metrics.json](prod_run2/silver_run_2_,metrics.json) |
| Second run ready-marker snapshot | [prod_run2/silver_ready.json](prod_run2/silver_ready.json) |
| Seven-table content comparison | [comparison.log](comparison.log) |

The reports and marker snapshots are preserved as recorded, including their absolute VM paths. Earlier dev and reduced-schema reports in this directory describe their own builds; `old_v2_evidence/` is the earlier 76-column build, not either final 106-column run. Historical Silver v1 evidence is in [../v1/](../v1/).

## Run results

| Measurement | First run | Second run |
|---|---|---|
| Run ID | `20261017T232322182032Z-ba450086` | `20261018T091117552005Z-3280e5d3` |
| Git commit (clean checkout) | `98868ccc685526e1ead42ed43c1cff72bef4353c` | `330496fb3cb2c89133c89b68ad3866266369af1b` |
| Started (UTC) | 2026-10-17 23:23:22 | 2026-10-18 09:11:17 |
| Finished (UTC) | 2026-10-18 05:33:47 | 2026-10-18 15:16:49 |
| Status / published | `SUCCESS` / `true` | `SUCCESS` / `true` |
| Reconciliation | 32/32 checks passed | 32/32 checks passed |
| Whole-recording batches | 82 | 82 |
| Total runtime | 22,225.411 s (6 h 10 min 25 s) | 21,932.017 s (6 h 5 min 32 s) |
| Transformation and writing | 9,273.619 s | 8,808.489 s |
| Read-back and validation | 12,518.428 s | 12,680.097 s |
| Input checks | 326.691 s | 332.489 s |
| Total saved Parquet | 10,534,195,883 bytes (10.53 GB decimal) | 10,534,195,883 bytes (10.53 GB decimal) |

Both use Python 3.11.15, PySpark 3.5.0, four local cores, a 4 GB driver, eight shuffle partitions and a one-million-observation batch target. Source metadata, resolved configuration, quality rules, reference version and batch settings agree. Each ready-marker snapshot agrees with its run report's ID, table metadata and reconciliation.

## Table comparison

| Table | Rows in each run | Saved comparison result |
|---|---:|---|
| `silver_observations` | 76,587,318 | MATCH |
| `instance_quality` | 2,228 | MATCH |
| `sensor_availability` | 60,156 | MATCH |
| `quality_issues` | 4,195,465 | MATCH |
| `quality_quarantine` | 167,065 | MATCH |
| `event_reference` | 10 | MATCH |
| `sensor_reference` | 27 | MATCH |

The log ends with `RESULT: All seven tables have identical schemas and complete contents.` The [run guide](../../../silver/SILVER_RUN_GUIDE.md#compare-two-saved-runs) contains the comparison command for these explicit paths:

```text
/home/ubuntu/3w-data/silver-v2/runs/20261017T232322182032Z-ba450086/
/mnt/silver-data/3w-data/silver-v2/runs/20261018T091117552005Z-3280e5d3/
```

The command uses the repository's `same_rows` implementation, which groups complete rows with opposing weights to detect unequal values or duplicate counts. It compares the tables directly and does not gate on code or report metadata. The reports retain different Git revisions and contract-document hashes, so the claim is equal saved outputs across those revisions. Neither code equality nor a controlled storage-performance improvement is inferred.

## Quality and preparation

The preparation and quality summaries agree across both reports. Each records three repaired production-choke readings, 161,869 unresolved production-choke readings, zero gas-lift-opening repairs, 5,193 rows without an observed valid sensor and zero rows with unusable time. Missing row classes remain unknown. Source errors remain in the issue/quarantine tables even where a separate clean estimate exists.

The original reports and logs are evidence; the Parquet datasets, virtual environment and Spark scratch remain outside Git. See the [milestone report](../../../silver/SILVER_MILESTONE_REPORT.md#full-dataset-v2-validation) for interpretation and the [Gold handover](../../../gold/GOLD_HANDOVER.md) for current input paths.
