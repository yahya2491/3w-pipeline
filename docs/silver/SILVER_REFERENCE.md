# Silver layer reference

This is the detailed reference for our Silver layer. For a quick start, read [the Gold handover](../gold/GOLD_HANDOVER.md). The [contract](SILVER_CONTRACT.md) describes our rules; this file explains the saved data, implementation, and measured results.

The code and original run reports are the source of truth. Results below are from the two full-dataset runs on 11 October 2026. The VM comparison also passed: all seven saved tables have matching schemas and contents, including missing values and repeated rows.

## 1. What Silver does

```text
Prepared Bronze observations + recording catalogue
    → check the input
    → rename columns and add recording/event details
    → build quality summaries, issue lists, and references
    → save seven tables and check what was saved
    → publish silver_ready.json
    → Gold reads one successful Silver result
```

Silver does not download the original dataset. It keeps every Bronze observation, original reading, timestamp, and missing value. It does not fill, clip, or automatically repair values. Gold can then make summaries and answer specific questions using the quality guidance.

## 2. Where the data is

| Item | Dev | VM full-dataset test |
|---|---|---|
| Output root | `data/dev/silver/` | `/home/ubuntu/3w-data/silver-oh/` |
| Ready marker | `<root>/silver_ready.json` | Same layout |
| Saved tables | `<root>/runs/<run_id>/<table_name>/` | Same layout |
| Run report | `<root>/_runs/<run_id>.json` | Same layout |
| Comparison logs | Not created by the runner | `<root>/comparisons/` when using the run-guide comparison command |

Read the table paths from the ready marker. It points to the latest successful result; it is not the data itself. Each table folder contains Spark-written Parquet files. Silver does not promise one file per recording, fixed file names, or a particular row order.

`cfg.storage.silver` points to the default `silver/` folder. **It does not change to `silver-oh/` after a run with `--output-root`.** Gold must select the actual input root and take its table paths from that root's marker. The current Silver runner protects the official production `silver/` folder and requires a separate output root for production input.

## 3. Tables and row counts

Both full-run reports contain these counts:

| Table | What one row means | Dev baseline | Full-run rows |
|---|---|---:|---:|
| `silver_observations` | One original observation, with all 27 sensors as columns. | 1,931,052 | 76,587,318 |
| `instance_quality` | One recording/file version. | 30 | 2,228 |
| `sensor_availability` | One recording and one sensor. | 810 | 60,156 |
| `quality_issues` | One observation, field, and failed quality rule. | Depends on input | 4,195,465 |
| `quality_quarantine` | One error-level issue from the issue list. | Depends on input | 167,065 |
| `event_reference` | One event definition. | 10 | 10 |
| `sensor_reference` | One sensor definition. | 27 | 27 |

A recording can have thousands of observations. One observation can have several issues. Do not treat issue counts as counts of distinct affected observations.

## 4. Observation identity and columns

The observation key is `(_source_path, _file_sha256, _row_in_file)`. The recording key is `(_source_path, _file_sha256)`. `instance_id` alone is not unique across the dataset.

| Column | Spark type | Meaning |
|---|---|---|
| `source_type` | string | `REAL`, `SIMULATED`, or `DRAWN`. |
| `instance_label` | int | Whole-recording event category, renamed from Bronze `label`. |
| `class_code` | smallint | Row-level class, renamed from `class`; can be missing. |
| `state_code` | smallint | Original operating-status code, renamed from `state`; can be missing. |
| `timestamp` | timestamp_ntz | Original time without a time zone; can be missing. |
| `_source_path` | string | Original dataset file path. |
| `_file_sha256` | string | Fingerprint of that source-file version. |
| `_row_in_file` | bigint | Original row position, starting at 0. |
| `_ingest_run_id` | string | Bronze run that prepared the input row. |
| `instance_id` | string | Recording identifier from the catalogue. |
| `well_id` | int | Well identifier when available. |
| `instance_start` | timestamp_ntz | Recording start time when available. |
| `instance_event_name` | string | Readable event name for the recording. |
| `row_event_name` | string | Readable event name from the row class; can be missing. |
| `row_event_phase` | string | `normal`, `steady`, `transient`, `unknown`, or `unrecognized`. |
| `class_label_status` | string | `consistent`, `inconsistent`, or `unlabeled`. |
| 27 sensor columns below | double | Original measurements; all allow missing values. |

That is 43 columns in total. The catalogue is matched using both source path and file fingerprint. `SIMULATED` and `DRAWN` rows have no real well ID or real recording start. We keep synthetic timestamps as source values.

### Sensor names

Units stay unchanged. The original names, meanings, and units come from [the Bronze reference](../bronze/BRONZE_REFERENCE.md), sections 6 and 7.

| Bronze name | Silver name | Unit |
|---|---|---|
| `ABER-CKGL` | `gas_lift_choke_opening_pct` | percent |
| `ABER-CKP` | `production_choke_opening_pct` | percent |
| `ESTADO-DHSV` | `downhole_safety_valve_state` | source code |
| `ESTADO-M1` | `production_master_valve_state` | source code |
| `ESTADO-M2` | `annulus_master_valve_state` | source code |
| `ESTADO-PXO` | `pig_crossover_valve_state` | source code |
| `ESTADO-SDV-GL` | `gas_lift_shutdown_valve_state` | source code |
| `ESTADO-SDV-P` | `production_shutdown_valve_state` | source code |
| `ESTADO-W1` | `production_wing_valve_state` | source code |
| `ESTADO-W2` | `annulus_wing_valve_state` | source code |
| `ESTADO-XO` | `crossover_valve_state` | source code |
| `P-ANULAR` | `annulus_pressure_pa` | Pa |
| `P-JUS-BS` | `service_pump_downstream_pressure_pa` | Pa |
| `P-JUS-CKGL` | `gas_lift_choke_downstream_pressure_pa` | Pa |
| `P-JUS-CKP` | `production_choke_downstream_pressure_pa` | Pa |
| `P-MON-CKGL` | `gas_lift_choke_upstream_pressure_pa` | Pa |
| `P-MON-CKP` | `production_choke_upstream_pressure_pa` | Pa |
| `P-MON-SDV-P` | `production_shutdown_valve_upstream_pressure_pa` | Pa |
| `P-PDG` | `downhole_pdg_pressure_pa` | Pa |
| `PT-P` | `subsea_production_line_pressure_pa` | Pa |
| `P-TPT` | `subsea_tree_tpt_pressure_pa` | Pa |
| `QBS` | `service_pump_flow_rate_m3_s` | m3/s |
| `QGL` | `gas_lift_flow_rate_m3_s` | m3/s |
| `T-JUS-CKP` | `production_choke_downstream_temperature_c` | degrees C |
| `T-MON-CKP` | `production_choke_upstream_temperature_c` | degrees C |
| `T-PDG` | `downhole_pdg_temperature_c` | degrees C |
| `T-TPT` | `subsea_tree_tpt_temperature_c` | degrees C |

Zero is a reading, not a missing-value marker. Finite means not null, NaN, or positive/negative infinity. A finite reading can still fail a value rule.

## 5. Events and phases

| Event code | Name | Supported row classes |
|---:|---|---|
| 0 | Normal Operation | 0 |
| 1 | Abrupt Increase of BSW | 0, 1, 101 |
| 2 | Spurious Closure of DHSV | 0, 2, 102 |
| 3 | Severe Slugging | 3 |
| 4 | Flow Instability | 4 |
| 5 | Rapid Productivity Loss | 0, 5, 105 |
| 6 | Quick Restriction in PCK | 0, 6, 106 |
| 7 | Scaling in PCK | 0, 7, 107 |
| 8 | Hydrate in Production Line | 0, 8, 108 |
| 9 | Hydrate in Service Line | 0, 9, 109 |

For an Event 1 recording, class 0 means normal operation, 101 means the event is developing, and 1 means the event is established. A missing class gives phase `unknown` and status `unlabeled`.

The phase mapping recognises 1–9 as steady and 101–109 as transient, but `class_label_status` separately checks whether that class fits the recording. For example, 103 would be described as transient Event 3 but flagged as inconsistent because Event 3 does not support that phase. Do not use the readable name alone as proof of a valid label.

## 6. Quality summaries

Both summary tables include the recording details: `source_type`, `instance_label`, `instance_event_name`, `_source_path`, `_file_sha256`, `instance_id`, `well_id`, and `instance_start`.

### `instance_quality`

| Field(s) | Meaning |
|---|---|
| `recording_rows` | Observation count for the recording. |
| `unlabeled_rows`, `null_state_rows`, `null_timestamp_rows` | Counts of missing class, state, and timestamps. |
| `inconsistent_class_rows`, `unrecognized_class_rows` | Counts of class/recording mismatches and unsupported class codes. |
| `duplicate_timestamp_rows` | Extra occurrences of non-null timestamps. Three rows at one time count as two extras. |
| `timestamp_intervals_compared` | Adjacent intervals that could be compared in original row order. |
| `one_second_intervals` | Intervals exactly one second long. |
| `repeated_adjacent_timestamps` | Adjacent rows with the same non-null time. |
| `backward_intervals` | Intervals where time moves backwards. |
| `gaps_over_one_second`, `largest_gap_seconds` | Gap count and largest gap; largest gap is null if there is none. |
| `sensors_present`, `sensors_missing_entire_recording`, `no_sensors_present` | Coverage based on whether each sensor has at least one finite reading. |

### `sensor_availability`

Alongside the shared recording details, it contains `recording_rows`, `sensor_name`, `rows_with_finite_values`, `rows_without_finite_values`, `percent_rows_with_finite_values`, and `has_finite_values`. The percentage is rounded to two decimals.

This measures presence, not full validity. An opening of 120 is finite and counted here, but also appears in `quality_issues` because it fails the opening rule.

## 7. Issues, quarantine, and usage rules

Both issue tables have these columns:

| Columns | Type | Purpose |
|---|---|---|
| `_source_path`, `_file_sha256` | string | Source-file version. |
| `_row_in_file` | bigint | Affected observation position. |
| `field_name`, `issue_code` | string | Affected field and failed rule. |
| `severity`, `affected_use` | string | Warning/error and the kind of work affected. |
| `reason`, `recommended_action` | string | Explanation and what the reader should do. |
| `observed_value` | string | Supplied value as text; can be null. |

Their entry key is the observation key plus `field_name` and `issue_code`. `quality_quarantine` contains only severity `error`. Complete observations remain in `silver_observations`; join using the three observation-key fields to inspect them.

| Issue code | Severity | Affected use |
|---|---|---|
| `missing_class` | warning | Labelled analysis; readings remain available. |
| `unsupported_class`, `class_recording_mismatch` | error | Labelled analysis; review the label. |
| `missing_timestamp`, `duplicate_timestamp`, `backward_timestamp` | error | Time-sequence analysis. |
| `timestamp_gap`, `short_timestamp_interval` | warning | Time-sequence analysis; preserve actual spacing. |
| `nonfinite_sensor` | error | That sensor measurement. |
| `unsupported_valve_state` | error | That valve sensor; allowed values are 0, 0.5, and 1. |
| `opening_out_of_range` | error | That choke sensor; current rule is 0–100 percent. |
| `no_finite_sensor_readings` | error | Sensor analysis for that row; its labels remain available. |

Individual missing sensor cells stay in the availability summaries instead of generating an issue for every cell. Missing state codes are counted in the recording summary; their allowed values are not documented. No pressure, temperature, or flow limits are invented.

Keep duplicate timestamps and flag every affected row. Stop the run on duplicate observation identities, including identical copies. Never guess corrected times, automatically relabel rows, round valve states, or clip openings. A new source-file fingerprint is a version change, not automatically a duplicate observation.

For time sequences, sort within a recording by `_row_in_file`. Parquet has no guaranteed row order. Source timestamps have no stated time zone, even though pipeline run times are recorded in UTC.

## 8. Shared reference tables

`event_reference` contains `event_code` and `event_name`, three boolean support flags (`supports_normal`, `supports_steady`, `supports_transient`), and integer `steady_class_code`/`transient_class_code` where supported.

`sensor_reference` contains `source_name`, `sensor_name`, `meaning`, and `unit`; `allowed_values` is an array of doubles for valve codes, while `minimum` and `maximum` are doubles for choke openings. Rules without documented bounds have null bounds.

Both tables include string `definition_source` and integer `reference_version`, currently 1. Gold should reuse these definitions rather than create a different event or sensor mapping.

## 9. How a result becomes ready

The runner checks the Bronze ready marker and current catalogue, total/per-recording counts, unique non-null observation keys, and non-negative row positions. Unexpected columns/types or unmatched/conflicting recording details stop the run.

It holds Bronze's input writer lock and its own output lock. It writes a new run folder, reads all seven tables back, and checks schemas, keys, preserved values, derived details, summaries, issues, quarantine, and references against expected results. The Bronze marker must remain unchanged. The two recorded full runs passed all 29 checks.

Only then is `silver_ready.json` replaced. A failed attempt preserves the previous marker. Handled failures and skipped runs get a report; an abrupt process kill can leave only an incomplete folder. Do not read a folder merely because it exists.

The marker records the run ID, environment, source, table paths/counts/sizes, check results, report path, quality summary, usage policy, and reference version. The full report additionally records code/package versions, configuration and its fingerprint, contract fingerprint, times, phase durations, and failures where applicable.

**Ready means the saved result passed the build checks. It does not mean every source value is suitable for every analysis.**

## 10. What the full runs showed

| Result | Run 1 | Run 2 |
|---|---|---|
| Run ID | `20261011T182624613993Z-1a810002` | `20261011T201028142426Z-7a99e5c0` |
| Status | SUCCESS, published | SUCCESS, published |
| Checks | 29 passed | 29 passed |
| Total duration | 3,497.639 s (58 min 18 s) | 3,236.232 s (53 min 56 s) |
| Transform and write | 882.135 s | 890.770 s |
| Read back and check | 2,542.809 s | 2,274.768 s |

Both reports used code commit `921d1204dcca57090d181d59a8cfa638efc1f89e` with no uncommitted changes, Bronze run `20261011T142406Z-74e619`, and source revision `c1a7e8d0cfefc0875974ea079d11349a37fa0c56`. Their configuration, contract fingerprints, and reference version match. The current contract has since been rewritten; that does not alter these saved results.

Both reports show:

- 4,028,400 missing-class warnings: about 5.26% of observations. The same number have missing state codes.
- 5,193 observations with no finite sensor readings.
- 161,872 opening-range error entries, requiring source/value review.
- 167,065 distinct observations with error-level issues: 0.2181% of all observations.
- Zero missing or duplicate timestamps, backward intervals, gaps over one second, or class/recording mismatches.
- Zero recordings with no finite sensors at all, even though some individual rows have none.

Four sensor columns have no finite readings across the dataset: `service_pump_downstream_pressure_pa`, `production_shutdown_valve_upstream_pressure_pa`, `subsea_production_line_pressure_pa`, and `service_pump_flow_rate_m3_s`. Do not choose them as available inputs without reviewing coverage.

**The full comparison passed for these two runs.** It checked the schemas and complete row contents of all seven tables, ignoring row order while preserving duplicate counts and missing values. The supplied VM terminal output ended with:

```text
event_reference MATCH
instance_quality MATCH
quality_issues MATCH
quality_quarantine MATCH
sensor_availability MATCH
sensor_reference MATCH
silver_observations MATCH
All seven tables match.
```

The comparison command saves its complete output on the VM under `/home/ubuntu/3w-data/silver-oh/comparisons/compare-<date>-<time>.log`. Find the latest log with:

```bash
ls -lt /home/ubuntu/3w-data/silver-oh/comparisons/
```

The complete log is saved in the repo as [comparison.log](../evidence/silver/comparison.log), alongside the two run reports. It records both run IDs, matching code commits, and all seven `MATCH` results. The comparison reads the saved tables; it does not build another Silver result.

The repeatability check passed. Milestone 11 still needs to review the quality findings and design against the lecture infographic, including the opening-range errors.

Run evidence: [run 1 report](../evidence/silver/run1/silver_run_1_,metrics.json), [run 2 report](../evidence/silver/run2/silver_run_2_,metrics.json). The Parquet data remains on the VM.

## 11. Files and commands

| File | What it does |
|---|---|
| [input.py](../../src/threew/silver/input.py) | Checks Bronze readiness and reads the current recording catalogue. |
| [transform.py](../../src/threew/silver/transform.py) | Checks input types, matches recording details, renames columns, and explains classes/phases. |
| [reference.py](../../src/threew/silver/reference.py) | Defines and builds the shared event/sensor references. |
| [quality.py](../../src/threew/silver/quality.py) | Builds recording and sensor summaries and timestamp intervals. |
| [issues.py](../../src/threew/silver/issues.py) | Builds issue/quarantine lists and the compact report summary. |
| [reconcile.py](../../src/threew/silver/reconcile.py) | Checks saved output; `same_rows` compares values including duplicate counts and nulls. |
| [run.py](../../src/threew/silver/run.py) | Runs the steps, manages locks, saves reports, and publishes readiness. |
| [profile.py](../../src/threew/silver/profile.py) | Prints a read-only profile of Bronze; it does not build Silver. |

From the project folder with the locked Python environment active:

```bash
python -m threew.silver.run --env dev
ruff check .
ruff format --check .
HF_HUB_OFFLINE=1 pytest -q
```

Tests are in `tests/test_silver_input.py`, `test_silver_transform.py`, `test_silver_quality.py`, `test_silver_issues.py`, `test_silver_run.py`, and `test_silver_profile.py`. They use small fixtures; the full VM check is separate.

For dev sample preparation, the full-dataset command, and the seven-table comparison, use [the run guide](SILVER_RUN_GUIDE.md). The [milestone report](SILVER_MILESTONE_REPORT.md) explains what we did at each step. No Gold runner or Gold storage configuration exists yet.
