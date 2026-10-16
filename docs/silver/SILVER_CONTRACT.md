# Silver contract

This contract defines the Silver layer's outputs and behavior. Silver reads the prepared Bronze data, makes it easier to understand, and reports problems. It keeps the original readings for comparison with the source.

## What we save

Silver saves seven Parquet tables:

Counts are from the [recorded full-dataset run](../evidence/silver/run1/silver_ready.json) on 11 October 2026. Issue and quarantine totals depend on the input data and quality rules.

| Table | What it contains | What one row means | Calculation and total rows in the full-dataset run |
|---|---|---|---|
| `silver_observations` | Original readings with clearer names and added recording/event details. | One original observation. | 1 source observation = 1 Silver row. Adding the observation counts from 2,228 recordings gives 76,587,318 rows in the full dataset. |
| `instance_quality` | Missing-label, timestamp, and sensor-coverage counts. | One recording. | 1 recording × 1 summary = 1 row per recording. 2,228 recordings × 1 summary = 2,228 rows in the full dataset. |
| `sensor_availability` | Counts and percentages of finite readings for each sensor. | One sensor in one recording. | 1 file × 27 sensors = 27 rows per recording. 2,228 recordings × 27 sensors = 60,156 rows in the full dataset. |
| `quality_issues` | Problems with their reason, severity, affected use, and suggested action. | One problem for one observation, field, and rule. | 1 detected problem for an observation, field, and rule = 1 issue row. 4,028,400 missing-class issues + 5,193 rows with no finite sensors + 161,872 opening-range issues = 4,195,465 rows in the full dataset. |
| `quality_quarantine` | The error entries from `quality_issues`, kept separately for review. | One error entry, not a copy of the whole observation. | 1 error-level issue = 1 quarantine row. 5,193 no-finite-sensor errors + 161,872 opening-range errors = 167,065 rows in the full dataset. |
| `event_reference` | Shared event names and supported class/phase combinations. | One event. | 1 event type × 1 definition = 1 row per event type. 10 event types × 1 definition = 10 rows in total. |
| `sensor_reference` | Shared sensor names, meanings, units, and value rules. | One sensor. | 1 sensor type × 1 definition = 1 row per sensor type. 27 sensor types × 1 definition = 27 rows in total. |

The 27 sensors are columns in each observation row. The source files have different lengths, so the observation total is calculated by adding their row counts.

Issue counts come from detected rule failures. One observation can have several issues, while the reference tables have one row per event or sensor type regardless of the number of source files.

## What one observation means

One row contains the timestamp, labels, and all 27 sensor fields from one original row in a Bronze file. We keep the same number of observations as Bronze; we do not turn each sensor into a separate observation row.

Each observation is identified by these three fields together:

- `_source_path`: which original file it came from.
- `_file_sha256`: the file's fingerprint, identifying its exact version.
- `_row_in_file`: its original position in that file, starting at 0.

`instance_id` alone is not unique. A recording is identified by the source path and file fingerprint together. Silver uses Bronze's current catalogue to match observations to recording details.

If the same observation key appears twice, Silver stops the run. It does not pick a winner or silently delete a copy. Different rows sharing a timestamp are kept and flagged. A corrected file with a new fingerprint is a new source version; Bronze's current catalogue decides which version Silver reads.

## What we keep and rename

These fields and the sensor fields are columns in the same `silver_observations` table:

| Bronze/catalogue field | Silver field | Meaning | Spark type |
|---|---|---|---|
| `source_type` | `source_type` | `REAL`, `SIMULATED`, or `DRAWN`. | string |
| `label` | `instance_label` | Event category for the whole recording: 0–9. | int |
| `class` | `class_code` | Event/phase label for this particular observation. | smallint |
| `state` | `state_code` | Original well operating-status code. | smallint |
| `timestamp` | `timestamp` | Original observation time, without a time zone. | timestamp_ntz |
| `_source_path` | `_source_path` | Original source path. | string |
| `_file_sha256` | `_file_sha256` | Source-file fingerprint. | string |
| `_row_in_file` | `_row_in_file` | Original row position. | bigint |
| `_ingest_run_id` | `_ingest_run_id` | Bronze run that prepared the observation. | string |
| Catalogue `instance_id` | `instance_id` | Recording identifier. | string |
| Catalogue `well_id` | `well_id` | Well identifier, when available. | int |
| Catalogue `instance_start` | `instance_start` | Recording start time, when available. | timestamp_ntz |

Class, state, timestamp, and sensor values can be missing. Well ID and recording start can also be missing; `SIMULATED` and `DRAWN` recordings must not have real well/start details. The other fields above are required. Original row positions must be non-negative.

Silver checks the expected Bronze columns and types. Missing or unexpected columns, wrong types, or a missing/conflicting catalogue match stop the run instead of being guessed or ignored.

### Sensor names and units

All 27 sensors remain nullable doubles in their original units. We rename the columns; we do not convert the readings.

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

The definitions come from [the Bronze reference](../bronze/BRONZE_REFERENCE.md), sections 6 and 7, and are shared through [reference.py](../../src/threew/silver/reference.py) and the two saved reference tables.

## How we explain the event labels

The original data uses `label` to identify the event represented by the whole recording and `class` to describe the condition at each row. A class of `0` represents normal operation. For event `N`, class `N` represents the established event and class `100 + N` represents its transient phase. For example, in an Event 1 recording, classes 0, 1, and 101 represent normal operation, steady Event 1, and transient Event 1 respectively. Silver preserves these original codes and adds readable event names, phase names, and consistency flags. It does not change or remove source rows; missing class values remain missing.

Its a little hard to follow, but essentially the dataset is split into 10 different event types (Event 0 through Event 9, with each with its own label numbering from 0 to 9), and each event type (like event label 1 is a abrupt increase of BSW). On the other hand, the class codes are used to identify which phase of the event is happening at each second in the recording. Look at the tabels below for more details. Silver adds readable names for these events and phases, and also checks for consistency between the class codes and the recording labels.

Event classes in original dataset:

| Recording label | Event | Description | Transient class |
|---:|---|---|---:|
| 0 | Normal Operation | The well is operating normally. | Not applicable |
| 1 | Abrupt Increase of BSW | A sudden increase in basic sediment and water in the produced fluid. | 101 |
| 2 | Spurious Closure of DHSV | The downhole safety valve closes unexpectedly. | 102 |
| 3 | Severe Slugging | Large, unstable surges of liquid and gas move through the production system. | None |
| 4 | Flow Instability | The production flow becomes unstable or oscillates. | None |
| 5 | Rapid Productivity Loss | The well's production capability decreases quickly. | 105 |
| 6 | Quick Restriction in PCK | A rapid restriction occurs in the production choke. | 106 |
| 7 | Scaling in PCK | Mineral deposits build up in the production choke and restrict flow. | 107 |
| 8 | Hydrate in Production Line | Hydrate deposits form in the production line and obstruct flow. | 108 |
| 9 | Hydrate in Service Line | Hydrate deposits form in the service line and obstruct flow. | 109 |

Meanings of the row level class codes:
| Class code | Meaning | Example for recording label 1 |
|---:|---|---|
| 0 | Normal operation | The well is operating normally before or around the event. |
| N | The event is established or in its steady phase | `class = 1` means established Abrupt Increase of BSW. |
| 100 + N | The event is in its transient phase | `class = 101` means transient Abrupt Increase of BSW. |
| Null | No row-level class label is available | The row is retained but cannot be assigned a phase. |

Example of an event 1 recording with class codes:
| Recording label | Class code | Row event name | Row phase |
|---:|---:|---|---|
| 1 | 0 | Normal Operation | Normal |
| 1 | 101 | Abrupt Increase of BSW | Transient |
| 1 | 1 | Abrupt Increase of BSW | Steady |
| 1 | Null | No row event name | Unknown |

The example starts with normal operation and transitions into the event. Event 0 supports class code 0. Silver adds readable names for these events and phases.


Silver adds four string fields:

- `instance_event_name`: readable name for the whole recording's event.
- `row_event_name`: readable name from the row class; missing or unrecognized classes have no name.
- `row_event_phase`: `normal`, `steady`, `transient`, `unknown`, or `unrecognized`.
- `class_label_status`: `consistent`, `inconsistent`, or `unlabeled`, according to the supported combinations above.

We keep the numeric codes as well. An inconsistent label is reported; it is not automatically corrected.

## How we handle missing values and problems

We do not fill missing values, replace them with zero, clip readings, or remove observations. Zero is a real reading. NaN (not a number) and infinity also stay in the observation table, but are flagged.

| Situation | What Silver reports and how to use the data |
|---|---|
| One or more sensors are missing | Counted in `sensor_availability`. Missing cells are not individually added to the issue list. Other sensors can still be used. |
| None of the 27 sensors has a finite reading in a row | Error for sensor analysis. Keep the row and labels, but it cannot supply a sensor measurement. |
| Missing class | Warning. Keep the readings; exclude this label from work requiring a known class. |
| Unsupported class or a class that conflicts with the recording label | Error for labelled analysis. Review the label; other measurements remain available. |
| Missing state | Counted in `instance_quality`. Keep it missing; we have no documented list of allowed `state_code` values. |
| Missing, repeated, or backward timestamp | Error for time-sequence analysis. Keep every reading and review the timing. |
| Interval shorter or longer than the expected one second | Warning. Keep fractional times and gaps; account for the actual spacing in analysis. |
| NaN or positive/negative infinity in a sensor | Error for that measurement. Exclude that value from numeric analysis. |
| Valve sensor value outside 0, 0.5, and 1 | Error for that measurement. Review it without rounding or replacing it. |
| Choke opening outside 0–100 percent | Error for that measurement. Review the value and source definition without clipping it. |

We do not invent pressure, temperature, or flow limits. These checks identify values needing review; they do not prove which replacement value would be correct.

### Timestamps and duplicate times

We compare timestamps in the original file order using `_row_in_file`. We preserve the source clock value without adding a time zone, shifting times, filling gaps, or rewriting the sequence. Synthetic times remain source values, not assumed real calendar dates.

Every row sharing a non-null timestamp within a recording is flagged. The recording summary's `duplicate_timestamp_rows` counts extra occurrences: three rows sharing one timestamp count as two extras, while the issue list flags all three rows.

Parquet does not guarantee row order. When reading a sequence, explicitly sort by original row position. An analysis can request a timestamp-sorted view, but the original position must remain available.

### Summaries and quarantine

`instance_quality` counts missing labels/state/timestamps, unsupported or inconsistent classes, duplicate times, repeated adjacent times, backward intervals, gaps, and sensor coverage. `sensor_availability` counts finite readings: values that are not null, NaN, or infinity. Zero counts as finite. **Finite does not automatically mean valid**: an opening of 120 is finite but still fails the opening rule.

Each issue includes the observation key, `field_name`, `issue_code`, `severity`, `affected_use`, `reason`, `recommended_action`, and `observed_value` as text. One observation can have several issues. Use the observation key to join issues or quarantine entries back to the complete readings.

Quarantine does not delete data or automatically block the whole row. Review the affected field and use: a bad class affects labelled analysis, while a bad sensor value affects that measurement.

## When a run is ready

Silver reads Bronze only after its ready marker confirms a successful build. It checks Bronze counts against the current catalogue and keeps the input protected by Bronze's writer lock. A separate output lock stops two Silver runs writing to the same destination at once.

Each attempt gets a separate folder. Silver reads the saved tables back and checks their columns/types, row identities, counts, original values, recording/event details, quality results, and reference definitions. It also checks that the Bronze marker did not change during the run.

Only after these checks pass does `silver_ready.json` point to the new result. **Ready means the build passed its checks; it does not mean the source data has no quality issues.** The report includes the quality summary and usage rules.

Under the selected output root:

```text
runs/<run_id>/<table_name>/   # The seven tables, saved as Parquet
_runs/<run_id>.json          # Counts, checks, quality, timings, sizes, and source/code/config/contract details
silver_ready.json           # Paths to the latest successful result; not the sensor data itself
```

A failed or skipped attempt preserves the previous ready marker. Handled failures have a report; an abruptly killed process can leave an incomplete folder without a final report.

## Running and repeating Silver

From the project folder with `.venv` active:

```bash
python -m threew.silver.run --env dev
```

On the VM, read production Bronze and write to a separate test destination:

```bash
python -m threew.silver.run --env prod --output-root /home/ubuntu/3w-data/silver-oh
```

Production input requires an explicit separate output root. The official production Silver folder is protected by the current runner.

For reproducibility, run twice with the same code, input, configuration, contract, and references. Compare the columns/types and complete contents of all seven tables, including repeated rows and missing values. Different row or file ordering is acceptable; matching counts alone is not enough. See [the run guide](SILVER_RUN_GUIDE.md) for the comparison command.

## New columns added in Silver

### Columns based on existing labels

These four columns are strings. We create them using the existing event codes and fixed rules.

| New column | How we create it | What it describes |
|---|---|---|
| `instance_event_name` | Look up the event name using `instance_label`. For example, label `1` becomes `Abrupt Increase of BSW`. | The event category for the whole recording. This name stays the same throughout the recording, including rows labelled normal. |
| `row_event_name` | Look up the event using `class_code`. Code `0` means normal operation, codes `1–9` identify the event directly, and codes `101–109` identify the event after subtracting `100`. Missing or unrecognized classes give a null name. | The event described by the individual row's class. This can differ from the recording's event name. |
| `row_event_phase` | Map class `0` to `normal`, classes `1–9` to `steady`, and classes `101–109` to `transient`. A missing class gives `unknown`; any other code gives `unrecognized`. | The phase described by the existing class code. |
| `class_label_status` | Compare `class_code` with the supported combinations for `instance_label`. A missing class gives `unlabeled`, a supported combination gives `consistent`, and any other combination gives `inconsistent`. | Whether the row's class agrees with the recording label and its supported phases. |

## Calculations in the summary tables

We calculate quality summaries from `silver_observations` in `quality.py`. The results are saved in two separate tables: `instance_quality` and `sensor_availability`. These calculations describe the data's quality and availability without changing the original readings.

### Recording summaries: `instance_quality`

We save one summary row per recording. The recording is identified by its source path and file fingerprint.

| Summary column | How we calculate it |
|---|---|
| `recording_rows` | Count every observation in the recording. |
| `unlabeled_rows` | Count observations where `class_code` is null. |
| `null_state_rows` | Count observations where `state_code` is null. |
| `null_timestamp_rows` | Count observations where `timestamp` is null. |
| `duplicate_timestamp_rows` | Subtract the number of distinct non-null timestamps from the total number of non-null timestamps. Three rows sharing one timestamp therefore count as two extra occurrences. |
| `timestamp_intervals_compared` | Count neighbouring row pairs where both timestamps are available, allowing us to calculate their time difference. |
| `one_second_intervals` | Count time differences equal to exactly one second. |
| `repeated_adjacent_timestamps` | Count time differences equal to zero. This checks neighbouring rows only, while `duplicate_timestamp_rows` includes duplicates anywhere in the recording. |
| `backward_intervals` | Count time differences below zero, meaning the timestamp moves backwards in the original row order. |
| `gaps_over_one_second` | Count time differences greater than one second. |
| `largest_gap_seconds` | Find the largest time difference greater than one second. The result is null when no such gap exists. |
| `inconsistent_class_rows` | Count observations where `class_label_status` is `inconsistent`. |
| `unrecognized_class_rows` | Count observations where `row_event_phase` is `unrecognized`. These rows can also be counted as inconsistent, so the two counts overlap. |
| `sensors_present` | Count how many of the 27 sensors have at least one finite reading anywhere in the recording. |
| `sensors_missing_entire_recording` | Calculate `27 - sensors_present`. This includes sensors containing only null, NaN, or infinite values. |
| `no_sensors_present` | Set to `true` when `sensors_present` is zero; otherwise set to `false`. This describes the entire recording, rather than an individual row without measurements. |

`interval_seconds = current_timestamp - previous_timestamp`

The difference is expressed in seconds. The first row has no preceding timestamp, so it has no interval. A pair containing a missing timestamp also has no interval.

We use the original row order because sorting by timestamp before checking would hide backward movement. The temporary interval columns are used during processing and are not saved in `silver_observations`.

### Sensor summaries: `sensor_availability`

We save one summary row per sensor per recording, giving 27 summary rows for each recording.

A finite reading is a value that is not null, NaN, positive infinity, or negative infinity. Zero counts as finite.

| Summary column | How we calculate it |
|---|---|
| `recording_rows` | Reuse the total number of observations in the recording. This is the denominator for the coverage percentage. |
| `rows_with_finite_values` | Count observations where this particular sensor has a finite reading. |
| `rows_without_finite_values` | Calculate `recording_rows - rows_with_finite_values`. |
| `percent_rows_with_finite_values` | Calculate `100 × rows_with_finite_values / recording_rows`, rounded to two decimal places. |
| `has_finite_values` | Set to `true` when `rows_with_finite_values` is greater than zero; otherwise set to `false`. |

For example, if a recording contains 10 rows and a sensor has finite readings in 8 of them, its summary contains:

- `rows_with_finite_values = 8`
- `rows_without_finite_values = 2`
- `percent_rows_with_finite_values = 80.00`
- `has_finite_values = true`

These calculations measure availability. A choke opening of 120 percent counts as finite but still fails the opening-range check. Similarly, missing measurements can reflect a sensor being unavailable by design; the availability summary alone does not establish an error.