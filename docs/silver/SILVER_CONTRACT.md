# Silver table contract

**Status:** Working contract; Milestone 7 checks implemented. Fractional timestamp intervals are supported. Sensor meanings and units come from `docs/BRONZE_REFERENCE.md` section 6. Silver keeps source units and values; it does not fill, clip, or convert measurements.

## What one row means

One row is one observation from one original Bronze file. Its stable key is `(_source_path, _file_sha256, _row_in_file)`. Keep the source path, file hash, and zero-based row number so each row can be traced to its exact source and position. `instance_id` alone is not unique.

## Observation fields
These fields and the sensor fields below are columns in the same silver_observations table.

| Bronze field | Silver field | Meaning | Type | Unit or values | Nullable? |
|---|---|---|---|---|---|
| `source_type` | `source_type` | Recording source | string | REAL, SIMULATED, DRAWN | No |
| `label` | `instance_label` | Event category for the whole recording | integer | 0-9 | No |
| `class` | `class_code` | Normal/event label for this row | smallint | Source class code | Yes |
| `state` | `state_code` | Well operating status for this row | smallint | Source state code | Yes |
| `timestamp` | `timestamp` | Source time for this observation | timestamp without timezone | Source clock value | Yes |
| `_source_path` | `_source_path` | Original file path; part of row key | string | Dataset path | No |
| `_file_sha256` | `_file_sha256` | Exact source-file version; part of row key | string | SHA-256 | No |
| `_row_in_file` | `_row_in_file` | Original row position; part of row key | bigint | Starts at 0 | No |
| `_ingest_run_id` | `_ingest_run_id` | Bronze run that produced the input | string | Bronze run ID | No |
| Manifest `instance_id` | `instance_id` | Source recording identifier | string | Not unique by itself | No |
| Manifest `well_id` | `well_id` | Well identifier, when present | integer | REAL recordings | Yes for SIMULATED/DRAWN |
| Manifest `instance_start` | `instance_start` | Recording start time | timestamp without timezone | Source clock value | Yes for SIMULATED/DRAWN |

Keep `class_code`, `instance_label`, and `state_code` separate. Missing class/state values stay missing. Synthetic timestamps stay as source clock values, not real calendar dates. SIMULATED and DRAWN recordings have no well ID or real start time.


## Timestamp and row order

Keep `timestamp` exactly as Bronze provides it: a timestamp without a time zone. Do not assign a time zone, shift times, fill gaps, sort rows by timestamp, or remove observations to make the sequence look regular. Synthetic timestamps remain synthetic source values, not real calendar dates.

For continuity checks, order rows within each original file by `_row_in_file`, then compare adjacent timestamps. The production profile of all 2,228 recordings found no null or duplicate timestamps, repeated adjacent times, backward intervals, or gaps over one second. Every adjacent interval was one second.

This is a quality result, not a reason to add a repeated time-delta column to every observation. Report future problems in the separate per-recording quality summary.

## Derived event fields

| Silver field | Meaning | Rule |
|---|---|---|
| `instance_event_name` | Readable name for the recording's event category | Map `instance_label` 0-9 using `docs/BRONZE_REFERENCE.md`. |
| `row_event_name` | Readable event name for this observation | Derive from `class_code`; class 0 means Normal Operation, 1-9 use that event label, and 101-109 use the label after subtracting 100. Null or unsupported codes have no event name. |
| `row_event_phase` | What part of the recording this row represents | 0 = `normal`; 1-9 = `steady`; 101-109 = `transient`; null = `unknown`; other codes = `unrecognized`. |
| `class_label_status` | Whether the row label fits the recording label | Null class = `unlabeled`; valid combinations = `consistent`; other combinations = `inconsistent`. This follows the measured dataset rules in the Bronze reference, including events 3 and 4 having no normal or transient rows. |

These fields explain the existing codes; they do not replace them. Silver keeps every observation and reports inconsistent labels rather than silently changing or dropping rows.

## Sensor fields

Each source sensor stays a nullable double in its original unit. A sensor may be null for many or all rows in a recording; that is reported as a data limitation, not a reason by itself to drop observations. Zero is a value, not a missing-value marker.

| Bronze field | Silver field | Meaning | Unit/domain |
|---|---|---|---|
| `ABER-CKGL` | `gas_lift_choke_opening_pct` | Gas-lift choke opening | percent |
| `ABER-CKP` | `production_choke_opening_pct` | Production choke opening | percent |
| `ESTADO-DHSV` | `downhole_safety_valve_state` | DHSV state | source code: 0, 0.5, 1 |
| `ESTADO-M1` | `production_master_valve_state` | Production master valve state | source code: 0, 0.5, 1 |
| `ESTADO-M2` | `annulus_master_valve_state` | Annulus master valve state | source code: 0, 0.5, 1 |
| `ESTADO-PXO` | `pig_crossover_valve_state` | Pig-crossover valve state | source code: 0, 0.5, 1 |
| `ESTADO-SDV-GL` | `gas_lift_shutdown_valve_state` | Gas-lift shutdown valve state | source code: 0, 0.5, 1 |
| `ESTADO-SDV-P` | `production_shutdown_valve_state` | Production shutdown valve state | source code: 0, 0.5, 1 |
| `ESTADO-W1` | `production_wing_valve_state` | Production wing valve state | source code: 0, 0.5, 1 |
| `ESTADO-W2` | `annulus_wing_valve_state` | Annulus wing valve state | source code: 0, 0.5, 1 |
| `ESTADO-XO` | `crossover_valve_state` | Crossover valve state | source code: 0, 0.5, 1 |
| `P-ANULAR` | `annulus_pressure_pa` | Pressure in the well annulus | Pa |
| `P-JUS-BS` | `service_pump_downstream_pressure_pa` | Service-pump downstream pressure | Pa |
| `P-JUS-CKGL` | `gas_lift_choke_downstream_pressure_pa` | Gas-lift choke downstream pressure | Pa |
| `P-JUS-CKP` | `production_choke_downstream_pressure_pa` | Production choke downstream pressure | Pa |
| `P-MON-CKGL` | `gas_lift_choke_upstream_pressure_pa` | Gas-lift choke upstream pressure | Pa |
| `P-MON-CKP` | `production_choke_upstream_pressure_pa` | Production choke upstream pressure | Pa |
| `P-MON-SDV-P` | `production_shutdown_valve_upstream_pressure_pa` | Production shutdown valve upstream pressure | Pa |
| `P-PDG` | `downhole_pdg_pressure_pa` | Permanent downhole gauge pressure | Pa |
| `PT-P` | `subsea_production_line_pressure_pa` | Production-line pressure downstream of the production wing valve | Pa |
| `P-TPT` | `subsea_tree_tpt_pressure_pa` | Subsea tree pressure at the TPT | Pa |
| `QBS` | `service_pump_flow_rate_m3_s` | Service-pump flow rate | m3/s |
| `QGL` | `gas_lift_flow_rate_m3_s` | Gas-lift flow rate | m3/s |
| `T-JUS-CKP` | `production_choke_downstream_temperature_c` | Production choke downstream temperature | degrees C |
| `T-MON-CKP` | `production_choke_upstream_temperature_c` | Production choke upstream temperature | degrees C |
| `T-PDG` | `downhole_pdg_temperature_c` | Permanent downhole gauge temperature | degrees C |
| `T-TPT` | `subsea_tree_tpt_temperature_c` | Subsea tree temperature at the TPT | degrees C |


## Quality summaries

These outputs are separate from the observation table; they do not change or remove observations.

- `instance_quality` has one row per source-file version, keyed by `_source_path` and `_file_sha256`. It reports observation count, unlabeled and inconsistent rows, null state/timestamps, timestamp continuity counts, and how many of the 27 sensors have at least one finite value.
- `sensor_availability` has one row per recording per sensor (27 rows per recording). It reports the number and percentage of observations with finite sensor values, along with how many do not. A zero reading is finite and counts as present; null, NaN, and infinite values do not.


## First-version rules and outputs

- Keep every Bronze observation unless a specific row cannot be represented safely. If any rows are rejected, keep their key and a reason in a separate output.
- Do not fill missing sensors, class, or state. Do not clip or convert sensor values.
- Keep the source timestamp and original row position. Report timestamp continuity in the separate per-recording quality summary; do not add a repetitive interval column to each observation.
- Keep observations in `silver_observations`; create separate `instance_quality` and `sensor_availability` summaries.
- A run is usable only after its outputs are checked against Bronze and a ready marker points to the completed run.

Milestone 4 adds readable event names and row phases, plus a status for checking row class against the recording label. The original class code and recording label remain available.

## Saving and checking a Silver run

Milestone 7 saves three Parquet outputs under the environment's storage root:

- silver/runs/<run_id>/silver_observations/
- silver/runs/<run_id>/instance_quality/
- silver/runs/<run_id>/sensor_availability/
- silver/_runs/<run_id>.json
- silver/silver_ready.json

Each run has a separate folder, so a failed attempt cannot replace a previously completed dev run. The JSON run report records the Bronze run used, output paths, row counts, and check results. The ready marker is written only after the saved tables pass read-back checks; it points to the latest successful run.

Before publishing the marker, the runner validates Bronze totals and per-recording counts against the captured ready marker and manifest. It checks saved schemas, required non-null fields, unique observation keys, exact key equality and all preserved values against Bronze. Recording/event details and both quality tables are checked against freshly computed expectations, including the exact recording-and-sensor pairs.

The input stays under the existing Bronze writer lock and the selected output has its own lock. A final marker check catches an input marker changed outside that protocol. Failed and skipped attempts write reports and preserve the previous Silver marker. Reports include timings, output sizes and source/code/config/contract information.

The separate run directories and final ready marker define which run consumers should read. Repeatability is checked in integration tests by comparing all three tables as sets of rows with duplicate counts included. See [the run guide](SILVER_RUN_GUIDE.md) and [functional review](SILVER_REVIEW.md).

Run the first version from the repository root in the WSL development checkout:

    python -m threew.silver.run --env dev

Local development uses dev. Production input requires an explicit isolated `--output-root`; the canonical production Silver directory remains protected. See the run guide for the VM command.
