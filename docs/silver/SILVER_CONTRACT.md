# Silver contract

This contract explains what Silver saves and what its columns mean. The [assumptions](SILVER_ASSUMPTIONS.md) explain the cleaning choices. The exact settings Silver runs with are in [`silver_rules.yaml`](../../conf/quality/silver_rules.yaml).

## What Silver saves

Silver writes seven Parquet tables:

| Table | What a row is for |
|---|---|
| `silver_observations` | One original sensor observation, with recording details and Silver's added columns. This is the main table. |
| `instance_quality` | A summary of missing labels, timestamps, and sensor coverage for one recording. |
| `sensor_availability` | A summary of one sensor's available readings in one recording. There are 27 rows per recording. |
| `quality_issues` | A list of individual problems. One observation can have several entries. |
| `quality_quarantine` | The error-level entries from `quality_issues`, for easier review. |
| `event_reference` | The event names and class/phase combinations supported by the project. |
| `sensor_reference` | The 27 sensor names, meanings, units, and applicable value rules. |

Silver keeps one row for each Bronze observation. It does not turn the sensors into separate rows or remove observations with missing or invalid values.

## The 76 columns in `silver_observations`

Each row in `silver_observations` is still one original sensor reading. It has 76 columns: 36 fields carried over from Bronze (some renamed), 3 recording fields matched from Bronze's catalogue, and 37 fields Silver adds. The rows are not the same schema as Bronze, and Silver has not made extra rows for the new fields. Some values can be null.

| Where the columns come from | Count | What they contain |
|---|---:|---|
| Bronze | 36 | 9 non-sensor fields and the 27 original sensor readings. Silver gives some of these clearer names. |
| Bronze catalogue | 3 | `instance_id`, `well_id`, and `instance_start`, matched to the observation's source file. |
| Added by Silver | 37 | Event descriptions, clean sensor values, choke-repair details, and sequence fields. The next tables explain them. |

The nine non-sensor Bronze fields are `source_type`, `label` (Silver calls it `instance_label`), `class` (`class_code`), `state` (`state_code`), `timestamp`, `_source_path`, `_file_sha256`, `_ingest_run_id`, and `_row_in_file`. The 27 sensor fields are renamed as shown below. Catalogue `instance_id` is required; `well_id` and `instance_start` can be null.

The observation key is the combination of `_source_path`, `_file_sha256`, and `_row_in_file`: it tells you which file, which version of that file, and which original row. `instance_id` alone is not unique. Parquet does not promise row order, so sort by `_row_in_file` when reading a sequence.

## Why Silver adds these columns

Silver adds columns that make observations easier to trust and reuse: readable event labels, prepared sensor values, choke-repair details, and sequence checks. It keeps the original readings and flags estimated choke values. Silver does not add per-row sensor counts or model-window statistics; Gold can calculate those from the reusable observations. The Gold model is planned work and has not yet been built or evaluated. The same Silver data can also support CNN experiments in the other course.

## The 37 columns Silver adds

### Event descriptions: 4 columns

The recording label (`instance_label`) says which event the whole recording represents. The row class (`class_code`) says what class applies at that observation. Silver keeps both codes and adds these descriptions:

| New column | What it means | What it is useful for |
|---|---|---|
| `instance_event_name` | The readable event name for the recording, looked up from `instance_label`. It stays the same on every row in that recording. | Makes the recording-level label easier to understand and group by. |
| `row_event_name` | The readable name for that row's `class_code`: 0 means normal operation; 1–9 map to their event; 101–109 map to events 1–9. Missing or unsupported codes have no name. | Shows the event, if any, described by this particular row. |
| `row_event_phase` | `normal` for class 0, `steady` for 1–9, `transient` for 101–109, `unknown` for a missing class, and `unrecognized` for any other class. | Shows which phase the row class describes. |
| `class_label_status` | `unlabeled` when the class is missing, `consistent` when it is supported for the recording's event, and `inconsistent` otherwise. | Helps identify rows whose class does not agree with the recording label. It does not fix the label. |

Event names and supported label/class combinations are listed in `event_reference`. These columns describe existing labels; they are not predictions or a future-event target.

### Clean sensor values: 27 columns

Silver adds one column for **each of the 27 sensors**. Replace `<sensor>` with a sensor name from the mapping below. The clean field is a nullable double.

| New column pattern | What it means | What it is useful for |
|---|---|---|
| `<sensor>_clean` | The prepared value for that sensor. It contains the original value when usable, a permitted choke repair when available, or null when no usable value is available. | Gives downstream work one consistent sensor field to use. |

The original sensor column is still present. For example, if the original production choke opening is 120, it stays 120 in `production_choke_opening_pct`; the clean column contains a recent permitted estimate or null. The quality tables explain why an original value was flagged, and the repair fields below show when Silver estimated an opening. See [Silver assumptions](SILVER_ASSUMPTIONS.md) for the repair conditions.

### Choke-repair details: 4 columns

These columns are added for the two choke openings only: `gas_lift_choke_opening_pct` and `production_choke_opening_pct`. Imputation fields are booleans; source ages are nullable doubles.

| New column pattern | What it means | What it is useful for |
|---|---|---|
| `<opening>_was_imputed` | `true` when Silver carried forward an earlier valid opening to fill a finite out-of-range value; otherwise `false`. | Shows which clean values are estimates. |
| `<opening>_source_age_seconds` | Seconds between the reading and the original valid opening used as its source. It is zero on a valid reading and null when no age can be calculated. | Shows how old the source value is, so downstream work can keep or reject older estimates. |

### Sequence details: 2 columns

| New column | What it means | What it is useful for |
|---|---|---|
| `sequence_usable` | `true` when this row has a timestamp and is later than the previous row in source order, or is the first timed row. | Flags rows whose time cannot be safely used in an ordered sequence. |
| `sequence_segment_id` | The ID of the uninterrupted time segment containing this row. Silver starts a new segment after a missing/bad time, a cadence break, or a break in row positions. | Keeps choke repairs within a continuous sequence and helps consumers identify gaps. |

These two fields describe time continuity. Sensor counts and pressure differences are not stored as extra columns; Gold can calculate them when its question needs them.

## Sensor names

The original readings stay in their original units. These names make the sensor fields easier to read; `sensor_reference` also records each field's meaning and unit.

| Bronze name | Silver column |
|---|---|
| `ABER-CKGL` | `gas_lift_choke_opening_pct` |
| `ABER-CKP` | `production_choke_opening_pct` |
| `ESTADO-DHSV` | `downhole_safety_valve_state` |
| `ESTADO-M1` | `production_master_valve_state` |
| `ESTADO-M2` | `annulus_master_valve_state` |
| `ESTADO-PXO` | `pig_crossover_valve_state` |
| `ESTADO-SDV-GL` | `gas_lift_shutdown_valve_state` |
| `ESTADO-SDV-P` | `production_shutdown_valve_state` |
| `ESTADO-W1` | `production_wing_valve_state` |
| `ESTADO-W2` | `annulus_wing_valve_state` |
| `ESTADO-XO` | `crossover_valve_state` |
| `P-ANULAR` | `annulus_pressure_pa` |
| `P-JUS-BS` | `service_pump_downstream_pressure_pa` |
| `P-JUS-CKGL` | `gas_lift_choke_downstream_pressure_pa` |
| `P-JUS-CKP` | `production_choke_downstream_pressure_pa` |
| `P-MON-CKGL` | `gas_lift_choke_upstream_pressure_pa` |
| `P-MON-CKP` | `production_choke_upstream_pressure_pa` |
| `P-MON-SDV-P` | `production_shutdown_valve_upstream_pressure_pa` |
| `P-PDG` | `downhole_pdg_pressure_pa` |
| `PT-P` | `subsea_production_line_pressure_pa` |
| `P-TPT` | `subsea_tree_tpt_pressure_pa` |
| `QBS` | `service_pump_flow_rate_m3_s` |
| `QGL` | `gas_lift_flow_rate_m3_s` |
| `T-JUS-CKP` | `production_choke_downstream_temperature_c` |
| `T-MON-CKP` | `production_choke_upstream_temperature_c` |
| `T-PDG` | `downhole_pdg_temperature_c` |
| `T-TPT` | `subsea_tree_tpt_temperature_c` |

## Quality tables in plain terms

`instance_quality` gives one summary for each recording: how many observations it has, how many labels or timestamps are missing or inconsistent, and whether sensors have any finite readings.

`sensor_availability` gives one summary for each sensor in each recording. It counts finite readings and calculates their percentage. Finite only means “not null, NaN, or infinity”; it does not mean the value is in range. For example, 120 is finite but is not a valid choke opening.

`quality_issues` lists individual problems with the affected observation key, field, rule, severity, reason, and recommended action. `quality_quarantine` contains the error-level issues. These tables help a consumer decide what to review or exclude; they do not remove the observation from `silver_observations`.

