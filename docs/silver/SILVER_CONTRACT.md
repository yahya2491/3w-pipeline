# Silver contract

This contract defines the current v2 output. The [quality rules](QUALITY_RULES.md) describe how Silver handles problems; the [assumptions](SILVER_ASSUMPTIONS.md) explain the choices.

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

## The 106 columns in `silver_observations`

Each row in `silver_observations` is still one original timestamped observation. It has 106 columns: 36 fields carried over from Bronze (some renamed), 3 recording fields matched from Bronze's catalogue, and 67 fields Silver adds. The new columns are explained below. They do not create extra rows, and some values can be null.

| Where the columns come from | Count | What they contain |
|---|---:|---|
| Bronze | 36 | 9 non-sensor fields and the 27 original sensor readings. Silver gives some of these clearer names. |
| Bronze catalogue | 3 | `instance_id`, `well_id`, and `instance_start`, matched to the observation's source file. |
| Added by Silver | 67 | 4 event descriptions, 27 clean readings, 4 repair details, 2 sequence fields, 28 sensor features, and 2 invalid-opening durations. |

The nine non-sensor Bronze fields are `source_type`, `label` (Silver calls it `instance_label`), `class` (`class_code`), `state` (`state_code`), `timestamp`, `_source_path`, `_file_sha256`, `_ingest_run_id`, and `_row_in_file`. The 27 sensor fields are renamed as shown below. Catalogue `instance_id` is required; `well_id` and `instance_start` can be null.

The observation key is the combination of `_source_path`, `_file_sha256`, and `_row_in_file`: it tells you which file, which version of that file, and which original row. `instance_id` alone is not unique. Parquet does not promise row order, so sort by `_row_in_file` when reading a sequence.

## Why Silver adds these columns

The clean columns give Gold usable readings, including limited repairs of invalid choke openings. The 120-second features describe recent sensor behaviour for the planned Spark classifier. Repair flags and quality tables explain which values need care. Original readings remain available, so Gold can calculate additional features.

## The 67 columns Silver adds

### Event descriptions: 4 columns

The recording label (`instance_label`) says which event the whole recording represents. The row class (`class_code`) says what class applies at that observation. Silver keeps both codes and adds these descriptions:

| New column | What it means | What it is useful for |
|---|---|---|
| `instance_event_name` | The readable event name for the recording, looked up from `instance_label`. It stays the same on every row in that recording. | Makes the recording-level label easier to understand and group by. |
| `row_event_name` | The readable name for that row's `class_code`: 0 means normal operation; 1–9 map to their event; 101–109 map to events 1–9. Missing codes and codes outside those ranges have no name. | Shows the event, if any, described by this particular row. |
| `row_event_phase` | `normal` for class 0, `steady` for 1–9, `transient` for 101–109, `unknown` for a missing class, and `unrecognized` for any other class. | Shows which phase the row class describes. |
| `class_label_status` | `unlabeled` when the class is missing, `consistent` when it is supported for the recording's event, and `inconsistent` otherwise. | Helps identify rows whose class does not agree with the recording label. It does not fix the label. |

Event names and supported label/class combinations are listed in `event_reference`. These are descriptions of supplied labels, so they must stay out of the classifier's inputs.

### Clean sensor values: 27 columns

Silver adds one column for **each of the 27 sensors**. Replace `<sensor>` with a sensor name from the mapping below. The clean field is a nullable double.

| New column pattern | What it means | What it is useful for |
|---|---|---|
| `<sensor>_clean` | The original reading when it passes that sensor's checks; otherwise null. Only the two choke-opening sensors can receive a three-second carry-forward repair for a finite out-of-range reading. | Gives downstream work one consistent sensor field to use. |

Only these two sensors can receive the three-second repair:

- `gas_lift_choke_opening_pct` — gas-lift choke opening.
- `production_choke_opening_pct` — production choke opening.

Repairs are written to their `_clean` columns. The other 25 sensors are not filled from earlier readings but instead switch to null when they are Null, NaN or infinity.

An opening of `120` means **120%**, which is outside the allowed 0–100% range. Silver puts the repaired value in `production_choke_opening_pct_clean`: for example, 40 if the most recent original valid opening was 40% at most three seconds earlier in the same continuous segment. Without an eligible reading, the clean value becomes null. The original column keeps 120 only to record what the source reported; use the clean column for sensor analysis. Silver does not assume the correct value was 100% by clipping it. See the [quality rules](QUALITY_RULES.md).

### Choke-repair details: 4 columns

These fields describe repairs for the two choke openings listed above. Imputation fields are booleans; source ages are nullable doubles.

| New column pattern | What it means | What it is useful for |
|---|---|---|
| `<opening>_was_imputed` | `true` when Silver carried forward an earlier valid opening to fill a finite out-of-range value; otherwise `false`. | Shows which clean values are estimates. |
| `<opening>_source_age_seconds` | Zero for a valid opening with usable time. Otherwise, the age of the earlier original valid opening, or null when there is no anchor or usable time. An age above three seconds can be reported even though no repair was made. | Use with `_was_imputed` to inspect the age of a repair's source. |

### Sequence details: 2 columns

| New column | What it means | What it is useful for |
|---|---|---|
| `sequence_usable` | Boolean: the current timestamp exists and the previous source row's timestamp is either missing or earlier. Also true for the first row when it has a timestamp. | Identifies rows that can start or continue an ordered segment. A cadence gap still starts a new segment. |
| `sequence_segment_id` | The ID of the uninterrupted time segment containing this row. Silver starts a new segment after a missing/bad time, a cadence break, or a break in row positions. | Keeps choke repairs within a continuous sequence and helps consumers identify gaps. |

`sequence_segment_id` is a long integer scoped to one recording. Repairs and features restart at segment boundaries.

### Recent sensor behaviour: 28 columns

Each of these **seven sensors** gets the four fields in the following table:

- `downhole_pdg_pressure_pa`
- `subsea_tree_tpt_pressure_pa`
- `production_choke_upstream_pressure_pa`
- `gas_lift_choke_downstream_pressure_pa`
- `subsea_tree_tpt_temperature_c`
- `production_choke_downstream_temperature_c`
- `gas_lift_flow_rate_m3_s`

| New column pattern | What it means | What it is useful for |
|---|---|---|
| `<sensor>_mean_120s` | Average of the available clean readings during the trailing 120 seconds. Nullable double. | Describes the recent operating level. |
| `<sensor>_stddev_120s` | Sample standard deviation of those readings; null with fewer than two values. Nullable double. | Describes recent variation. |
| `<sensor>_change` | Current clean value minus the previous row's clean value in the same segment. Null when either value is missing or this is the segment's first row. Nullable double. | Shows sudden changes. |
| `<sensor>_count_120s` | Number of non-null clean readings supporting the trailing statistics. Nullable long, from 0 to 120. | Lets Gold reject examples with insufficient sensor coverage. |

The time range is **after `current time - 120 seconds`, through the current time**. At the documented one-second cadence, it holds at most 120 observations. Early rows use the available history; they do not wait for a full window. A missing current reading can still have a recent mean. A window with no usable readings has count 0 and null mean/standard deviation. All four fields are null when the row's time is unusable.

These fields use only current and earlier readings within the same recording and sequence segment. They do not cross gaps, use future values, fill sensors, or create new window rows. Gold decides which observations become labelled model examples.

### Persistent invalid choke readings: 2 columns

| New column pattern | What it means | What it is useful for |
|---|---|---|
| `<opening>_invalid_duration_seconds` | Elapsed time in the current consecutive run of finite out-of-range **original** openings. Added for both choke-opening sensors. Nullable long. | Describes a persistent problem with the reported opening, even while a short clean repair is available. |

The first invalid reading has duration 0; further consecutive invalid readings have 1, 2, and so on seconds. A valid, missing, NaN, or infinite reading resets the run to 0. A recording/sequence boundary also resets it, and unusable time gives null. For example, `40, 120, 120, 120, null, 120` gives durations `0, 0, 1, 2, 0, 0`. This column describes sensor reporting; it is not a confirmed undesirable well-event label.

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
