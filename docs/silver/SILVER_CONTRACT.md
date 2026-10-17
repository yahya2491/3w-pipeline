# Silver contract

This contract explains what Silver saves and what its columns mean. The [assumptions](SILVER_ASSUMPTIONS.md) explain why we made the cleaning and feature choices. The exact settings Silver runs with are in [`silver_rules.yaml`](../../conf/quality/silver_rules.yaml).

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

## The 135 columns in `silver_observations`

Each row in `silver_observations` is still one original sensor reading. That row now has 135 columns: 36 fields carried over from Bronze (some renamed), 3 recording fields matched from Bronze's catalogue, and 96 fields Silver calculates. The rows are not the same schema as Bronze, and Silver has not made extra rows for the new fields. Some values can be null.

| Where the columns come from | Count | What they contain |
|---|---:|---|
| Bronze | 36 | 9 non-sensor fields and the 27 original sensor readings. Silver gives some of these clearer names. |
| Bronze catalogue | 3 | `instance_id`, `well_id`, and `instance_start`, matched to the observation's source file. |
| Added by Silver | 96 | Event descriptions, sensor validity and clean values, choke-repair details, sequence and evidence fields, and time-series features. The next tables explain them. |

The nine non-sensor Bronze fields are `source_type`, `label` (Silver calls it `instance_label`), `class` (`class_code`), `state` (`state_code`), `timestamp`, `_source_path`, `_file_sha256`, `_ingest_run_id`, and `_row_in_file`. The 27 sensor fields are renamed as shown below. Catalogue `instance_id` is required; `well_id` and `instance_start` can be null.

The observation key is the combination of `_source_path`, `_file_sha256`, and `_row_in_file`: it tells you which file, which version of that file, and which original row. `instance_id` alone is not unique. Parquet does not promise row order, so sort by `_row_in_file` when reading a sequence.

## Why Silver adds these columns

Silver adds prepared columns for two reasons. First, it marks each of the 27 sensor readings as valid or invalid and puts usable values in a clean column. Invalid values are null in that column, except that a finite out-of-range choke opening can use the most recent original valid opening from within 3 seconds; Silver flags this estimate. This does not fill missing readings or detect general abnormal pressure, temperature, or flow values, and the original readings stay unchanged. Second, it adds rolling averages, variation, and deviation from recent behavior as useful prepared features for Gold anomaly detection. Simple rates and rolling coverage can be calculated from the clean values, validity flags, timestamps, and sequence segments, so Silver leaves those out. The Gold model is planned work and has not yet been built or evaluated. The same Silver data can also support CNN experiments in the other course.

## The 96 columns Silver adds

### Event descriptions: 4 columns

The recording label (`instance_label`) says which event the whole recording represents. The row class (`class_code`) says what class applies at that observation. Silver keeps both codes and adds these descriptions:

| New column | What it means | What it is useful for |
|---|---|---|
| `instance_event_name` | The readable event name for the recording, looked up from `instance_label`. It stays the same on every row in that recording. | Makes the recording-level label easier to understand and group by. |
| `row_event_name` | The readable name for that row's `class_code`: 0 means normal operation; 1–9 map to their event; 101–109 map to events 1–9. Missing or unsupported codes have no name. | Shows the event, if any, described by this particular row. |
| `row_event_phase` | `normal` for class 0, `steady` for 1–9, `transient` for 101–109, `unknown` for a missing class, and `unrecognized` for any other class. | Shows which phase the row class describes. |
| `class_label_status` | `unlabeled` when the class is missing, `consistent` when it is supported for the recording's event, and `inconsistent` otherwise. | Helps identify rows whose class does not agree with the recording label. It does not fix the label. |

Event names and supported label/class combinations are listed in `event_reference`. These columns describe existing labels; they are not predictions or a future-event target.

### Sensor validity and clean values: 54 columns

Silver adds two columns for **each of the 27 sensors**. Replace `<sensor>` with a sensor name from the mapping below. The validity field is boolean; the clean field is a nullable double.

| New column pattern | What it means | What it is useful for |
|---|---|---|
| `<sensor>_is_valid` | `true` when the original reading is finite and meets its configured value rule; otherwise `false`. | Lets you filter or count usable original readings without changing the source value. |
| `<sensor>_clean` | The prepared value for that sensor. It contains the original value when valid, a permitted choke repair when available, or null when no usable value is available. | Provides a consistent sensor series for downstream features and models. |

The original sensor column is still present. For example, if the original production choke opening is 120, it stays 120 in `production_choke_opening_pct`, its validity flag is false, and its clean column contains a recent permitted estimate or null. See [Silver assumptions](SILVER_ASSUMPTIONS.md) for the repair conditions.

### Choke-repair details: 4 columns

These columns are added for the two choke openings only: `gas_lift_choke_opening_pct` and `production_choke_opening_pct`. Imputation fields are booleans; source ages are nullable doubles.

| New column pattern | What it means | What it is useful for |
|---|---|---|
| `<opening>_was_imputed` | `true` when Silver carried forward an earlier valid opening to fill a finite out-of-range value; otherwise `false`. | Shows which clean values are estimates. |
| `<opening>_source_age_seconds` | Seconds between the reading and the original valid opening used as its source. It is zero on a valid reading and null when no age can be calculated. | Shows how old the source value is, so downstream work can keep or reject older estimates. |

### Sequence and sensor-evidence details: 5 columns

| New column | What it means | What it is useful for |
|---|---|---|
| `sequence_usable` | `true` when this row has a timestamp and is later than the previous row in source order, or is the first timed row. | Identifies rows that can take part in time-based features. |
| `sequence_segment_id` | The ID of the uninterrupted time segment containing this row. Silver starts a new segment after a missing/bad time, a cadence break, or a break in row positions. | Keeps lag and rolling features from crossing a gap or sequence break. |
| `observed_valid_sensor_count` | Number of the 27 original sensor readings that are valid on this row. | Shows how much sensor evidence was actually recorded. |
| `prepared_sensor_count` | Number of non-null clean sensor values on this row, including any permitted opening repair. | Shows how many prepared sensor values are available. This may be larger than the observed count. |
| `has_observed_sensor_evidence` | `true` when at least one original sensor reading on the row is valid. | A quick check for rows with any original sensor evidence. It is not a full model eligibility rule. |

### Time-series features: 28 columns

These 4 columns are added for each of seven selected sensors: `downhole_pdg_pressure_pa`, `subsea_tree_tpt_pressure_pa`, `production_choke_upstream_pressure_pa`, `production_choke_downstream_pressure_pa`, `production_choke_downstream_temperature_c`, `gas_lift_flow_rate_m3_s`, and `production_choke_opening_pct`. In the table below, replace `<sensor>` with one of those names. They are doubles. Silver leaves out simple rates and rolling coverage because Gold can calculate them from clean readings, validity flags, timestamps, and sequence segments. Separate counts and time-since-valid fields are also left out. The difference between the two means can be calculated as needed.

| New column pattern | What it means | What it is useful for |
|---|---|---|
| `<sensor>_mean_120s` / `<sensor>_mean_300s` | Average clean value in the short or long window. | Summarizes the sensor's recent and longer-term level. |
| `<sensor>_stddev_120s` | Sample standard deviation of clean values in the short window. | Shows how much the sensor varied recently. |
| `<sensor>_deviation_120s` | Current clean value minus the mean of the preceding 120 seconds `[t-120s, t)`. The current value is excluded from that mean. | Shows whether the current reading differs from its recent baseline. |

The configured minimum coverage is 50% of the expected readings: 60 original valid readings for 120 seconds and 150 for 300 seconds. Statistics are null below the required coverage; standard deviation also needs at least two valid readings. Temporal features do not cross sequence segments. These are candidate model inputs, not a proven best feature set.

### Choke pressure relationship: 1 column

| New column | What it means | What it is useful for |
|---|---|---|
| `production_choke_pressure_drop_pa` | Clean production-choke upstream pressure minus clean downstream pressure. | Shows the pressure difference across the choke. Negative results are kept for review. |

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

