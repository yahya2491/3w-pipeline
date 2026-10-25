# Silver contract

The current observation export has **79 columns** (internal contract version 4).
[Quality rules](QUALITY_RULES.md) define validation and repairs; the
[assumptions](SILVER_ASSUMPTIONS.md) explain inferred values.

## Saved tables

| Table | Purpose |
|---|---|
| silver_observations | One original observation with one clean value per sensor and one clean class code. |
| instance_quality | Original-source quality for each recording. |
| sensor_availability | Original-source finite-reading coverage for each sensor/recording. |
| quality_issues | Source problems, original bad values and observation keys. |
| quality_quarantine | Error-level source issues. |
| event_reference | Event names and supported class/phase combinations. |
| sensor_reference | Logical sensor identifiers, original names, units and domains. |

Bronze preserves the original dataset. Silver does not export the 27 original
sensor-value columns, original class_code, or repeated _ingest_run_id. Original values remain
available in Bronze through the source path, exact file hash and row position.
The source Bronze run ID is recorded once in the Silver report and ready marker.
Quality tables still describe source issues even when a clean value is inferred.

## Observation columns

| Group | Count | Fields |
|---|---:|---|
| Recording, time and lineage context | 10 | source_type, instance_label, state_code, timestamp, _source_path, _file_sha256, _row_in_file, instance_id, well_id, instance_start |
| Clean class and inference provenance | 2 | class_code_clean, class_was_inferred |
| Event descriptions | 4 | instance_event_name, row_event_name, row_event_phase, class_label_status |
| Clean sensor readings | 27 | One <sensor>_clean field per sensor. |
| Choke repair provenance | 4 | Two was_imputed flags and two source_age_seconds fields. |
| Sequence context | 2 | sequence_usable, sequence_segment_id |
| Recent sensor behaviour | 28 | Four trailing features for each of seven sensors. |
| Invalid-opening durations | 2 | One source-invalid streak duration for each choke opening. |
| **Total** | **79** | One row per original observation. |

_source_path, _file_sha256 and _row_in_file form the observation key. instance_id
alone is not unique. Sort by _row_in_file within the source path/file hash when
reading sequences; Parquet does not promise physical row order. instance_label
is the recording event, not the row class. state_code is operational status,
not an event class. These fields have distinct meanings and are retained.

## Class values and descriptions

class_code_clean is a nullable short integer. Supplied codes are retained.
Leading nulls inherit the first supplied class when it is consistent with the
recording event. Internal gaps receive a class only when their nearest original
classes on both sides match and both are consistent. Inferred values never
become new anchors. Different-class gaps, trailing gaps, entirely unlabelled
recordings and inconsistent boundaries remain unfilled.

class_was_inferred is a non-null boolean: true when an original null class was
filled, otherwise false, including unresolved missing classes. The label rules
use later labels under explicit continuity assumptions. Both class columns are
target information and must stay out of classifier predictors.

row_event_name and row_event_phase describe class_code_clean: 0 is normal, 1-9
is steady, 101-109 is transient, null is unknown and other codes are unrecognized.
class_label_status is unlabeled for a null clean class, consistent when the clean
class agrees with its recording event, and inconsistent otherwise. It can be
consistent while class_was_inferred is true. instance_event_name describes the
recording event. These descriptions also stay out of predictors.

The [two full builds](../evidence/silver/v3/README.md) report 4,028,400 inferred
class labels and zero unresolved clean classes. These are assumed targets,
not newly recovered expert annotations.

## Sensor values and repairs

Each <sensor>_clean is a nullable double, in the original units. Finite pressure,
temperature and flow readings have no invented physical bounds. Valve states
must be 0, 0.5 or 1. Missing, NaN, infinite and invalid valve-state readings give
null clean values. Only the gas-lift and production choke openings can receive
numerical replacements, for finite originals outside inclusive 0-100%.

The replacement is the most recent original valid opening in the same recording
and continuous one-second segment, at an age greater than zero and at most
360 seconds. Estimates never become anchors. Missing/non-finite originals are
not filled; source/time/row-position breaks reset eligibility. Without an anchor,
the clean value stays null. There is no clipping to 0 or 100%. For example,
40, 120, 120 can produce 40, 40, 40 in the clean field.

For each opening, <opening>_was_imputed identifies actual estimates and
<opening>_source_age_seconds records anchor age. Age is zero for an original
valid opening with usable time, and may exceed 360 on unresolved readings;
interpret it with the flag. Both [full builds](../evidence/silver/v3/README.md)
report 334 production-choke estimates and 161,538 unresolved invalid openings.
A longer age limit does not create a missing original-valid anchor.

## Sequence and temporal features

sequence_usable requires a usable timestamp later than the previous timestamp
(or a usable first timestamp). sequence_segment_id starts a new segment after
missing, repeated or backward times, a cadence other than one second, or a
break in original row positions. Segment IDs are scoped to one recording.

Seven sensors receive mean_120s, stddev_120s, change and count_120s:
downhole_pdg_pressure_pa, subsea_tree_tpt_pressure_pa,
production_choke_upstream_pressure_pa, gas_lift_choke_downstream_pressure_pa,
subsea_tree_tpt_temperature_c, production_choke_downstream_temperature_c and
gas_lift_flow_rate_m3_s. Feature prefixes are logical sensor identifiers;
readings themselves are in the matching _clean fields.

Trailing history is after current time minus 120 seconds through current time,
within the same recording/segment. Means use available clean values, sample
standard deviation needs two values, change needs both current and previous
clean values, and count is 0-120 at one-second cadence. Empty windows have count
zero and null mean/deviation. All features are null at unusable times. No sensor
feature reads future values. Counts distinguish partial from complete history.

Each <opening>_invalid_duration_seconds describes the source's consecutive finite
out-of-range streak: the first invalid row is zero, then one, two and so on.
Valid/missing/non-finite originals and segment boundaries reset the streak;
unusable time gives null. It is a sensor-quality indicator, not a well-event label.

## Validation and publications

Source-quality summaries and issues are recomputed from Bronze. The export is
checked against freshly prepared clean values, retained source context, exact
observation identities and recording counts. All seven tables must pass before
a ready marker is atomically published. A failure preserves the previous marker.
Preparation reports count repairs, inferred/unresolved classes and original
sensor validity without exporting duplicate raw columns.

Two full VM builds of this 79-column schema passed all 32 checks; their saved
comparison reports identical schemas and complete contents across seven tables.
The [evidence index](../evidence/silver/v3/README.md) links reports and markers.
Gold resolves one current ready marker, uses clean class targets, retains the
inference flag, and uses supplied labels for held-out evaluation.

## Sensor-name mapping

sensor_reference keeps logical base sensor identifiers for matching feature
prefixes; the exported sensor value is always in the corresponding _clean field.

| Original source sensor | Exported clean field |
|---|---|
| ABER-CKGL | gas_lift_choke_opening_pct_clean |
| ABER-CKP | production_choke_opening_pct_clean |
| ESTADO-DHSV | downhole_safety_valve_state_clean |
| ESTADO-M1 | production_master_valve_state_clean |
| ESTADO-M2 | annulus_master_valve_state_clean |
| ESTADO-PXO | pig_crossover_valve_state_clean |
| ESTADO-SDV-GL | gas_lift_shutdown_valve_state_clean |
| ESTADO-SDV-P | production_shutdown_valve_state_clean |
| ESTADO-W1 | production_wing_valve_state_clean |
| ESTADO-W2 | annulus_wing_valve_state_clean |
| ESTADO-XO | crossover_valve_state_clean |
| P-ANULAR | annulus_pressure_pa_clean |
| P-JUS-BS | service_pump_downstream_pressure_pa_clean |
| P-JUS-CKGL | gas_lift_choke_downstream_pressure_pa_clean |
| P-JUS-CKP | production_choke_downstream_pressure_pa_clean |
| P-MON-CKGL | gas_lift_choke_upstream_pressure_pa_clean |
| P-MON-CKP | production_choke_upstream_pressure_pa_clean |
| P-MON-SDV-P | production_shutdown_valve_upstream_pressure_pa_clean |
| P-PDG | downhole_pdg_pressure_pa_clean |
| PT-P | subsea_production_line_pressure_pa_clean |
| P-TPT | subsea_tree_tpt_pressure_pa_clean |
| QBS | service_pump_flow_rate_m3_s_clean |
| QGL | gas_lift_flow_rate_m3_s_clean |
| T-JUS-CKP | production_choke_downstream_temperature_c_clean |
| T-MON-CKP | production_choke_upstream_temperature_c_clean |
| T-PDG | downhole_pdg_temperature_c_clean |
| T-TPT | subsea_tree_tpt_temperature_c_clean |
