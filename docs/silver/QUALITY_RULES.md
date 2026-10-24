# Silver quality rules

Silver keeps original observations and records the problems below. Sensor corrections go into separate `_clean` columns. The [contract](SILVER_CONTRACT.md) defines those columns; the [assumptions](SILVER_ASSUMPTIONS.md) explain the choices.

| Finding | What Silver does |
|---|---|
| Missing class | Preserves the original unknown and warning. Fills leading nulls in `class_code_clean` from the first consistent supplied boundary. Also fills internal gaps between equal consistent original classes. Different-class gaps, trailing gaps, inconsistent boundaries and entirely unlabelled recordings stay unknown. |
| Unsupported class or class/recording mismatch | Records a label error; does not infer a replacement. |
| Missing sensor value | Leaves its clean value null. Counts missingness in `sensor_availability` rather than creating an issue for every missing cell. |
| NaN or infinity | Leaves its clean value null and records a sensor error. |
| Valve state outside `0`, `0.5`, `1` | Leaves its clean value null and records a sensor error. |
| Finite choke opening outside inclusive 0–100% | Records the original error. Carries forward the last original valid opening for at most 360 seconds (six minutes) within the same continuous segment; otherwise leaves the clean value null. |
| No finite reading in any sensor | Records a sensor error and retains the observation for traceability. |
| Missing, repeated, or backward timestamp | Records a time error. Keeps the original timestamp and row position. |
| Positive interval shorter or longer than one second | Records a time warning. Starts a new segment for repairs and features. |
| Repeated observation key, incompatible input schema, or input/catalogue mismatch | Stops publication. Does not select an arbitrary row or silently change the schema. |

The class-fill policy assumes the initial period matches its first supplied
consistent class, and that equal original classes surrounding a gap stayed
unchanged inside it. These are explicit assumptions,
not recovery of an expert annotation. `class_code_inferred_rows` and
`class_code_unresolved_rows` in each preparation summary describe clean-label
coverage. Raw missing-class warnings still describe the original source.

The choke repair uses a previous original valid reading, never another estimated value. If no eligible reading exists, the clean value is null; Silver does not clip to 0% or 100%. The 120-second sensor summaries do not fill or repair readings.

The [full-source repair review](../evidence/silver/choke-repair-review/README.md)
found that 161,538 of the 161,872 out-of-range production-choke readings are
`-99.99` throughout one entire recording, with no valid opening anywhere in it.
Only the other 334 readings have an earlier original valid opening. The
historical three-second rule estimated three of those. The current 360-second
default estimates all 334, as verified with the Spark transformation on both
affected recordings in the [extension review](../evidence/silver/repair-window-extension/README.md).
The oldest source reading used there is 334 seconds old. A longer limit adds no
further coverage in this source. These remain estimates; neither eligibility
nor masking known-valid data establishes the true faulty readings.

`quality_issues` contains warnings and errors; `quality_quarantine` contains the errors only. They refer to the original values, including openings that received a clean repair. An error affects the stated field and use, rather than automatically excluding the whole row. `has_quality_errors: true` in a successful report means source problems were found and recorded; it does not mean the pipeline failed.

## Configuration

[`silver_rules.yaml`](../../conf/quality/silver_rules.yaml) holds the domains, 360-second repair limit, missing-value policies, expected cadence, and feature settings. The loader rejects unknown keys and unsupported policies. This contract fixes the seven feature sensors, 120-second history, and one-second cadence. Each report saves the resolved settings and their SHA-256 fingerprint.
