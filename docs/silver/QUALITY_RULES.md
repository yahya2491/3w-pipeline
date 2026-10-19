# Silver quality rules

Silver keeps original observations and records the problems below. Sensor corrections go into separate `_clean` columns. The [contract](SILVER_CONTRACT.md) defines those columns; the [assumptions](SILVER_ASSUMPTIONS.md) explain the choices.

| Finding | What Silver does |
|---|---|
| Missing class | Keeps it unknown and records a warning for labelled analysis. |
| Unsupported class or class/recording mismatch | Records a label error; does not infer a replacement. |
| Missing sensor value | Leaves its clean value null. Counts missingness in `sensor_availability` rather than creating an issue for every missing cell. |
| NaN or infinity | Leaves its clean value null and records a sensor error. |
| Valve state outside `0`, `0.5`, `1` | Leaves its clean value null and records a sensor error. |
| Finite choke opening outside inclusive 0–100% | Records the original error. Carries forward the last original valid opening for at most three seconds within the same continuous segment; otherwise leaves the clean value null. |
| No finite reading in any sensor | Records a sensor error and retains the observation for traceability. |
| Missing, repeated, or backward timestamp | Records a time error. Keeps the original timestamp and row position. |
| Positive interval shorter or longer than one second | Records a time warning. Starts a new segment for repairs and features. |
| Repeated observation key, incompatible input schema, or input/catalogue mismatch | Stops publication. Does not select an arbitrary row or silently change the schema. |

The choke repair uses a previous original valid reading, never another estimated value. If no eligible reading exists, the clean value is null; Silver does not clip to 0% or 100%. The 120-second sensor summaries do not fill or repair readings.

`quality_issues` contains warnings and errors; `quality_quarantine` contains the errors only. They refer to the original values, including openings that received a clean repair. An error affects the stated field and use, rather than automatically excluding the whole row. `has_quality_errors: true` in a successful report means source problems were found and recorded; it does not mean the pipeline failed.

## Configuration

[`silver_rules.yaml`](../../conf/quality/silver_rules.yaml) holds the domains, three-second repair limit, missing-value policies, expected cadence, and feature settings. The loader rejects unknown keys and unsupported policies. This contract fixes the seven feature sensors, 120-second history, and one-second cadence. Each report saves the resolved settings and their SHA-256 fingerprint.
