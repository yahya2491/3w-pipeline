# Silver quality rules

[`conf/quality/silver_rules.yaml`](../../conf/quality/silver_rules.yaml) is the executable configuration. This page lists its current settings and how to change them. Cleaning rationale is in [Silver assumptions](SILVER_ASSUMPTIONS.md); output fields are listed in the [contract](SILVER_CONTRACT.md).

| Setting | Current value | Applied by Silver |
|---|---|---|
| Choke-opening domain | Inclusive 0–100 percent | Defines validity for the two choke-opening sensors. |
| Valve-state domain | `0`, `0.5`, `1` | Defines validity for the valve-state sensors. |
| Maximum opening repair age | 3 seconds | Limits carry-forward from an original valid opening in the same uninterrupted sequence. |
| Missing classes | `preserve_unknown` | Does not infer or fill a row class. |
| Missing sensors | `preserve_missing` | Keeps missing original readings missing. |
| Other invalid prepared sensor values | `null_in_clean_column` | Preserves the source value and leaves its clean value null. |
| Expected interval | 1 second | Defines continuity for sequence segments. |
| Feature history | 120 seconds | Uses current and earlier clean readings within one sequence segment. |
| Feature sensors | Seven named pressure, temperature, and gas-lift-flow sensors | Adds mean, sample standard deviation, previous-reading change, and usable count for each. Exact names are in the YAML and contract. |
| Invalid-opening duration | Both original choke openings | Measures elapsed consecutive finite-invalid time; resets on valid, missing/non-finite readings or a sequence break. |

The current contract supports the documented seven sensors, 120-second history, and one-second cadence. The rules loader rejects unsupported settings. The resolved rules and fingerprint are saved with each run. Gold chooses its labelled examples, predictor columns, model, and evaluation; Silver does not change labels or fit model parameters.

