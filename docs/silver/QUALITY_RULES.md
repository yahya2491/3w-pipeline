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
| Short and long windows | 120 and 300 seconds | Set elapsed-time feature windows. |
| Minimum observed fraction | 0.5 | Requires at least 60 original valid readings in 120 seconds and 150 in 300 seconds for statistics that use those windows. |
| Selected continuous sensors | 7 names in YAML | Receive the configured causal features. All 27 sensors still receive validity and clean columns. |

