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

Silver does not set model-window lengths or calculate rolling statistics. Gold owns those choices.

