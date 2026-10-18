# Gold expectations and Silver reference

## Silver and Gold documents

| Document | What I use it for |
|---|---|
| [Gold handover](GOLD_HANDOVER.md) | What v1/v2 and `silver-fixes` mean, the current mounted input, and the planned Random Forest/XGBoost evaluation. Gold is planned work. |
| [Silver contract](../silver/SILVER_CONTRACT.md) | The seven output tables and Silver's 106 observation columns, including its 67 added fields. |
| [Silver assumptions](../silver/SILVER_ASSUMPTIONS.md) | Why Silver leaves labels unknown, how choke repairs/features work, and their limitations. |
| [Silver quality rules](../silver/QUALITY_RULES.md) | The quality checks and configured rules used by Silver. The executable settings are in [`silver_rules.yaml`](../../conf/quality/silver_rules.yaml). |
| [Silver run guide](../silver/SILVER_RUN_GUIDE.md) | Commands for preparing the dev sample, running Silver, and running it on the VM. |
| [Silver milestone report](../silver/SILVER_MILESTONE_REPORT.md) | How the Silver work developed, including both successful 106-column full VM runs and their content comparison. |
| [V2 evidence summary](../evidence/silver/v2/README.md) | Both run reports, ready-marker snapshots, measured timings and seven matching tables. |
| [V2 comparison log](../evidence/silver/v2/comparison.log) | Exact saved table comparison, including duplicate rows and nulls. |

The validated current input is `/mnt/silver-data/3w-data/silver-v2/runs/20261018T091117552005Z-3280e5d3/`. Gold should select one successful Silver ready marker and follow all of its table paths. Silver v2 is v1 extended with feedback and prepared columns; `silver-fixes` is the branch and `feat(silverv2)` is a commit scope. The [handover](GOLD_HANDOVER.md#what-silver-v2-and-silver-fixes-mean) explains this distinction for review.
