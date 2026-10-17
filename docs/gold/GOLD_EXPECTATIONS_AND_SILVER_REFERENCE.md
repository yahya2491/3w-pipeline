# Gold expectations and Silver reference

## Silver and Gold documents

| Document | What I use it for |
|---|---|
| [Gold handover](GOLD_HANDOVER.md) | The proposed Gold inputs, labels, model, evaluation, and limitations. Gold is planned work. |
| [Silver contract](../silver/SILVER_CONTRACT.md) | The seven output tables and the meaning of Silver's 76 observation columns, including its 37 added fields. |
| [Silver assumptions](../silver/SILVER_ASSUMPTIONS.md) | Why Silver leaves labels unknown, how choke repairs work, and what Gold must calculate. |
| [Silver quality rules](../silver/QUALITY_RULES.md) | The quality checks and configured rules used by Silver. The executable settings are in [`silver_rules.yaml`](../../conf/quality/silver_rules.yaml). |
| [Silver run guide](../silver/SILVER_RUN_GUIDE.md) | Commands for preparing the dev sample, running Silver, and running it on the VM. |
| [Silver milestone report](../silver/SILVER_MILESTONE_REPORT.md) | How the Silver work developed, including earlier milestones and the current preparation milestone. |
| [Choke repair evaluation](../evidence/silver/v2/dev_evidence/choke_repair_evaluation.json) | The masked-reading results for the bounded choke repair. |

The previous v2 development run processed 30 recordings and 1,931,052 observations, produced a 193-column `silver_observations` table, and passed 30 saved-output checks in about 17 minutes. It predates the simplification described in the current Silver contract. Rerun the dev sample before treating it as evidence for the current 76-column schema; a full production-scale run on the VM is still required.
