# Gold expectations and Silver reference

## Silver and Gold documents

| Document | What I use it for |
|---|---|
| [Gold handover](GOLD_HANDOVER.md) | The proposed Gold inputs, labels, model, evaluation, and limitations. Gold is planned work. |
| [Silver contract](../silver/SILVER_CONTRACT.md) | The seven output tables and Silver's 106 observation columns, including its 67 added fields. |
| [Silver assumptions](../silver/SILVER_ASSUMPTIONS.md) | Why Silver leaves labels unknown, how choke repairs/features work, and their limitations. |
| [Silver quality rules](../silver/QUALITY_RULES.md) | The quality checks and configured rules used by Silver. The executable settings are in [`silver_rules.yaml`](../../conf/quality/silver_rules.yaml). |
| [Silver run guide](../silver/SILVER_RUN_GUIDE.md) | Commands for preparing the dev sample, running Silver, and running it on the VM. |
| [Silver milestone report](../silver/SILVER_MILESTONE_REPORT.md) | How the Silver work developed, including earlier milestones and the current preparation milestone. |
| [Choke repair evaluation](../evidence/silver/v2/dev_evidence/choke_repair_evaluation.json) | The masked-reading results for the bounded choke repair. |

I prepare the cleaned readings and 30 sensor-behaviour features in Silver. Gold can select labelled examples, choose model inputs and coverage rules, split whole recordings into training/evaluation sets, and use Spark MLlib to train and assess the classifier. The 120-second features are attached to individual observations; Gold still decides which rows or intervals become examples. Existing recording-level summaries help explain data quality and are not model predictors.

The earlier 193-column run and 76-column storage-fix run are historical evidence. Current 106-column validation is recorded in Milestone 15 of the milestone report. A full production-scale VM run remains required.
