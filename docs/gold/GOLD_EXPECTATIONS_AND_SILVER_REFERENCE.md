# Gold and Silver reading guide

| Document | What it establishes |
|---|---|
| [Gold handover](GOLD_HANDOVER.md) | Current 79-column input, Gold command, target handling, recording splits and evaluation limits. |
| [Silver contract](../silver/SILVER_CONTRACT.md) | Seven tables, the 79 exported observation columns and field semantics. |
| [Silver assumptions](../silver/SILVER_ASSUMPTIONS.md) | Class inference, six-minute choke persistence and their limitations. |
| [Quality rules](../silver/QUALITY_RULES.md) | Source checks, clean values and retained issues. |
| [Run guide](../silver/SILVER_RUN_GUIDE.md) | Manual/scheduled builds and exact seven-table comparison. |
| [Full-run evidence](../evidence/silver/v3/README.md) | Two successful current-schema builds, reports, markers and matching output contents. |

Gold selects one successful current-contract ready marker. The validated manual
root is `/mnt/silver-data/3w-data/silver-v3/`; the scheduled default is separately
configured as `/mnt/silver-data/silver-schedule/`.
