# Gold run and consumer guide

The [Gold contract](GOLD_CONTRACT.md) defines the tables, window selection,
train/test split, SQL metrics, publication checks and known limits.
The [Silver contract](../silver/SILVER_CONTRACT.md) defines the 79-column input;
[quality rules](../silver/QUALITY_RULES.md) and
[assumptions](../silver/SILVER_ASSUMPTIONS.md) explain inferred values.

## Build and query

For setup, use the repository [README](../../README.md). Development reads
`data/dev/silver` and writes `data/dev/gold`:

```bash
python gold/build_gold.py --env dev
python gold/query.py --env dev
```

The scheduled Silver root is `/mnt/silver-data/silver-schedule/`. The saved
[Gold production build](../evidence/gold/prod/gold_ready.json) consumed that
publication. The separately validated manual root is
`/mnt/silver-data/3w-data/silver-v3/`; its [two full builds](../evidence/silver/v3/README.md)
passed all 32 checks. Select the root whose live marker you intend to consume.

```bash
cd /home/ubuntu/3w-pipeline
source .venv/bin/activate
mountpoint -q /mnt/silver-data &&
SPARK_LOCAL_DIRS=/mnt/silver-data/spark-tmp \
TMPDIR=/mnt/silver-data/spark-tmp \
python gold/build_gold.py --env prod
python gold/query.py --env prod
```

For the manual publication, add
`--silver-root /mnt/silver-data/3w-data/silver-v3` to the build command.
`--gold-root <folder>` overrides the build destination or query input;
`--csv <folder>` on the query command exports both summary views.
Gold is built by hand, separately from the Bronze/Silver workflows.

## Select one publication

The builder resolves `silver_ready.json` once, requires contract version 4 and
successful reconciliation, and checks that observations belong to that run.
Gold writes under `<gold root>/runs/<run_id>/` and replaces `gold_ready.json`
only after all four checks pass. Read whole table directories selected by the
ready marker, rather than individual part files or the entire `runs/` tree.
Copied evidence markers retain their original absolute paths; they are not live
publications. Do not mix tables from different runs.

## Use the data

Original sensor/class values remain in Bronze, recoverable with source path,
file hash and row position. `instance_id` alone is not unique. Silver quality
issues describe the original source even after cleaning; match their affected
field/use rather than dropping every observation with an issue. Multiple issues
can refer to one observation, so unrestricted joins can multiply rows.

The full Silver builds report 334 repaired production-choke readings, 161,538
unresolved invalid openings and 5,193 observations without any finite sensor
reading. Four channels have no finite readings: service-pump downstream pressure,
production-shutdown-valve upstream pressure, subsea-production-line pressure and
service-pump flow. Finite presence alone does not establish domain validity;
zero is not missing.

The [classifier demo](../../consumers/event_classifier/README.md) reads Gold's
`window_features`. Labels, inference flags, event descriptions and recording
identifiers are not predictors. Fit preprocessing on training data only. The
recording split does not establish performance on unseen wells, and a class
without held-out support has no measured test performance. Windows classify the
condition at their endpoint, rather than predicting an event before it begins.

The saved model results were retrained at commit `9feb823` on Gold run
`20261026T194903Z-e2521f`, from Silver run `20261025T150850992057Z-ceebdeae`.
Macro F1 is 0.925 (Random Forest) and 0.928 (XGBoost). The consumer caps training
at 200,000 windows per class and uses the test split in full. Compare models on
the same supplied-label test examples, with per-event support, precision, recall,
macro F1, a confusion matrix and baseline/source-shortcut checks. Silver validation
establishes preparation under its rules, not model benefit.
