# Gold layer handover

## Current Silver input

Silver has **79 observation columns** (internal contract version 4) and seven
Parquet tables. Both [full VM builds](../evidence/silver/v3/README.md) passed all
32 checks over 76,587,318 observations in 2,228 recordings. Their saved comparison
reports equal schemas and complete contents for all seven tables.

The current validated manual root is `/mnt/silver-data/3w-data/silver-v3/`.
Its second saved run is `20261025T010710680886Z-216d5afd`. The Gold builder's
scheduled default is `/mnt/silver-data/silver-schedule/`; use an explicit root
to consume the manual publication before that scheduled destination is populated.

```bash
cd /home/ubuntu/3w-pipeline
source .venv/bin/activate
mountpoint -q /mnt/silver-data &&
SPARK_LOCAL_DIRS=/mnt/silver-data/spark-tmp \
TMPDIR=/mnt/silver-data/spark-tmp \
python gold/build_gold.py --env prod \
  --silver-root /mnt/silver-data/3w-data/silver-v3
```

The builder writes a new run under `/mnt/silver-data/gold/runs/` and publishes
`/mnt/silver-data/gold/gold_ready.json` only after its checks pass; the
[Gold contract](GOLD_CONTRACT.md) defines its tables. It resolves `silver_ready.json` once, requires
contract version 4 and successful reconciliation, and checks that the selected
observation table belongs to that run. Read whole table directories, not single
part files. Do not mix tables from different runs or read the whole `runs/` tree.

## Tables and columns to use

| Table | Purpose in Gold |
|---|---|
| `silver_observations` | Clean readings, class targets, provenance, sequence context and trailing features. |
| `instance_quality` | Source-quality summaries per recording. |
| `sensor_availability` | Finite-reading coverage per sensor and recording. |
| `quality_issues` | Original-source warnings/errors and affected observation keys. |
| `quality_quarantine` | Error-level source issues; not whole-row exclusion instructions. |
| `event_reference` | Supported event and phase definitions. |
| `sensor_reference` | Source sensor names, logical names, units and domains. |

Use `<sensor>_clean` for sensor readings. Original sensor/class values are in
Bronze, recoverable with `_source_path`, `_file_sha256` and `_row_in_file`.
`instance_id` alone is not unique. `instance_label` describes the recording;
`class_code_clean` describes the observation. `state_code` is operational status.
The [contract](../silver/SILVER_CONTRACT.md) gives the complete 79-column breakdown.

The seven featured sensors have trailing means, sample standard deviations,
previous-reading changes and usable counts. A count of 120 indicates complete
one-second history; partial coverage and unusable sequence times remain visible.
Sensor features use current/past values within a sequence segment. Class inference
uses later labels under a separate target assumption; it is not a sensor feature.

## Target assumptions and leakage controls

All 4,028,400 missing classes in the recorded builds receive inferred targets,
identified by `class_was_inferred`. These are assumed labels, not newly recovered
expert annotations. Original labels and source missingness remain in Bronze and
the quality summaries. See [assumptions](../silver/SILVER_ASSUMPTIONS.md).

The current [Gold builder](../../gold/build_gold.py) selects consistent,
non-null clean targets and maps transient classes 101-109 to event identities
1-9. It retains `class_was_inferred`, permits inferred targets in training only,
and excludes them from held-out evaluation. Labels, inference flags,
`instance_label`, event descriptions and recording identifiers must not be model
predictors. The model configuration excludes this target/context information.

Whole recordings are assigned deterministically to train/test within recording
event/source groups. Windows from one recording never span both splits. Real
recordings are eligible; simulated and drawn examples are distinguishable by
`source_type`. Splitting by recording does not establish generalization to unseen
wells; a well-grouped evaluation would be a separate experiment.

The builder selects 21 mean/deviation/change predictors from the seven sensor
families, requires complete history for each retained sensor group, applies its
existing numerical feature guard and keeps rows with at least one usable group.
It samples endpoints every 60 source rows. Class balancing and sampling are left
to the model consumer. This is detection at a sampled endpoint,
not prediction before an event begins or a majority label over a whole window.
Fit any model preprocessing using training data only.

## Quality limitations

The full builds report 334 estimated production-choke openings and 161,538
unresolved invalid openings. Inspect repair flags and source ages when using
these channels. Source issues remain even after an allowed clean estimate;
do not automatically exclude every row with any issue. Match issues to the
observation key and affected field/use. Multiple issue entries can refer to one
observation, so an unrestricted issue join can multiply rows.

There are 5,193 source observations without any finite sensor reading. Four
logical sensors have no finite readings anywhere: service-pump downstream
pressure, production-shutdown-valve upstream pressure, subsea-production-line
pressure and service-pump flow. Other sensors have uneven coverage. Finite
presence alone does not prove domain validity; zero is not missing.

## Models and evidence

Random Forest and XGBoost implementations and saved results are under
[consumers/event_classifier](../../consumers/event_classifier/), a consumer of
Gold. The saved results were retrained at commit `9feb823` on Gold run
`20261026T194903Z-e2521f`, built from Silver run `20261025T150850992057Z-ceebdeae`.
Macro F1 is 0.925 (Random Forest) and 0.928 (XGBoost). Training windows are
capped at 200,000 per class; the test split is used in full.

Compare models on the same supplied-label test examples and report per-event
support, precision, recall, macro F1 and a confusion matrix. Include a baseline
and inspect source-type shortcuts. A class without held-out support has no
measured test performance. The Silver repeatability evidence validates the
prepared data under its rules; it does not prove model benefit.
