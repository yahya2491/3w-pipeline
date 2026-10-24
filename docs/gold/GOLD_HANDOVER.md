# Gold layer handover

This explains the saved Silver inputs and the assumptions behind future Silver builds. Gold now has a [table builder](../../gold/build_gold_table.py), model training scripts and [comparison results](../../gold/model/results/comparison_summary.csv). The design guidance below includes the original handover proposals. For field definitions, use the [contract](../silver/SILVER_CONTRACT.md); for validation, use the [milestone report](../silver/SILVER_MILESTONE_REPORT.md).

## Where we are

New Silver builds have **108 columns**, adding `class_code_clean` and its
boolean `class_was_inferred` flag under the
[leading-class assumption](../evidence/silver/class-label-backfill/README.md).
Use the clean class for assumed-label training; identify inferred rows using
`class_was_inferred`. Keep this target-provenance flag out of predictor inputs. Report source-label evaluation separately from
assumed-label evaluation. The saved full VM baseline described below has
**106 columns** and needs rebuilding to include the new target field.

The historical 106-column Silver baseline is available for Gold. It provides clean readings for all 27 sensors, bounded choke-opening estimates, sequence and quality details, 120-second features for seven sensors, and two invalid-opening durations. Original measurements, labels and row identities remain available. Read [Silver assumptions](../silver/SILVER_ASSUMPTIONS.md) for the limits of cleaning and feature selection.

The saved full VM runs used a three-second repair age. The default for new
runs is now 360 seconds; see the [extension review](../evidence/silver/repair-window-extension/README.md).
A ready marker and its resolved rules identify which policy an output uses.

Both [full VM runs](../evidence/silver/v2/README.md) passed all 32 checks and published all seven tables. Each covers 2,228 recordings and 76,587,318 observations across 82 batches, saving 10.53 GB of Parquet. The [v2 comparison log](../evidence/silver/v2/comparison.log) confirms matching schemas and complete contents for every table, including duplicates and nulls. Runtimes were 6 h 10 min 25 s and 6 h 5 min 32 s; this is not a controlled storage-performance benchmark.

The current saved input is the second run on the attached volume: `/mnt/silver-data/3w-data/silver-v2/runs/20261018T091117552005Z-3280e5d3/`. The original baseline remains under `/home/ubuntu/3w-data/silver-v2/runs/20261017T232322182032Z-ba450086/`. Resolve one successful ready marker and keep that run's paths together, as explained below.

## What Silver v2 and silver-fixes mean

Silver v2 is the existing Silver v1 implementation extended with review feedback and additional prepared columns. It is the next revision of the same Silver layer, with the same observation grain and seven table roles. There is no separate `silver-v2` Python package: both revisions use `threew.silver` and `python -m threew.silver.run`.

| Name seen in the project | Meaning |
|---|---|
| Silver v1 | The original preservation, readable-metadata and quality implementation. It had 43 observation columns and its own [two-run evidence](../evidence/silver/v1/). Its VM output is `silver-oh/`. |
| `silver-fixes` | The Git branch containing the feedback changes and current handover work. This is the review branch, not a data directory. |
| `feat(silverv2)` / `feat(silver(v2))` | Commit-message scopes used while implementing and adjusting that feedback on `silver-fixes`. They describe the Silver revision; they do not imply a differently named branch or package. |
| Silver contract v2 | The revised output contract. The final observation table has 106 columns; earlier intermediate v2 builds had different column counts. |
| `silver-v2/` | The explicit VM output folder used to retain revised Silver runs separately from v1. It now sits under `/mnt/silver-data/3w-data/`. |

The upstream dataset version `3W v2.0.0` is a separate version name; it does not identify the Silver contract.

The Git history makes the feedback progression visible:

| Commit | What it records |
|---|---|
| `2569412`, `c14e733` | `feat(silverv2)` commits implementing supervisor feedback: clean sensor values, repair provenance and derived behaviour features. The initial observation extension reached 193 columns. |
| `ddccb80`, `d3f70f1` | Reduce derived columns after VM storage failures, first to 135 and then 76. |
| `b3ccd25` | Bound temporary work by complete-recording batches and remove the full prepared-table cache. |
| `98868cc` | Restore a focused set of 120-second features, giving the final 106-column schema; first full run revision. |
| `fd4579f`, `330496f` | Code cleanup, validation refactoring and clearer documentation; second full run revision is `330496f`. |
| `55175a8` | Add saved run reports and evidence to the branch; the seven-table comparison is now saved as well. |

V1 already carried 36 Bronze fields, three catalogue fields and four event descriptions. V2 retains them and adds 63 fields. The historical 106-column breakdown is:

| Column group | Count | Use in Gold |
|---|---:|---|
| Original Bronze fields | 36 | 27 original sensor readings plus nine time, label, state and lineage fields; use originals for inspection and lineage for grouping. |
| Recording catalogue fields | 3 | `instance_id`, `well_id`, `instance_start`; context and split bookkeeping. |
| Event descriptions | 4 | Readable supplied labels and consistency status; exclude from predictors. |
| Clean sensor readings | 27 | One `<sensor>_clean` value per sensor. |
| Choke repair details | 4 | Two `_was_imputed` flags and two `_source_age_seconds` fields. |
| Sequence details | 2 | `sequence_usable`, `sequence_segment_id`; keep histories within recording segments. |
| Recent sensor behaviour | 28 | Mean, sample standard deviation, previous-reading change and usable count for each of seven sensors. |
| Invalid-opening durations | 2 | Duration of consecutive finite invalid original openings; candidate quality indicators. |
| **Total** | **106** | Gold selects a suitable subset for its model examples. |

Relative to Bronze, this is 70 additional columns: three catalogue fields and 67 Silver fields. Relative to the 43-column v1 observation table, it is 63 additional columns. Definitions, types and the seven featured sensors are in the [contract](../silver/SILVER_CONTRACT.md). Historical v1 and reduced-column v2 runs remain evidence for those schemas; Gold should use the validated final 106-column run.

## The seven folders and how Gold can use them

Each folder holds a Parquet table. **All event categories 0–9 remain together in `silver_observations`; the seven folders are different kinds of output, not different events.**

| Folder/table | What it contains | Possible use in Gold |
|---|---|---|
| `silver_observations` | Original observations, 27 clean sensors, 30 sensor-behaviour features, repair/sequence details, metadata and source keys. | Select labelled model examples from prepared features; retain ordered clean sequences for CNNs. |
| `instance_quality` | One summary per recording: observation count, missing labels/state, time problems, and sensor coverage. | Check recording quality and length; summarise limitations by source type or event. |
| `sensor_availability` | One row per recording and sensor, showing finite-reading counts and percentages. | Choose sensors and recordings with useful coverage. Finite presence does not guarantee that every value passes a domain rule. |
| `quality_issues` | All detected warnings and errors, with row keys, affected fields/uses, observed values, reasons, and suggested actions. | Match problems to observations, decide which fields are suitable for the calculation, and explain exclusions. |
| `quality_quarantine` | The error-level entries from `quality_issues`. | Identify sensor measurements, labels, or times that need to be excluded or reviewed for the stated use. It contains issue entries, not full observation copies. |
| `event_reference` | The 10 event definitions and supported normal, steady, and transient classes. | Use consistent event names and phase definitions in results. |
| `sensor_reference` | The 27 sensor definitions, original/Silver names, units, and documented value rules. | Give results the correct sensor names and units, using the same definitions as Silver. |

Gold can select the tables its question needs. It does not need to download data from Hugging Face or join Bronze's recording catalogue again.

The observations also identify the source as `REAL`, `SIMULATED`, or `DRAWN` in `source_type`. Real well IDs are available where supplied; simulated and drawn recordings have no real well ID. Use these fields to keep different sources separate where appropriate.

## How to use the saved quality tables with observations

**Use the prepared columns explicitly.** `<sensor>_clean` coexists with the original columns. Opening replacements carry `_was_imputed` and `_source_age_seconds`. Raw quality issues remain even after a repair, so do not automatically reject a permitted clean replacement just because its raw measurement is quarantined. Conversely, a repair does not make the original measurement valid. Review sequence timing, sensor availability and target eligibility for the chosen task.

Gold can read the existing `quality_issues` or `quality_quarantine` Parquet table alongside `silver_observations`. It does not need to rerun Silver or rediscover the same problems.

1. Decide which measurements, labels, and times the result needs.
2. Read the relevant issue entries. `quality_issues` includes warnings such as missing classes; `quality_quarantine` contains errors.
3. Match each entry to its observation using `_source_path`, `_file_sha256`, and `_row_in_file` together.
4. Use `field_name`, `affected_use`, and `recommended_action` to decide whether that particular measurement or label belongs in the result.
5. Keep other readings when their issues are unrelated to the question. Record how many inputs were included and excluded.

For example, an observation may have a missing class and an original opening of −99.99%, while another sensor has a suitable finite reading. Gold can use that other sensor without a class label. The opening's clean value may be a permitted bounded estimate or null; inspect its repair flag and age. The unknown class cannot become confirmed evaluation ground truth. Original columns remain unchanged.

The issue tables describe problems; the full measurements still come from `silver_observations`. Their `observed_value` field is text for inspection, not a replacement numeric sensor table.

One observation can have several issue entries. Matching every issue directly to observations and then counting or averaging can repeat an observation's contribution. Identify each affected observation/field once when selecting inputs. Similarly, `sensor_availability` has 27 entries per recording; select the needed sensor before matching coverage to observation rows.

## What the quality review found

| Finding | Result | Implication for Gold |
|---|---|---|
| Missing row class | 4,028,400 observations, about 5.26%. | Readings remain available for work that does not require a row class. Do not infer the missing class from the whole-recording event label. |
| No finite sensor readings | 5,193 observations. | They cannot supply sensor measurements; their source rows and labels remain preserved. |
| Production-choke opening outside 0–100% | 161,872 original readings: 161,538 below zero and 334 above 100. Flagged range: −99.99 to 100.4375. | Retain raw issue counts; Silver clean columns provide a bounded estimate or null with repair provenance. |
| Time and label consistency | No missing/duplicate timestamps, backward intervals, gaps over one second, or class/recording mismatches were detected by the implemented checks. | Use the reported results, while preserving original row order and source time meaning. |

Repeated −99.99 values appeared in the historical opening examples, but their source meaning is unconfirmed. Silver applies the same out-of-range rule to every finite invalid opening, without interpreting a particular number as a confirmed missing-value marker. The clean value is a bounded estimate or null; originals remain unchanged.

The subsequent [full-source repair audit](../evidence/silver/choke-repair-review/README.md)
locates all 161,538 negative readings in
`8/WELL-00030_20141108012623.parquet`: every row reports `-99.99`, and no valid
production-choke opening exists anywhere in that recording. The other 334
out-of-range readings occur at the end of one different recording and have an
earlier valid opening. Even unlimited within-recording carry-forward can reach
only those 334 readings (about 0.21% of the invalid openings). The default
was extended to 360 seconds on 24 October 2026, and the
[extension review](../evidence/silver/repair-window-extension/README.md) verifies
334 estimates with the actual Spark transformation. Existing full VM exports
still use their saved three-second policy; regenerate Silver to obtain the
extended repairs. The age cutoff is not the main cause of the low coverage. Treat this
sensor as unavailable in the entirely invalid recording; other measured fields
remain available under their own quality rules. Any reconstruction using other
sensors needs separate model evaluation.

## Planned Gold classification model

Compare Spark MLlib `RandomForestClassifier` and XGBoost using the same labelled examples, Silver features and held-out recording split. Evaluate all event categories with labelled examples and report each event's test support and scores. Save the selected features, true label, each model's prediction and evaluation results as Gold outputs. Include a simple majority-class baseline and a clean-sensor-only feature comparison to test whether the added temporal features help.

One option is one Gold example at each chosen 120-second interval endpoint, using that observation's trailing features and supported `class_code` as its target. This is a labelled history ending at a particular observation, not a claim that every row in the history has the same class. Agree the sampling frequency, required history and coverage before training. Class 0 means normal; nonzero supported codes describe event and phase. Map transient codes 101–109 to event identities 1–9 for event-type scoring, while retaining the original phase code for separate analysis. Exclude unknown or inconsistent targets from supervised evaluation. Keep labels, `instance_label` and event-derived metadata out of predictor columns.

Read `<sensor>_mean_120s`, `_stddev_120s`, `_change` and `_count_120s` from Silver. Partial history and missing sensors remain visible through the counts; a count of 120 means that sensor has a full history. Features are null at unusable times and restart at segment boundaries. The choke-duration fields are candidate quality indicators, not confirmed well-event labels. Gold must decide whether they help its task. Fit any missing-feature treatment on training data only.

Use `instance_quality` and `sensor_availability` to assess recordings, and `quality_issues`/`quality_quarantine` to inspect affected fields and rows. Explain inclusion rules and report kept/excluded examples. Keep recording-level summaries and event metadata out of predictors. Gold may calculate additional statistics from clean readings when its design needs them.

The removed derived columns do not remove source measurements. Gold can calculate the earlier 300-second means, short-versus-long trends, deviations from prior readings, and production-choke pressure differences from the saved clean readings. Preserve recording and sequence boundaries and use current/past values when deriving temporal features. Validity indicators can be derived from original values and the saved rules; usable indicators can be derived from clean values. The existing 120-second counts also support coverage calculations. Some earlier fields overlap with current features: during an uninterrupted one-second sequence, the per-second rate equals the existing previous-reading change. Additional features remain candidates whose model benefit needs evaluation; the full VM validation establishes the saved data's conformity to Silver's implemented rules.

Split whole recordings into training and held-out test sets before fitting preprocessing or models; never put windows from one recording in both sets. Withhold test labels from all predictors and training, then compare predictions to the saved true labels. Report a confusion matrix, per-event precision and recall, macro F1 and test support so rare events are visible. Real recordings provide labelled examples too; see [Labels in real recordings](#labels-in-real-recordings). An event with no labelled examples in the held-out test split has no measured test score. This evaluates detection at the window endpoint, not prediction before an event begins.

### Labels in real recordings

Real recordings can be used for training and testing. Each of the 1,119 real recordings has some rows without a class, but none is entirely unlabelled: 28,843,850 of the 32,872,250 real rows (87.75%) have a class. All simulated and drawn rows have a class.

The [full-source class-label review](../evidence/silver/class-label-review/README.md)
locates every missing class in the first 3,600 rows of each real recording.
New Silver builds expose assumed targets through `class_code_clean` and mark
them with `class_was_inferred`; original classes and the historical counts
below are preserved. The current Gold builder still selects original
`class_code` from the saved 106-column input. Using inferred targets requires
an explicit future change to Gold input selection, with source-label and
assumed-label evaluation reported separately.

Leaving real data out would remove these labelled rows:

| Row class | Real | Simulated | Drawn |
|---|---:|---:|---:|
| 0 (normal) | 14,570,142 | 2,381,896 | 353,231 |
| 4 (no 104 rows exist) | 2,454,883 | 0 | 0 |
| 7 and 107 | 6,700,921 | 0 | 2,151,707 |

Event 4 appears only in real data. Class 0 appears in every source, but real recordings hold about 84% of the normal rows, and the only ones from real wells. Without real data, event 7 would come from the 10 drawn recordings alone. Counts are from `silver_observations` and `instance_quality` in run `20261018T091117552005Z-3280e5d3`.

Proposed: rows with no class stay in Silver but are excluded from Gold training and evaluation, as are inconsistent classes. Keep each recording in either the training or the test split, never both; consider grouping real recordings by well. To be confirmed in the Gold design.

Four sensor columns have no finite readings anywhere in these full runs: `service_pump_downstream_pressure_pa`, `production_shutdown_valve_upstream_pressure_pa`, `subsea_production_line_pressure_pa`, and `service_pump_flow_rate_m3_s`. Other sensors have different coverage across recordings. Check availability before choosing inputs.

Finite means not missing, NaN, or infinity. A finite value can still be flagged: an opening above 100 is counted as present in `sensor_availability` but fails its value rule. Zero is a real reading and should not be treated as missing.

## Choose inputs according to the question

| Gold question | Input requirements | What can still be kept |
|---|---|---|
| Average a sensor within each recording | Finite readings without errors affecting that sensor. | Rows with missing classes or problems in unrelated sensors. |
| Compare readings by event phase | Eligible readings and a row class consistent with its recording. | Other suitable measurements can support separate calculations even when one sensor is excluded. |
| Calculate time-based rates or windows | Suitable sensor readings and review of timestamp issues and actual spacing. | Original row positions and gaps; do not invent corrected times. |
| Report quality and coverage | Recording/sensor summaries and distinct affected observation counts. | Missing and flagged inputs must remain visible in this quality result. |

`instance_label` describes the whole recording's event. `class_code` and `row_event_phase` describe the condition at an observation. A normal period inside an Event 1 recording does not turn it into an Event 0 recording.

Source timestamps have no stated time zone. Simulated and drawn times are not real calendar dates. Parquet does not guarantee row order; `_row_in_file` identifies the original sequence within a recording.

If a recording has no suitable readings for a result, show that absence clearly. Do not invent a zero mean or fill every missing/bad reading with zero.

## Possible first Gold outputs

These are recommendations, not a fixed design or implemented Gold tables. Start with the questions you want to answer.

| Possible output | What one row means | Purpose |
|---|---|---|
| Recording/sensor statistics | One recording and sensor. | Show included-reading count, coverage, mean, minimum, maximum, and unit after applying sensor-specific quality guidance. |
| Event quality overview | One source type and recording event. | Show recording/observation totals, missing-label rates, sensor coverage, and the share of observations with errors. |
| Event/phase comparison | One source type, recording event, phase, and sensor. | Compare normal, transient, and steady readings where the event supports those phases. |

For a result across recordings, decide whether each observation or each recording has equal weight. Longer recordings contribute more to an average over all observations. Report that choice.

Keep `REAL`, `SIMULATED`, and `DRAWN` separate by default unless combining them answers a deliberate question. If preparing machine-learning data, keep observations from one recording together when choosing training/test groups; consider grouping real recordings by well.

## Where the input is and how to work

| Item | Location |
|---|---|
| Local dev Silver root | `data/dev/silver/` |
| Historical full-dataset v1 Silver root | `/home/ubuntu/3w-data/silver-oh/` |
| Original validated v2 baseline root | `/home/ubuntu/3w-data/silver-v2/` |
| Current mounted v2 output root | `/mnt/silver-data/3w-data/silver-v2/` |
| Second validated v2 run | `/mnt/silver-data/3w-data/silver-v2/runs/20261018T091117552005Z-3280e5d3/` |
| Copied pipeline data, including Bronze | `/mnt/silver-data/3w-data/` |
| Spark temporary data on the volume | `/mnt/silver-data/spark-tmp/` |
| Saved repository evidence | `docs/evidence/silver/v2/` |
| Ready marker | `silver_ready.json` under the selected root |
| Actual tables | `runs/<run_id>/<table_name>/` under that root |
| Individual run reports | `_runs/<run_id>.json` under that root |

After connecting to `group20` as `ubuntu`, access the mount as a normal directory inside the VM:

```bash
mountpoint /mnt/silver-data
df -h /mnt/silver-data
cd /mnt/silver-data/3w-data/silver-v2
ls
ls runs/20261018T091117552005Z-3280e5d3
```

The final command lists the seven table folders. For example, the current observations are at `/mnt/silver-data/3w-data/silver-v2/runs/20261018T091117552005Z-3280e5d3/silver_observations/`. These are Parquet datasets; read the entire selected table folder with Spark, rather than choosing one part file.

Read `/mnt/silver-data/3w-data/silver-v2/silver_ready.json` once at the beginning of Gold. The [saved second-run marker](../evidence/silver/v2/prod_run2/silver_ready.json) selects `20261018T091117552005Z-3280e5d3`, has contract version 2, and records successful reconciliation. Its linked [report](../evidence/silver/v2/prod_run2/silver_run_2_metrics.json) records `SUCCESS` and `published: true`. Use its `tables[<name>]["path"]` values throughout the Gold run. If the live marker later selects a newer run, validate that selection before using it. Do not read the whole `runs` directory or combine tables from different Silver runs. The configured default Silver path is `silver/`; it does not automatically change to this explicit volume output root.

The attached 100 GB `/dev/vdb` volume is mounted at `/mnt/silver-data`; the original approximately 40 GB `/dev/vda` contains `/home/ubuntu/`. The volume's name does not restrict it to Silver data. Existing pipeline files were copied under `/mnt/silver-data/3w-data/`, and the second build wrote new output there. Copying does not rewrite absolute paths inside old reports or markers, so the copied first-run marker can still point to `/home/ubuntu/3w-data/`. Use the saved second run or its verified current marker when selecting volume input.

SSH logout leaves the mount and its files available. Data persists on the volume; automatic mounting after a VM reboot depends on `/etc/fstab`. The supplied evidence has not confirmed that configuration or a reboot test. Check `mountpoint` before writes after a restart; see [VM storage](../silver/VM_STORAGE.md) for the UUID and layout.

`conf/prod.yaml` still reads Bronze from `/home/ubuntu/3w-data/`; `--output-root` changes only the Silver destination. Gold should use the selected Silver ready marker rather than deriving its input root from this Bronze configuration. See [VM storage](../silver/VM_STORAGE.md) for the volume layout, persistence and evidence locations.

Earlier v2 builds used different column sets. Check that the required feature columns are
present in the 106-column table, and that the saved rules describe the 120-second history;
the version number alone is not sufficient.

Develop on the 30-file dev sample, which contains 1,931,052 observations. Use the repo's locked Python 3.11/PySpark 3.5.0 environment and Java 8 or 17. Use your own VM checkout because the VM account is shared. Setup, sample preparation, and checking commands are in the [Silver run guide](../silver/SILVER_RUN_GUIDE.md).

Use small offline test inputs with known expected results. Check that quality matching does not duplicate contributions, excluded values do not enter calculations, and missing results are represented honestly. Then test the agreed Gold design on the VM in its own output destination.

Record the selected Silver run ID, source, reference version, chosen inputs, inclusion rules, and output counts. Check repeated results by their contents, not just equal counts. Work on a short-lived branch and use a PR with the repo's lint/test checks.

Gold still needs its own runner, storage configuration, and agreed output definitions. The [Silver contract](../silver/SILVER_CONTRACT.md) defines the saved fields; the [milestone report](../silver/SILVER_MILESTONE_REPORT.md) records validation.
