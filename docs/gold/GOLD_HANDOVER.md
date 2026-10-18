# Gold layer handover

This explains what Silver gives Gold and recommends a concrete Gold classification baseline that matches the [project description](../3W_Project_Description_full.md). Gold has not been implemented yet. For definitions and implementation details, use the [contract](../silver/SILVER_CONTRACT.md) and [milestone report](../silver/SILVER_MILESTONE_REPORT.md).

## Where we are

Silver contract v2 has 106 observation columns. Alongside clean readings, bounded choke repair and quality details, it stores 120-second mean, sample standard deviation, change and usable-count features for seven sensors, plus two invalid-choke durations. Gold chooses labelled examples and model inputs, then trains and evaluates its classifier. Read [Silver assumptions](../silver/SILVER_ASSUMPTIONS.md) for the limitations. The [106-column full VM run](../evidence/silver/v2/prod_run1/silver_run_1_,metrics.json) passed all 32 checks and published all seven tables.

The [comparison log](../evidence/silver/comparison.log) and [milestone report](../silver/SILVER_MILESTONE_REPORT.md) record earlier evidence. The historical quality inspection found missing labels, observations without finite sensor readings, and out-of-range openings. Originals remain available.

Gold has not been implemented. The validated 106-column baseline is saved under `/home/ubuntu/3w-data/silver-v2/runs/20261017T232322182032Z-ba450086/`. Existing pipeline data has also been copied to the attached volume under `/mnt/silver-data/3w-data/`. New Silver builds use `/mnt/silver-data/3w-data/silver-v2/` as their explicit output root. The historical full-dataset Silver v1 output uses an earlier schema.

The earlier 193-column and 76-column runs used different schemas. The current [full-dataset v2 validation](../silver/SILVER_MILESTONE_REPORT.md#full-dataset-v2-validation) covers 2,228 recordings and 76,587,318 observations across 82 batches, saving 10.53 GB of Parquet in 6 hours 10 minutes.

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

## Planned Gold classification model

Compare Spark MLlib `RandomForestClassifier` and XGBoost using the same labelled examples, Silver features and held-out recording split. Evaluate all event categories with labelled examples and report each event's test support and scores. Save the selected features, true label, each model's prediction and evaluation results as Gold outputs. Include a simple majority-class baseline and a clean-sensor-only feature comparison to test whether the added temporal features help.

One option is one Gold example at each chosen 120-second interval endpoint, using that observation's trailing features and supported `class_code` as its target. This is a labelled history ending at a particular observation, not a claim that every row in the history has the same class. Agree the sampling frequency, required history and coverage before training. Class 0 means normal; nonzero supported codes describe event and phase. Map transient codes 101–109 to event identities 1–9 for event-type scoring, while retaining the original phase code for separate analysis. Exclude unknown or inconsistent targets from supervised evaluation. Keep labels, `instance_label` and event-derived metadata out of predictor columns.

Read `<sensor>_mean_120s`, `_stddev_120s`, `_change` and `_count_120s` from Silver. Partial history and missing sensors remain visible through the counts; a count of 120 means that sensor has a full history. Features are null at unusable times and restart at segment boundaries. The choke-duration fields are candidate quality indicators, not confirmed well-event labels. Gold must decide whether they help its task. Fit any missing-feature treatment on training data only.

Use `instance_quality` and `sensor_availability` to assess recordings, and `quality_issues`/`quality_quarantine` to inspect affected fields and rows. Explain inclusion rules and report kept/excluded examples. Keep recording-level summaries and event metadata out of predictors. Gold may calculate additional statistics from clean readings when its design needs them.

The removed derived columns do not remove source measurements. Gold can calculate the earlier 300-second means, short-versus-long trends, deviations from prior readings, and production-choke pressure differences from the saved clean readings. Preserve recording and sequence boundaries and use current/past values when deriving temporal features. Validity indicators can be derived from original values and the saved rules; usable indicators can be derived from clean values. The existing 120-second counts also support coverage calculations. Some earlier fields overlap with current features: during an uninterrupted one-second sequence, the per-second rate equals the existing previous-reading change. Additional features remain candidates whose model benefit needs evaluation; the full VM validation establishes the saved data's conformity to Silver's implemented rules.

Split whole recordings into training and held-out test sets before fitting preprocessing or models; never put windows from one recording in both sets. Withhold test labels from all predictors and training, then compare predictions to the saved true labels. Report a confusion matrix, per-event precision and recall, macro F1 and test support so rare events are visible. All real recordings have missing row-level classes, so they cannot provide supervised training or verified test labels; score them separately as unverified inference if useful. An event with no labelled examples in the held-out test split has no measured test score. This evaluates detection at the window endpoint, not prediction before an event begins.

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
| Copied pipeline data, including Bronze | `/mnt/silver-data/3w-data/` |
| Spark temporary data on the volume | `/mnt/silver-data/spark-tmp/` |
| Ready marker | `silver_ready.json` under the selected root |
| Actual tables | `runs/<run_id>/<table_name>/` under that root |
| Individual run reports | `_runs/<run_id>.json` under that root |

Read `/mnt/silver-data/3w-data/silver-v2/silver_ready.json` once at the beginning, check that it describes a successful result for the chosen environment and contract, and use its table paths throughout the Gold run. Do not read the whole `runs` directory or combine tables from different Silver runs. The default configured Silver path is `silver/`; it does not automatically change to the explicit output root on the volume.

The volume is mounted at `/mnt/silver-data`; it stores the whole pipeline, despite the mount's name. Copying data does not rewrite absolute paths inside existing reports or ready markers. A copied baseline marker can still point to the original `/home/ubuntu/3w-data/` tables. Confirm the selected run ID and actual table paths before describing Gold input as a new mounted-volume build. A new successful build publishes a marker with the new output paths. No second mounted-volume run or v2 comparison result is claimed here without its saved evidence.

`conf/prod.yaml` still reads Bronze from `/home/ubuntu/3w-data/`; `--output-root` changes only the Silver destination. Gold should use the selected Silver ready marker rather than deriving its input root from this Bronze configuration. See [VM storage](../silver/VM_STORAGE.md) for the volume layout, persistence and evidence locations.

Earlier v2 builds used different column sets. Check that the required feature columns are
present in the 106-column table, and that the saved rules describe the 120-second history;
the version number alone is not sufficient.

Develop on the 30-file dev sample, which contains 1,931,052 observations. Use the repo's locked Python 3.11/PySpark 3.5.0 environment and Java 8 or 17. Use your own VM checkout because the VM account is shared. Setup, sample preparation, and checking commands are in the [Silver run guide](../silver/SILVER_RUN_GUIDE.md).

Use small offline test inputs with known expected results. Check that quality matching does not duplicate contributions, excluded values do not enter calculations, and missing results are represented honestly. Then test the agreed Gold design on the VM in its own output destination.

Record the selected Silver run ID, source, reference version, chosen inputs, inclusion rules, and output counts. Check repeated results by their contents, not just equal counts. Work on a short-lived branch and use a PR with the repo's lint/test checks.

Gold still needs its own runner, storage configuration, and agreed output definitions. The [Silver contract](../silver/SILVER_CONTRACT.md) defines the saved fields; the [milestone report](../silver/SILVER_MILESTONE_REPORT.md) records validation.

# VM storage for Silver and Gold

## Current layout

The UiS OpenStack VM `group20` runs Ubuntu 24.04 under the `ubuntu` account. Its original `/dev/vda` disk is about 40 GB. The user's VM output showed 4.7 GB free on the root filesystem before the storage change. The attached volume `group20-silver` appears as a 100 GB `/dev/vdb` device, formatted as ext4 and mounted at `/mnt/silver-data`. The VM reported about 93 GB available on that filesystem before copying pipeline data; this is a point-in-time observation, not its current free-space figure.

| Purpose | Path |
|---|---|
| Mounted volume | `/mnt/silver-data/` |
| Copied pipeline data | `/mnt/silver-data/3w-data/` |
| New Silver output root | `/mnt/silver-data/3w-data/silver-v2/` |
| Spark temporary data | `/mnt/silver-data/spark-tmp/` |
| Execution and comparison evidence | `/mnt/silver-data/evidence/silver-v2/` |
| Configured production Bronze input | `/home/ubuntu/3w-data/bronze/3w_observations/` |
| Original successful v2 baseline | `/home/ubuntu/3w-data/silver-v2/runs/20261017T232322182032Z-ba450086/` |

The copied `3w-data/` contains `_staging/`, `landing/`, `bronze/`, `silver-oh/` and `silver-v2/`. There is no need for a second output root directly at `/mnt/silver-data/silver-v2/`. The mount name does not restrict it to Silver data.

`conf/prod.yaml` still sets `storage.root` to `/home/ubuntu/3w-data`. A Silver `--output-root` override selects the destination independently. Copying the dataset to the volume does not switch Bronze input to the copy, and does not rewrite absolute paths inside saved reports or ready markers. Keep the original baseline available for comparison. Gold must read the chosen marker once and follow its recorded table paths; a copied baseline marker can still reference the original disk.

## Running and inspecting storage

Run these commands inside the VM, from the repository with `.venv` active:

```bash
mountpoint /mnt/silver-data
df -h / /mnt/silver-data

SPARK_LOCAL_DIRS=/mnt/silver-data/spark-tmp \
TMPDIR=/mnt/silver-data/spark-tmp \
python -u -m threew.silver.run \
  --env prod \
  --output-root /mnt/silver-data/3w-data/silver-v2
```

Only launch after the mount check succeeds. Keep SSH connected for this foreground command. Use `nohup` with redirected output or an existing terminal multiplexer when the job must survive SSH disconnects. Redirect logs to the volume, not the nearly full root disk. Follow the project's branch policy: production deployment uses `main`, development changes use `dev`, and an identical-code comparison requires matching recorded commits and clean working trees.

To inspect the data footprint without modifying anything:

```bash
du -sh /mnt/silver-data/3w-data/*
du -sh /mnt/silver-data/spark-tmp
```

Each Silver attempt writes `runs/<run_id>/` and `_runs/<run_id>.json`; a successful build updates `silver_ready.json` under the selected output root. The copied baseline is not evidence that a second run completed. Confirm the new report's status, reconciliation, publication and distinct run ID before comparing it.

## Persistence

Disconnecting SSH does not unmount the volume or erase files. Data remains on the volume across a VM reboot, but automatic remounting requires the corresponding `/etc/fstab` configuration. The UUID reported by the VM is `94ea1fc0-7bdf-4495-9222-d115b5cb029a`. The intended entry is:

```text
UUID=94ea1fc0-7bdf-4495-9222-d115b5cb029a /mnt/silver-data ext4 defaults,nofail 0 2
```

Check for an existing entry before adding one, back up `/etc/fstab` before editing, and validate with `sudo findmnt --verify --verbose`. The supplied VM transcript confirmed the live mount; it did not confirm the entry had been installed or a reboot tested. Do not format the existing volume again. Since `nofail` allows the VM to boot without the volume, always check the mount before a large write.

## Evidence and Git

Preserve both reports, separately named ready-marker snapshots, execution logs and the complete seven-table comparison result. Small reviewed evidence belongs under `docs/evidence/silver/v2/`; the Parquet data, Spark temporary files, virtual environments and filesystem backups remain outside Git. The new volume adds capacity; neither a speedup nor a completed v2 reproducibility comparison follows from adding it. Use the [run guide](SILVER_RUN_GUIDE.md) for the exact comparison.
