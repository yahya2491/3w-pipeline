# 3W Data Engineering Project Description

## 1 Project statement

`How do Random Forest and XGBoost compare in detecting and classifying undesirable operating conditions across labelled Petrobras 3W event categories using Silver-prepared sensor values and 120-second features?`

We build a batch data pipeline on Spark that turns the raw Petrobras 3W dataset (v2.0.0) into trusted, analysis-ready data for detecting and classifying undesirable events in offshore oil wells. The pipeline follows the Medallion architecture (Bronze, Silver, Gold) and runs end to end through GitHub Actions on our UiS OpenStack VM.

The planned Gold output is a labelled, window-level feature table for comparing Random Forest and XGBoost on the same examples and feature sets. Silver also serves as a reusable prepared dataset for other analyses.

Out of scope: real-time or streaming ingestion, live well data, and dashboards beyond what is needed to validate the outputs.

## 2 Dataset description

For the dataset we use the 3W version 2.0.0, the dataset source is initially [here](https://github.com/petrobras/3W), however we mirror it on Hugging Face for easier access. The dataset contains recordings of well behaviour, including sensor readings and event classifications ([`Yahya2491/3w-pipeline`](https://huggingface.co/datasets/Yahya2491/3w-pipeline)). The dataset consists of 2,228 Parquet recordings, approximately 1.87 GB, containing 76,587,318 observations across ten event categories. Each observation contains a timestamp, an observation label (class), a well operational-status field (state), and 27 measurement fields, including continuous readings, choke openings, and valve states. The recordings are split into 1,119 real, 1,089 simulated, and 20 hand-drawn examples. We use `dataset.ini` for the event and sensor definitions.

The dataset contaisn enough information to support a data engineering project with spark. However the dataset has some limitations. The number of observations makes the dataset suitable for parallel scans, joins, and aggregations using Spark. However, the event categories and source types are unevenly represented, and synthetic recordings cannot be assumed to represent real well behaviour. Sensor coverage varies, four sensor columns have no finite readings, some row labels are missing, and the source timestamps have no documented time zone.

Additionally some source field names use Portuguese terminology or abbreviations, including `ESTADO-*` for valve states and `ABER-*` for choke openings.

## 3 Bronze layer

*Source and preservation.* We publish the 3W dataset unchanged to our own Hugging Face dataset repo (Yahya2491/3w-pipeline). The pipeline pins one exact commit, so every run reads the same data. Upgrading the data is a deliberate config change recorded in Git. Files land on the VM byte for byte as published, organized by ingestion date and event label.

*Ingestion.* Each run lists the files at the pinned commit and compares them with a manifest of what is already stored. It downloads only new or changed files and verifies each one by its sha256 checksum. Files are written atomically, so a failed run never leaves a partial file behind. A second run with no source changes downloads nothing, which makes ingestion idempotent.

*Traceability.* The manifest has one row per file: path, checksum, size, source commit, run ID and status. Each run also writes a JSON run report (files discovered, downloaded, skipped, failed, duration). We key instances by path rather than file name, because 225 instance IDs are reused across different label folders.

*Schema handling.* We check each file's Parquet footer against a schema contract and record any drift.

*Bronze table.* Spark 3.5 cannot read the source files directly, because of the Brotli compression and the nanosecond timestamps. We therefore keep two zones. The landing zone holds the original bytes. The Bronze table holds the same data converted with pyarrow into a Spark-readable format, with lineage columns added. A Spark reconciliation step checks that row counts match the source footers. The full run covers 2,228 files and 76,587,318 rows.

*Testing and deployment.* Separate dev and prod configs set the scale: dev ingests a small sample per class, prod ingests everything. CI runs unit and end-to-end tests against a fake Hugging Face source. A self-hosted runner on the VM executes a weekly scheduled Bronze workflow and publishes the run report as a job summary.

## 4 Silver design

The purpose of the Silver layer is to make the prepared Bronze data easier to understand and use in Gold. We standardise field names, add recording and event details, identify data-quality problems, and prepare clean sensor values. We preserve the source readings while also attempting to repair them for some sensors, because detecting a problem does not necessarily tell us what the correct value should be.

### Grain and data model
One row in the main Silver table represents one original observation, with all 27 sensor fields kept as columns. This means that Silver retains the same 76,587,318 observations as Bronze. We do not create a separate observation row for each sensor.

To trace an observation back to its source, we use three fields together: `_source_path` identifies the original file, `_file_sha256` identifies its exact version, and `_row_in_file` identifies the row's original position. The recording name, `instance_id`, is not sufficient on its own because some names appear in more than one event folder. We use the source path and file fingerprint to join each observation to Bronze's current file catalogue and add the recording details.

Silver saves seven Parquet tables. The counts below agree across the two final 106-column v2 full runs on 17–18 October 2026. Both passed all 32 checks, and the [saved comparison](evidence/silver/v2/comparison.log) reports identical schemas and complete contents for all seven tables. Reports and timings are in the [v2 evidence summary](evidence/silver/v2/README.md).

| Table | Purpose | Rows |
| --- | --- | ---: |
| `silver_observations` | All original observations, with clearer field names and added recording/event details. Missing and flagged values remain preserved. | 76,587,318 |
| `instance_quality` | One summary per recording (one original Parquet file), containing observation counts, missing labels, timestamp problems, and overall sensor coverage. | 2,228 |
| `sensor_availability` | One summary per sensor per recording, showing counts and percentages of finite versus missing or nonfinite readings. This describes coverage, not whether every reading is valid. | 60,156 |
| `quality_issues` | Warnings and errors linked to individual observations, with the affected field, reason, and recommended action. Covers label, timestamp, and sensor-value problems; individual null sensor cells are summarised in `sensor_availability` instead. | 4,195,465 |
| `quality_quarantine` | The error-only subset of `quality_issues`, identifying affected measurements, labels, or timestamps for exclusion or further investigation. The original observations remain in `silver_observations`. | 167,065 |
| `event_reference` | One definition per event category, containing its readable name and supported observation classes and phases, such as normal, transient, and steady. | 10 |
| `sensor_reference` | One definition per sensor, connecting its original name to its readable English name, meaning, unit, and applicable value rules. | 27 |
`silver_observations` is the main table, while the other six tables provide summaries, issues or reference definitions. The `sensor_reference` and `event_reference` tables are static definitions; the other five tables are generated from the source data.

### Standardisation and event labels

We rename the abbreviated and Portuguese sensor fields using the dataset definitions. For example, `P-PDG` becomes `downhole_pdg_pressure_pa`, `ESTADO-DHSV` becomes `downhole_safety_valve_state`, and `ABER-CKP` becomes `production_choke_opening_pct`. The readings remain in their original units, and the reference table connects each new name to its source field. We also check the expected columns and data types before processing.

The dataset uses two labels that describe different things. `instance_label` identifies the event category of the whole recording, while `class_code` describes the condition at a particular observation. For example, a recording belonging to Event 1 can contain the following classes:

| Class code | Meaning within an Event 1 recording |
| ---: | --- |
| 0 | Normal operation. |
| 101 | The event is developing, referred to as the transient phase. |
| 1 | The event is established, referred to as the steady phase. |
| Missing | No observation label is available. |

These events are usually faults or undesirable operating conditions. Silver adds readable event and phase names alongside the original codes.

### Missing values and quality rules

We preserve original readings in `silver_observations` and add one `_clean` value for each of the 27 sensors. The clean value is the original when it is usable, a limited choke repair when allowed, or null when no usable value is available. We do not replace missing readings with zero or clip values to an allowed range. Zero is not automatically treated as missing. Separate quality tables describe the limitations and identify which observations or fields they affect.

For a finite choke opening outside the documented range of 0–100%, Silver can use the most recent original valid reading from the same uninterrupted sequence, if it is no more than three seconds old. The repaired value is flagged and its source age is recorded. Missing, NaN, infinite, long-gap, and sequence-boundary values are not repaired. If there is no eligible earlier reading, the clean value stays null. The original value is kept unchanged; Silver does not clip an opening such as 120% to 100%.

Silver keeps one observation per original row and now stores 106 columns: 36 Bronze fields, 3 catalogue fields, and 67 added fields. These include 120-second means, sample standard deviations, previous-reading changes and usable counts for seven pressure, temperature and gas-lift-flow sensors, plus two durations describing consecutive invalid original choke readings. Features use current and earlier clean values within the same sequence segment. Gold will select labelled examples and compare Random Forest and XGBoost using these prepared values; it can calculate additional statistics if its design requires them.

**`instance_quality`: quality of each recording**

This table contains one summary per recording. It includes the observation count, counts of missing classes and operational-status values, timestamp problems, unsupported or inconsistent classes, and the number of sensors with or without finite readings. It provides an overview of the recording rather than listing individual problems.

**`sensor_availability`: coverage of each sensor within a recording**

This table contains one row per sensor per recording. Each row identifies the sensor and recording and contains:

- The total number of observations.
- The number of readings with finite values.
- The number of readings without finite values.
- The percentage of readings with finite values.
- Whether the sensor has at least one finite reading.

A finite value is neither null, NaN, nor infinite. This table measures availability, not validity: a choke opening of 120% is finite but still outside its allowed range. Individual null sensor readings are counted here rather than generating separate quality issues.

**`quality_issues`: individual warnings and errors**

This table identifies specific problems affecting observations. Each entry contains the observation's source key, affected field, failed rule, observed value, severity, reason, affected use, and recommended action. One observation can have several issue entries.

The checks cover:

- Missing, unsupported, or inconsistent observation classes.
- Missing or repeated timestamps and backward time intervals.
- Sampling intervals shorter or longer than the expected one second.
- NaN or infinite sensor readings.
- Valve states outside the documented codes of `0`, `0.5`, and `1`.
- Choke openings outside `0–100%`.
- Observations where none of the 27 sensors has a finite reading.

Different observations sharing a timestamp are retained (although there were none in this dataset), and each affected observation is flagged.

Issues have two severity levels:

| Severity | Meaning | Examples |
| --- | --- | --- |
| **Warning** | A limitation that requires the consumer to account for missing information or unexpected sampling. | Missing observation class; sampling interval shorter or longer than one second. |
| **Error** | A problem affecting the suitability of a measurement, label, or timestamp for its intended use. | Out-of-range opening; unsupported valve state; inconsistent class; missing, repeated, or backward timestamp; NaN or infinity; no finite sensor readings. |

**`quality_quarantine`: the error-only subset**

This table contains the same issue records as `quality_issues`, filtered to severity `error`. It provides convenient access to the errors without including warnings. 

The distinction is therefore:

- `instance_quality` summarises the quality of each recording.
- `sensor_availability` summarises sensor coverage within each recording.
- `quality_issues` identifies individual warnings and errors.
- `quality_quarantine` provides the error-only selection.

### Validation and measured results

The current Silver implementation was run on a development sample of 30 recordings containing 1,931,052 observations. All 32 build checks passed across the seven output tables. The checks confirmed that Silver kept every observation, preserved the source values required by the contract, and produced the expected schemas and prepared values.

### Spark processing and future scaling

Silver uses PySpark DataFrames, built-in Spark SQL functions, and window functions to prepare sensor values and calculate time-based features. It processes complete recordings in batches targeting one million observations per batch, so a recording is never split across batches. The development run used three batches; each final full VM run used 82.

The final full VM builds each saved 10.53 GB of Parquet and took 6 h 10 min 25 s and 6 h 5 min 32 s. The second wrote to the attached volume under `/mnt/silver-data/3w-data/silver-v2/` and matched all seven first-run tables. These are recorded results, not a controlled performance comparison. See the [milestone report](silver/SILVER_MILESTONE_REPORT.md#full-dataset-v2-validation).


## 5 Gold layer

*Consumer.* An anomaly detection model that classifies undesirable well events. It needs fixed-size, labelled examples rather than raw per-second readings.

*Structure.* One row per recording per fixed time window, for example 5 minutes. The final length will be set together with the model work. Each row holds per-sensor statistics (mean, standard deviation, min, max, change over the window), the window label taken from the dominant `class_code` in the window, the `instance_label`, the source type (real, simulated or drawn), the well ID and the recording key. Readings listed in `quality_quarantine` are left out of the statistics, and windows with too few usable readings or labels are dropped. A split column assigns whole recordings to train or test, so windows from the same recording never leak across the split.

*Why it differs from Silver.* Silver keeps reusable, cleaned per-observation records. Gold is much smaller and shaped for one purpose, so the model can read it without any further processing.

*Correctness.* Window counts per recording must match what Silver's time range implies, and no window may cross a recording boundary. Aggregates on a sample of windows are recomputed from Silver and compared. Label distributions in Gold must match Silver.

*Refresh and readiness.* Gold is built only from a Silver run that passed its checks. It is written to a versioned folder with a ready marker that records the Silver run it came from. If Silver fails its quality checks, Gold is not rebuilt and the last good version stays in place.

## 6 End to end delivery

Another engineer can clone the repository and use `README.md` for setup and pipeline instructions, with `docs/silver/SILVER_RUN_GUIDE.md` for Silver-specific commands. The project uses Python 3.11, PySpark 3.5.0, and the pinned `requirements.lock` and `requirements-dev.lock` files. It also contains `dataset.ini` for event and sensor definitions and a `tests` folder. GitHub Actions runs continuous integration.

During development we will use a 30 sample file; production configuration processes the full dataset on the UiS OpenStack VM. Changes are reviewed by the other group memberthrough pull requests before promotion to `main` (production). A self-hosted VM runner executes Bronze. The intended schedule is Monday at 03:17 UTC. Silver has been validated using production input in an isolated output folder. 

We treat 3W as a fixed batch dataset and use its documented definitions for validation. We leave missing labels unknown and report uneven sensor coverage. Gold will use Silver's clean readings and 120-second features to build labelled examples for Random Forest and XGBoost. We will evaluate both on the same held-out recordings and report per-event precision and recall, macro F1, test support and a majority-class baseline. Gold and the complete scheduled Bronze-to-Gold workflow are planned work.
