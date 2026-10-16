# 3W Data Engineering Project Description

## 1 Project statement

We build a batch data pipeline on Spark that turns the raw Petrobras 3W dataset (v2.0.0) into trusted, analysis-ready data for detecting and classifying undesirable events in offshore oil wells. The pipeline follows the Medallion architecture (Bronze, Silver, Gold) and runs end to end through GitHub Actions on our UiS OpenStack VM.

The final output is a labelled, window-level feature table that an anomaly detection model consumes directly. Silver also serves as a reusable cleaned dataset for other analyses.

Out of scope: real-time or streaming ingestion, live well data, model training and evaluation, and dashboards beyond what is needed to validate the outputs.

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

The purpose of the Silver layer is to make the prepared Bronze data easier to understand and use in Gold. We standardise field names, add recording and event details, and identify data-quality problems. We preserve the source readings because detecting a problem does not necessarily tell us what the correct value should be.

### Grain and data model

One row in the main Silver table represents one original observation, with all 27 sensor fields kept as columns. This means that Silver retains the same 76,587,318 observations as Bronze. We do not create a separate observation row for each sensor.

To trace an observation back to its source, we use three fields together: `_source_path` identifies the original file, `_file_sha256` identifies its exact version, and `_row_in_file` identifies the row's original position. The recording name, `instance_id`, is not sufficient on its own because some names appear in more than one event folder. We use the source path and file fingerprint to join each observation to Bronze's current file catalogue and add the recording details.

Silver saves seven Parquet tables. The counts below are from the recorded full-dataset runs on 11 October 2026.

| Table | Purpose | Rows |
| --- | --- | ---: |
| `silver_observations` | All original observations, with clearer field names and added recording/event details. Missing and flagged values remain preserved. | 76,587,318 |
| `instance_quality` | One summary per recording (one original Parquet file), containing observation counts, missing labels, timestamp problems, and overall sensor coverage. | 2,228 |
| `sensor_availability` | One summary per sensor per recording, showing counts and percentages of finite versus missing or nonfinite readings. This describes coverage, not whether every reading is valid. | 60,156 |
| `quality_issues` | Warnings and errors linked to individual observations, with the affected field, reason, and recommended action. Covers label, timestamp, and sensor-value problems; individual null sensor cells are summarised in `sensor_availability` instead. | 4,195,465 |
| `quality_quarantine` | The error-only subset of `quality_issues`, identifying affected measurements, labels, or timestamps for exclusion or further investigation. The original observations remain in `silver_observations`. | 167,065 |
| `event_reference` | One definition per event category, containing its readable name and supported observation classes and phases, such as normal, transient, and steady. | 10 |
| `sensor_reference` | One definition per sensor, connecting its original name to its readable English name, meaning, unit, and applicable value rules. | 27 |
`silver_observations` is the main table, while the other six tables are reference or summary tables. The `sensor_reference` and `event_reference` tables are static definitions, while the other six tables are generated from the source data.

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

We preserve missing and questionable values in `silver_observations`. We do not replace missing readings with zero, estimate replacements, or clip values to an allowed range. Zero is not automatically treated as missing. Separate quality tables describe the limitations and identify which observations or fields they affect. 

We do not directly modify the source readings because we cannot assume that a missing or out-of-range value is incorrect. For example, a choke opening of 120% is outside the documented range of 0–100%, but you cant just be sure that the reading can simplify to 100% or that it should be treated as missing. The Silver layer flags the problem, but leaves the original value in place.

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

After we run the silver layers and produce the tables, we run python code to check the resutls like row counts, perserved values and summaries against the expected results. We also ran two full-dataset runs and compared the results. The two runs passed all 29 build checks, and a separate comparison found matching schemas and complete contents across all seven tables.

Both runs retained all observations and reported 4,028,400 missing-class warnings, 5,193 observations without finite sensor readings, and 161,872 choke-opening range errors. Error-level issues affected 167,065 distinct observations, or 0.2181% of the dataset. This percentage excludes missing-class warnings, which affected approximately 5.26% of observations. The runs reported no inconsistent class labels or timestamp errors under the implemented rules.

These results show that Silver preserved the data and made its limitations explicit. 

### Spark processing and future scaling

We implemented Silver using PySpark Dataframes and built-in Spark SQL functions. And we use window functions to compare timestamps within each recording. The two recorded full runs took approximately 54 to 58 minutes. Further tuning should focus on optimizing the execution plans.

Additionally, for the future we could also see how to handle those quality issues rather than just flagging them. For example, we could impute missing values, or correct invalid readings based on the surrounding context.

## 5 Gold layer

*Consumer.* An anomaly detection model that classifies undesirable well events. It needs fixed-size, labelled examples rather than raw per-second readings.

*Structure.* One row per recording per fixed time window, for example 5 minutes. The final length will be set together with the model work. Each row holds per-sensor statistics (mean, standard deviation, min, max, change over the window), the window label taken from the dominant `class_code` in the window, the `instance_label`, the source type (real, simulated or drawn), the well ID and the recording key. Readings listed in `quality_quarantine` are left out of the statistics, and windows with too few usable readings or labels are dropped. A split column assigns whole recordings to train or test, so windows from the same recording never leak across the split.

*Why it differs from Silver.* Silver keeps reusable, cleaned per-observation records. Gold is much smaller and shaped for one purpose, so the model can read it without any further processing.

*Correctness.* Window counts per recording must match what Silver's time range implies, and no window may cross a recording boundary. Aggregates on a sample of windows are recomputed from Silver and compared. Label distributions in Gold must match Silver.

*Refresh and readiness.* Gold is built only from a Silver run that passed its checks. It is written to a versioned folder with a ready marker that records the Silver run it came from. If Silver fails its quality checks, Gold is not rebuilt and the last good version stays in place.

## 6 End to end delivery

Another engineer can clone the repository and follow the run instructions using Python 3.11, PySpark 3.5.0, and using `run.md` as reference. The main branch is the prod configuration, and other branches are created and named depending on the specific task. Each branch is for development. The repository contains a `requirements.txt` file, a `run.md` file with instructions, and a `dataset.ini` file with event and sensor definitions. The repository also contains a `tests` folder with unit tests. The repository is structured to support continuous integration and delivery through GitHub Actions.

During development we will use a 30 sample file; production configuration processes the full dataset on the UiS OpenStack VM. Changes are reviewed by the other group memberthrough pull requests before promotion to `main` (production). A self-hosted VM runner executes Bronze. The intended schedule is Monday at 03:17 UTC. Silver has been validated using production input in an isolated output folder. 

We treat 3W as a fixed batch dataset and use its documented definitions for validation. We dont fix missing labels and uneven sensor coverage. Future improvements include fixing missing and inconsistent data, automating all stages (from ingestion to gold delivery).