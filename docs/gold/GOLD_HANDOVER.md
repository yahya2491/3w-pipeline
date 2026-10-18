# Gold layer handover

This explains what Silver gives Gold and recommends a concrete Gold anomaly-detection baseline that matches the [project description](../3W_Project_Description_full.md). Gold has not been implemented yet. For definitions and implementation details, use the [contract](../silver/SILVER_CONTRACT.md) and [milestone report](../silver/SILVER_MILESTONE_REPORT.md).

## Where we are

Silver contract v2 has 106 observation columns. Alongside clean readings, bounded choke repair and quality details, it stores 120-second mean, sample standard deviation, change and usable-count features for seven sensors, plus two invalid-choke durations. Gold chooses labelled examples and model inputs, then trains and evaluates its classifier. Read [Silver assumptions](../silver/SILVER_ASSUMPTIONS.md) for the limitations. The historical full-dataset v1 runs do not validate this schema.

The [comparison log](../evidence/silver/comparison.log) and [milestone report](../silver/SILVER_MILESTONE_REPORT.md) record earlier evidence. The historical quality inspection found missing labels, observations without finite sensor readings, and out-of-range openings. Originals remain available.

Gold has not been implemented. The historical full-dataset Silver v1 output is in `/home/ubuntu/3w-data/silver-oh/`; it does not match the current contract. Use the separate `/home/ubuntu/3w-data/silver-v2/` destination for a future v2 full-dataset test.

The earlier 193-column and 76-column dev runs used different schemas. Current 106-column
validation is recorded in Milestone 15 of the milestone report. Full VM validation remains pending.

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

## Planned Gold anomaly-detection model

A practical first model is a Spark MLlib `RandomForestClassifier`, using Silver's prepared sensor features to classify eligible examples as normal or as an undesirable event. If enough examples exist, also evaluate event-type classification. Save the selected features, true label, prediction and evaluation results as Gold outputs. Compare against a baseline using clean sensor levels to test whether the added temporal features help.

One option is one Gold example at each chosen 120-second interval endpoint, using that observation's trailing features. Agree the sampling frequency, required history and coverage before training. If the label describes an interval, derive it from supported `class_code` values in that same interval and document the rule. Class 0 means normal; supported nonzero classes describe events/phases. Exclude unknown or inconsistent targets from supervised evaluation. Keep labels and event-derived metadata out of predictor columns.

Read `<sensor>_mean_120s`, `_stddev_120s`, `_change` and `_count_120s` from Silver. Partial history and missing sensors remain visible through the counts; a count of 120 means that sensor has a full history. Features are null at unusable times and restart at segment boundaries. The choke-duration fields are candidate quality indicators, not confirmed well-event labels. Gold must decide whether they help its task. Fit any missing-feature treatment on training data only.

Use `instance_quality` and `sensor_availability` to assess recordings, and `quality_issues`/`quality_quarantine` to inspect affected fields and rows. Explain inclusion rules and report kept/excluded examples. Keep recording-level summaries and event metadata out of predictors. Gold may calculate additional statistics from clean readings when its design needs them.

Evaluate on recordings that were not used for training; consider grouping real recordings by well. Fit any learned preprocessing on the training set only. Report a confusion matrix, per-class precision and recall, and macro F1 so performance on rare events is visible. Compare results with a simple majority-class baseline. This demonstrates whether the Gold model can detect the labelled undesirable events using Silver's prepared records; it does not establish that it can predict an event before it begins.

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
| Planned isolated v2 full-dataset test root | `/home/ubuntu/3w-data/silver-v2/` |
| Ready marker | `silver_ready.json` under the selected root |
| Actual tables | `runs/<run_id>/<table_name>/` under that root |
| Individual run reports | `_runs/<run_id>.json` under that root |

Read the ready marker once at the beginning, check that it describes a successful result for the chosen environment and contract, and use its table paths throughout the Gold run. Do not read the whole `runs` directory or combine tables from different Silver runs. The default configured Silver path is `silver/`; it does not automatically change to the isolated test destination `silver-v2/`.

Earlier v2 builds used different column sets. Check that the required feature columns are
present in the 106-column table, and that the saved rules describe the 120-second history;
the version number alone is not sufficient.

Develop on the 30-file dev sample, which contains 1,931,052 observations. Use the repo's locked Python 3.11/PySpark 3.5.0 environment and Java 8 or 17. Use your own VM checkout because the VM account is shared. Setup, sample preparation, and checking commands are in the [Silver run guide](../silver/SILVER_RUN_GUIDE.md).

Use small offline test inputs with known expected results. Check that quality matching does not duplicate contributions, excluded values do not enter calculations, and missing results are represented honestly. Then test the agreed Gold design on the VM in its own output destination.

Record the selected Silver run ID, source, reference version, chosen inputs, inclusion rules, and output counts. Check repeated results by their contents, not just equal counts. Work on a short-lived branch and use a PR with the repo's lint/test checks.

Gold still needs its own runner, storage configuration, and agreed output definitions. The [Silver contract](../silver/SILVER_CONTRACT.md) defines the saved fields; the [milestone report](../silver/SILVER_MILESTONE_REPORT.md) records validation.
