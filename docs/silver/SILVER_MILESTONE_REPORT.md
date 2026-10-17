# Silver milestone report

## Where we are

This report describes the work completed in each commit milestone. The [contract](SILVER_CONTRACT.md) defines the tables and field meanings. The report was updated throughout the Silver implementation and records changes to the plan along the way.

Milestones 1–11 describe the original preservation/quality implementation. Milestone 12
added cleaning and experimental model-window features. Milestone 13 keeps the reusable
cleaning and quality work in Silver and moves model-window aggregates to Gold. Milestone 14
reduces temporary storage; Milestone 15 adds a focused set of sensor features. Earlier run
counts and timings are historical evidence, not validation of the current 106-column schema.

## Pipeline

```text
Original dataset files
    -> Bronze: download, verify, preserve observations and source identifiers
    -> Silver: rename fields, attach recording details, explain event codes
         -> v2: clean readings, bounded choke repairs, quality/sequence facts and 120-second features
         -> silver_observations: one row per original observation
         -> instance_quality: one summary per recording
         -> sensor_availability: one summary per recording and sensor
         -> quality_issues: quality problems and reasons
         -> quality_quarantine: error-level issues for review
         -> event_reference: shared event definitions
         -> sensor_reference: shared sensor definitions and units
    -> Gold / analysis: a consumer of these checked tables
```

Silver keeps original readings, timestamps and labels. Contract v2 adds clean sensor values, explicitly marked choke estimates, readable event descriptions, sequence details and 30 sensor-behaviour features. Quality tables explain flagged source values. Gold chooses labelled examples and trains/evaluates its model.

## Milestone 1 - Read Bronze and produce a first profile

We added [profile.py](../../src/threew/silver/profile.py). This is a read-only inspection command. It reports recording and row counts, source categories, missing class/state values, sensor missingness, sensor coverage per recording, and timestamp consistency.

It was used to check the Bronze output and uses it as a overall reference for Silver. The profile does not download or rebuild Bronze. It reads the existing ready marker and manifest, then reads the Bronze Parquet files to report the observations.

The development Bronze run downloaded 30 original Parquet files, verified them and converted them into 1,931,052 observations. The profile reads those observations; it does not download another sample or build Silver.

The development bronze run was run using

```bash
python -m threew.bronze.run --env dev
```

We also ran the extended profile on the VM's complete Bronze table: 2,228 recordings and 76,587,318 observations. The recorded output showed no null or duplicate timestamps, repeated adjacent times, backward intervals, or gaps over one second. Every compared adjacent interval was one second. These results refer to the VM profile run from 10 October; they do not verify later code changes.

The whole extended run was ran using

```bash
threew.silver.profile --env prod
```

Missing sensors means that a sensor was not present in a recording, or that all its readings were null, NaN or positive/negative infinity. There were 27 sensors in the original dataset. The profile found that every recording had at least four sensors with usable readings, and no recording had all 27 sensors missing.

## Milestone 2 - Write down the Silver table contract


We wrote [SILVER_CONTRACT.md](SILVER_CONTRACT.md). A contract is a shared description of the output: its columns, types, meanings and rules. 

One Silver observation is one original Bronze observation. Its identity is the source path, source-file hash and original row number together. The contract lists all 27 sensor names and units, the recording details, the row labels, and the two quality tables.

Main silver decisions are to preserve the original readings, missing values, timestamps and labels; to rename the Portuguese sensor abbreviations to readable English names; and to add recording details and readable event names.

So basically in the silver we transformed the original bronze data into a more understandable format, while keeping the original data intact. The contract ensures that everyone knows what to expect from the Silver tables and how to use them correctly.

## Milestone 3 - Add recording details and clearer column names

A Bronze row contains one moment of sensor readings. Some useful facts describe the whole recording, such as its well ID and recording ID. Those facts are in Bronze's manifest, a small catalogue with one entry per source file.

[input.py](../../src/threew/silver/input.py) checks that Bronze is ready and reads the catalogue. [transform.py](../../src/threew/silver/transform.py) matches each observation to its recording details using the source path and file hash (a fingerprint of the file). It then gives hard-to-read columns clearer names. For example, `P-PDG` becomes `downhole_pdg_pressure_pa`. It also renames `class`, `label`, and `state`, and adds recording details when available. The original readings and number of rows stay the same. So essentially we look at the catalogue to find the details of the bronze file recordings and then we apply our transformations to the observations to make them more understandable.

**Files**

- `input.py` checks the Bronze receipt and reads the catalogue.
- `transform.py` adds recording details and renames columns.
- [test_silver_input.py](../../tests/test_silver_input.py) and [test_silver_transform.py](../../tests/test_silver_transform.py) check that the match and renames work.

**To test this part:**

```bash
pytest -q tests/test_silver_input.py tests/test_silver_transform.py
```

The tests were run on the 30 file sample, but they were also designed to be run on the full dataset to ensure that the code works correctly.

## Milestone 4 - Explain the event labels

The recording label tells us what kind of event the whole recording is about. The class code tells us what is happening at each second inside it.

The original data uses `label` to identify the event represented by the whole recording and `class` to describe the condition at each row. A class of `0` represents normal operation. For event `N`, class `N` represents the established event and class `100 + N` represents its transient phase. For example, in an Event 1 recording, classes 0, 1, and 101 represent normal operation, steady Event 1, and transient Event 1 respectively. Silver preserves these original codes and adds readable event names, phase names, and consistency flags. It does not change or remove source rows; missing class values remain missing.

Its a little hard to follow, but essentially the dataset is split into 10 different event types (Event 0 through Event 9, with each with its own label numbering from 0 to 9), and each event type (like event label 1 is a abrupt increase of BSW). On the other hand, the class codes are used to identify which phase of the event is happening at each second in the recording. Look at the tabels below for more details. Silver adds readable names for these events and phases, and also checks for consistency between the class codes and the recording labels.

Event classes in original dataset:

| Recording label | Event | Description | Transient class |
|---:|---|---|---:|
| 0 | Normal Operation | The well is operating normally. | Not applicable |
| 1 | Abrupt Increase of BSW | A sudden increase in basic sediment and water in the produced fluid. | 101 |
| 2 | Spurious Closure of DHSV | The downhole safety valve closes unexpectedly. | 102 |
| 3 | Severe Slugging | Large, unstable surges of liquid and gas move through the production system. | None |
| 4 | Flow Instability | The production flow becomes unstable or oscillates. | None |
| 5 | Rapid Productivity Loss | The well's production capability decreases quickly. | 105 |
| 6 | Quick Restriction in PCK | A rapid restriction occurs in the production choke. | 106 |
| 7 | Scaling in PCK | Mineral deposits build up in the production choke and restrict flow. | 107 |
| 8 | Hydrate in Production Line | Hydrate deposits form in the production line and obstruct flow. | 108 |
| 9 | Hydrate in Service Line | Hydrate deposits form in the service line and obstruct flow. | 109 |

Meanings of the row level class codes:
| Class code | Meaning | Example for recording label 1 |
|---:|---|---|
| 0 | Normal operation | The well is operating normally before or around the event. |
| N | The event is established or in its steady phase | `class = 1` means established Abrupt Increase of BSW. |
| 100 + N | The event is in its transient phase | `class = 101` means transient Abrupt Increase of BSW. |
| Null | No row-level class label is available | The row is retained but cannot be assigned a phase. |

Example of an event 1 recording with class codes:
| Recording label | Class code | Row event name | Row phase |
|---:|---:|---|---|
| 1 | 0 | Normal Operation | Normal |
| 1 | 101 | Abrupt Increase of BSW | Transient |
| 1 | 1 | Abrupt Increase of BSW | Steady |
| 1 | Null | No row event name | Unknown |

The example starts with normal operation and transitions into the event. Event 0 supports class code 0. Silver adds readable names for these events and phases.


What silver adds:

| Original Bronze field | Silver field | Meaning |
|---|---|---|
| `label` | `instance_label` | Numeric event type for the complete recording. |
| `class` | `class_code` | Numeric event or phase code for the individual row. |
| — | `instance_event_name` | Readable name for the recording-level event. |
| — | `row_event_name` | Readable name for the event represented by the row class. |
| — | `row_event_phase` | `normal`, `steady`, `transient`, `unknown`, or `unrecognized`. |
| — | `class_label_status` | Whether the row class is `consistent`, `inconsistent`, or `unlabeled` for the recording. |
**Files**

- The label rules are in [transform.py](../../src/threew/silver/transform.py).
- [test_silver_transform.py](../../tests/test_silver_transform.py) checks normal, steady, transient, missing, and inconsistent labels.

**Check this part:**

```bash
pytest -q tests/test_silver_transform.py
```

## Milestone 5 - Keep timestamps and check the time sequence

We decided to keep each timestamp exactly as Bronze provides it. To check the order, Silver uses the original row number in the file and compares each timestamp with the next one. It can report missing times, repeated times, time moving backwards, or gaps.

Silver currently as of this milestone does not change timestamps, fill gaps, sort rows by time, or remove readings. We also decided here not to add a repeated time-gap column to every observation. The time results belong in a separate summary for each recording.


**Files**

- [quality.py](../../src/threew/silver/quality.py) calculates the time checks.
- [test_silver_quality.py](../../tests/test_silver_quality.py) checks unusual times, including fractional seconds.

**Check this part:**

```bash
pytest -q tests/test_silver_quality.py
```

## Milestone 6 - Make two quality summaries

A recording is one original source file, identified by its source path and file hash. 

So in this milestone we created two quality summaries to avoid repeating recording-wide facts on millions of rows. The first summary is `instance_quality`, which has one row per recording. The second summary is `sensor_availability`, which has one row per recording and sensor. 

The observation table keeps every reading. Instead of repeating recording-wide facts on millions of rows, Silver puts them in two smaller tables:

- `instance_quality`: one row per recording. It counts missing labels, timestamp problems, and how many sensors have usable readings. 1 file = 1 row. 2,228 recordings = 2,228 rows in the full dataset.
- `sensor_availability`: one row per recording and sensor. It shows how many readings for that sensor can be used. 1 file x 27 sesnsors = 27 rows per recording. 2,228 recordings x 27 sensors = 60,156 rows in the full dataset.

Instance quality:
| Column | Meaning |
|---|---|
| `source_type` | Recording source: `REAL`, `SIMULATED`, or `DRAWN`. |
| `instance_label` | Event label for the complete recording. |
| `instance_event_name` | Readable name of the recording-level event. |
| `_source_path` | Original source-file path. |
| `_file_sha256` | SHA-256 hash identifying the exact source-file version. |
| `instance_id` | Source recording identifier. |
| `well_id` | Well identifier, when available. |
| `instance_start` | Recording start time, when available. |
| `recording_rows` | Number of observations in the recording. |
| `unlabeled_rows` | Number of rows with a missing `class_code`. |
| `null_state_rows` | Number of rows with a missing `state_code`. |
| `null_timestamp_rows` | Number of rows with a missing timestamp. |
| `duplicate_timestamp_rows` | Number of repeated non-null timestamps. |
| `timestamp_intervals_compared` | Number of adjacent timestamp intervals checked. |
| `one_second_intervals` | Number of adjacent intervals exactly one second long. |
| `repeated_adjacent_timestamps` | Number of adjacent rows with the same timestamp. |
| `backward_intervals` | Number of intervals where time moves backwards. |
| `gaps_over_one_second` | Number of intervals longer than one second. |
| `largest_gap_seconds` | Size of the largest detected timestamp gap. |
| `inconsistent_class_rows` | Number of rows whose class does not fit the recording label. |
| `unrecognized_class_rows` | Number of rows with an unsupported class code. |
| `sensors_present` | Number of the 27 sensors with at least one usable value. |
| `sensors_missing_entire_recording` | Number of sensors with no usable value anywhere in the recording. |
| `no_sensors_present` | Whether all 27 sensors are unavailable in the recording. |

Sensor availability:

| Column | Meaning |
|---|---|
| `source_type` | Recording source: `REAL`, `SIMULATED`, or `DRAWN`. |
| `instance_label` | Event label for the complete recording. |
| `instance_event_name` | Readable name of the recording-level event. |
| `_source_path` | Original source-file path. |
| `_file_sha256` | SHA-256 hash identifying the exact source-file version. |
| `instance_id` | Source recording identifier. |
| `well_id` | Well identifier, when available. |
| `instance_start` | Recording start time, when available. |
| `recording_rows` | Total number of observations in the recording. |
| `sensor_name` | Silver name of the sensor. |
| `rows_with_finite_values` | Number of rows with a usable finite sensor value. |
| `rows_without_finite_values` | Number of rows where the sensor value is unavailable. |
| `percent_rows_with_finite_values` | Percentage of rows with a usable finite sensor value. |
| `has_finite_values` | Whether the sensor has at least one usable value. |

A zero sensor reading counts as a real value. Null, NaN (not a number), and infinite values count as unavailable in the summaries. The observation table still keeps the original values.

**Files**

- [quality.py](../../src/threew/silver/quality.py) builds the summaries.
- [test_silver_quality.py](../../tests/test_silver_quality.py) checks the counts and percentages.

**Check this part:**

```bash
pytest -q tests/test_silver_quality.py
```

## Milestone 7 - Build Silver and check what was saved

[run.py](../../src/threew/silver/run.py) puts the Silver steps together. It
reads the verified Bronze table, applies the Silver transformation, builds
the two quality summaries, and now writes seven Parquet tables (the
original three from this milestone plus four added in Milestone 8):

- `silver_observations`: one row for each original Bronze observation;
- `instance_quality`: one row per recording with recording-level quality
  checks;
- `sensor_availability`: one row per recording and sensor with sensor-level
  availability counts;
- `quality_issues`: quality problems with the affected row, field, reason,
  severity, and recommended action;
- `quality_quarantine`: the error-level issues kept separately for review;
- `event_reference`: the 10 shared event definitions; and
- `sensor_reference`: the 27 shared sensor definitions and units.

| Saved Silver Parquet table | Contents | Related milestones |
|---|---|---|
| `silver_observations` | Original Bronze observations with renamed fields, recording details, event explanations, and the original timestamp preserved unchanged. | Milestones 2–5 |
| `instance_quality` | One row per recording containing timestamp-quality metrics, missing-label checks, event-label checks, and sensor coverage. | Milestones 5–6 |
| `sensor_availability` | One row per recording and sensor containing usable-reading counts and availability percentages. | Milestone 6 |
| `quality_issues` | Quality problems with their affected rows and fields, reasons, severities, and recommended actions. | Milestone 8 |
| `quality_quarantine` | Error-level issue entries for review; the original observations remain in `silver_observations`. | Milestone 8 |
| `event_reference` | The 10 shared event definitions and supported class/phase combinations. | Milestone 8 |
| `sensor_reference` | The 27 shared sensor definitions, names, units, and documented value rules. | Milestone 8 |

Each run receives its own folder under `silver/runs/<run_id>/`. After writing,
Silver reads the saved Parquet tables back and checks their schemas, keys,
row counts, preserved Bronze values, transformed event details, and quality
summary values. Only after these checks pass does it publish
`silver_ready.json` for that run.

The `silver_observations` table contains the Milestone 4 transformations:
renamed columns, recording metadata, readable event names, event phases, and
class-label consistency status. The two quality summary tables are the
outputs defined in Milestone 6.

**Files**

- `run.py` builds and saves the tables.
- `reconcile.py` checks the saved tables.
- [test_silver_run.py](../../tests/test_silver_run.py) checks the normal run and failure cases.

**Build and check the development sample:**

```bash
python -m threew.silver.run --env dev
```

Two recorded builds passed all 19 Silver checks on the 30-file dev sample. Each contained 1,931,052 observations, 30 recording summaries, and 810 sensor summaries. The saved tables had the same contents both times. 

See the [dev validation record](../evidence/silver/old_evidence/silver_fixes_20261011.json)
and the [earlier Silver review](../evidence/silver/old_evidence/silver_review_20261011.json).

These record the earlier development checks. The later full-dataset runs
and seven-table comparison are documented in Milestone 10.

## Milestone 8 - Explain quality problems and publish shared definitions

This step makes the quality checks useful to someone reading Silver. We still keep every original observation. We now show exactly where a problem occurred, explain what it affects, and tell the reader what to do with it.



### Preserve timestamps and define duplicates

We wrote down and enforced these rules:

| Situation | What Silver does |
|---|---|
| Time moves backward | Keeps the original timestamp and row position; identifies the affected row for sequence review. |
| Different observations share a timestamp | Keeps all readings and flags every affected row. A repeated time does not prove that a row was copied. |
| A time is missing, or the interval is shorter/longer than one second | Reports the affected row. Fractional timestamps stay unchanged; gaps are not filled. |
| The same source path, file hash and original row position appear twice | Stops the run before writing output, whether the copies are identical or conflicting. No arbitrary winner is chosen. |
| A source file is corrected and has a new hash | Uses the file version selected by Bronze's current catalogue. This is different from a duplicate observation. |

Firs tthe first 2 rows of the table above, the timestamp problems, we ensure that we dont modify the timestamps or the data directly since we cant gurantee that the timestamps are correct, or if there is an internal error, so we just keep it as is and report the affected rows for manual review.

### Add issue reasons and a quarantine list

[issues.py](../../src/threew/silver/issues.py) creates two additional Parquet tables:

- `quality_issues`: one entry for each affected observation, field and quality rule. It contains the original row key, the field name, an issue code, a severity, a readable reason, the affected use, a suggested action, and the supplied value as text.
- `quality_quarantine`: the error entries from that list, kept separately for inspection. It identifies questionable fields; it does not remove the complete observation or declare every other measurement in the row unusable. Join its source path, file hash and row position to `silver_observations` to inspect the complete source reading.

An observation can have more than one issue. The quarantine table therefore contains issue entries, not necessarily one entry per observation.

| Problem | Severity and how to use the data |
|---|---|
| Missing class | Warning: keep the sensor readings, but do not use this label for work requiring a known class. |
| Unsupported class or class that conflicts with the recording category | Error affecting labelled analysis: inspect the label; other readings remain available. |
| Missing, repeated or backward timestamp | Error affecting time-sequence analysis: inspect the sequence before using it. |
| Interval shorter or longer than the expected one second | Warning: keep the actual interval visible and account for it in analysis. |
| NaN or positive/negative infinity in a sensor | Error affecting that sensor measurement: exclude that value from numeric analysis and inspect it. |
| Valve code outside its documented values, or opening percentage outside 0–100 | Error affecting that sensor measurement: inspect it without rounding or clipping. |
| No finite reading in any of the 27 sensors in an observation | Error affecting sensor analysis: retain the row and labels; it cannot supply a sensor measurement. |

**Build the development project folder:**

```bash
python -m threew.silver.run --env dev
```

To see the quality result and paths of the new outputs without starting Spark again:

```bash
python - <<'PY'
import json
from pathlib import Path

ready = json.loads(Path("data/dev/silver/silver_ready.json").read_text())
summary = ready["quality_summary"]
print("Observations:", summary["observation_rows"])
print("Observations with quarantine issues:", summary["rows_with_quarantine_issues"])
for rule in summary["by_rule"]:
    print(rule["issue_code"], rule["severity"], rule["count"])
for name in ("quality_issues", "quality_quarantine", "event_reference", "sensor_reference"):
    print(name, ready["tables"][name]["path"])
PY
```

After the Silver run completes, this read-only Python command reads silver_ready.json and prints a brief quality summary. It shows the number of observations, the number with quarantine issues, the issue types and severities detected, and the paths to the quality_issues, quality_quarantine, event_reference, and sensor_reference Parquet tables. It does not start Spark, rebuild Silver, or modify any data. It is a quick way to inspect and verify the reported quality results without opening the Parquet output tables individually. 

## Milestone 9 - Make runs save and Test good runs

The first part of the milestone is about stopping one run from interfering with another. The runner gives each attempt its own folder and uses a temporary lock (a guard that stops two runs writing to the same place at once). If a run fails, the previous ready marker stays in place. A production test must write to a separate folder so it cannot overwrite the official Silver output.

These safeguards are part of `run.py`; they are not more data transformations. The failure and lock tests are in [test_silver_run.py](../../tests/test_silver_run.py).

**Status:** implemented locally. The full-dataset VM run in Milestone 10 is still needed to check it at production scale.

The tests use small sample data, so they do not need to download the full dataset. They check that good inputs work and that problems—like a missing row, changed reading, wrong summary, or failed write—are caught.

**Run the checks from the project folder, with the environment active:**

```bash
ruff check .
ruff format --check .
HF_HUB_OFFLINE=1 pytest -q

```

## Milestone 10 - Run Silver on the full dataset x2

The dev sample is small. This milestone checks Silver against the full Bronze table on the VM. Run from the VM checkout with a separate output folder:

```bash
python -m threew.silver.run --env prod --output-root /home/ubuntu/3w-data/silver-oh
```

Complete two successful runs using the same code, configuration, and Bronze input. Compare the contents of all seven output tables: silver_observations, instance_quality, sensor_availability, quality_issues, quality_quarantine, event_reference, and sensor_reference.
Record both run IDs, row counts, run times, and output sizes. The expected baseline is 76,587,318 observations, 2,228 recording summaries, 60,156 sensor summaries, 10 event references, and 27 sensor references. Quality issue counts depend on the input and rules, but should match between these unchanged runs.

The `silver-oh` folder is just a test destination. The command still reads production Bronze; it does not write to the official production Silver folder. See the [run guide](SILVER_RUN_GUIDE.md) for how to compare the outputs.

## Final Milestone (11)

We check the silver implementation, the runs match. We also check the quality issues and quarantine tables, and the event and sensor reference tables. The final milestone is to check that the Silver implementation is complete and correct, and that it meets the contract and requirements. 

We then make a PR to merge to Silver.

## Milestone 12 - Prepare measurements and causal features for forecasting

Added [SILVER_ASSUMPTIONS.md](SILVER_ASSUMPTIONS.md), [QUALITY_RULES.md](QUALITY_RULES.md)
and [`conf/quality/silver_rules.yaml`](../../conf/quality/silver_rules.yaml). The code validates
the rules, uses the same configured domains for issues/cleaning/references, and records resolved
settings plus their fingerprint. Contract version is now 2; table count remains seven.

All 27 sensors gain `_clean` and `_is_valid` columns. Finite invalid choke openings can use
the most recent original valid reading from at most three seconds earlier in the same continuous
sequence. `_was_imputed` and `_source_age_seconds` explain replacements. Estimated values
never become anchors. For example `40, -99.99, 120, 120, 120, 42` becomes
`40, 40, 40, 40, null, 42` in the clean column. Null/NaN/infinite openings, missing classes
and missing sensor readings are not filled. No source rows are removed or original values changed.

## Milestone 13 - Keep Silver reusable and move model aggregates to Gold

The Silver infographic separates reusable trusted records from consumer-specific answers.
Contract v2 follows that boundary: it keeps renamed observations, recording identity, conformed
event and sensor references, clean sensor values, bounded choke-repair provenance, quality outputs,
and sequence checks. It removes per-sensor validity flags that duplicate the clean values and issue
table, row-level counts and pressure differences that Gold can calculate, and the rolling means,
variation, rates, coverage and other model-window calculations from Silver. Gold will create
labelled windows and calculate the statistics required by the Random Forest.

The observation schema is now 76 columns: 36 Bronze fields, 3 catalogue fields, and 37
Silver-derived fields. The previous 17-minute dev run tested the larger v2 schema. Run the
dev sample again to verify contract v2 and measure its output size before another VM-scale run.

## Milestone 14 - Bound temporary storage for the VM

The VM ran out of disk while Spark was caching prepared observations. V1 wrote observations
directly; V2 introduced a complete disk cache that remained even after the schema was reduced
to 76 columns. Free space measured after failure did not show the peak temporary usage.

The runner now writes without that observation cache. It processes complete recordings in
batches targeting one million observations, verifies each batch's saved files, and stops its
Spark context before continuing. Recording boundaries, cleaning rules, the 76 columns, and
all seven tables stay the same. Reference tables are written once. Exact comparisons cancel
signed row counts in one aggregation instead of running two directional comparisons. The
ready marker is published only when every batch and the accumulated counts pass; a later
batch failure preserves the previous marker.

The actual updated runner processed all **30 dev recordings / 1,931,052 observations** in
three batches and passed **32 checks**. Sampled temporary-plus-output storage peaked at
**302 MB**, compared with **951 MB** for the cached runner on the same sample. No Spark
scratch files remained after the run. Counts, quality totals, preparation totals, and saved
rules matched the previous unbatched dev result. All 59 focused tests passed, including an
exact seven-table comparison between single-batch and three-batch fixture runs and a failure
in a later batch. The final catalogue guards also passed six targeted tests; lint and formatting
passed. At that milestone, the optional choke evaluator read clean columns without the removed validity flags.

The dev run took 400.6 seconds locally while regression tests were also running; this is not a
controlled speed comparison. Smaller batches add startup and repeated-check overhead. The
full VM run is still required to measure production storage and runtime. Each successful run
retains its own output, and a recording larger than the batch target runs alone.

See [the saved measurements](../evidence/silver/v2/storage_fix_dev.json) and the
[VM command](SILVER_RUN_GUIDE.md#run-on-the-full-dataset-on-the-vm).

## Milestone 15 - Add focused sensor features and remove diagnostic scripts

The 76-column version handled cleaning and storage reliably, but it left all temporal feature
preparation to Gold. We add 30 selected columns so Silver also prepares useful model inputs:
120-second mean, sample standard deviation, previous-reading change and usable count for
seven pressure, temperature and gas-lift-flow sensors, plus invalid-duration fields for both
original choke openings. The schema is **106 columns: 36 Bronze + 3 catalogue + 67 derived**.
The contract label remains **v2**, and the same seven tables are written.

The seven sensors have recorded coverage in the dataset and represent operating conditions
relevant to the intended model. This is a justified candidate set, not a claim of measured
feature importance. Gold must compare it with a clean-reading baseline.

The implementation uses DataFrame expressions and Spark SQL windows. The trailing range is
`(time - 120 seconds, time]`; it never crosses a recording or sequence break. Means skip null
clean readings, counts expose incomplete history, and sample standard deviation needs two
values. Changes remain null when either adjacent clean reading is missing. Invalid-opening
duration uses the original readings, so an estimated clean repair does not hide a continuing
raw fault. The [contract](SILVER_CONTRACT.md) defines the exact fields and reset behaviour.

The one-million-row recording batches, direct Parquet writes and exact comparisons from
Milestone 14 remain. The rules file now records the feature history and sensor list. Temporary
Python benchmarks/probes and the optional choke-evaluation script were removed; historical
JSON results remain. Correctness checks stay in the normal `tests/` suite.

Unit checks cover the exact time boundary, missing readings, segment resets, invalid-duration
resets and future-data isolation. All **279 offline regression tests passed**; lint and formatting
passed. The normal Silver CLI processed all **30 recordings / 1,931,052 observations** in
three batches and passed **32 reconciliation checks**. Independent Parquet footer checks
confirmed all seven totals and 106 columns in every observation file. Raw quality totals and
the existing cleaning counts matched the 76-column result.

The run saved about **235 MB of Parquet**. Sampled temporary-plus-output storage peaked at
**544 MB**, and scratch usage was zero after completion. The 302 MB peak in Milestone 14
belongs to the 76-column build. These values exclude Bronze and older retained outputs.
Local runtime was about **13 minutes**, while regression tests ran concurrently; it is not
a controlled speed comparison. Adding temporal features increases computation and saved
storage, while recording batches keep temporary work bounded.

See [the current dev evidence](../evidence/silver/v2/features_106_dev.json). Simple row scaling
suggests about 9.3 GB of saved Parquet for the full dataset, plus working space. Full VM
storage/runtime and the features' contribution to Gold classification remain to be evaluated.

