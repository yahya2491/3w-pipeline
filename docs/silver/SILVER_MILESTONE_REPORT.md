# Silver milestone report

## Where we are

Milestones 1–11 record the original preservation and quality implementation. The [contract](SILVER_CONTRACT.md) defines the current 106-column output. Commands belong in the [run guide](SILVER_RUN_GUIDE.md), and data handling is defined in the [quality rules](QUALITY_RULES.md).

The [106-column full VM run](../evidence/silver/v2/prod_run1/silver_run_1_,metrics.json) passed all 32 checks across 2,228 recordings and 76,587,318 observations. It published all seven tables, saving 10.53 GB of Parquet in 6 hours 10 minutes across 82 batches. Gold model implementation and evaluation remain planned work.

The [106-column dev run](../evidence/silver/v2/features_106_dev.json) passed all 32 checks on 30 recordings, saving 235 MB of Parquet with a sampled temporary-plus-output peak of 544 MB. The [earlier 76-column VM run](../evidence/silver/v2/old_v2_evidence/run_,metrics.json) passed 32 checks across 82 batches and saved 4.75 GB. These earlier reports describe their respective schemas and run environments.

## Full-dataset v2 validation

Run `20261017T232322182032Z-ba450086` validated the 106-column implementation at commit `98868ccc685526e1ead42ed43c1cff72bef4353c`, with a clean checkout. The [original VM report](../evidence/silver/v2/prod_run1/silver_run_1_,metrics.json) records `SUCCESS`, successful reconciliation and publication, and all 32 checks passing.

The run started at 23:23:22 UTC on 17 October 2026 and finished at 05:33:47 UTC on 18 October. It used four local Spark cores, a 4 GB driver and a target of one million observations per batch, with complete recordings kept together.

| Table | Rows |
|---|---:|
| `silver_observations` | 76,587,318 |
| `instance_quality` | 2,228 |
| `sensor_availability` | 60,156 |
| `quality_issues` | 4,195,465 |
| `quality_quarantine` | 167,065 |
| `event_reference` | 10 |
| `sensor_reference` | 27 |

The seven tables contain 10,534,195,883 bytes of Parquet (10.53 GB, decimal). Transformation and writing took 9,273.619 seconds; read-back and validation took 12,518.428 seconds; input checks took 326.691 seconds. Total runtime was 22,225.411 seconds. The report measures saved Parquet size; it does not measure peak temporary disk usage.

The isolated Silver root was `/home/ubuntu/3w-data/silver-v2/`, with this run's tables under `runs/20261017T232322182032Z-ba450086/`. Gold should resolve the selected successful run through its ready marker and keep its table paths together. This report establishes one successful full-dataset v2 run; comparison of repeated v2 runs is separate evidence.

## Pipeline

The storage decisions behind the current build are recorded in Git. The initial v2 extension (`2569412`) reached 193 observation columns. Storage pressure led to reductions to 135 (`ddccb80`) and then 76 (`d3f70f1`). Commit `b3ccd25` added complete-recording batches, removed the full observation cache and streamlined exact comparisons. Commit `98868cc` restored a focused set of 120-second features, reaching the current 106 columns while retaining bounded batch processing. These revisions changed the derived columns; original sensor readings remained available.

The VM subsequently received the attached volume described in [VM storage](VM_STORAGE.md). Existing pipeline data was copied under `/mnt/silver-data/3w-data/`; new Silver output uses its `silver-v2/` directory and Spark scratch uses `/mnt/silver-data/spark-tmp/`. The measured 6-hour-10-minute run above preceded this storage change. The mount provides capacity for retained outputs and temporary data; no measured speedup or completed repeated v2 comparison is inferred from it.

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
python -m threew.silver.profile --env prod
```

Missing sensors means that a sensor was not present in a recording, or that all its readings were null, NaN or positive/negative infinity. There were 27 sensors in the original dataset. The profile found that every recording had at least four sensors with usable readings, and no recording had all 27 sensors missing.

## Milestone 2 - Write down the Silver table contract


We wrote [SILVER_CONTRACT.md](SILVER_CONTRACT.md). A contract is a shared description of the output: its columns, types, meanings and rules. 

One Silver observation is one original Bronze observation. Its identity is the source path, source-file hash and original row number together. The contract lists all 27 sensor names and units, the recording details, the row labels, and the two quality tables.

Main silver decisions are to preserve the original readings, missing values, timestamps and labels; to rename the Portuguese sensor abbreviations to readable English names; and to add recording details and readable event names.

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

These tests use small offline fixtures. The separate dev pipeline run processes the 30-file sample.

## Milestone 4 - Explain the event labels

The recording label tells us what kind of event the whole recording is about. The class code tells us what is happening at each second inside it.

The original data uses `label` to identify the event represented by the whole recording and `class` to describe the condition at each row. A class of `0` represents normal operation. For event `N`, class `N` represents the established event and class `100 + N` represents its transient phase. For example, in an Event 1 recording, classes 0, 1, and 101 represent normal operation, steady Event 1, and transient Event 1 respectively. Silver preserves these original codes and adds readable event names, phase names, and consistency flags. It does not change or remove source rows; missing class values remain missing.

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

We keep each timestamp exactly as Bronze provides it. Silver uses the original row number to compare each timestamp with the previous one. It reports missing times, repeated times, time moving backwards, and gaps.

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
| `rows_with_finite_values` | Number of original readings that are neither null, NaN, nor infinite. |
| `rows_without_finite_values` | Number of null, NaN, or infinite original readings. |
| `percent_rows_with_finite_values` | Percentage of original readings that are finite. |
| `has_finite_values` | Whether the sensor has at least one finite original reading. |

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

See the [dev validation record](../evidence/silver/v1/dev_evidence/silver_fixes_20261011.json)
and the [earlier Silver review](../evidence/silver/v1/dev_evidence/silver_review_20261011.json).

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

## Milestone 9 - Make runs save and Test good runs

The first part of the milestone is about stopping one run from interfering with another. The runner gives each attempt its own folder and uses a temporary lock (a guard that stops two runs writing to the same place at once). If a run fails, the previous ready marker stays in place. A production test must write to a separate folder so it cannot overwrite the official Silver output.

These safeguards are part of `run.py`; they are not more data transformations. The failure and lock tests are in [test_silver_run.py](../../tests/test_silver_run.py).

The full-dataset v1 evidence is recorded in Milestone 10.

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

The [first v1 run](../evidence/silver/v1/prod_run1/silver_run_1_,metrics.json) and [second v1 run](../evidence/silver/v1/prod_run2/silver_run_2_,metrics.json) both passed 29 checks. Each saved 2.76 GB of Parquet containing 76,587,318 observations, 2,228 recording summaries, 60,156 sensor summaries, 10 event references, and 27 sensor references. Runtimes were 58.3 and 53.9 minutes. The [comparison log](../evidence/silver/comparison.log) records matching contents for all seven tables. These results validate the original v1 schema.

The `silver-oh` folder is just a test destination. The command still reads production Bronze; it does not write to the official production Silver folder. See the [run guide](SILVER_RUN_GUIDE.md) for how to compare the outputs.

## Final Milestone (11)

We check the silver implementation, the runs match. We also check the quality issues and quarantine tables, and the event and sensor reference tables. The final milestone is to check that the Silver implementation is complete and correct, and that it meets the contract and requirements. 

We then make a PR to merge to Silver.
