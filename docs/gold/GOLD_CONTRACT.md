# Gold contract: the well-event product

Gold turns validated Silver data into one product, **well events**, for two consumers. This document defines who
uses it, what each table means, how the metrics are calculated and what is checked before Gold is published.
Contract version: **1**.

## 1 Consumers and purpose

| Consumer | Question | Decision it supports | Interface |
|---|---|---|---|
| Well-operations engineers | Which undesirable events occur on which real wells, how long does each take to develop, and how much of its labelling is inferred? | Which wells and event types to prioritise for follow-up | SQL views `well_event_summary` and `event_type_summary` |
| Event-classifier team | Can a model recognise the event at a point in time from the last 120 seconds of sensor data? | Which model to deploy | Table `window_features` |

The classifier in [`gold/model/`](../../gold/model/) is a **demo consumer**. Its Random Forest and XGBoost results
show that `window_features` can be used as delivered. They are not part of the Gold product.

Gold does not serve real-time detection, raw per-second readings (use Silver) or individual sensor statistics.

## 2 Tables

Silver keeps one row per observation (76.6 million rows). Gold reshapes that into three tables, each with a
grain chosen for its consumer. `recording_id` is Silver's `_source_path`, because `instance_id` is not unique.

### `recordings`: one row per recording

| Column | Meaning |
|---|---|
| `recording_id` | Key. The recording's source path. |
| `instance_id`, `well_id`, `source_type` | Recording name, real well number (null for simulated and drawn), and `REAL`, `SIMULATED` or `DRAWN`. |
| `event_id`, `event_name` | The event the whole recording belongs to (0 = normal operation, 1-9 = events). |
| `start_ts`, `end_ts`, `duration_s` | First and last timestamp and the seconds between them. Timestamps have no time zone. |
| `n_rows`, `n_inferred`, `inferred_label_share` | Observations, observations whose class Silver inferred, and their share. |

### `event_phases`: one row per continuous phase inside a recording

A phase is a run of consecutive observations with the same class, in original row order. A typical event recording
has three phases: normal, then transient (the event developing), then steady (the event established).

| Column | Meaning |
|---|---|
| `recording_id`, `phase_seq` | Key. `phase_seq` numbers the phases 1, 2, ... in time order. |
| `class_code` | Silver's `class_code_clean`: 0 normal, 1-9 steady, 101-109 transient, null if unknown. |
| `event_id`, `event_name`, `phase` | Event 0-9 (`class_code` modulo 100), its name, and `normal`, `transient`, `steady` or `unknown`. |
| `start_row`, `end_row`, `start_ts`, `end_ts` | First and last observation of the phase. |
| `start_offset_s`, `end_offset_s`, `duration_s` | Seconds from the recording's start to the phase's first and last observation, and the difference. |
| `n_rows`, `n_inferred` | Observations in the phase and how many have an inferred class. |

### `window_features`: one labelled 120-second window per minute of recording

| Column | Meaning |
|---|---|
| `recording_id`, `_row_in_file` | Key. The window ends at this observation. |
| `source_type`, `well_id` | Context for grouping results. Not predictors. |
| `class_code`, `label`, `class_was_inferred` | Class at the window end, event 0-9, and whether Silver inferred it. Not predictors. |
| 21 feature columns | `<sensor>_mean_120s`, `_stddev_120s` and `_change` for seven sensors, from Silver. A sensor's three features are null unless its 120-second history is complete and no value exceeds 10^12 in magnitude. |
| `recording_event`, `split` | The recording's highest label, and `train` or `test`. |

Rules: only labelled observations consistent with their recording are used, one window every 60 rows, and at
least one sensor must have usable features. Whole recordings are assigned to `train` or `test`: 20% of the
recordings in each event and source group go to test, chosen by a seeded hash so the split is the same every
build. Inferred classes are kept for training but removed from `test`, so scores are measured against supplied
labels only. Class balancing and sampling are the consumer's choice and are not applied in Gold.

## 3 Metric definitions

Every view uses these definitions. Each recording counts once, however long it is.

| Metric | Definition |
|---|---|
| Normal lead time | Recording start to its first transient or steady observation. |
| Time to steady | First transient observation to first steady observation. Empty for events with no transient phase. |
| Event duration | First to last transient or steady observation. |
| Inferred label share | Share of the recording's observations whose class Silver inferred. A high share means the timings rest on assumed labels. |

Default filters: `well_event_summary` uses `REAL` recordings only, because simulated and drawn recordings have no
real well. `event_type_summary` keeps the three sources apart instead of mixing them.

## 4 Interfaces

| Interface | For | How to use it |
|---|---|---|
| `recording_event_timing` view | Engineers | Per-recording lead time, time to steady and event duration. The two summaries are built from it. |
| `well_event_summary` view | Engineers | Events per real well: recordings, average lead time and time to steady (minutes), total event hours. |
| `event_type_summary` view | Engineers | The same per event type and source. |
| `window_features` table | Classifier team | Read the Parquet folder listed in `gold_ready.json`. |

The views are defined in [`gold/views.sql`](../../gold/views.sql). Run them with `python gold/query.py --env prod`
(add `--csv <folder>` to save the summaries). All interfaces use the same event IDs and names, taken from Silver.
Consumers read only what `gold_ready.json` points to; they do not read Silver or the `runs/` folder directly.

## 5 What is stored and what is computed on demand

| Item | Strategy | Why |
|---|---|---|
| `recordings`, `event_phases`, `window_features` | Stored as Parquet | Each needs a full pass over 76.6 million Silver rows, and the phases need a window function over every recording. |
| The three views | Computed when queried | They read only the small `recordings` and `event_phases` tables, so they are fast and always agree with them. |

Each table is written as one Parquet file because it is small.

## 6 Build, checks and readiness

```bash
python gold/build_gold.py --env dev     # data/dev/silver -> data/dev/gold
python gold/build_gold.py --env prod --silver-root /mnt/silver-data/3w-data/silver-v3   # -> /mnt/silver-data/gold
```

1. Gold reads `silver_ready.json` and continues only if Silver is contract version 4 and passed its reconciliation.
2. It writes the three tables to a new folder, `<gold root>/runs/<run_id>/`.
3. It reads the written tables back and runs four checks:

| Check | Passes when |
|---|---|
| `phases_cover_every_silver_row` | The phase rows of every recording add up to its row count, and the total equals Silver's. |
| `keys_unique_and_resolve` | Recording and phase keys are unique, and every phase and window belongs to a known recording. |
| `each_recording_in_one_split` | No recording has windows in both train and test. |
| `no_inferred_labels_in_test` | No test window has an inferred class. |

4. It always writes `runs/<run_id>/_report.json` with the results.
5. Only if every check passes does it replace `<gold root>/gold_ready.json` with the same report.

If Silver is not ready, Gold stops before writing anything. If a check fails, the previous `gold_ready.json`
stays, so consumers keep the last good build. Its `built_at` and `silver.run_id` show how old it is.

## 7 Ownership, lineage and change

- **Owner:** yahya2491 owns Gold; ohassane100 owns the Silver input.
- **Lineage:** `gold_ready.json` records the Gold run, the code commit, and the Silver run it was built from.
  Silver's marker leads to the Bronze run, and Bronze's to the pinned Hugging Face commit.
- **Refresh:** rebuild Gold after each new Silver publication. Silver runs after the weekly Bronze run.
- **Versioning:** adding a column keeps contract version 1. Renaming or removing a column, or changing a
  definition, means version 2, announced in this document. Old runs stay under `runs/` until removed by hand.

## 8 Limits

- Phases and timings depend on the supplied and inferred row classes; Gold does not detect events itself.
- Each real recording has 3,600 observations (one hour) whose class Silver inferred: 1,119 × 3,600 is exactly
  Silver's 4,028,400 inferred classes. In the dev sample they fall in the recording's first phase, so the normal
  lead time of a real recording includes up to an hour of inferred normal operation. `avg_inferred_label_share`
  in the views shows how much each figure rests on inferred labels.
- An event recording can contain only normal rows (in the dev sample, both real Event 9 recordings). It is
  counted in `recordings`, but its timings are empty.
- Access is a convention on the shared VM account, not enforced by file permissions.
- Gold is rebuilt by hand; it is not yet part of the scheduled workflow.
