---
license: cc-by-4.0
pretty_name: Petrobras 3W Dataset (mirror)
tags: [time-series, anomaly-detection, oil-and-gas]
---

# Petrobras 3W Dataset (mirror)

## Source

This is a mirror of the [petrobras/3W](https://github.com/petrobras/3W) dataset, **version 2.0.0**, taken from upstream commit
[`d57c058af6baf65f1b285f3e56f46e4f018a07d9`](https://github.com/petrobras/3W/tree/d57c058af6baf65f1b285f3e56f46e4f018a07d9/dataset).

The files are byte-identical to upstream: the SHA-256 of every Parquet file here matches the file at that commit.
Only the `dataset/` folder's Parquet files, `dataset.ini` and `LICENSE-CC-BY` are mirrored; the upstream demos and toolkit are not.

## Attribution

The 3W Dataset is created and published by **Petróleo Brasileiro S.A. (Petrobras)** under the
[Creative Commons Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/) license
(see [`data/LICENSE-CC-BY`](data/LICENSE-CC-BY)). This mirror is not affiliated with Petrobras.

If you use the data, cite it as described in the upstream
[CITATION.md](https://github.com/petrobras/3W/blob/d57c058af6baf65f1b285f3e56f46e4f018a07d9/CITATION.md).

## Layout

```
README.md
data/
  dataset.ini          # upstream config: version, column descriptions, event labels
  LICENSE-CC-BY
  <label>/<file>.parquet
```

Each Parquet file is one **instance**: a multivariate time series from one well over one period.
The folder name `<label>` (0–9) is the instance's event type. 2,228 files in total.

## File naming

| Pattern | Kind | Files |
|---|---|---|
| `WELL-XXXXX_YYYYMMDDhhmmss.parquet` | Real: well number + start time of the instance | 1,119 |
| `SIMULATED_XXXXX.parquet` | Simulated | 1,089 |
| `DRAWN_XXXXX.parquet` | Hand-drawn | 20 |

## Labels

| Label | Event | Transient label | Files |
|---|---|---|---|
| 0 | Normal Operation | – | 594 |
| 1 | Abrupt Increase of BSW | 101 | 128 |
| 2 | Spurious Closure of DHSV | 102 | 38 |
| 3 | Severe Slugging | – | 106 |
| 4 | Flow Instability | – | 343 |
| 5 | Rapid Productivity Loss | 105 | 450 |
| 6 | Quick Restriction in PCK | 106 | 221 |
| 7 | Scaling in PCK | 107 | 46 |
| 8 | Hydrate in Production Line | 108 | 95 |
| 9 | Hydrate in Service Line | 109 | 207 |

The per-row `class` column labels each observation: `0` for normal operation, the event label for the event's steady
state, and **event label + 100** for the transient period leading into it (only for event types that have one).

## Known quirks

- `timestamp` is `timestamp[ns]` and is stored as the pandas index, so `pandas.read_parquet` returns it as the index, not a column.
- `class` and `state` are stored as `int16`, not `int64`. Sensor columns are `float64`.
- `class` and `state` are nullable: every real (`WELL-*`) file has some unlabeled rows (12.25% of real rows in total). Simulated and hand-drawn files have none.
- All files are Brotli-compressed, so the reader needs Brotli support (pyarrow has it built in).
- Column names are the upstream Portuguese abbreviations (e.g. `P-PDG`, `T-TPT`, `ABER-CKP`); `data/dataset.ini` describes each one with its unit.
