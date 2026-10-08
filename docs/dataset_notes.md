# Dataset notes

Measured facts about the 3W dataset 2.0.0 as pinned in [`source.md`](source.md) (HF commit `c1a7e8d0…`, upstream
`d57c058a…`). Each fact says where it was measured. Facts that constrain Silver or Gold say so.

## Files

- **2,228 Parquet files, 1,873,131,359 bytes** (`scripts/hub_probe.py`). All 2,228 have a sha256 on Hugging Face and
  all match the upstream checkout byte for byte (`docs/source.md`).
- **By source type:** 1,119 real (`WELL-*`), 1,089 simulated (`SIMULATED_*`), 20 hand-drawn (`DRAWN_*`)
  (`hub_probe.py`).
- **By class and type** (`scripts/plan_preview.py --env prod`):

  | Class | Event | Real | Simulated | Drawn | Total |
  |---|---|---|---|---|---|
  | 0 | Normal Operation | 594 | – | – | 594 |
  | 1 | Abrupt Increase of BSW | 4 | 114 | 10 | 128 |
  | 2 | Spurious Closure of DHSV | 22 | 16 | – | 38 |
  | 3 | Severe Slugging | 32 | 74 | – | 106 |
  | 4 | Flow Instability | 343 | – | – | 343 |
  | 5 | Rapid Productivity Loss | 11 | 439 | – | 450 |
  | 6 | Quick Restriction in PCK | 6 | 215 | – | 221 |
  | 7 | Scaling in PCK | 36 | – | 10 | 46 |
  | 8 | Hydrate in Production Line | 14 | 81 | – | 95 |
  | 9 | Hydrate in Service Line | 57 | 150 | – | 207 |

- **Hand-drawn files exist only in classes 1 and 7, 10 in each.** Any per-source-type comparison in Silver or Gold
  can only include drawn data for those two event types.
- **Classes 0 and 4 are real-only, and class 7 has no simulated files.** A real-vs-simulated comparison can't include
  those three classes.
- **Real files are scarce in some classes:** 4 in class 1 and 6 in class 6.

## Names and keys

- **The path is the unique key, not the instance ID.** 225 instance IDs appear in more than one label folder:
  215 simulated IDs covering 865 files and 10 drawn IDs covering all 20 drawn files; no real ID repeats
  (`hub_probe.py`). No two files are byte-identical, so these are different instances numbered from `00001` in each
  folder, not duplicates. Never join or deduplicate on `instance_id` alone.
- **Wells:** 40 distinct, numbered 1 to 42; wells 17 and 18 don't exist (`hub_probe.py`).
- **Real start times** range from 2011-08-30 23:53:21 to 2023-09-18 17:01:06. The timestamp in a `WELL-*` file name
  equals the first `timestamp` in the file for all 1,119 real files (checked against the upstream checkout).
- **Known limitation: no timezone.** 3W doesn't say which timezone its timestamps use. Bronze keeps them naive, in
  file names and data; Silver decides how to handle them.

## Contents

Measured on all 2,228 files of the upstream checkout with pyarrow (Parquet footers and column statistics).

- **Rows:** 40,748,658 simulated, 32,872,250 real, 2,966,410 drawn.
- **One schema:** all 2,228 files have the same 30 columns with the same types.
- **Types:** `timestamp` is `timestamp[ns]` and stored as the pandas index; `class` and `state` are `int16`; the
  27 sensor columns are `float64`. Every file is Brotli-compressed.
- **Unlabeled rows:** `class` and `state` are null in every real file, 4,028,400 rows in total (12.25% of real rows).
  Simulated and drawn files have no nulls in these columns.
- **Label values per folder:** the per-row `class` is 0 (normal), the folder's label (steady state) or label + 100
  (transient). Classes 1, 2 and 5 to 9 have transients; classes 3 and 4 have none and contain only their own label,
  never 0.
