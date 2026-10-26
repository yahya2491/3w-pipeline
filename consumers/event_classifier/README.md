# Event-classifier demo (a Gold consumer)

This folder is a **consumer** of Gold, not part of it. It shows that Gold's `window_features` table can be used as
delivered: a Random Forest and an XGBoost model are trained on it and compared on held-out recordings.
The table itself is defined in the [Gold contract](../../docs/gold/GOLD_CONTRACT.md).

## What it reads

Only `gold_ready.json`, and the `window_features` folder that marker lists. The loader refuses a marker whose
build did not pass its checks or whose contract version differs from the one this code was written for
(`config.py`). It never reads Silver.

Choices made here, not in Gold:
- **Class cap:** at most 200,000 training windows per class (random, seed 42). The test split is used in full.
- **Class weights:** "balanced" sample weights, so rare events count as much as normal operation.
- **Predictors:** the 21 feature columns. Labels, recording and well IDs, the source type and the split are excluded.

## Run on the VM

Uses its own environment, `~/venvs/3w-model` (`pip install -r requirements.txt`), separate from the locked pipeline
environment. Run from this folder:

```bash
cd consumers/event_classifier
python check_data.py                    # class counts, missing features
python train_rf.py                      # grouped CV tuning, final fit, test scores
python analyze_errors.py random_forest
python train_xgb.py                     # validation-recording tuning, final fit, test scores
python analyze_errors.py xgboost
python check_shortcut.py                # can missing-sensor patterns alone predict the event?
python compare_models.py                # recording-level bootstrap and side-by-side summary
```

`GOLD_READY` and `RESULTS_DIR` change the input marker and output folder. Results are saved in `results/`:
per model `metrics.json`, the confusion matrix, errors by recording, well and source, pre-fault false alarms and
permutation importance; plus `bootstrap.json` and the two comparison tables.
