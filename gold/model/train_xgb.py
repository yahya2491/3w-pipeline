import time

import numpy as np
import xgboost
from sklearn.metrics import f1_score
from sklearn.model_selection import StratifiedGroupKFold
from xgboost import XGBClassifier

from config import SEED
from data import load_gold
from evaluate import evaluate, save_importance

d = load_gold()

# 1. validation split inside train: 20% of training recordings, grouped and stratified
splitter = StratifiedGroupKFold(n_splits=5, shuffle=True, random_state=SEED)
fit_idx, val_idx = next(splitter.split(d.X_train, d.y_train, groups=d.groups_train))
X_fit, y_fit, w_fit = d.X_train.iloc[fit_idx], d.y_train.iloc[fit_idx], d.w_train.iloc[fit_idx]
X_val, y_val, w_val = d.X_train.iloc[val_idx], d.y_train.iloc[val_idx], d.w_train.iloc[val_idx]
assert not set(d.groups_train.iloc[fit_idx]) & set(d.groups_train.iloc[val_idx])

COMMON = {
    "learning_rate": 0.1,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "tree_method": "hist",
    "n_jobs": 4,
    "random_state": SEED,
}

# 2. tune max_depth with early stopping on the validation recordings
tuning = {}
t0 = time.time()
for depth in [4, 6, 8]:
    m = XGBClassifier(n_estimators=2000, max_depth=depth, early_stopping_rounds=50, eval_metric="mlogloss", **COMMON)
    m.fit(X_fit, y_fit, sample_weight=w_fit, eval_set=[(X_val, y_val)], sample_weight_eval_set=[w_val], verbose=False)
    f1 = f1_score(y_val, m.predict(X_val), average="macro")
    tuning[depth] = {"val_macro_f1": f1, "best_iteration": int(m.best_iteration)}
    print(f"max_depth {depth}: val macro F1 {f1:.3f}, best iteration {m.best_iteration}", flush=True)
tune_s = time.time() - t0

# best_iteration counts from 0, hence the + 1; 25% more trees because the final model sees all of train
best_depth = max(tuning, key=lambda k: tuning[k]["val_macro_f1"])
n_trees = int(np.ceil((tuning[best_depth]["best_iteration"] + 1) * 1.25))
print(f"best max_depth {best_depth}, final model with {n_trees} trees", flush=True)

# 3. final model on all of train
model = XGBClassifier(n_estimators=n_trees, max_depth=best_depth, **COMMON)
t0 = time.time()
model.fit(d.X_train, d.y_train, sample_weight=d.w_train, verbose=False)
fit_s = time.time() - t0

# 4. test set, used once
evaluate(
    "xgboost",
    d.y_test,
    model.predict(d.X_test),
    d.meta_test,
    extra={
        "best_params": {
            "max_depth": best_depth,
            "n_estimators": n_trees,
            **{k: v for k, v in COMMON.items() if k != "n_jobs"},
        },
        "tuning": {str(k): v for k, v in tuning.items()},
        "tune_seconds": tune_s,
        "fit_seconds": fit_s,
        "xgboost_version": xgboost.__version__,
    },
)
save_importance("xgboost", model, d.X_test, d.y_test)
