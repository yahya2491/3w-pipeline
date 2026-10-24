import json
import os

import numpy as np
import pandas as pd

from config import LABEL_NAMES, RESULTS_DIR, SEED

MODELS = ["random_forest", "xgboost"]
N_BOOT = 2000
K = 10

preds, metrics = {}, {}
for name in MODELS:
    preds[name] = pd.read_parquet(os.path.join(RESULTS_DIR, name, "predictions.parquet"))
    with open(os.path.join(RESULTS_DIR, name, "metrics.json")) as f:
        metrics[name] = json.load(f)

# both models must have predicted the same test rows in the same order
a, b = preds["random_forest"], preds["xgboost"]
assert len(a) == len(b)
assert (a["recording_id"].to_numpy() == b["recording_id"].to_numpy()).all()
assert (a["y_true"].to_numpy() == b["y_true"].to_numpy()).all()

rec_ids, rec_idx = np.unique(a["recording_id"].to_numpy(), return_inverse=True)
rec_info = a.groupby("recording_id")[["source_type", "recording_event"]].first().loc[rec_ids].reset_index(drop=True)
is_real = (rec_info["source_type"] == "REAL").to_numpy()


def confusion_per_recording(p):
    cm = np.zeros((len(rec_ids), K, K))
    np.add.at(cm, (rec_idx, p["y_true"].to_numpy(), p["y_pred"].to_numpy()), 1)
    return cm


def macro_f1(cm):
    # works on one matrix (K, K) or a stack (B, K, K); only classes present in the truth count
    tp = np.diagonal(cm, axis1=-2, axis2=-1)
    support = cm.sum(axis=-1)
    predicted = cm.sum(axis=-2)
    f1 = np.where(support > 0, 2 * tp / np.maximum(support + predicted, 1), np.nan)
    return np.nanmean(f1, axis=-1)


cm_rec = {name: confusion_per_recording(preds[name]) for name in MODELS}

# resampling weights: how many times each recording is drawn, within each (source, event) group
rng = np.random.default_rng(SEED)
W = np.zeros((N_BOOT, len(rec_ids)))
for idx in rec_info.groupby(["source_type", "recording_event"]).indices.values():
    draws = rng.choice(idx, size=(N_BOOT, len(idx)), replace=True)
    rows = np.repeat(np.arange(N_BOOT), len(idx))
    np.add.at(W, (rows, draws.ravel()), 1)

boot = {}
for scope, weights in [("all", W), ("real", W * is_real)]:
    scores = {name: macro_f1(np.tensordot(weights, cm_rec[name], axes=(1, 0))) for name in MODELS}
    point_w = np.ones(len(rec_ids)) if scope == "all" else is_real.astype(float)
    point = {name: float(macro_f1(np.tensordot(point_w, cm_rec[name], axes=(0, 0)))) for name in MODELS}
    diff = scores["xgboost"] - scores["random_forest"]
    boot[scope] = {
        **{f"{n}_point": point[n] for n in MODELS},
        **{f"{n}_ci95": np.percentile(scores[n], [2.5, 97.5]).tolist() for n in MODELS},
        "diff_point": point["xgboost"] - point["random_forest"],
        "diff_ci95": np.percentile(diff, [2.5, 97.5]).tolist(),
        "share_xgb_ahead": float((diff > 0).mean()),
    }
    print(f"\nmacro F1 ({scope}):")
    for n in MODELS:
        lo, hi = boot[scope][f"{n}_ci95"]
        print(f"  {n:14s} {point[n]:.3f}  (95% interval {lo:.3f} to {hi:.3f})")
    lo, hi = boot[scope]["diff_ci95"]
    print(
        f"  XGBoost minus forest: {boot[scope]['diff_point']:+.3f}  (95% interval {lo:+.3f} to {hi:+.3f}), "
        f"XGBoost ahead in {boot[scope]['share_xgb_ahead']:.0%} of resamples"
    )

with open(os.path.join(RESULTS_DIR, "bootstrap.json"), "w") as f:
    json.dump(boot, f, indent=2)


# side-by-side summary for the report
def prefault_real(name):
    pf = pd.read_csv(os.path.join(RESULTS_DIR, name, "errors_prefault.csv"))
    row = pf[(pf["source_type"] == "REAL") & (pf["kind"] == "pre-fault")]
    return float(row["false_alarm"].iloc[0])


def tuning_score(m):
    # the forest saves a cross-validation score, XGBoost a validation score per depth
    if "cv_macro_f1" in m:
        return m["cv_macro_f1"]
    return max(t["val_macro_f1"] for t in m["tuning"].values())


summary = pd.DataFrame(
    {
        name: {
            "test macro F1": m["macro_f1"],
            "tuning macro F1 (CV or validation)": tuning_score(m),
            "fault recall": m["fault_recall"],
            "false alarm rate on normal": 1 - m["per_class"][LABEL_NAMES[0]]["recall"],
            "pre-fault false alarms (real)": prefault_real(name),
            **{f"macro F1 {s.lower()}": v for s, v in m["per_source_macro_f1"].items()},
            "training seconds (final fit)": m["fit_seconds"],
        }
        for name, m in metrics.items()
    }
).round(3)

recall = pd.DataFrame(
    {
        name: {f"{i} {LABEL_NAMES[i]}": m["per_class"][LABEL_NAMES[i]]["recall"] for i in range(K)}
        for name, m in metrics.items()
    }
).round(3)

summary.to_csv(os.path.join(RESULTS_DIR, "comparison_summary.csv"))
recall.to_csv(os.path.join(RESULTS_DIR, "comparison_recall_per_class.csv"))
print("\n", summary.to_string())
print("\n", recall.to_string())
