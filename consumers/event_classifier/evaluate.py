import json
import os
import subprocess

import numpy as np
import pandas as pd
from matplotlib.figure import Figure  # draws straight to a file, no screen or backend needed
from sklearn.inspection import permutation_importance
from sklearn.metrics import (
    ConfusionMatrixDisplay,
    classification_report,
    confusion_matrix,
    f1_score,
    recall_score,
)

from config import LABEL_NAMES, RESULTS_DIR, SEED

LABELS = list(range(10))


def _out_dir(model_name):
    path = os.path.join(RESULTS_DIR, model_name)
    os.makedirs(path, exist_ok=True)
    return path


def evaluate(model_name, y_true, y_pred, meta, extra=None):
    out = _out_dir(model_name)
    y_true = np.asarray(y_true)
    y_pred = np.asarray(y_pred)
    source = meta["source_type"].to_numpy()

    per_source = {}
    for src in np.unique(source):
        m = source == src
        present = sorted(np.unique(y_true[m]))
        per_source[src] = f1_score(y_true[m], y_pred[m], labels=present, average="macro", zero_division=0)

    metrics = {
        "macro_f1": f1_score(y_true, y_pred, labels=LABELS, average="macro", zero_division=0),
        "fault_recall": recall_score(y_true != 0, y_pred != 0),
        "per_source_macro_f1": per_source,
        "per_class": classification_report(
            y_true,
            y_pred,
            labels=LABELS,
            target_names=[LABEL_NAMES[i] for i in LABELS],
            output_dict=True,
            zero_division=0,
        ),
        **(extra or {}),
    }
    try:
        commit = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
    except (OSError, subprocess.CalledProcessError):  # git not installed, or not run inside a git checkout
        commit = "unknown"
    metrics["git_commit"] = commit

    # row-level predictions for error analysis; output data, not committed (*.parquet is in .gitignore)
    preds = meta.copy()
    preds["y_true"] = y_true
    preds["y_pred"] = y_pred
    preds.to_parquet(os.path.join(out, "predictions.parquet"), index=False)

    with open(os.path.join(out, "metrics.json"), "w") as f:
        json.dump(metrics, f, indent=2, default=float)

    # confusion matrix, each row divided by its total, so the diagonal is recall per class
    cm = confusion_matrix(y_true, y_pred, labels=LABELS, normalize="true")
    pd.DataFrame(cm, index=LABELS, columns=LABELS).round(3).to_csv(os.path.join(out, "confusion_matrix.csv"))
    fig = Figure(figsize=(9, 8))
    ax = fig.subplots()
    ConfusionMatrixDisplay(cm, display_labels=LABELS).plot(ax=ax, values_format=".2f", colorbar=False)
    ax.set_title(f"{model_name}: confusion matrix (share of true class)")
    fig.tight_layout()
    fig.savefig(os.path.join(out, "confusion_matrix.png"), dpi=150)

    print(f"\n{model_name}: macro F1 {metrics['macro_f1']:.3f}, fault recall {metrics['fault_recall']:.3f}")
    print("per source macro F1:", {k: round(v, 3) for k, v in per_source.items()})
    print("recall per class:", {i: round(float(cm[i, i]), 3) for i in LABELS})
    return metrics


def save_importance(model_name, model, X, y, n_rows=30_000):
    X_s = X.sample(n=min(n_rows, len(X)), random_state=SEED)
    y_s = y.loc[X_s.index]
    pi = permutation_importance(model, X_s, y_s, scoring="f1_macro", n_repeats=5, random_state=SEED, n_jobs=1)
    imp = pd.DataFrame(
        {"feature": X.columns, "drop_mean": pi.importances_mean, "drop_std": pi.importances_std}
    ).sort_values("drop_mean", ascending=False)
    imp.to_csv(os.path.join(_out_dir(model_name), "permutation_importance.csv"), index=False)
    print("\ntop 8 features by permutation importance:")
    print(imp.head(8).to_string(index=False))
    return imp
