import os
import sys

import pandas as pd

from config import RESULTS_DIR

name = sys.argv[1] if len(sys.argv) > 1 else "random_forest"
out = os.path.join(RESULTS_DIR, name)
p = pd.read_parquet(os.path.join(out, "predictions.parquet"))
p["correct"] = p["y_true"] == p["y_pred"]

# 1. recall per source and class
by_src = p.groupby(["source_type", "y_true"]).agg(rows=("correct", "size"), recall=("correct", "mean")).round(3)
by_src.to_csv(os.path.join(out, "errors_by_source_class.csv"))
print("recall per source and class:\n", by_src.to_string())

# 2. accuracy per test recording
rec = (
    p.groupby(["recording_id", "source_type", "recording_event", "well_id"], dropna=False)
    .agg(rows=("correct", "size"), accuracy=("correct", "mean"))
    .reset_index()
)
rec.to_csv(os.path.join(out, "errors_by_recording.csv"), index=False)
bands = pd.cut(rec["accuracy"], [0, 0.5, 0.8, 0.95, 1.0], include_lowest=True)
print("\ntest recordings per accuracy band:\n", rec.groupby(["source_type", bands], observed=True).size().to_string())
print("\n10 worst recordings:\n", rec.sort_values("accuracy").head(10).to_string(index=False))

# 3. real data per well
real = p[p["source_type"] == "REAL"]
well = real.groupby("well_id").agg(rows=("correct", "size"), accuracy=("correct", "mean"))
well["normal_false_alarm"] = real[real["y_true"] == 0].groupby("well_id")["y_pred"].apply(lambda s: (s != 0).mean())
well = well.round(3)
well.to_csv(os.path.join(out, "errors_by_well.csv"))
print("\nreal data per well:\n", well.to_string())

# 4. are the big errors concentrated in a few recordings?
for label in [0, 8]:
    err = p[(p["y_true"] == label) & ~p["correct"]]
    share = err["recording_id"].value_counts().head(3).sum() / len(err)
    print(f"\nlabel {label}: {len(err)} wrong rows, the 3 worst recordings hold {share:.0%} of them")
