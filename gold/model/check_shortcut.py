from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score, recall_score

from config import SEED
from data import load_gold

d = load_gold()

# one yes/no column per sensor: is its 120-second mean missing?
cols = [c for c in d.feature_cols if c.endswith("_mean_120s")]
Xm_train = d.X_train[cols].isna().astype(int)
Xm_test = d.X_test[cols].isna().astype(int)

model = RandomForestClassifier(n_estimators=100, min_samples_leaf=50, n_jobs=4, random_state=SEED)
model.fit(Xm_train, d.y_train, sample_weight=d.w_train)
pred = model.predict(Xm_test)

print("missingness only, macro F1:", round(f1_score(d.y_test, pred, average="macro"), 3))
print("fault recall:", round(recall_score(d.y_test != 0, pred != 0), 3))
for src in sorted(d.meta_test["source_type"].unique()):
    m = (d.meta_test["source_type"] == src).to_numpy()
    present = sorted(d.y_test[m].unique())  # same per-source definition as evaluate()
    print(f"  {src}: macro F1 {f1_score(d.y_test[m], pred[m], labels=present, average='macro', zero_division=0):.3f}")
