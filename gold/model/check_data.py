from config import LABEL_NAMES
from data import load_gold

d = load_gold()

print("train:", d.X_train.shape, " test:", d.X_test.shape)
print("features:", d.feature_cols)

print("\nshare of missing values per feature (train):")
print(d.X_train.isna().mean().sort_values(ascending=False).round(3))

print("\nrows and weight per class (train):")
summary = d.y_train.value_counts().sort_index().to_frame("rows")
summary["weight"] = d.w_train.groupby(d.y_train).first().round(3)
summary["name"] = summary.index.map(LABEL_NAMES)
print(summary)
