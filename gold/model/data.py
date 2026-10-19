from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.utils.class_weight import compute_sample_weight

from config import EXPECTED_N_FEATURES, FEATURE_SUFFIXES, GOLD_PATH, NOT_FEATURES


@dataclass
class GoldData:
    X_train: pd.DataFrame
    y_train: pd.Series
    w_train: pd.Series  # sample weights for class balance
    groups_train: pd.Series  # recording_id, for grouped cross-validation later
    X_test: pd.DataFrame
    y_test: pd.Series
    meta_test: pd.DataFrame  # recording_id and source_type, for per-source results
    feature_cols: list


def load_gold(path=GOLD_PATH):
    df = pd.read_parquet(path)

    feature_cols = sorted(c for c in df.columns if c.endswith(FEATURE_SUFFIXES))

    # leakage guard and sanity checks
    assert len(feature_cols) == EXPECTED_N_FEATURES, f"expected {EXPECTED_N_FEATURES} features, got {len(feature_cols)}"
    assert not set(feature_cols) & NOT_FEATURES, "a bookkeeping column ended up in the features"
    assert set(df["split"].unique()) == {"train", "test"}
    assert sorted(df["label"].unique()) == list(range(10)), "labels must be 0 to 9"

    train = df[df["split"] == "train"]
    test = df[df["split"] == "test"]
    assert not set(train["recording_id"]) & set(test["recording_id"]), "a recording is in both splits"

    X_train = train[feature_cols].astype("float32")
    X_test = test[feature_cols].astype("float32")
    # NaN is allowed (missing data), infinity is not
    assert not np.isinf(X_train.to_numpy()).any(), "infinite value in train features"
    assert not np.isinf(X_test.to_numpy()).any(), "infinite value in test features"

    y_train = train["label"].astype(int)

    return GoldData(
        X_train=X_train,
        y_train=y_train,
        w_train=pd.Series(compute_sample_weight("balanced", y_train), index=y_train.index),
        groups_train=train["recording_id"],
        X_test=X_test,
        y_test=test["label"].astype(int),
        meta_test=test[["recording_id", "source_type"]],
        feature_cols=feature_cols,
    )
