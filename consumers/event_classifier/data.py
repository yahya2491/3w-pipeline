import json
from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.utils.class_weight import compute_sample_weight

from config import (
    EXPECTED_N_FEATURES,
    FEATURE_SUFFIXES,
    GOLD_CONTRACT_VERSION,
    GOLD_READY,
    NOT_FEATURES,
    SEED,
    TRAIN_CAP,
)


@dataclass
class GoldData:
    X_train: pd.DataFrame
    y_train: pd.Series
    w_train: pd.Series  # sample weights for class balance
    groups_train: pd.Series  # recording_id, for grouped cross-validation later
    X_test: pd.DataFrame
    y_test: pd.Series
    meta_test: pd.DataFrame  # recording_id, source_type, well_id, recording_event: for error analysis
    feature_cols: list


def window_features_path(ready=GOLD_READY):
    """The window_features folder of the published Gold run; refuse a build that failed or changed contract."""
    with open(ready) as f:
        marker = json.load(f)
    assert marker["passed"], "gold_ready.json points to a build that did not pass its checks"
    assert marker["contract_version"] == GOLD_CONTRACT_VERSION, f"Gold contract {marker['contract_version']} is new"
    print(f"Gold run {marker['run_id']} (Silver run {marker['silver']['run_id']})")
    return marker["tables"]["window_features"]["path"]


def cap_per_class(df, cap=TRAIN_CAP):
    """Keep at most `cap` rows of each label, chosen at random with a fixed seed; smaller classes keep everything."""
    shuffled = df.sample(frac=1, random_state=SEED)
    return shuffled[shuffled.groupby("label").cumcount() < cap]


def load_gold(ready=GOLD_READY):
    df = pd.read_parquet(window_features_path(ready))

    feature_cols = sorted(c for c in df.columns if c.endswith(FEATURE_SUFFIXES))

    # leakage guard and sanity checks
    assert len(feature_cols) == EXPECTED_N_FEATURES, f"expected {EXPECTED_N_FEATURES} features, got {len(feature_cols)}"
    assert not set(feature_cols) & NOT_FEATURES, "a bookkeeping column ended up in the features"
    assert set(df["split"].unique()) == {"train", "test"}
    assert sorted(df["label"].unique()) == list(range(10)), "labels must be 0 to 9"

    # balancing is the consumer's choice: cap the training classes, but test on every test window
    train = cap_per_class(df[df["split"] == "train"]).sort_index()
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
        meta_test=test[["recording_id", "source_type", "well_id", "recording_event"]],
        feature_cols=feature_cols,
    )
