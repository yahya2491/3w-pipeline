"""Settings for the classifier demo: where Gold is, what the features are, and the event names."""

import os

# The demo reads Gold only through its ready marker, as the Gold contract asks of every consumer.
# Paths can be changed with environment variables, e.g. GOLD_READY=data/dev/gold/gold_ready.json on a laptop.
GOLD_READY = os.environ.get("GOLD_READY", "/mnt/silver-data/gold/gold_ready.json")
GOLD_CONTRACT_VERSION = 1  # the window_features columns this code was written for
RESULTS_DIR = os.environ.get("RESULTS_DIR", "results")  # relative to the folder the scripts are run from

SEED = 42  # one seed for the class cap, the CV folds, the models and the bootstrap
TRAIN_CAP = 200_000  # at most this many training windows per class, so the largest classes don't dominate

# the predictors: Gold gives 3 statistics for each of 7 sensors
FEATURE_SUFFIXES = ("_mean_120s", "_stddev_120s", "_change")
EXPECTED_N_FEATURES = 21  # 7 x 3; load_gold() stops if the table has a different number

# columns that must never be used as features: ids, labels and the split would leak the answer
NOT_FEATURES = {
    "recording_id",
    "source_type",
    "well_id",
    "_row_in_file",
    "class_code",
    "class_code_clean",
    "class_was_inferred",
    "label",
    "recording_event",
    "split",
}

# event names exactly as in Silver's event_reference table
LABEL_NAMES = {
    0: "Normal Operation",
    1: "Abrupt Increase of BSW",
    2: "Spurious Closure of DHSV",
    3: "Severe Slugging",
    4: "Flow Instability",
    5: "Rapid Productivity Loss",
    6: "Quick Restriction in PCK",
    7: "Scaling in PCK",
    8: "Hydrate in Production Line",
    9: "Hydrate in Service Line",
}
