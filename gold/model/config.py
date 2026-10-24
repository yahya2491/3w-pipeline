import os

# paths can be changed with environment variables, e.g. on a laptop for DAT540
GOLD_PATH = os.environ.get("GOLD_PATH", "/mnt/silver-data/gold/gold_table")
RESULTS_DIR = os.environ.get("RESULTS_DIR", "results")

SEED = 42

FEATURE_SUFFIXES = ("_mean_120s", "_stddev_120s", "_change")
EXPECTED_N_FEATURES = 21

# columns that must never be used as features
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
