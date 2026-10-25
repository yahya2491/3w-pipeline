"""The public Silver observation projection; raw readings remain in Bronze."""

from threew.silver.reference import OPENINGS, SENSOR_FIELDS
from threew.silver.rules import FEATURE_SENSORS
from threew.silver.transform import add_event_descriptions

CONTRACT_VERSION = 4
CONTEXT_COLUMNS = (
    "source_type",
    "instance_label",
    "state_code",
    "timestamp",
    "_source_path",
    "_file_sha256",
    "_row_in_file",
    "instance_id",
    "well_id",
    "instance_start",
)
OUTPUT_COLUMNS = (
    *CONTEXT_COLUMNS,
    "class_code_clean",
    "class_was_inferred",
    "instance_event_name",
    "row_event_name",
    "row_event_phase",
    "class_label_status",
    *(f"{sensor}_clean" for sensor in SENSOR_FIELDS),
    *(f"{opening}_{suffix}" for opening in OPENINGS for suffix in ("was_imputed", "source_age_seconds")),
    "sequence_usable",
    "sequence_segment_id",
    *(
        f"{sensor}_{suffix}"
        for sensor in FEATURE_SENSORS
        for suffix in ("mean_120s", "stddev_120s", "change", "count_120s")
    ),
    *(f"{opening}_invalid_duration_seconds" for opening in OPENINGS),
)


def export_observations(prepared):
    """Save one clean value per sensor/class and describe the exported class."""
    return add_event_descriptions(prepared, "class_code_clean").select(*OUTPUT_COLUMNS)
