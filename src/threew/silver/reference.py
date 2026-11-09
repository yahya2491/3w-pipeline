"""Shared 3W vocabulary; published tables and transformations use these definitions.

Meanings/domains: docs/bronze/BRONZE_REFERENCE.md sections 6 and 7.
Opening percentages use 0..100; valve states use the documented 0, 0.5, 1.
No physical limits are inferred for pressure, temperature, flow or state_code.
"""

from pyspark.sql import SparkSession

REFERENCE_VERSION = 1
REFERENCE_SOURCE = "docs/bronze/BRONZE_REFERENCE.md sections 6 and 7"
SOURCE_TYPES = ("REAL", "SIMULATED", "DRAWN")
RECORDING_KEY = ("_source_path", "_file_sha256")  # Names alone are not unique across event folders.
OBSERVATION_KEY = (*RECORDING_KEY, "_row_in_file")  # Keep row identity even when timestamps repeat.

SENSOR_RENAMES = {  # Translate source abbreviations while retaining their original measurement units.
    "ABER-CKGL": "gas_lift_choke_opening_pct",
    "ABER-CKP": "production_choke_opening_pct",
    "ESTADO-DHSV": "downhole_safety_valve_state",
    "ESTADO-M1": "production_master_valve_state",
    "ESTADO-M2": "annulus_master_valve_state",
    "ESTADO-PXO": "pig_crossover_valve_state",
    "ESTADO-SDV-GL": "gas_lift_shutdown_valve_state",
    "ESTADO-SDV-P": "production_shutdown_valve_state",
    "ESTADO-W1": "production_wing_valve_state",
    "ESTADO-W2": "annulus_wing_valve_state",
    "ESTADO-XO": "crossover_valve_state",
    "P-ANULAR": "annulus_pressure_pa",
    "P-JUS-BS": "service_pump_downstream_pressure_pa",
    "P-JUS-CKGL": "gas_lift_choke_downstream_pressure_pa",
    "P-JUS-CKP": "production_choke_downstream_pressure_pa",
    "P-MON-CKGL": "gas_lift_choke_upstream_pressure_pa",
    "P-MON-CKP": "production_choke_upstream_pressure_pa",
    "P-MON-SDV-P": "production_shutdown_valve_upstream_pressure_pa",
    "P-PDG": "downhole_pdg_pressure_pa",
    "PT-P": "subsea_production_line_pressure_pa",
    "P-TPT": "subsea_tree_tpt_pressure_pa",
    "QBS": "service_pump_flow_rate_m3_s",
    "QGL": "gas_lift_flow_rate_m3_s",
    "T-JUS-CKP": "production_choke_downstream_temperature_c",
    "T-MON-CKP": "production_choke_upstream_temperature_c",
    "T-PDG": "downhole_pdg_temperature_c",
    "T-TPT": "subsea_tree_tpt_temperature_c",
}
SENSOR_FIELDS = tuple(SENSOR_RENAMES.values())
OPENINGS = tuple(target for source, target in SENSOR_RENAMES.items() if source.startswith("ABER-"))

EVENT_NAMES = {
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
TRANSIENT_LABELS = (1, 2, 5, 6, 7, 8, 9)  # Events 3 and 4 have no documented transient class.


NORMAL_LABELS = (0, *TRANSIENT_LABELS)  # Events 3 and 4 do not include normal periods in their recordings.


def sensor_definition(source_name: str, sensor_name: str) -> dict:
    """Document the source unit and only domains justified by the source meaning."""
    allowed_values = None
    minimum = maximum = None  # Do not invent physical limits for pressure, temperature or flow.
    if source_name.startswith("ESTADO-"):
        unit, allowed_values = "source_code", [0.0, 0.5, 1.0]  # Valve states are codes, not percentages.
    elif source_name.startswith("ABER-"):
        unit, minimum, maximum = "percent", 0.0, 100.0  # Only openings have a documented continuous range here.
    elif sensor_name.endswith("_pa"):
        unit = "Pa"
    elif sensor_name.endswith("_m3_s"):
        unit = "m3/s"
    else:
        unit = "degrees C"
    return {
        "source_name": source_name,
        "sensor_name": sensor_name,
        "meaning": sensor_name.removesuffix("_m3_s")
        .removesuffix("_pa")
        .removesuffix("_c")
        .removesuffix("_pct")
        .replace("_", " "),
        "unit": unit,
        "allowed_values": allowed_values,
        "minimum": minimum,
        "maximum": maximum,
        "definition_source": REFERENCE_SOURCE,
        "reference_version": REFERENCE_VERSION,
    }


SENSOR_DEFINITIONS = tuple(sensor_definition(source, name) for source, name in SENSOR_RENAMES.items())


def sensor_definitions(rules=None):
    """Resolve domain rules once for validation, cleaning and published references."""
    if rules is None:  # Use source defaults when no explicit rules were supplied.
        return SENSOR_DEFINITIONS
    return tuple(
        {
            **definition,
            "allowed_values": list(rules.valve_states) if definition["allowed_values"] is not None else None,
            "minimum": rules.opening_min_pct if definition["minimum"] is not None else None,
            "maximum": rules.opening_max_pct if definition["maximum"] is not None else None,
        }
        for definition in SENSOR_DEFINITIONS
    )


EVENT_REFERENCE_SCHEMA = (
    "event_code int, event_name string, supports_normal boolean, supports_steady boolean, "
    "supports_transient boolean, steady_class_code int, transient_class_code int, "
    "definition_source string, reference_version int"
)
SENSOR_REFERENCE_SCHEMA = (
    "source_name string, sensor_name string, meaning string, unit string, allowed_values array<double>, "
    "minimum double, maximum double, definition_source string, reference_version int"
)


def build_reference_tables(spark: SparkSession, rules=None) -> dict:
    """Ten event definitions and 27 sensor definitions, ready for Spark/SQL consumers."""
    events = [
        {
            "event_code": code,
            "event_name": name,
            "supports_normal": code in NORMAL_LABELS,
            "supports_steady": code != 0,
            "supports_transient": code in TRANSIENT_LABELS,
            "steady_class_code": code if code != 0 else None,
            "transient_class_code": 100 + code if code in TRANSIENT_LABELS else None,  # For example, Event 5 uses 105.
            "definition_source": REFERENCE_SOURCE,
            "reference_version": REFERENCE_VERSION,
        }
        for code, name in EVENT_NAMES.items()
    ]
    return {
        "event_reference": spark.createDataFrame(events, EVENT_REFERENCE_SCHEMA),
        "sensor_reference": spark.createDataFrame(list(sensor_definitions(rules)), SENSOR_REFERENCE_SCHEMA),
    }
