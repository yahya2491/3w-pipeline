"""Shared 3W vocabulary; published tables and transformations use these definitions.

Meanings/domains: docs/bronze/BRONZE_REFERENCE.md sections 6 and 7.
Opening percentages use 0..100; valve states use the documented 0, 0.5, 1.
No physical limits are inferred for pressure, temperature, flow or state_code.
"""

from pyspark.sql import SparkSession

REFERENCE_VERSION = 1
REFERENCE_SOURCE = "docs/bronze/BRONZE_REFERENCE.md sections 6 and 7"
SOURCE_TYPES = ("REAL", "SIMULATED", "DRAWN")

SENSOR_RENAMES = {
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
TRANSIENT_LABELS = (1, 2, 5, 6, 7, 8, 9)


NORMAL_LABELS = (0, *TRANSIENT_LABELS)


def sensor_definition(source_name: str, sensor_name: str) -> dict:
    """Document the source unit and only domains justified by the source meaning."""
    allowed_values = None
    minimum = maximum = None
    if source_name.startswith("ESTADO-"):
        unit, allowed_values = "source_code", [0.0, 0.5, 1.0]
    elif source_name.startswith("ABER-"):
        unit, minimum, maximum = "percent", 0.0, 100.0
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
EVENT_REFERENCE_SCHEMA = (
    "event_code int, event_name string, supports_normal boolean, supports_steady boolean, "
    "supports_transient boolean, steady_class_code int, transient_class_code int, "
    "definition_source string, reference_version int"
)
SENSOR_REFERENCE_SCHEMA = (
    "source_name string, sensor_name string, meaning string, unit string, allowed_values array<double>, "
    "minimum double, maximum double, definition_source string, reference_version int"
)


def build_reference_tables(spark: SparkSession) -> dict:
    """Ten event definitions and 27 sensor definitions, ready for Spark/SQL consumers."""
    events = [
        {
            "event_code": code,
            "event_name": name,
            "supports_normal": code in NORMAL_LABELS,
            "supports_steady": code != 0,
            "supports_transient": code in TRANSIENT_LABELS,
            "steady_class_code": code if code != 0 else None,
            "transient_class_code": 100 + code if code in TRANSIENT_LABELS else None,
            "definition_source": REFERENCE_SOURCE,
            "reference_version": REFERENCE_VERSION,
        }
        for code, name in EVENT_NAMES.items()
    ]
    return {
        "event_reference": spark.createDataFrame(events, EVENT_REFERENCE_SCHEMA),
        "sensor_reference": spark.createDataFrame(list(SENSOR_DEFINITIONS), SENSOR_REFERENCE_SCHEMA),
    }
