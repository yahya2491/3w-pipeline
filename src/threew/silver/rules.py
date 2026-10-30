"""Strict, versioned Silver transformation settings."""

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path

import yaml

from threew.config import REPO_ROOT

DEFAULT_RULES_PATH = REPO_ROOT / "conf" / "quality" / "silver_rules.yaml"
FEATURE_SENSORS = (
    "downhole_pdg_pressure_pa",
    "subsea_tree_tpt_pressure_pa",
    "production_choke_upstream_pressure_pa",
    "gas_lift_choke_downstream_pressure_pa",
    "subsea_tree_tpt_temperature_c",
    "production_choke_downstream_temperature_c",
    "gas_lift_flow_rate_m3_s",
)


@dataclass(frozen=True)
class QualityRules:
    version: int
    opening_min_pct: float
    opening_max_pct: float
    valve_states: tuple[float, ...]
    choke_forward_fill_max_seconds: int
    expected_interval_seconds: int
    feature_window_seconds: int = 120  # The exported feature names assume this window length.
    feature_sensors: tuple[str, ...] = FEATURE_SENSORS
    missing_classes: str = "backfill_leading_and_matching"  # Fill leading gaps and matching internal gaps by default.

    def snapshot(self) -> dict:
        resolved = asdict(self)  # Save the settings we actually used, including defaults.
        return {
            "version": self.version,
            "sha256": hashlib.sha256(json.dumps(resolved, sort_keys=True).encode()).hexdigest(),
            "resolved": resolved,
        }


def _mapping(value, keys, context):
    if not isinstance(value, dict) or set(value) != set(keys):  # Reject typos or missing settings instead of guessing.
        raise ValueError(f"Silver rules {context}: expected exactly {', '.join(sorted(keys))}")
    return value


def load_rules(path: Path = DEFAULT_RULES_PATH) -> QualityRules:
    raw = _mapping(  # Read the rules and check the top-level structure first.
        yaml.safe_load(Path(path).read_text()), ("version", "domains", "cleaning", "sequence", "features"), "root"
    )
    domains = _mapping(raw["domains"], ("opening_min_pct", "opening_max_pct", "valve_states"), "domains")
    cleaning = _mapping(
        raw["cleaning"],
        ("choke_forward_fill_max_seconds", "missing_classes", "missing_sensors", "other_invalid_sensors"),
        "cleaning",
    )
    sequence = _mapping(raw["sequence"], ("expected_interval_seconds",), "sequence")
    features = _mapping(raw["features"], ("window_seconds", "sensors"), "features")
    if not isinstance(features["sensors"], list) or tuple(features["sensors"]) != FEATURE_SENSORS:
        raise ValueError("Silver features.sensors must contain the seven contract sensors in the documented order")
    if cleaning["missing_classes"] not in ("preserve_unknown", "backfill_leading", "backfill_leading_and_matching"):
        raise ValueError(
            "Silver rules cleaning.missing_classes: expected preserve_unknown, backfill_leading or backfill_leading_and_matching"
        )
    for field, expected in (
        ("missing_sensors", "preserve_missing"),
        ("other_invalid_sensors", "null_in_clean_column"),
    ):
        if cleaning[field] != expected:
            raise ValueError(f"Silver rules cleaning.{field}: only {expected!r} is supported")
    integers = {
        "version": raw["version"],
        "choke_forward_fill_max_seconds": cleaning["choke_forward_fill_max_seconds"],
        "expected_interval_seconds": sequence["expected_interval_seconds"],
        "feature_window_seconds": features["window_seconds"],
    }
    if any(type(value) is not int or value <= 0 for value in integers.values()):
        raise ValueError("Silver rules versions, durations and intervals must be positive integers")
    if raw["version"] != 1 or sequence["expected_interval_seconds"] != 1:
        raise ValueError("Only Silver rule version 1 and the source's one-second sampling are supported")
    if features["window_seconds"] != 120:  # A different history length would need a contract change.
        raise ValueError("The current Silver contract supports a 120-second feature window")
    bounds = [domains["opening_min_pct"], domains["opening_max_pct"]]
    states = domains["valve_states"]
    if not isinstance(states, list) or not states:
        raise ValueError("Silver rules valve_states must be a non-empty list")
    if (
        any(type(v) not in (int, float) or not math.isfinite(v) for v in [*bounds, *states])
        or bounds[0] >= bounds[1]
        or len(set(states)) != len(states)  # Repeated valve codes usually mean the rules file has a mistake.
    ):
        raise ValueError("Silver rules have invalid opening bounds or duplicate valve states")
    return QualityRules(
        **integers,
        opening_min_pct=float(bounds[0]),
        opening_max_pct=float(bounds[1]),
        valve_states=tuple(float(v) for v in states),  # Normalise numeric types before saving the resolved rules.
        feature_sensors=tuple(features["sensors"]),
        missing_classes=cleaning["missing_classes"],
    )
