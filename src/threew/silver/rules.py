"""Strict, versioned Silver transformation settings."""

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path

import yaml

from threew.config import REPO_ROOT

DEFAULT_RULES_PATH = REPO_ROOT / "conf" / "quality" / "silver_rules.yaml"


@dataclass(frozen=True)
class QualityRules:
    version: int
    opening_min_pct: float
    opening_max_pct: float
    valve_states: tuple[float, ...]
    choke_forward_fill_max_seconds: int
    expected_interval_seconds: int
    short_window_seconds: int
    long_window_seconds: int
    minimum_observed_fraction: float
    sensors: tuple[str, ...]

    def minimum_count(self, seconds: int) -> int:
        return math.ceil(seconds / self.expected_interval_seconds * self.minimum_observed_fraction)

    def snapshot(self) -> dict:
        resolved = asdict(self)
        return {
            "version": self.version,
            "sha256": hashlib.sha256(json.dumps(resolved, sort_keys=True).encode()).hexdigest(),
            "resolved": resolved,
        }


def _mapping(value, keys, context):
    if not isinstance(value, dict) or set(value) != set(keys):
        raise ValueError(f"Silver rules {context}: expected exactly {', '.join(sorted(keys))}")
    return value


def load_rules(path: Path = DEFAULT_RULES_PATH) -> QualityRules:
    from threew.silver.reference import SENSOR_RENAMES

    raw = _mapping(yaml.safe_load(Path(path).read_text()), ("version", "domains", "cleaning", "temporal"), "root")
    domains = _mapping(raw["domains"], ("opening_min_pct", "opening_max_pct", "valve_states"), "domains")
    cleaning = _mapping(
        raw["cleaning"],
        ("choke_forward_fill_max_seconds", "missing_classes", "missing_sensors", "other_invalid_sensors"),
        "cleaning",
    )
    temporal = _mapping(
        raw["temporal"],
        (
            "expected_interval_seconds",
            "short_window_seconds",
            "long_window_seconds",
            "minimum_observed_fraction",
            "sensors",
        ),
        "temporal",
    )
    for field, expected in (
        ("missing_classes", "preserve_unknown"),
        ("missing_sensors", "preserve_missing"),
        ("other_invalid_sensors", "null_in_clean_column"),
    ):
        if cleaning[field] != expected:
            raise ValueError(f"Silver rules cleaning.{field}: only {expected!r} is supported")
    integers = {
        "version": raw["version"],
        "choke_forward_fill_max_seconds": cleaning["choke_forward_fill_max_seconds"],
        **{key: temporal[key] for key in ("expected_interval_seconds", "short_window_seconds", "long_window_seconds")},
    }
    if any(type(value) is not int or value <= 0 for value in integers.values()):
        raise ValueError("Silver rules versions, durations and intervals must be positive integers")
    if raw["version"] != 1 or temporal["expected_interval_seconds"] != 1:
        raise ValueError("Only Silver rule version 1 and the source's one-second sampling are supported")
    fraction = temporal["minimum_observed_fraction"]
    bounds = [domains["opening_min_pct"], domains["opening_max_pct"]]
    states = domains["valve_states"]
    if not isinstance(states, list) or not states:
        raise ValueError("Silver rules valve_states must be a non-empty list")
    if any(type(v) not in (int, float) or not math.isfinite(v) for v in [fraction, *bounds, *states]):
        raise ValueError("Silver rule bounds, valve states and coverage must be finite numbers")
    if not 0 < fraction <= 1 or bounds[0] >= bounds[1] or len(set(states)) != len(states):
        raise ValueError("Silver rules have invalid coverage, opening bounds or duplicate valve states")
    if temporal["short_window_seconds"] >= temporal["long_window_seconds"]:
        raise ValueError("The short window must be smaller than the long window")
    sensors = temporal["sensors"]
    continuous = {target for source, target in SENSOR_RENAMES.items() if not source.startswith("ESTADO-")}
    if (
        not isinstance(sensors, list)
        or not sensors
        or any(not isinstance(s, str) or s not in continuous for s in sensors)
        or len(set(sensors)) != len(sensors)
    ):
        raise ValueError("Silver temporal sensors must be unique, supported continuous sensor names")
    return QualityRules(
        **integers,
        opening_min_pct=float(bounds[0]),
        opening_max_pct=float(bounds[1]),
        valve_states=tuple(float(v) for v in states),
        minimum_observed_fraction=float(fraction),
        sensors=tuple(sensors),
    )
