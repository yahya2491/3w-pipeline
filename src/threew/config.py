"""Pipeline configuration: conf/base.yaml with conf/<env>.yaml deep-merged on top.

from threew.config import load_config
cfg = load_config("dev")
cfg.storage.bronze, cfg.ingest.workers, cfg.to_dict()
"""

import dataclasses
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CONF_DIR = REPO_ROOT / "conf"
SHA_RE = re.compile(r"[0-9a-f]{40}")


class ConfigError(ValueError):
    """The configuration files are invalid."""


@dataclass(frozen=True)
class SourceConfig:
    repo_id: str
    repo_type: str
    revision: str
    revision_label: str
    include: tuple[str, ...]


@dataclass(frozen=True)
class StorageConfig:
    root: Path
    # Derived from root (not settable in YAML), so the rest of the code never builds paths by hand
    landing: Path = field(init=False)
    bronze: Path = field(init=False)
    state: Path = field(init=False)
    runs: Path = field(init=False)
    # Downloads go to <staging>/<run_id>/ first. Same filesystem as landing, so step 13's os.replace is atomic
    staging: Path = field(init=False)
    # Written only after a fully successful run; next to the table, not inside it, so Spark never reads it
    bronze_ready: Path = field(init=False)

    def __post_init__(self) -> None:
        # object.__setattr__ because the dataclass is frozen
        object.__setattr__(self, "landing", self.root / "landing" / "3w")
        object.__setattr__(self, "bronze", self.root / "bronze" / "3w_observations")
        object.__setattr__(self, "state", self.root / "bronze" / "_state")
        object.__setattr__(self, "runs", self.root / "bronze" / "_runs")
        object.__setattr__(self, "staging", self.root / "_staging")
        object.__setattr__(self, "bronze_ready", self.root / "bronze" / "bronze_ready.json")


@dataclass(frozen=True)
class IngestConfig:
    workers: int
    max_retries: int
    retry_backoff_seconds: float
    sample_per_class: int | None  # None = all files


@dataclass(frozen=True)
class SchemaConfig:
    fail_on_drift: bool
    contract: Path  # expected columns and types; a relative path is resolved against the conf dir


@dataclass(frozen=True)
class SparkConfig:
    app_name: str
    master: str
    driver_memory: str
    shuffle_partitions: int


@dataclass(frozen=True)
class Config:
    env: str
    source: SourceConfig
    storage: StorageConfig
    ingest: IngestConfig
    schema: SchemaConfig
    spark: SparkConfig

    def to_dict(self) -> dict[str, Any]:
        """The resolved config as plain JSON-serializable data, for run reports."""
        return _plain(dataclasses.asdict(self))

    def __str__(self) -> str:
        return yaml.safe_dump(self.to_dict(), sort_keys=False)


SECTIONS = {
    "source": SourceConfig,
    "storage": StorageConfig,
    "ingest": IngestConfig,
    "schema": SchemaConfig,
    "spark": SparkConfig,
}


def load_config(env: str, conf_dir: Path = CONF_DIR) -> Config:
    """Load conf_dir/base.yaml, deep-merge conf_dir/<env>.yaml on top, validate and resolve paths.

    A relative storage.root is resolved against conf_dir's parent (the repo root), not the current directory.
    """
    conf_dir = Path(conf_dir)
    env_file = conf_dir / f"{env}.yaml"
    if env == "base" or not env_file.is_file():
        available = sorted(p.stem for p in conf_dir.glob("*.yaml") if p.stem != "base")
        raise ConfigError(f"unknown env {env!r}, expected one of: {', '.join(available)}")

    merged: dict[str, Any] = {}
    for path in (conf_dir / "base.yaml", env_file):
        raw = _read_yaml(path)
        _check_keys(raw, path)
        merged = _deep_merge(merged, raw)
    return _build(env, merged, conf_dir=conf_dir)


def _read_yaml(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text()) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"{path.name}: expected a mapping at the top level")
    return data


def _check_keys(raw: dict[str, Any], path: Path) -> None:
    """Unknown keys fail instead of being silently ignored (e.g. a typo like `worker: 4`)."""
    for section, values in raw.items():
        if section not in SECTIONS:
            raise ConfigError(f"{path.name}: unknown section {section!r}")
        if not isinstance(values, dict):
            raise ConfigError(f"{path.name}: section {section!r} must be a mapping")
        unknown = values.keys() - _settable_fields(SECTIONS[section])
        if unknown:
            raise ConfigError(f"{path.name}: unknown key(s) {', '.join(f'{section}.{k}' for k in sorted(unknown))}")


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Nested keys override one by one; a section in override doesn't replace the whole base section."""
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _build(env: str, merged: dict[str, Any], conf_dir: Path) -> Config:
    for section, cls in SECTIONS.items():
        missing = _settable_fields(cls) - merged.get(section, {}).keys()
        if missing:
            raise ConfigError(f"missing key(s) {', '.join(f'{section}.{k}' for k in sorted(missing))}")

    source = dict(merged["source"])
    revision = source["revision"]
    if not (isinstance(revision, str) and SHA_RE.fullmatch(revision)):
        raise ConfigError(f"source.revision must be a full 40-character commit SHA, got {revision!r}")
    source["include"] = tuple(source["include"])

    root = merged["storage"]["root"]
    if root is None:
        raise ConfigError(f"storage.root must be set (in {env}.yaml)")
    root = Path(root).expanduser()
    if not root.is_absolute():
        root = conf_dir.parent / root

    schema = dict(merged["schema"])
    contract = Path(schema["contract"]).expanduser()
    if not contract.is_absolute():
        contract = conf_dir / contract
    if not contract.is_file():
        raise ConfigError(f"schema.contract not found: {contract}")
    schema["contract"] = contract.resolve()

    return Config(
        env=env,
        source=SourceConfig(**source),
        storage=StorageConfig(root=root.resolve()),
        ingest=IngestConfig(**merged["ingest"]),
        schema=SchemaConfig(**schema),
        spark=SparkConfig(**merged["spark"]),
    )


def _settable_fields(cls: type) -> set[str]:
    return {f.name for f in dataclasses.fields(cls) if f.init}


def _plain(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    return value
