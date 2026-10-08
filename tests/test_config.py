import json
import shutil
from pathlib import Path

import pytest

from threew.config import CONF_DIR, ConfigError, load_config


@pytest.mark.parametrize("env", ["dev", "prod"])
def test_env_loads_with_absolute_paths(env):
    cfg = load_config(env)
    assert cfg.env == env
    for path in (
        cfg.storage.root,
        cfg.storage.landing,
        cfg.storage.bronze,
        cfg.storage.state,
        cfg.storage.runs,
        cfg.storage.staging,
    ):
        assert path.is_absolute()


def test_sample_per_class():
    assert load_config("dev").ingest.sample_per_class == 3
    assert load_config("prod").ingest.sample_per_class is None


def test_merge_overrides_key_by_key():
    dev = load_config("dev")
    assert dev.ingest.workers == 4  # base value, not in dev.yaml
    assert dev.spark.master == "local[2]"  # overridden in dev.yaml
    assert dev.spark.app_name == "3w-bronze"  # base value kept inside an overridden section


def test_derived_paths():
    storage = load_config("prod").storage
    assert storage.root == Path("/home/ubuntu/3w-data")
    assert storage.landing == storage.root / "landing" / "3w"
    assert storage.bronze == storage.root / "bronze" / "3w_observations"
    assert storage.state == storage.root / "bronze" / "_state"
    assert storage.runs == storage.root / "bronze" / "_runs"
    assert storage.staging == storage.root / "_staging"  # inside the root, never /tmp


def test_dev_root_is_resolved_against_repo_root():
    assert load_config("dev").storage.root == CONF_DIR.parent / "data" / "dev"


def test_to_dict_is_json_serializable():
    d = load_config("dev").to_dict()
    json.dumps(d)
    assert d["env"] == "dev"
    assert d["storage"]["bronze"].endswith("bronze/3w_observations")
    assert d["source"]["include"] == ["data/[0-9]/*.parquet"]


@pytest.fixture
def conf_dir(tmp_path):
    """A conf dir with the real base.yaml; each test writes its own test.yaml."""
    shutil.copy(CONF_DIR / "base.yaml", tmp_path / "base.yaml")
    shutil.copytree(CONF_DIR / "contracts", tmp_path / "contracts")
    return tmp_path


def test_unknown_env_raises():
    with pytest.raises(ConfigError, match="unknown env"):
        load_config("staging")


@pytest.mark.parametrize(
    "env_yaml, match",
    [
        ("storage: {root: /tmp/x}\ningest: {worker: 4}\n", "ingest.worker"),
        ("storage: {root: /tmp/x}\ningets: {workers: 4}\n", "unknown section 'ingets'"),
        ("storage: {root: /tmp/x, bronze: /tmp/y}\n", "storage.bronze"),  # derived paths aren't settable
    ],
)
def test_unknown_key_raises(conf_dir, env_yaml, match):
    (conf_dir / "test.yaml").write_text(env_yaml)
    with pytest.raises(ConfigError, match=match):
        load_config("test", conf_dir)


@pytest.mark.parametrize(
    "revision",
    [
        "c1a7e8d",  # short SHA
        "c1a7e8d0cfefc0875974ea079d11349a37fa0c5z",  # 40 chars, not hex
        "v2.0.0",  # a tag, not a commit
    ],
)
def test_invalid_revision_raises(conf_dir, revision):
    (conf_dir / "test.yaml").write_text(f"storage: {{root: /tmp/x}}\nsource: {{revision: '{revision}'}}\n")
    with pytest.raises(ConfigError, match="source.revision"):
        load_config("test", conf_dir)


def test_missing_root_raises(conf_dir):
    (conf_dir / "test.yaml").write_text("ingest: {workers: 2}\n")
    with pytest.raises(ConfigError, match="storage.root"):
        load_config("test", conf_dir)


def test_contract_path_is_resolved_against_the_conf_dir():
    contract = load_config("prod").schema.contract
    assert contract == CONF_DIR / "contracts" / "3w_v2.yaml"
    assert contract.is_file()


def test_missing_contract_raises(conf_dir):
    (conf_dir / "test.yaml").write_text("storage: {root: /tmp/x}\nschema: {contract: contracts/nope.yaml}\n")
    with pytest.raises(ConfigError, match="schema.contract not found"):
        load_config("test", conf_dir)
