# 3W pipeline

Data pipeline for the Petrobras [3W dataset](https://github.com/petrobras/3W) (DAT535 Data Engineering project).

The raw data is read from a Hugging Face mirror, [`Yahya2491/3w-pipeline`](https://huggingface.co/datasets/Yahya2491/3w-pipeline),
pinned to one commit. See [`docs/source.md`](docs/source.md) for the pinned revision and how its integrity was checked,
and [`docs/dataset_notes.md`](docs/dataset_notes.md) for measured facts about the data that later layers depend on.

## Layout

```
src/threew/          Python package (named threew because package names can't start with a digit)
  source/            reading the pinned dataset from Hugging Face
  bronze/            Bronze ingestion job
tests/               tests, run against the installed package
conf/                dev / prod configuration
scripts/             one-off scripts, e.g. publish_to_hf.py
hf/README.md         dataset card shown on Hugging Face
docs/                project documentation
.github/workflows/   CI
```

## Setup

Needs Python 3.11 and Java 8, 11 or 17 (for Spark 3.5). `requirements.lock` pins every runtime dependency and was resolved
with Python 3.11.

```bash
python3.11 -m venv .venv            # or: uv venv --python 3.11 --seed .venv
source .venv/bin/activate
pip install -r requirements.lock && pip install pytest && pip install -e . --no-deps
pytest -q
```

To change dependencies, edit `pyproject.toml` and regenerate the lock inside the Python 3.11 venv:

```bash
pip install pip-tools
pip-compile --strip-extras --output-file=requirements.lock pyproject.toml
```

## Configuration

`conf/base.yaml` holds the shared settings; `conf/dev.yaml` (laptop, 3 files per class) and `conf/prod.yaml` (VM, all files)
override them key by key. Code reads settings only through `threew.config.load_config(env)`. Unknown keys are errors.

```bash
python -c "from threew.config import load_config; print(load_config('prod'))"
```

`HF_TOKEN` is never in a config file; it comes from the environment.

## Publishing the mirror

`scripts/publish_to_hf.py` uploads the upstream Parquet files to the Hugging Face repo, uploads the dataset card and creates the
`v2.0.0` tag. It needs a Hugging Face login with write access (`hf auth login`); tokens are never stored in this repo.

```bash
python scripts/publish_to_hf.py --only 2   # test with one class
python scripts/publish_to_hf.py            # full upload + card + tag
```
