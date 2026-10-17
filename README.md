# 3W pipeline

[![CI](https://github.com/yahya2491/3w-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/yahya2491/3w-pipeline/actions/workflows/ci.yml)

Data pipeline for the Petrobras [3W dataset](https://github.com/petrobras/3W) (DAT535 Data Engineering project).

The raw data is read from a Hugging Face mirror, [`Yahya2491/3w-pipeline`](https://huggingface.co/datasets/Yahya2491/3w-pipeline),
pinned to one commit. See [`docs/source.md`](docs/source.md) for the pinned revision and how its integrity was checked,
and [`docs/bronze/dataset_notes.md`](docs/bronze/dataset_notes.md) for measured facts about the data that later layers depend on.
To build on Bronze, start with the [Bronze reference](docs/bronze/BRONZE_REFERENCE.md).

Silver progress is documented in the [milestone report](docs/silver/SILVER_MILESTONE_REPORT.md).
See the [Silver contract](docs/silver/SILVER_CONTRACT.md) for table definitions.
The [Silver run guide](docs/silver/SILVER_RUN_GUIDE.md) covers commands, failures and comparison of saved runs.

Silver contract v2 standardizes observations, adds cleaned sensor columns and three-second bounded
repairs of finite invalid choke openings, and records quality, event references and sensor references.
Its 106 observation columns include 120-second sensor features and invalid-choke durations.
Gold selects labelled examples and trains/evaluates its Spark model from those prepared values. Read
[Silver assumptions](docs/silver/SILVER_ASSUMPTIONS.md) and
[quality rules](docs/silver/QUALITY_RULES.md). Original measurements and all seven tables remain.
With Bronze ready, run `python -m threew.silver.run --env dev` in the locked Linux environment.
Gold anomaly classification and the other course's CNN experiments consume the prepared output;
neither model is implemented by this Silver milestone.

Pipeline run dates and times are UTC. Source observation clocks remain timezone-free and unchanged.

## Layout

```
src/threew/          Python package (named threew because package names can't start with a digit)
  source/            reading the pinned dataset from Hugging Face
  bronze/            Bronze ingestion job
  silver/            Silver profiling, transformations, quality summaries and dev runner
tests/               tests, run against the installed package
conf/                dev / prod configuration
scripts/             one-off scripts, e.g. publish_to_hf.py
hf/README.md         dataset card shown on Hugging Face
docs/                project documentation
.github/workflows/   CI
```

## Setup

Needs Python 3.11 and Java 8, 11 or 17 (for Spark 3.5). Both lock files were resolved with Python 3.11:
`requirements.lock` pins every runtime dependency (what the VM runs), and `requirements-dev.lock` pins the same
versions plus pytest and ruff (what CI and development use).

```bash
python3.11 -m venv .venv            # or: uv venv --python 3.11 --seed .venv
source .venv/bin/activate
pip install -r requirements-dev.lock && pip install -e . --no-deps
pytest -q
ruff check . && ruff format --check .
```

To change dependencies, edit `pyproject.toml` and regenerate both locks inside the Python 3.11 venv. Starting the dev
lock from a copy of the runtime lock keeps their shared versions identical:

```bash
pip install pip-tools
pip-compile --strip-extras --output-file=requirements.lock pyproject.toml
cp requirements.lock requirements-dev.lock
pip-compile --strip-extras --extra dev --output-file=requirements-dev.lock pyproject.toml
```

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs ruff and the tests on every push and pull request,
with Java 8 (the VM's) and Java 17. The tests run with `HF_HUB_OFFLINE=1` and no secrets: they never use the network.

Automated tests use small fixtures; full-dataset validation requires separate pipeline runs and saved evidence.
Silver reconciliation reuses the transformation functions, so passing it alone does not prove the transformation
rules are correct. Tests with explicit expected values provide additional checks.

## Configuration

`conf/base.yaml` holds the shared settings; `conf/dev.yaml` (laptop, 3 files per class) and `conf/prod.yaml` (VM, all files)
override them key by key. Code reads settings only through `threew.config.load_config(env)`. Unknown keys are errors.

```bash
python -c "from threew.config import load_config; print(load_config('prod'))"
```

`HF_TOKEN` is never in a config file; it comes from the environment.

## Running the Bronze layer

```bash
python -m threew.bronze.run --env dev       # laptop: 3 files per class into ./data/dev
python -m threew.bronze.run --env prod      # VM: all 2,228 files into /home/ubuntu/3w-data
python -m threew.bronze.ingest --env dev    # only the ingest phase, e.g. for debugging
```

One run, under one lock and one report, goes through these phases:

| Phase | Does | On failure |
|---|---|---|
| ingest | downloads files that are new or changed upstream, verifies each one (sha256, then the schema contract), lands it under `landing/3w/ingest_date=<date>/run_id=<run_id>/label=<label>/` and records it in the manifest | PARTIAL, or FAILED |
| convert | converts landed files that have no Bronze output yet into `bronze/3w_observations/source_type=<type>/label=<label>/` | PARTIAL |
| prune | deletes Bronze outputs no current manifest row points to (e.g. the old version of a changed file) | FAILED |
| reconcile | six Spark checks of the table against the manifest and the landing files (`--deep` adds a strict row-order proof) | FAILED |
| publish | writes `bronze/bronze_ready.json`, only if everything above succeeded | |

The ready marker is withdrawn when a run starts and written again only by a fully successful run: read Bronze only
when `bronze_ready.json` exists, and use its `run_id`. Only one run at a time per storage root: a second one is
SKIPPED.

| Status | Exit code | Meaning |
|---|---|---|
| SUCCESS | 0 | every phase succeeded (schema drift is listed; it fails the run only with `schema.fail_on_drift`) |
| SKIPPED | 0 | another run held the lock |
| PARTIAL | 1 | the run finished, but some files failed to download, land or convert |
| FAILED | 2 | the run stopped on an error, reconciliation failed, or files drifted with `schema.fail_on_drift` on |

### Scheduled runs

The intended weekly Bronze schedule is Monday at 03:17 UTC on the VM's self-hosted runner.
This `silver-profiling` checkout's [workflow](.github/workflows/bronze.yml) still contains a temporary 15-minute
schedule; check the workflow on the deployed branch for the active schedule. It also runs on demand from the
Actions tab (`env`: prod or dev, `deep`: strict reconcile). Each run builds a
fresh environment from `requirements.lock`, writes a summary to the run page (status, one row per phase, every
problem) and keeps the report, log and reconcile plan as an artifact for 90 days. PARTIAL and FAILED runs turn red;
GitHub emails whoever last changed the schedule when a scheduled run fails.

Every run writes a report, whatever happens, to `<storage root>/bronze/_runs/<run_id>.json`: config, code version,
plan, download speed, schema checks, conversion, pruning, reconciliation, the marker, failures, orphaned files in
landing and leftover staging folders. `scripts/plan_preview.py` and `scripts/inspect_landing.py` show the plan and
the landed files without changing anything; `scripts/reconcile_bronze.py` runs the checks on their own.

## Publishing the mirror

`scripts/publish_to_hf.py` uploads the upstream Parquet files to the Hugging Face repo, uploads the dataset card and creates the
`v2.0.0` tag. It needs a Hugging Face login with write access (`hf auth login`); tokens are never stored in this repo.

```bash
python scripts/publish_to_hf.py --only 2   # test with one class
python scripts/publish_to_hf.py            # full upload + card + tag
```
