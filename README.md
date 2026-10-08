# 3W pipeline

Data pipeline for the Petrobras [3W dataset](https://github.com/petrobras/3W) (DAT535 Data Engineering project).

The raw data is read from a Hugging Face mirror, [`Yahya2491/3w-pipeline`](https://huggingface.co/datasets/Yahya2491/3w-pipeline),
pinned to one commit. See [`docs/source.md`](docs/source.md) for the pinned revision and how its integrity was checked.

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

## Publishing the mirror

`scripts/publish_to_hf.py` uploads the upstream Parquet files to the Hugging Face repo, uploads the dataset card and creates the
`v2.0.0` tag. It needs a Hugging Face login with write access (`hf auth login`); tokens are never stored in this repo.

```bash
python scripts/publish_to_hf.py --only 2   # test with one class
python scripts/publish_to_hf.py            # full upload + card + tag
```
