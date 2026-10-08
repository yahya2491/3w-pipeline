# Data source

The pipeline reads the 3W dataset from a Hugging Face mirror, pinned to one immutable commit.

| | |
|---|---|
| Hugging Face repo | [`Yahya2491/3w-pipeline`](https://huggingface.co/datasets/Yahya2491/3w-pipeline) (dataset) |
| Tag | `v2.0.0` |
| **HF commit to pin** | **`c1a7e8d0cfefc0875974ea079d11349a37fa0c56`** |
| Upstream | [`petrobras/3W`](https://github.com/petrobras/3W) @ `d57c058af6baf65f1b285f3e56f46e4f018a07d9` |
| Dataset version | 2.0.0 (from `data/dataset.ini`) |
| Files | 2,228 Parquet files under `data/<label>/`, plus `data/dataset.ini` and `data/LICENSE-CC-BY` |
| License | CC BY 4.0, Petrobras |

`v2.0.0` is an annotated tag: `list_repo_refs(...).tags` shows the tag object (`23b9c646…`), not the commit.
Pin the commit SHA above, which is what `dataset_info(REPO, revision="v2.0.0").sha` returns.

## How it was published

`scripts/publish_to_hf.py` (run on 2026-10-08) uploaded `~/data-src/3W/dataset` after checking that the upstream
checkout was clean and had 2,228 Parquet files. It then uploaded the dataset card (`hf/README.md`) and created the tag on top of it.

## Integrity check

The SHA-256 of all 2,228 Parquet files at `v2.0.0` (the `lfs.sha256` from `HfApi().list_repo_tree`) matches `sha256sum` of
the upstream checkout at `d57c058`. Example:

```
data/2/SIMULATED_00001.parquet  743f61fcd36aed3070c817fb035c9ca14aee838126ab55a868da383e31aa4048  (HF, v2.0.0)
                                743f61fcd36aed3070c817fb035c9ca14aee838126ab55a868da383e31aa4048  (sha256sum, local)
```

Bronze repeats this check for every file it downloads.
