"""Mirror the petrobras/3W dataset (Parquet files) to our Hugging Face dataset repo, unchanged.

Usage:
    python scripts/publish_to_hf.py --only 2   # test with one small class (no card, no tag)
    python scripts/publish_to_hf.py            # full upload + dataset card + version tag
                                               # (already-uploaded files are skipped)
"""

import argparse
import subprocess
import sys
from pathlib import Path

from huggingface_hub import HfApi

EXPECTED_PARQUET_FILES = 2228
CLASSES = [str(c) for c in range(10)]
CARD = Path(__file__).resolve().parent.parent / "hf" / "README.md"
TAG = "v2.0.0"


def fail(msg: str) -> None:
    sys.exit(f"Pre-flight check failed: {msg}")


def git(source_dir: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=source_dir, check=True, capture_output=True, text=True).stdout.strip()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source-dir", type=Path, default=Path("~/data-src/3W"))
    parser.add_argument("--repo-id", default="Yahya2491/3w-pipeline")
    parser.add_argument("--only", choices=CLASSES, help="upload a single class folder (for testing)")
    args = parser.parse_args()

    source_dir = args.source_dir.expanduser().resolve()
    dataset_dir = source_dir / "dataset"

    # Pre-flight checks
    sha = git(source_dir, "rev-parse", "HEAD")
    if git(source_dir, "status", "--porcelain"):
        fail(f"{source_dir} has uncommitted changes")
    n_local = len(list(dataset_dir.rglob("*.parquet")))
    if n_local != EXPECTED_PARQUET_FILES:
        fail(f"found {n_local} Parquet files under {dataset_dir}, expected {EXPECTED_PARQUET_FILES}")
    print(f"Source OK: {source_dir} @ {sha}, {n_local} Parquet files")

    # Upload
    parquet_pattern = f"{args.only}/*.parquet" if args.only else "*/*.parquet"
    commit_message = f"Mirror petrobras/3W dataset 2.0.0 @ {sha}"
    if args.only:
        commit_message += f" (class {args.only} only)"

    api = HfApi()
    print(f"Uploading {parquet_pattern} to {args.repo_id} ...")
    commit = api.upload_folder(
        repo_id=args.repo_id,
        repo_type="dataset",
        folder_path=dataset_dir,
        path_in_repo="data",
        allow_patterns=[parquet_pattern, "dataset.ini", "LICENSE-CC-BY"],
        commit_message=commit_message,
    )
    print(f"Commit: {commit.commit_url}")

    # Post-check
    repo_files = api.list_repo_files(args.repo_id, repo_type="dataset")
    n_remote = sum(f.endswith(".parquet") for f in repo_files)
    print(f"Parquet files in {args.repo_id}: {n_remote}")
    if args.only:
        return
    if n_remote != EXPECTED_PARQUET_FILES:
        sys.exit(f"Post-check failed: expected {EXPECTED_PARQUET_FILES}, found {n_remote}")

    # Dataset card, then the tag, so the tag includes the card
    api.upload_file(
        path_or_fileobj=CARD,
        path_in_repo="README.md",
        repo_id=args.repo_id,
        repo_type="dataset",
        commit_message="Add dataset card",
    )
    # exist_ok: a rerun leaves an existing tag where it is rather than moving it
    api.create_tag(
        args.repo_id,
        tag=TAG,
        repo_type="dataset",
        tag_message=f"3W dataset 2.0.0 from petrobras/3W@{sha[:8]}",
        exist_ok=True,
    )

    # Verify: the commit the pipeline pins
    info = api.dataset_info(args.repo_id, revision=TAG)
    print(f"{TAG} -> {info.sha}")
    print(api.list_repo_refs(args.repo_id, repo_type="dataset").tags)


if __name__ == "__main__":
    main()
