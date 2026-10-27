"""The only module that talks to Hugging Face: resolve a tag, list files, download one.

The rest of the pipeline depends on the HubClient protocol, never on huggingface_hub, so tests can use a fake
hub and a different source (for example S3) only needs a new client.
"""

import fnmatch
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from huggingface_hub import HfApi, hf_hub_download
from huggingface_hub.hf_api import RepoFile

from threew.config import SHA_RE, SourceConfig

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RemoteFile:
    path: str
    size: int
    sha256: str | None  # from the LFS metadata; None for small files stored directly in git
    blob_id: str  # git object ID, the fallback when there is no sha256


class HubClient(Protocol):
    def resolve_revision(self, label: str) -> str:
        """The commit SHA a tag or branch points to right now."""
        ...

    def list_files(self, revision: str, include: Sequence[str]) -> list[RemoteFile]:
        """Files at commit `revision` whose path matches any include pattern, sorted by path."""
        ...

    def download(self, path: str, revision: str, dest_dir: Path) -> Path:
        """Download one file at commit `revision` to dest_dir/<path> and return that local path."""
        ...


class HfHubClient:
    """HubClient backed by the Hugging Face Hub.

    token=None throughout: huggingface_hub finds HF_TOKEN or the cached `hf auth login` by itself.
    """

    def __init__(self, repo_id: str, repo_type: str = "dataset") -> None:
        self.repo_id = repo_id
        self.repo_type = repo_type
        self._api = HfApi(token=None)

    def resolve_revision(self, label: str) -> str:
        return self._api.repo_info(self.repo_id, revision=label, repo_type=self.repo_type).sha

    def list_files(self, revision: str, include: Sequence[str]) -> list[RemoteFile]:
        _require_sha(revision)
        # The whole repo, so the include patterns alone decide what is in scope
        tree = self._api.list_repo_tree(self.repo_id, recursive=True, revision=revision, repo_type=self.repo_type)
        files = [
            to_remote_file(entry) for entry in tree if isinstance(entry, RepoFile) and matches(entry.path, include)
        ]
        return sorted(files, key=lambda f: f.path)

    def download(self, path: str, revision: str, dest_dir: Path) -> Path:
        # local_dir also creates dest_dir/.cache/, so download into a staging folder, never straight into landing
        _require_sha(revision)
        local = hf_hub_download(
            self.repo_id,
            filename=path,
            repo_type=self.repo_type,
            revision=revision,
            local_dir=dest_dir,
            token=None,
        )
        return Path(local)


def matches(path: str, include: Sequence[str]) -> bool:
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in include)


def to_remote_file(entry: RepoFile) -> RemoteFile:
    return RemoteFile(
        path=entry.path,
        size=entry.size,
        sha256=entry.lfs.sha256 if entry.lfs is not None else None,
        blob_id=entry.blob_id,
    )


@dataclass(frozen=True)
class PinCheck:
    label: str
    pinned: str
    resolved: str

    @property
    def tag_moved(self) -> bool:
        return self.resolved != self.pinned


def verify_pin(client: HubClient, source: SourceConfig) -> PinCheck:
    """Check that source.revision_label still points to the pinned source.revision.

    The pinned SHA always wins: if the tag moved, this logs a warning and the run carries on with the pin.
    """
    check = PinCheck(source.revision_label, source.revision, client.resolve_revision(source.revision_label))
    if check.tag_moved:
        log.warning(
            "tag moved: %s now points to %s; continuing with the pinned revision %s",
            check.label,
            check.resolved,
            check.pinned,
        )
    return check


def _require_sha(revision: str) -> None:
    """Data is always read at a commit, never at a tag or branch that could move."""
    if not SHA_RE.fullmatch(revision):
        raise ValueError(f"revision must be a full commit SHA, not a tag or branch: {revision!r}")
