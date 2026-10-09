"""Test doubles shared by the ingest and Bronze run tests."""

import hashlib
from collections import defaultdict
from pathlib import Path

import httpx2
from huggingface_hub.errors import HfHubHTTPError

from threew.config import load_config
from threew.source.hub import RemoteFile, matches

REVISION = load_config("dev").source.revision


class FakeHub:
    """A hub that serves files from a dict; change the dict between runs to simulate upstream changes."""

    def __init__(self, files: dict[str, bytes], errors: dict[str, Exception] | None = None) -> None:
        self.files = files
        self.errors = errors or {}  # path -> exception raised on every download of it
        self.list_error: Exception | None = None
        self.downloads: dict[str, int] = defaultdict(int)

    def resolve_revision(self, label: str) -> str:
        return REVISION

    def list_files(self, revision, include):
        if self.list_error:
            raise self.list_error
        return [
            RemoteFile(path, len(data), hashlib.sha256(data).hexdigest(), f"blob-{path}")
            for path, data in sorted(self.files.items())
            if matches(path, include)
        ]

    def download(self, path, revision, dest_dir):
        self.downloads[path] += 1
        if path in self.errors:
            raise self.errors[path]
        target = Path(dest_dir) / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(self.files[path])
        return target


def http_error(status: int) -> HfHubHTTPError:
    request = httpx2.Request("GET", "https://huggingface.co/datasets/x/resolve/abc/f.parquet")
    return HfHubHTTPError(f"{status} Client Error", response=httpx2.Response(status, request=request))
