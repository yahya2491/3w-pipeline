"""Hub client pieces that work without the network. The full fake hub comes later."""

import logging

import httpx2
import pytest
from huggingface_hub.errors import HfHubHTTPError
from huggingface_hub.hf_api import RepoFile, RepoFolder

from threew.config import load_config
from threew.source.hub import HfHubClient, RemoteFile, matches, to_remote_file, verify_pin
from threew.source.retry import with_retry

SOURCE = load_config("dev").source
SHA256 = "743f61fcd36aed3070c817fb035c9ca14aee838126ab55a868da383e31aa4048"


@pytest.mark.parametrize(
    "path, kept",
    [
        ("data/2/x.parquet", True),
        ("data/0/WELL-00001_20170201010207.parquet", True),
        ("data/dataset.ini", False),
        ("data/LICENSE-CC-BY", False),
        ("README.md", False),
        (".gitattributes", False),
    ],
)
def test_include_filter(path, kept):
    assert matches(path, SOURCE.include) is kept


def test_to_remote_file_with_lfs():
    entry = RepoFile(
        path="data/2/SIMULATED_00001.parquet",
        size=800964,
        oid="b1ob",
        lfs={"size": 800964, "oid": SHA256, "pointerSize": 131},
    )
    assert to_remote_file(entry) == RemoteFile("data/2/SIMULATED_00001.parquet", 800964, SHA256, "b1ob")


def test_to_remote_file_without_lfs():
    entry = RepoFile(path="data/dataset.ini", size=5120, oid="b1ob")
    assert to_remote_file(entry) == RemoteFile("data/dataset.ini", 5120, None, "b1ob")


def test_list_files_keeps_matching_files_only(monkeypatch):
    tree = [
        RepoFile(path="README.md", size=10, oid="a"),
        RepoFolder(path="data", oid="b"),
        RepoFolder(path="data/2", oid="c"),
        RepoFile(path="data/dataset.ini", size=10, oid="d"),
        RepoFile(path="data/2/B.parquet", size=20, oid="e", lfs={"size": 20, "oid": SHA256, "pointerSize": 131}),
        RepoFile(path="data/2/A.parquet", size=30, oid="f", lfs={"size": 30, "oid": SHA256, "pointerSize": 131}),
    ]
    client = HfHubClient(SOURCE.repo_id, SOURCE.repo_type)
    monkeypatch.setattr(client._api, "list_repo_tree", lambda *args, **kwargs: iter(tree))
    files = client.list_files(SOURCE.revision, SOURCE.include)
    assert [f.path for f in files] == ["data/2/A.parquet", "data/2/B.parquet"]  # sorted, folders and non-data dropped


@pytest.mark.parametrize("method", ["list_files", "download"])
def test_data_is_only_read_at_a_commit_sha(method, tmp_path):
    client = HfHubClient(SOURCE.repo_id, SOURCE.repo_type)
    args = (
        (SOURCE.revision_label, SOURCE.include) if method == "list_files" else ("data/2/x.parquet", "v2.0.0", tmp_path)
    )
    with pytest.raises(ValueError, match="commit SHA"):
        getattr(client, method)(*args)


def http_error(status: int) -> HfHubHTTPError:
    request = httpx2.Request("GET", "https://huggingface.co/api/datasets/x")
    return HfHubHTTPError(f"HTTP {status}", response=httpx2.Response(status, request=request))


class Flaky:
    """Raises the given errors one per call, then returns "ok"."""

    def __init__(self, *errors: Exception) -> None:
        self.errors = list(errors)
        self.calls = 0

    def __call__(self) -> str:
        self.calls += 1
        if self.errors:
            raise self.errors.pop(0)
        return "ok"


@pytest.mark.parametrize(
    "make_error",
    [lambda: httpx2.ReadTimeout("timed out"), ConnectionResetError, lambda: http_error(429), lambda: http_error(503)],
    ids=["timeout", "connection-reset", "429", "503"],
)
def test_retry_recovers_from_a_temporary_error(make_error):
    waits = []
    fn = Flaky(make_error(), make_error())
    assert with_retry(fn, max_retries=3, backoff=5, sleep=waits.append) == "ok"
    assert fn.calls == 3
    assert waits == [5, 10]


@pytest.mark.parametrize("status", [401, 404])
def test_retry_raises_a_permanent_error_immediately(status):
    waits = []
    fn = Flaky(http_error(status))
    with pytest.raises(HfHubHTTPError):
        with_retry(fn, max_retries=3, backoff=5, sleep=waits.append)
    assert fn.calls == 1
    assert waits == []


def test_retry_gives_up_after_max_retries():
    waits = []
    fn = Flaky(*(http_error(503) for _ in range(4)))
    with pytest.raises(HfHubHTTPError):
        with_retry(fn, max_retries=3, backoff=5, sleep=waits.append)
    assert fn.calls == 4
    assert waits == [5, 10, 20]


class TagsOnlyHub:
    def __init__(self, tags: dict[str, str]) -> None:
        self.tags = tags

    def resolve_revision(self, label: str) -> str:
        return self.tags[label]


def test_verify_pin_ok(caplog):
    check = verify_pin(TagsOnlyHub({"v2.0.0": SOURCE.revision}), SOURCE)
    assert not check.tag_moved
    assert "tag moved" not in caplog.text


def test_verify_pin_warns_and_keeps_the_pin_when_tag_moved(caplog):
    moved_to = "0" * 40
    with caplog.at_level(logging.WARNING):
        check = verify_pin(TagsOnlyHub({"v2.0.0": moved_to}), SOURCE)
    assert check.tag_moved
    assert check.pinned == SOURCE.revision
    assert check.resolved == moved_to
    assert "tag moved" in caplog.text
