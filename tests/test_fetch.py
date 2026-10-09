"""Fetch tests with a fake client that writes chosen bytes: no network."""

import hashlib
import os
from collections import defaultdict
from pathlib import Path

import httpx2
import pytest
from huggingface_hub.errors import HfHubHTTPError

from threew.bronze.fetch import (
    FetchStatus,
    TooManyFailuresError,
    VerifiedBy,
    fetch_all,
    fetch_one,
    fetch_stats,
    sha256_of,
)
from threew.bronze.naming import parse_path
from threew.bronze.plan import Candidate, Reason
from threew.source.hub import RemoteFile

REVISION = "c1a7e8d0cfefc0875974ea079d11349a37fa0c56"
GOOD = b"PAR1 pretend parquet bytes PAR1"
BAD = b"PAR1 same length, wrong bytes!!"  # same size as GOOD, different content
assert len(GOOD) == len(BAD)


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class FakeHub:
    """Each path gets a list of responses, one per download call; the last one repeats.

    A response is the bytes to write to dest_dir/<path>, or an exception to raise.
    """

    def __init__(self, responses: dict[str, list]) -> None:
        self.responses = {path: list(r) for path, r in responses.items()}
        self.calls: dict[str, int] = defaultdict(int)

    def download(self, path: str, revision: str, dest_dir: Path) -> Path:
        self.calls[path] += 1
        queue = self.responses[path]
        response = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(response, Exception):
            raise response
        target = Path(dest_dir) / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(response)
        return target


def candidate(path="data/2/SIMULATED_00001.parquet", data=GOOD, sha256="from-data"):
    remote = RemoteFile(path=path, size=len(data), sha256=sha(data) if sha256 == "from-data" else sha256, blob_id="b")
    return Candidate(remote, parse_path(path))


def http_error(status: int) -> HfHubHTTPError:
    request = httpx2.Request("GET", "https://huggingface.co/datasets/x/resolve/abc/data.parquet")
    return HfHubHTTPError(f"{status} Client Error: Entry Not Found", response=httpx2.Response(status, request=request))


def fetch(hub, cand, staging, waits=None):
    return fetch_one(
        hub,
        cand,
        Reason.NEW,
        REVISION,
        staging,
        max_retries=3,
        backoff=5,
        sleep=(waits.append if waits is not None else lambda _: None),
    )


def files_in(staging: Path) -> list[Path]:
    return [p for p in staging.rglob("*") if p.is_file()]


def test_correct_bytes(tmp_path):
    cand = candidate()
    result = fetch(FakeHub({cand.path: [GOOD]}), cand, tmp_path)
    assert result.status is FetchStatus.OK
    assert result.attempts == 1
    assert result.verified_by is VerifiedBy.SHA256
    assert result.sha256_actual == sha(GOOD)
    assert result.staged_path.read_bytes() == GOOD
    assert result.error is None
    assert (result.attempts, result.checksum_retries) == (1, 0)


def test_wrong_bytes_once_then_correct(tmp_path):
    cand = candidate()
    result = fetch(FakeHub({cand.path: [BAD, GOOD]}), cand, tmp_path)
    assert result.status is FetchStatus.OK
    assert (result.attempts, result.checksum_retries) == (2, 1)
    assert result.staged_path.read_bytes() == GOOD


def test_wrong_bytes_twice(tmp_path):
    cand = candidate()
    hub = FakeHub({cand.path: [BAD]})
    result = fetch(hub, cand, tmp_path)
    assert result.status is FetchStatus.FAILED
    assert result.error == f"checksum mismatch: expected {sha(GOOD)[:12]} got {sha(BAD)[:12]}"
    assert result.staged_path is None
    assert hub.calls[cand.path] == 2  # the second and final attempt
    assert (result.attempts, result.checksum_retries) == (2, 1)
    assert files_in(tmp_path) == []  # no bad file left in staging


def test_wrong_size(tmp_path):
    cand = candidate()
    result = fetch(FakeHub({cand.path: [GOOD + b"extra"]}), cand, tmp_path)
    assert result.status is FetchStatus.FAILED
    assert "size mismatch" in result.error
    assert files_in(tmp_path) == []


def test_no_sha256_is_verified_by_size(tmp_path):
    cand = candidate(sha256=None)
    result = fetch(FakeHub({cand.path: [GOOD]}), cand, tmp_path)
    assert result.status is FetchStatus.OK
    assert result.verified_by is VerifiedBy.SIZE_ONLY
    assert result.sha256_actual == sha(GOOD)  # still recorded


def test_temporary_error_recovers_through_retry(tmp_path):
    cand = candidate()
    waits = []
    result = fetch(FakeHub({cand.path: [httpx2.ReadTimeout("timed out"), GOOD]}), cand, tmp_path, waits)
    assert result.status is FetchStatus.OK
    assert (result.attempts, result.checksum_retries) == (2, 0)  # the timed-out call counts
    assert waits == [5]


def test_network_retries_and_checksum_retries_are_counted_separately(tmp_path):
    cand = candidate()
    timeout = httpx2.ReadTimeout("timed out")
    # Round 1: timeout, timeout, bad bytes. Round 2: timeout, good bytes
    hub = FakeHub({cand.path: [timeout, timeout, BAD, timeout, GOOD]})
    result = fetch(hub, cand, tmp_path)
    assert result.status is FetchStatus.OK
    assert (result.attempts, result.checksum_retries) == (5, 1)
    stats = fetch_stats([result], elapsed=1.0)
    assert (stats["download_calls"], stats["checksum_redownloads"], stats["network_retries"]) == (5, 1, 3)


def test_permanent_error_is_not_retried(tmp_path):
    cand = candidate()
    hub = FakeHub({cand.path: [http_error(404)]})
    waits = []
    result = fetch(hub, cand, tmp_path, waits)
    assert result.status is FetchStatus.FAILED
    assert result.error.startswith("download failed: HfHubHTTPError: 404")
    assert hub.calls[cand.path] == 1
    assert (result.attempts, result.checksum_retries) == (1, 0)
    assert waits == []


def many(n: int) -> list[Candidate]:
    return [candidate(f"data/{i % 10}/SIMULATED_{i:05d}.parquet") for i in range(n)]


def run_all(hub, cands, staging, workers=4):
    items = [(c, Reason.NEW) for c in cands]
    return fetch_all(hub, items, REVISION, staging, workers, max_retries=3, backoff=5, sleep=lambda _: None)


def test_one_bad_file_does_not_stop_the_others(tmp_path):
    cands = many(20)
    hub = FakeHub({c.path: [BAD if i == 7 else GOOD] for i, c in enumerate(cands)})
    results = run_all(hub, cands, tmp_path)
    assert [r.candidate.path for r in results] == [c.path for c in cands]  # plan order
    assert sum(r.status is FetchStatus.OK for r in results) == 19
    assert [r.candidate.path for r in results if r.status is FetchStatus.FAILED] == [cands[7].path]


def test_fetch_stats_sum_both_counters(tmp_path):
    cands = many(4)
    responses = [[GOOD], [httpx2.ConnectError("reset"), GOOD], [BAD, GOOD], [BAD]]
    hub = FakeHub({c.path: r for c, r in zip(cands, responses, strict=True)})
    stats = fetch_stats(run_all(hub, cands, tmp_path), elapsed=1.0)
    assert (stats["ok"], stats["failed"]) == (3, 1)
    # Calls: 1 + 2 (reset, then good) + 2 (bad, then good) + 2 (bad twice) = 7 for 4 files
    assert (stats["download_calls"], stats["checksum_redownloads"], stats["network_retries"]) == (7, 2, 1)


def test_first_10_failures_trip_the_circuit_breaker(tmp_path):
    cands = many(15)
    hub = FakeHub({c.path: [http_error(401)] for c in cands})
    with pytest.raises(
        TooManyFailuresError, match="10 consecutive failures, likely auth/repo problem: download failed"
    ):
        run_all(hub, cands, tmp_path, workers=1)


def test_breaker_is_off_after_a_success(tmp_path):
    cands = many(12)
    hub = FakeHub({c.path: [GOOD if i == 0 else http_error(404)] for i, c in enumerate(cands)})
    results = run_all(hub, cands, tmp_path, workers=1)  # one worker: the success finishes first
    assert [r.status for r in results] == [FetchStatus.OK] + [FetchStatus.FAILED] * 11


def test_sha256_of_streams_a_5mb_file(tmp_path):
    data = os.urandom(5 * 1024 * 1024)
    path = tmp_path / "random.bin"
    path.write_bytes(data)
    assert sha256_of(path) == hashlib.sha256(data).hexdigest()
    assert sha256_of(path, chunk_size=4096) == hashlib.sha256(data).hexdigest()
