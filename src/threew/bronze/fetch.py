"""Download planned files into a staging folder and prove each one arrived intact.

Bronze promises files byte-for-byte as published, and a download can be corrupted without any error. So every file's
size and sha256 are checked against what Hugging Face reported in the listing. Nothing here touches landing or the
manifest; land.py does that.
"""

import hashlib
import logging
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from threew.bronze.plan import Candidate, Reason
from threew.source.hub import HubClient, RemoteFile
from threew.source.retry import with_retry

log = logging.getLogger(__name__)

MIB = 1 << 20
# A file that fails verification is deleted and downloaded once more; if it fails again it is FAILED
VERIFY_ATTEMPTS = 2


class FetchStatus(StrEnum):
    """The outcome of fetching one file."""

    OK = "OK"
    FAILED = "FAILED"


class VerifiedBy(StrEnum):
    """How a downloaded file was proven intact: by its sha256, or only by its size when there is no sha256."""

    SHA256 = "sha256"
    SIZE_ONLY = "size_only"  # Hugging Face gave no sha256 for the file


@dataclass(frozen=True)
class FetchResult:
    """Everything the fetch learned about one file. land.py turns it into a manifest row."""

    candidate: Candidate
    reason: Reason
    status: FetchStatus
    staged_path: Path | None  # OK only
    sha256_actual: str | None  # of the last file downloaded, if it could be read
    verified_by: VerifiedBy
    attempts: int  # download calls: 1, plus retries after network errors, plus a re-download after a bad file
    seconds: float
    error: str | None  # FAILED only
    checksum_retries: int = 0  # 1 if the file was downloaded again after failing verification (size or sha256)


class TooManyFailuresError(RuntimeError):
    """The first files to finish all failed: most likely auth or the repo, not individual files."""


def sha256_of(path: Path, chunk_size: int = MIB) -> str:
    """sha256 of a file, read in chunks so a large file is never held in memory."""
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def fetch_one(
    client: HubClient,
    candidate: Candidate,
    reason: Reason,
    revision: str,
    staging_dir: Path,
    *,
    max_retries: int,
    backoff: float,
    sleep: Callable[[float], None] = time.sleep,
) -> FetchResult:
    """Download one file into staging_dir and verify it. Never raises: a failure is a FAILED result."""
    remote = candidate.remote
    verified_by = VerifiedBy.SHA256 if remote.sha256 else VerifiedBy.SIZE_ONLY
    start = time.monotonic()
    calls = 0  # every download call
    rounds = 0  # download-then-verify rounds: the second one only after a bad file

    # counts every call, including the extra calls with_retry makes after a network error
    def download() -> Path:
        nonlocal calls
        calls += 1
        return Path(client.download(remote.path, revision, staging_dir))

    def result(status: FetchStatus, staged: Path | None, actual: str | None, error: str | None) -> FetchResult:
        seconds = time.monotonic() - start
        checksum_retries = rounds - 1  # a second round only happens after a file failed verification
        return FetchResult(
            candidate, reason, status, staged, actual, verified_by, calls, seconds, error, checksum_retries
        )

    error: str | None = None
    actual: str | None = None
    # at most VERIFY_ATTEMPTS rounds of download, then verify
    for _ in range(VERIFY_ATTEMPTS):
        rounds += 1
        # download, retrying network errors; a permanent error (or retries used up) ends this file as FAILED
        try:
            staged = with_retry(download, max_retries, backoff, sleep=sleep)
        except Exception as exc:  # noqa: BLE001  one file failing must never stop the others
            return result(FetchStatus.FAILED, None, None, f"download failed: {_describe(exc)}")
        try:
            error, actual = _verify(staged, remote)
        except OSError as exc:
            error, actual = f"could not read the downloaded file: {_describe(exc)}", None
        if error is None:
            return result(FetchStatus.OK, staged, actual, None)  # size and sha256 match: done
        staged.unlink(missing_ok=True)  # never leave a bad file in staging
    # still wrong after the second download: FAILED, with the last mismatch as the error
    return result(FetchStatus.FAILED, None, actual, error)


def fetch_all(
    client: HubClient,
    plan_items: Sequence[tuple[Candidate, Reason]],
    revision: str,
    staging_dir: Path,
    workers: int,
    max_retries: int,
    backoff: float,
    *,
    max_consecutive_failures: int = 10,
    sleep: Callable[[float], None] = time.sleep,
) -> list[FetchResult]:
    """Fetch every planned file into staging_dir, `workers` at a time. One result per file, in plan order.

    Circuit breaker: if the first max_consecutive_failures files to finish all fail, the rest are cancelled and
    TooManyFailuresError is raised. Once any file succeeds the breaker is off, and failures are just results.
    """
    staging_dir = Path(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)
    start = time.monotonic()
    by_path: dict[str, FetchResult] = {}
    failures_before_first_ok = 0
    breaker_armed = True

    # Threads, because the work is waiting on the network and the disk
    pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="fetch")
    try:
        # submit every file at once; the pool runs `workers` of them at the same time
        futures = [
            pool.submit(
                fetch_one,
                client,
                candidate,
                reason,
                revision,
                staging_dir,
                max_retries=max_retries,
                backoff=backoff,
                sleep=sleep,
            )
            for candidate, reason in plan_items
        ]
        # results arrive in the order files finish, not plan order
        for done, future in enumerate(as_completed(futures), start=1):
            res = future.result()  # never raises: fetch_one turns every error into a FAILED result
            by_path[res.candidate.path] = res
            # circuit breaker: one success proves the token and repo work, so later failures are about single files
            if res.status is FetchStatus.OK:
                breaker_armed = False
            elif breaker_armed:
                failures_before_first_ok += 1
                # 10 failures and not one success: something global is wrong, stop instead of trying 2,000 files
                if failures_before_first_ok >= max_consecutive_failures:
                    raise TooManyFailuresError(
                        f"{failures_before_first_ok} consecutive failures, likely auth/repo problem: {res.error}"
                    )
            if done % 100 == 0:
                log.info("fetched %d/%d: %s", done, len(futures), format_stats(by_path.values(), start))
    finally:
        # cancel_futures: after a breaker trip, files that haven't started yet are dropped
        pool.shutdown(wait=True, cancel_futures=True)

    # back in plan order, so the manifest rows and the report come out the same way every run
    results = [by_path[candidate.path] for candidate, _ in plan_items]
    log.info("fetch done: %s", format_stats(results, start))
    return results


def fetch_stats(results: Sequence[FetchResult], elapsed: float) -> dict[str, Any]:
    """The fetch numbers for the run report: files, bytes, speed and retry counts."""
    ok = [r for r in results if r.status is FetchStatus.OK]
    ok_bytes = sum(r.candidate.remote.size for r in ok)
    return {
        "files": len(results),
        "ok": len(ok),
        "failed": len(results) - len(ok),
        "bytes": ok_bytes,
        "seconds": round(elapsed, 3),
        "mb_per_s": round(ok_bytes / elapsed / 1e6, 3) if elapsed > 0 else None,
        **retry_counts(results),
    }


def retry_counts(results: Sequence[FetchResult]) -> dict[str, int]:
    """download_calls = files + checksum_redownloads + network_retries: each file is downloaded once, plus once more
    if it failed verification, plus once per retry after a temporary network error."""
    download_calls = sum(r.attempts for r in results)
    checksum_redownloads = sum(r.checksum_retries for r in results)
    return {
        "download_calls": download_calls,
        "checksum_redownloads": checksum_redownloads,
        "network_retries": download_calls - len(results) - checksum_redownloads,
    }


def format_stats(results, start: float) -> str:
    """One progress line for the log."""
    s = fetch_stats(list(results), time.monotonic() - start)
    return (
        f"{s['files']} files, {s['ok']} OK, {s['failed']} FAILED, {s['bytes']:,} bytes "
        f"in {s['seconds']:.1f} s ({s['mb_per_s']} MB/s)"
    )


def _verify(staged: Path, remote: RemoteFile) -> tuple[str | None, str | None]:
    """(error or None, actual sha256 or None). The size is checked first because it's cheap."""
    size = staged.stat().st_size
    if size != remote.size:
        return f"size mismatch: expected {remote.size:,} bytes got {size:,}", None
    actual = sha256_of(staged)
    # with no sha256 from Hugging Face the size was the only check; actual is still recorded
    if remote.sha256 and actual != remote.sha256:
        return f"checksum mismatch: expected {remote.sha256[:12]} got {actual[:12]}", actual
    return None, actual


def _describe(exc: BaseException) -> str:
    """A one-line error for the report: the exception type plus at most 300 characters of its message."""
    message = " ".join(str(exc).split())  # Hugging Face errors span several lines
    return f"{type(exc).__name__}: {message[:300]}"
