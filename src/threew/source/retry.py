"""Retry network calls on errors that may fix themselves; fail fast on the rest."""

import logging
import time
from collections.abc import Callable
from typing import TypeVar

import httpx2
from huggingface_hub.errors import HfHubHTTPError

log = logging.getLogger(__name__)

T = TypeVar("T")

# Dropped connections and timeouts. Setup errors (bad proxy, unsupported protocol) are left out on purpose.
TRANSIENT_NETWORK_ERRORS = (
    httpx2.TimeoutException,
    httpx2.NetworkError,
    httpx2.RemoteProtocolError,
    ConnectionError,  # includes ConnectionResetError
    TimeoutError,
)


def is_transient(exc: BaseException) -> bool:
    """True for timeouts, connection errors, HTTP 408, 429 and 5xx. False for 401, 404 and everything else."""
    if isinstance(exc, HfHubHTTPError):
        response = getattr(exc, "response", None)
        status = response.status_code if response is not None else None
        return status is not None and (status in (408, 429) or status >= 500)
    return isinstance(exc, TRANSIENT_NETWORK_ERRORS)


def with_retry(
    fn: Callable[[], T],
    max_retries: int,
    backoff: float,
    *,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Call fn(); retry transient errors up to max_retries times, waiting backoff, 2*backoff, 4*backoff, ...

    Errors that won't fix themselves (bad token, wrong repo or path) are raised on the first attempt.
    """
    if max_retries < 0:
        raise ValueError(f"max_retries must be >= 0, got {max_retries}")
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except Exception as exc:
            if attempt == max_retries or not is_transient(exc):
                raise
            wait = backoff * 2**attempt
            log.warning(
                "attempt %d/%d failed (%s: %s), retrying in %gs",
                attempt + 1,
                max_retries + 1,
                type(exc).__name__,
                exc,
                wait,
            )
            sleep(wait)
    raise AssertionError("unreachable")
