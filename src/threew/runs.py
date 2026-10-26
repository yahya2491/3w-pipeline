"""Run identity: every run gets one ID, a UTC time plus a random suffix, used for staging, manifest rows and report."""

import uuid
from datetime import UTC, datetime


def new_run_id() -> tuple[str, datetime]:
    """(run_id, started_at in UTC). The ID sorts by start time; the random suffix keeps two runs apart."""
    started_at = datetime.now(UTC)
    return f"{started_at:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}", started_at
