"""One run at a time per storage root, using an advisory flock on a lock file.

The lock is released when the holding process exits, even if it crashes, so it can never be left stuck. The lock
file itself stays; it holds a note on who has the lock, for the run that gets turned away.
"""

import fcntl
import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any


class LockHeld(RuntimeError):
    """Raised when another run already holds the lock. holder is that run's note from the lock file."""

    def __init__(self, path: Path, holder: str) -> None:
        super().__init__(f"{path} is held by another run: {holder or 'unknown holder'}")
        self.path = path
        self.holder = holder


@contextmanager
def run_lock(path: Path, owner: dict[str, Any] | None = None) -> Iterator[None]:
    """Hold an exclusive lock on path, or raise LockHeld at once if another run has it."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # "a+" creates the file if it is missing, without wiping the holder's note before we have the lock
    with open(path, "a+") as f:
        try:
            # LOCK_EX: only one holder. LOCK_NB: don't wait for the other run, fail at once
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            # someone else has it: read their note, so the skipped run's report says who
            f.seek(0)
            raise LockHeld(path, f.read().strip()) from None
        try:
            # we have the lock: replace the old note with ours (process ID plus run ID, start time and host)
            f.seek(0)
            f.truncate()
            f.write(json.dumps({"pid": os.getpid(), **(owner or {})}))
            f.flush()
            yield
        finally:
            # clear the note and release. If the process dies instead, the OS releases the lock by itself
            f.seek(0)
            f.truncate()
            f.flush()
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
