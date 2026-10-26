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
    def __init__(self, path: Path, holder: str) -> None:
        super().__init__(f"{path} is held by another run: {holder or 'unknown holder'}")
        self.path = path
        self.holder = holder


@contextmanager
def run_lock(path: Path, owner: dict[str, Any] | None = None) -> Iterator[None]:
    """Hold an exclusive lock on path, or raise LockHeld at once if another run has it."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a+") as f:
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            f.seek(0)
            raise LockHeld(path, f.read().strip()) from None
        try:
            f.seek(0)
            f.truncate()
            f.write(json.dumps({"pid": os.getpid(), **(owner or {})}))
            f.flush()
            yield
        finally:
            f.seek(0)
            f.truncate()
            f.flush()
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
