"""Lock tests: a second holder is turned away, and the lock is released after an exception."""

import json

import pytest

from threew.lock import LockHeld, run_lock


def test_second_holder_is_refused_with_the_first_holders_details(tmp_path):
    path = tmp_path / "state" / "ingest.lock"
    with run_lock(path, {"run_id": "run-1"}):
        assert json.loads(path.read_text())["run_id"] == "run-1"
        with pytest.raises(LockHeld, match="run-1") as exc_info, run_lock(path, {"run_id": "run-2"}):
            pass
        assert json.loads(exc_info.value.holder)["run_id"] == "run-1"
    with run_lock(path):  # free again after release
        pass
    assert path.read_text() == ""  # the holder note is cleared on release


def test_released_after_an_exception(tmp_path):
    path = tmp_path / "ingest.lock"
    with pytest.raises(RuntimeError), run_lock(path):
        raise RuntimeError("the run blew up")
    with run_lock(path):
        pass
