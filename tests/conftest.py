import pytest

from threew.config import load_config
from threew.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    """One Spark session for the whole test run, built the same way the jobs build theirs."""
    session = get_spark(load_config("dev"))
    yield session
    session.stop()
