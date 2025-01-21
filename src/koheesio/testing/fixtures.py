import uuid

from koheesio import LoggingFactory
from koheesio.logger import Logger
from koheesio.testing import pytest


@pytest.fixture(scope="session")
def random_uuid() -> str:
    """
    Generate a random UUID4 string with hyphens replaced by underscores.

    Returns
    -------
    str
        A randomly generated UUID4 string with hyphens replaced by underscores.
    """
    return str(uuid.uuid4()).replace("-", "_")

@pytest.fixture(scope="session")
def logger(random_uuid: str) -> Logger:
    """
    Create and return a logger instance with a unique name.

    Parameters
    ----------
    random_uuid : str
        A random UUID string to ensure the logger name is unique.

    Returns
    -------
    Logger
        A logger instance with a unique name based on the provided UUID.
    """
    return LoggingFactory.get_logger(name="conf_test" + random_uuid)
