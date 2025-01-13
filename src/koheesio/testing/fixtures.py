import uuid

from koheesio import LoggingFactory
from koheesio.logger import Logger
from koheesio.testing import pytest


def random_uuid() -> str:
    """
    Generate a random UUID4 string with hyphens replaced by underscores.

    Returns
    -------
    str
        A randomly generated UUID4 string with hyphens replaced by underscores.
    """
    return str(uuid.uuid4()).replace("-", "_")


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


def main():
    from pytest import fixture
    from _pytest.config import Config
    from _pytest.fixtures import showfixtures

    def print_hello_world():
        print("hello world!")

    _ = fixture(fixture_function=print_hello_world, scope="function", name="bla_bla")

    config = Config.fromdictargs({}, [])
    showfixtures(config)
    return _
