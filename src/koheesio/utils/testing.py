"""TODO: add documentation"""
from typing import Generator
import uuid

from _pytest.fixtures import FixtureFunction, FixtureValue
import pytest
from pytest import fixture

from koheesio import LoggingFactory
from koheesio.logger import Logger

__all__ = [
    "pytest",
    "FixtureFunction",
    "FixtureValue",
    "fixture",
]


def register_fixtures(*fixture_functions: FixtureFunction, scope: str = "function") -> Generator[FixtureFunction]:
    """
    Register multiple fixtures with the provided scope.

    This function is used to register a list of fixtures with the provided scope. It is meant to be used in conjunction
    with fixtures that are defined in the `utils` module.

    Simply importing a fixture from the `utils` module and using it in a test will not register the fixture. This
    function allows you to register multiple fixtures at once.

    Note
    ----
    This function is used to do simple fixture registration and hence only supports passing the `scope` argument.
    If you need more complex fixture registration, such as when you want to use params or ids, you should use the
    `pytest.fixture` decorator directly.

    An example of the shorthand for directly calling the decorator on an existing function:
    `fixture(scope="session", autouse=True)(existing_function)`.

    Example
    -------
    In conftest.py of your test directory, you can register multiple fixtures like this:
    ```python
    from koheesio.utils.testing import register_fixtures, random_uuid, logger

    register_fixtures(random_uuid, logger)
    ```

    Parameters
    ----------
    *fixture_functions : FixtureFunction
        The list of fixtures to register. Provide these as positional args.
    scope : str, optional
        The scope of the fixtures to register, by default "session"
    """
    for fixture_function in fixture_functions:
        yield pytest.fixture(scope=scope)(fixture_function)  # type: ignore



@pytest.fixture
def random_uuid() -> str:
    return str(uuid.uuid4()).replace("-", "_")


@pytest.fixture
def logger(random_uuid: str) -> Logger:
    return LoggingFactory.get_logger(name="conf_test" + random_uuid)
