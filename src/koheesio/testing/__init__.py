"""
Returns
-------
Generator[FixtureFunction]
    A generator that yields registered fixture functions.
Utility functions for pytest fixture registration.

This module provides utility functions for registering pytest fixtures with a specified scope. 
Additionally, it provides a safe import of the `pytest` module and the `fixture` decorator.

Koheesio's customized pytest module is a drop-in replacement for the standard pytest module.

Attributes
----------
pytest : module
    The pytest module, used for writing and running tests.
fixture : function
    The pytest fixture decorator, used to define fixtures.
FixtureFunction : type
    The type of a pytest fixture function.
FixtureValue : type
    The type of the value returned by a pytest fixture.

Functions
---------
register_fixture(fixture_function: FixtureFunction, scope: str = "function") -> FixtureFunction
    Register a single fixture with the provided scope.

register_fixtures(*fixture_functions: FixtureFunction, scope: str = "function") -> Generator[FixtureFunction]
    Register multiple fixtures with the provided scope.
"""

from typing import Generator
from unittest.mock import MagicMock

# safe import pytest and fixture
try:
    from _pytest.fixtures import FixtureFunction, FixtureValue
    import pytest
    from pytest import fixture
except (ImportError, ModuleNotFoundError):
    pytest = MagicMock()
    fixture = MagicMock()
    FixtureFunction = MagicMock()  # type: ignore
    FixtureValue = MagicMock()  # type: ignore

__all__ = [
    "pytest",
    "fixture",
    "FixtureFunction",
    "FixtureValue",
    "register_fixture",
    "register_fixtures",
]


def register_fixture(fixture_function: FixtureFunction, scope: str = "function") -> FixtureFunction:  # type: ignore
    """
    Register a single fixture with the provided scope.

    This function is used to register a fixture with the provided scope. It is meant to be used in conjunction
    with fixtures that are defined in the `utils` module.

    Simply importing a fixture from the `utils` module and using it in a test will not register the fixture. This
    function allows you to register a fixture.

    Note
    ----
    This function is used to do simple fixture registration and hence only supports passing the `scope` argument.
    If you need more complex fixture registration, such as when you want to use params or ids, you should use the
    `pytest.fixture` decorator directly.

    An example of the shorthand for directly calling the decorator on an existing function:
    `fixture(scope="session", autouse=True)(existing_function)`.

    Example
    -------
    In conftest.py of your test directory, you can register a fixture like this:
    ```python
    from koheesio.utils.testing import register_fixture, random_uuid

    register_fixture(random_uuid, scope="session")
    ```

    Parameters
        The scope of the fixture to register, by default "function"
    fixture_function : FixtureFunction
        The fixture to register.
    scope : str, optional
        The scope of the fixture to register, by default "function"
    """
    return pytest.fixture(scope=scope)(fixture_function)  # type: ignore


def register_fixtures(
        *fixture_functions: FixtureFunction,  # type: ignore
        scope: str = "function"
    ) -> Generator[FixtureFunction]:  # type: ignore
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

    register_fixtures(random_uuid, logger, scope="session")
    ```

    Parameters
    ----------
    *fixture_functions : FixtureFunction
        The list of fixtures to register. Provide these as positional args.
    scope : str, optional
        The scope of the fixtures to register, by default "function"
    """
    for fixture_function in fixture_functions:
        yield register_fixture(fixture_function, scope)
