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
register_fixtures(*fixture_functions: FixtureFunction, scope: str = "function") -> None
    Register multiple fixtures with the provided scope.
"""

from unittest.mock import MagicMock

# TODO: # safe import pytest and fixture
# print("@@@@ Trying to import pytest and fixture from pytest")
# try:
#     from _pytest.fixtures import FixtureFunction, FixtureValue
#     import pytest
#     from pytest import fixture
#     print("@@@@ Imported pytest and fixture from pytest")
# except (ImportError, ModuleNotFoundError):
#     pytest = MagicMock()
#     fixture = MagicMock()
#     FixtureFunction = MagicMock()  # type: ignore
#     FixtureValue = MagicMock()  # type: ignore
from _pytest.fixtures import FixtureFunction, FixtureValue
import pytest
from pytest import fixture

__all__ = [
    "pytest",
    "fixture",
    "FixtureFunction",
    "FixtureValue",
    "register_fixtures",
]


def register_fixtures(
        *fixture_functions: FixtureFunction,  # type: ignore
        scope: str = "function"
    ) -> None:  # type: ignore
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
    `fixture(fixture_function=existing_function, scope="session", autouse=True)`.

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
    # TODO: handle "ValueError: @pytest.fixture is being applied more than once to the same function '...'""
    for fixture_function in fixture_functions:
        name = fixture_function.__name__
        print(f"Registering fixture: {name}")
        # FIXME: globals call does not work. Problem is that fixture definition needs to be local to a module the way 
        #        it is normally set up
        globals()[name] = fixture(fixture_function=fixture_function, scope=scope, name=name)  # type: ignore
