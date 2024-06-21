"""
Utility functions
"""

import inspect
import uuid
from typing import Any, Callable, Dict, Optional, Tuple
from functools import partial
from importlib import import_module
from pathlib import Path

__all__ = [
    "get_args_for_func",
    "get_project_root",
    "import_class",
    "get_random_string",
    "convert_str_to_bool",
]


def get_args_for_func(func: Callable, params: Dict) -> Tuple[Callable, Dict[str, Any]]:
    """Helper function that matches keyword arguments (params) on a given function

    This function uses inspect to extract the signature on the passed Callable, and then uses functools.partial to
     construct a new Callable (partial) function on which the input was mapped.

    Example
    -------
    ```python
    input_dict = {"a": "foo", "b": "bar"}


    def example_func(a: str):
        return a


    func, kwargs = get_args_for_func(example_func, input_dict)
    ```

    In this example,
    - `func` would be a callable with the input mapped toward it (i.e. can be called like any normal function)
    - `kwargs` would be a dict holding just the output needed to be able to run the function (e.g. {"a": "foo"})

    Parameters
    ----------
    func: Callable
        The function to inspect
    params: Dict
        Dictionary with keyword values that will be mapped on the 'func'

    Returns
    -------
    Tuple[Callable, Dict[str, Any]]
        - Callable
            a partial() func with the found keyword values mapped toward it
        - Dict[str, Any]
            the keyword args that match the func
    """
    _kwargs = {k: v for k, v in params.items() if k in inspect.getfullargspec(func).args}
    return (
        partial(func, **_kwargs),
        _kwargs,
    )


def get_project_root() -> Path:
    """Returns project root path."""
    cmd = Path(__file__)
    return Path([i for i in cmd.parents if i.as_uri().endswith("src")][0]).parent


def import_class(module_class: str) -> Any:
    """Import class and module based on provided string.

    Parameters
    ----------
    module_class module+class to be imported.

    Returns
    -------
    object  Class from specified input string.

    """
    module_path, class_name = module_class.rsplit(".", 1)
    module = import_module(module_path)

    return getattr(module, class_name)


def get_random_string(length: int = 64, prefix: Optional[str] = None) -> str:
    """Generate a random string of specified length"""
    if prefix:
        return f"{prefix}_{uuid.uuid4().hex}"[0:length]
    return f"{uuid.uuid4().hex}"[0:length]


def convert_str_to_bool(value) -> Any:
    """Converts a string to a boolean if the string is either 'true' or 'false'"""
    if isinstance(value, str) and (v := value.lower()) in ["true", "false"]:
        value = v == "true"
    return value
