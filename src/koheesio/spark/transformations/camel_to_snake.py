"""
Class for converting DataFrame column names from camel case to snake case.
"""

import re

from collections.abc import Callable 

from pydantic import Field

from koheesio.spark.transformations.rename_columns import RenameColumns



def camel_to_snake(name: str) -> str:
    """Convert a camelCase string to snake_case.

    Args:
        name: The camelCase string to be converted.

    Returns:
        str: The converted snake_case string.
    """
    # Replace any lowercase letter or digit followed by an uppercase
    # letter with the same characters separated by an underscore
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    # Replace any lowercase letter or digit followed by an uppercase letter
    # with the same characters separated by an underscore and convert to lowercase
    s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
    # Remove any double underscores
    res = re.sub("_+", "_", s2)
    return res


class CamelToSnakeTransformation(RenameColumns):
    rename_func:Callable[[str], str] | None = Field(default=camel_to_snake, description="Function to convert camelCase to snake_case") # type: ignore
