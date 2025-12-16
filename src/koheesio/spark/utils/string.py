"""Utilities for string transformations and formatting."""

from typing import Union
from functools import singledispatchmethod
import re

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.connect.column import Column as ConnectColumn


class AnyToSnakeConverter:
    """Converter class that transforms any naming convention to snake_case.

    Supported Formats:
    - camelCase (dromedaryCase): `firstName` -> `first_name`
    - PascalCase (UpperCamelCase): `FirstName` -> `first_name`
    - Ada_Case (Pascal_Snake_Case): `First_Name` -> `first_name`
    - CONSTANT_CASE (SCREAMING_SNAKE_CASE, ALL_CAPS): `FIRST_NAME` -> `first_name`
    - camel_Snake_Case: `first_Name` -> `first_name`
    - kebab-case (dash-case, lisp-case, spinal-case): `first-name` -> `first_name`
    - TRAIN-CASE (COBOL-CASE, SCREAMING-KEBAB-CASE): `FIRST-NAME` -> `first_name`
    - Train-Case (HTTP-Header-Case): `First-Name` -> `first_name`
    - flatcase: `firstname` -> `firstname`
    - UPPERCASE: `FIRSTNAME` -> `firstname`
    - snake_case: `first_name` -> `first_name` (already in target format)

    The class uses singledispatchmethod to automatically dispatch to the appropriate implementation:
    - Python regex implementation for str (fast for schema/column names)
    - PySpark functions implementation for Column (optimized for data transformations)

    For more information on naming conventions, see:
    https://en.wikipedia.org/wiki/Naming_convention_(programming)#Examples_of_multiple-word_identifier_formats

    Class Attributes:
        PATTERN_1: Regex pattern to separate lowercase/digit followed by uppercase letter with lowercase
        PATTERN_2: Regex pattern to separate lowercase/digit followed by uppercase letter
        PATTERN_3: Regex pattern to remove consecutive underscores

    Example:
        ```python
        converter = AnyToSnakeConverter()

        # Automatically uses Python implementation for strings
        converter.convert("camelCase")  # -> "camel_case"
        converter.convert("PascalCase")  # -> "pascal_case"
        converter.convert("Ada_Case")  # -> "ada_case"
        converter.convert("CONSTANT_CASE")  # -> "constant_case"

        # Automatically uses PySpark implementation for Columns
        df = df.select(
            F.transform_keys(map_col, lambda k, v: converter.convert(k))
        )
        ```
    """

    # Regex patterns for PySpark (strings passed to Spark SQL engine)
    PATTERN_1 = "(.)([A-Z][a-z]+)"  # Matches: lowercase/any char followed by uppercase+lowercase
    PATTERN_2 = "([a-z0-9])([A-Z])"  # Matches: lowercase/digit followed by uppercase
    PATTERN_3 = "[_-]+"  # Matches: one or more underscores or dashes

    # Pre-compiled patterns for Python (better performance with re module)
    _COMPILED_PATTERN_1 = re.compile(PATTERN_1)
    _COMPILED_PATTERN_2 = re.compile(PATTERN_2)
    _COMPILED_PATTERN_3 = re.compile(PATTERN_3)

    @singledispatchmethod
    def convert(self, name):
        """Convert various naming conventions to snake_case.

        Dispatches to the appropriate implementation based on input type.
        See class docstring for supported naming conventions.

        Args:
            name: Either a Python string or a PySpark Column expression

        Returns:
            Converted snake_case string or Column expression (same type as input)

        Raises:
            NotImplementedError: If called with an unsupported type
        """
        raise NotImplementedError(f"convert not implemented for type {type(name)}")

    @convert.register
    def _(self, name: str) -> str:
        """Convert various naming conventions to snake_case using Python regex.

        The conversion works by first replacing dashes with underscores (to handle kebab-case),
        then inserting underscores before uppercase letters that follow lowercase letters or digits,
        converting everything to lowercase, and finally collapsing any consecutive separators
        into a single underscore.

        Args:
            name: The string to convert

        Returns:
            The converted snake_case string
        """
        # Replace dashes with underscores (handles kebab-case variants)
        s0 = name.replace("-", "_")

        # Insert underscore before uppercase letters following lowercase/digit
        s1 = self._COMPILED_PATTERN_1.sub(r"\1_\2", s0)

        # Insert underscore and convert to lowercase
        s2 = self._COMPILED_PATTERN_2.sub(r"\1_\2", s1).lower()

        # Collapse consecutive underscores/dashes into single underscore
        result = self._COMPILED_PATTERN_3.sub("_", s2)

        return result

    def _spark_convert(self, column_name: Column | ConnectColumn) -> Column:
        """Convert various naming conventions to snake_case using PySpark functions.

        The conversion works by first replacing dashes with underscores (to handle kebab-case),
        then inserting underscores before uppercase letters that follow lowercase letters or digits,
        converting everything to lowercase, and finally collapsing any consecutive separators
        into a single underscore. This implementation uses PySpark functions for optimal
        performance in distributed data processing.

        Args:
            column_name: The PySpark Column expression to convert

        Returns:
            A PySpark Column expression with the converted snake_case values
        """
        # Replace dashes with underscores (handles kebab-case variants)
        s0 = F.regexp_replace(column_name, "-", "_")

        # Insert underscore before uppercase letters following lowercase/digit
        s1 = F.regexp_replace(s0, self.PATTERN_1, "$1_$2")

        # Insert underscore and convert to lowercase
        s2 = F.lower(F.regexp_replace(s1, self.PATTERN_2, "$1_$2"))

        # Collapse consecutive underscores/dashes into single underscore
        result = F.regexp_replace(s2, self.PATTERN_3, "_")

        return result

    @convert.register
    def _(self, column_name: Column) -> Column:
        return self._spark_convert(column_name)

    @convert.register
    def _(self, column_name: ConnectColumn) -> ConnectColumn:
        return self._spark_convert(column_name)
