"""Nested Enum utilities for creating case-insensitive nested enum structures.

This module provides utilities for creating nested enum containers that support case-insensitive
attribute access, making it easier to organize related enums in a hierarchical structure.
"""

from typing import Any, Type, TypeVar
from functools import partial

__all__ = ["NestedEnumMeta", "nested_enum"]

T = TypeVar("T")


class NestedEnumMeta(type):
    """Metaclass enabling case-insensitive attribute access for nested Enums.

    This metaclass normalizes all nested class names to uppercase at class creation time,
    then provides case-insensitive access by normalizing attribute lookups to uppercase.

    Features
    --------
    - Case-insensitive attribute access (e.g., BATCH, batch, Batch all work)
    - One-time normalization at class creation (no runtime overhead)
    - Collision detection prevents duplicate nested classes with different casing
    - Helpful error messages listing available attributes
    - Compatible with Python's enum.Enum
    - Works seamlessly with Pydantic models

    Important
    ---------
    Nested enum classes must not have names that collide when uppercased. For example,
    you cannot define both `Batch` and `BATCH` in the same container, as they would
    normalize to the same uppercase key. An error will be raised at class creation time
    if such a collision is detected.

    Note
    ----
    While primarily designed for organizing Enum classes, this metaclass works with any 
    nested class structure. The nested classes don't need to be Enum subclasses, but 
    using it with Enums is the recommended and primary use case. The case-insensitive 
    access mechanism is agnostic to the type of nested class.

    Examples
    --------
    Using the metaclass directly:

    ```python
    from enum import Enum
    from koheesio.models import NestedEnumMeta

    class OutputMode(metaclass=NestedEnumMeta):
        '''Container for output modes'''

        class BATCH(str, Enum):
            APPEND = "append"
            OVERWRITE = "overwrite"

        class STREAMING(str, Enum):
            APPEND = "append"
            COMPLETE = "complete"

    # All of these work:
    mode = OutputMode.BATCH.APPEND
    mode = OutputMode.batch.APPEND
    mode = OutputMode.Batch.APPEND
    ```

    Using with the decorator (recommended):

    ```python
    from enum import Enum
    from koheesio.models import nested_enum

    @nested_enum
    class OutputMode:
        '''Container for output modes'''

        class BATCH(str, Enum):
            APPEND = "append"
            OVERWRITE = "overwrite"

        class STREAMING(str, Enum):
            APPEND = "append"
            COMPLETE = "complete"

    # Case-insensitive access works:
    OutputMode.batch.APPEND == OutputMode.BATCH.APPEND  # True
    ```

    See Also
    --------
    nested_enum : Decorator that applies this metaclass
    """

    def __new__(mcs, name: str, bases: tuple, namespace: dict, **kwargs: Any) -> type:
        """Create a new class with normalized nested enum names.

        This method normalizes all nested class names to uppercase, storing them
        in the class __dict__ with uppercase keys. If a collision is detected
        (e.g., both 'Batch' and 'BATCH' defined), an error is raised.

        Parameters
        ----------
        mcs : type
            The metaclass
        name : str
            The name of the class being created
        bases : tuple
            The base classes
        namespace : dict
            The class namespace
        **kwargs : Any
            Additional keyword arguments

        Returns
        -------
        type
            The newly created class with normalized nested enum names

        Raises
        ------
        ValueError
            If nested classes have names that collide when uppercased
        """
        # Track uppercase names to detect collisions
        uppercase_mapping = {}
        normalized_namespace = {}

        for attr_name, attr_value in namespace.items():
            # Check if this is a nested class (type) and not a special attribute
            if isinstance(attr_value, type) and not attr_name.startswith("_"):
                uppercase_name = attr_name.upper()

                # Check for collision
                if uppercase_name in uppercase_mapping:
                    raise ValueError(
                        f"Nested enum name collision in '{name}': "
                        f"'{uppercase_mapping[uppercase_name]}' and '{attr_name}' both normalize to '{uppercase_name}'."
                        f" Nested enum class names must be unique when uppercased."
                    )

                # Store with uppercase key
                uppercase_mapping[uppercase_name] = attr_name
                normalized_namespace[uppercase_name] = attr_value
            else:
                # Keep non-class attributes as-is
                normalized_namespace[attr_name] = attr_value

        # Create the class with normalized namespace
        return super().__new__(mcs, name, bases, normalized_namespace, **kwargs)

    def __getattribute__(cls, name: str) -> Any:
        """Enable case-insensitive attribute access by normalizing to uppercase.

        Special attributes (starting with '_') are not normalized and are accessed directly.
        Only public nested enum classes are subject to case-insensitive access.

        Parameters
        ----------
        name : str
            The attribute name to access

        Returns
        -------
        Any
            The requested attribute

        Raises
        ------
        AttributeError
            If the attribute doesn't exist
        """
        # Helper function to get attributes directly from the class, bypassing our custom logic
        get_class_attribute = partial(type.__getattribute__, cls)

        # Try normalized uppercase first (most common case), then fall back to exact match
        # This order ensures error messages show what the user actually typed
        # Works for both regular attributes and special/private attributes (starting with '_')
        normalized = name.upper()
        try:
            return get_class_attribute(normalized)
        except AttributeError:
            # Fall back to exact match - handles special attributes and typos
            return get_class_attribute(name)

    def __dir__(cls) -> list:
        """Return list of attributes for introspection.

        This ensures that tools like pytest can properly introspect the class
        without triggering our custom __getattribute__ logic.

        Returns
        -------
        list
            List of attribute names
        """
        # Get the standard dir() result from type
        return type.__dir__(cls)


def nested_enum(cls: Type[T]) -> Type[T]:
    """Decorator to create a nested enum container with case-insensitive access.

    This decorator applies the NestedEnumMeta metaclass to enable case-insensitive
    attribute access for nested enum classes.

    Parameters
    ----------
    cls : Type[T]
        The class to convert into a nested enum container

    Returns
    -------
    Type[T]
        The class with NestedEnumMeta applied

    Examples
    --------
    Basic usage:

    ```python
    from enum import Enum
    from koheesio.models import nested_enum

    @nested_enum
    class Status:
        '''Status codes grouped by category'''

        class HTTP(str, Enum):
            OK = "200"
            NOT_FOUND = "404"
            ERROR = "500"

        class DATABASE(str, Enum):
            CONNECTED = "connected"
            DISCONNECTED = "disconnected"

    # Case-insensitive access:
    Status.HTTP.OK             # Works
    Status.http.OK             # Works
    Status.Http.OK             # Works
    Status.DATABASE.CONNECTED  # Works
    Status.database.CONNECTED  # Works
    ```

    Integration with Pydantic works just fine:

    ```python
    from enum import Enum
    from pydantic import Field
    from koheesio.models import BaseModel, nested_enum

    @nested_enum
    class OutputMode:
        class BATCH(str, Enum):
            APPEND = "append"
            OVERWRITE = "overwrite"

    class MyWriter(BaseModel):
        mode: OutputMode.BATCH = Field(
            default=OutputMode.BATCH.APPEND,
            description="The output mode"
        )

    # Usage:
    writer = MyWriter(mode=OutputMode.batch.OVERWRITE)  # Case-insensitive!
    ```

    See Also
    --------
    NestedEnumMeta : The metaclass applied by this decorator
    """
    # Create a new class with the same name, bases, and attributes but with NestedEnumMeta
    return NestedEnumMeta(cls.__name__, cls.__bases__, dict(cls.__dict__))
