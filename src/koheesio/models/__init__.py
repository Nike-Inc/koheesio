"""Models package creates models that can be used to base other classes on.

- Every model should be at least a pydantic BaseModel, but can also be a Step, or a StepOutput.
- Every model is expected to be an ABC (Abstract Base Class)
- Optionally a model can inherit ExtraParamsMixin that provides unpacking of kwargs into `extra_params` dict property
  removing need to create a dict before passing kwargs to a model initializer.

A Model class can be exceptionally handy when you need similar Pydantic models in multiple places, for example across
Transformation and Reader classes.
"""

from __future__ import annotations

from typing import Annotated, Any, Dict, List, Optional, Union
from abc import ABC
from functools import cached_property, partial
import inspect
from pathlib import Path
import re
import sys
import warnings

# to ensure that koheesio.models is a drop in replacement for pydantic
from pydantic import BaseModel as PydanticBaseModel
from pydantic import (
    BeforeValidator,
    ConfigDict,
    DirectoryPath,
    Field,
    FilePath,
    InstanceOf,
    PositiveInt,
    PrivateAttr,
    SkipValidation,
    ValidationError,
    conint,
    conlist,
    constr,
    field_serializer,
    field_validator,
    model_validator,
)
from pydantic import SecretBytes as PydanticSecretBytes
from pydantic import SecretStr as PydanticSecretStr

# noinspection PyProtectedMember
from pydantic._internal._generics import PydanticGenericMetadata

# noinspection PyProtectedMember
from pydantic._internal._model_construction import ModelMetaclass

from koheesio.context import Context
from koheesio.logger import Logger, LoggingFactory

__all__ = [
    "BaseModel",
    "ExtraParamsMixin",
    "Field",
    "ListOfColumns",
    # Directly from pydantic
    "ConfigDict",
    "DirectoryPath",
    "FilePath",
    "InstanceOf",
    "ModelMetaclass",
    "PositiveInt",
    "PrivateAttr",
    "PydanticGenericMetadata",
    "SecretBytes",
    "SecretStr",
    "SkipValidation",
    "ValidationError",
    "conint",
    "conlist",
    "constr",
    "field_serializer",
    "field_validator",
    "model_validator",
]


# pylint: disable=function-redefined
class BaseModel(PydanticBaseModel, ABC):  # type: ignore[no-redef]
    """
    Base model for all models.

    Extends pydantic BaseModel with some additional configuration.
    To be used as a base class for all models in Koheesio instead of pydantic.BaseModel.

    Additional methods and properties:
    ---------------------------------
    ### Fields
    Every Koheesio BaseModel has two predefined fields: `name` and `description`. These fields are used to provide a
    name and a description to the model.

    - `name`: This is the name of the Model. If not provided, it defaults to the class name.

    - `description`: This is the description of the Model. It has several default behaviors:
        - If not provided, it defaults to the docstring of the class.
        - If the docstring is not provided, it defaults to the name of the class.
        - For multi-line descriptions, it has the following behaviors:
            - Only the first non-empty line is used.
            - Empty lines are removed.
            - Only the first 3 lines are considered.
            - Only the first 120 characters are considered.

    ### Validators
    - `_set_name_and_description`: Set the name and description of the Model as per the rules mentioned above.

    ### Properties
    - `log`: Returns a logger with the name of the class.

    ### Class Methods
    - `partial`: Create a partial object of the BaseModel, allowing you to set/override default values for some fields.
    - `from_basemodel`: Returns a new BaseModel instance based on the data of another BaseModel.
    - `from_context`: Creates BaseModel instance from a given Context.
    - `from_dict`: Creates BaseModel instance from a given dictionary.
    - `from_json`: Creates BaseModel instance from a given JSON string.
    - `from_toml`: Creates BaseModel object from a given toml file.
    - `from_yaml`: Creates BaseModel object from a given yaml file.
    - `lazy`: Constructs the model without doing validation.

    ### Dunder Methods
    - `__add__`: Allows to add two BaseModel instances together.
    - `__enter__`: Allows for using the model in a with-statement.
    - `__exit__`: Allows for using the model in a with-statement.
    - `__setitem__`: Set Item dunder method for BaseModel.
    - `__getitem__`: Get Item dunder method for BaseModel.

    ### Instance Methods
    - `hasattr`: Check if given key is present in the model.
    - `get`: Get an attribute of the model, but don't fail if not present.
    - `merge`: Merge key,value map with self.
    - `set`: Allows for subscribing / assigning to `class[key]`.
    - `to_context`: Converts the BaseModel instance to a Context object.
    - `to_dict`: Converts the BaseModel instance to a dictionary.
    - `to_json`: Converts the BaseModel instance to a JSON string.
    - `to_yaml`: Converts the BaseModel instance to a YAML string.

    Different Modes
    ---------------
    This BaseModel class supports lazy mode. This means that validation of the items stored in the class can be called
    at will instead of being forced to run it upfront.

    * _Normal mode_:
        you need to know the values ahead of time
        ```python
        normal_mode = YourOwnModel(a="foo", b=42)
        ```

    * _Lazy mode_:
        being able to defer the validation until later
        ```python
        lazy_mode = YourOwnModel.lazy()
        lazy_mode.a = "foo"
        lazy_mode.b = 42
        lazy_mode.validate_output()
        ```
        The prime advantage of using lazy mode is that you don't have to know all your outputs up front, and can add
        them as they become available. All while still being able to validate that you have collected all your output
        at the end.

    * _With statements_:
        With statements are also allowed. The `validate_output` method from the earlier example will run upon exit of
        the with-statement.
        ```python
        with YourOwnModel.lazy() as with_output:
            with_output.a = "foo"
            with_output.b = 42
        ```
        Note: that a lazy mode BaseModel object is required to work with a with-statement.

    Examples
    --------
    ```python
    from koheesio.models import BaseModel


    class Person(BaseModel):
        name: str
        age: int


    # Using the lazy method to create an instance without immediate validation
    person = Person.lazy()

    # Setting attributes
    person.name = "John Doe"
    person.age = 30

    # Now we validate the instance
    person.validate_output()

    print(person)
    ```

    In this example, the Person instance is created without immediate validation. The attributes name and age are set
    afterward. The `validate_output` method is then called to validate the instance.

    Koheesio specific configuration:
    -------------------------------
    Koheesio models are configured differently from Pydantic defaults. The configuration looks like this:

    1. *extra="allow"*\n
        This setting allows for extra fields that are not specified in the model definition. If a field is present in
        the data but not in the model, it will not raise an error.
        Pydantic default is "ignore", which means that extra attributes are ignored.

    2. *arbitrary_types_allowed=True*\n
        This setting allows for fields in the model to be of any type. This is useful when you want to include fields
        in your model that are not standard Python types.
        Pydantic default is False, which means that fields must be of a standard Python type.

    3. *populate_by_name=True*\n
        This setting allows an aliased field to be populated by its name as given by the model attribute, as well as
        the alias. This was known as allow_population_by_field_name in pydantic v1.
        Pydantic default is False, which means that fields can only be populated by their alias.

    4. *validate_assignment=False*\n
        This setting determines whether the model should be revalidated when the data is changed. If set to `True`,
        every time a field is assigned a new value, the entire model is validated again.\n
        Pydantic default is (also) `False`, which means that the model is not revalidated when the data is changed.
        By default, Pydantic validates the data when creating the model. If the user changes the data after creating
        the model, it does _not_ revalidate the model.

    5. *revalidate_instances="subclass-instances"*\n
        This setting determines whether to revalidate models during validation if the instance is a subclass of the
        model. This is important as inheritance is used a lot in Koheesio.
        Pydantic default is `never`, which means that the model and dataclass instances are not revalidated during
        validation.

    6. *validate_default=True*\n
        This setting determines whether to validate default values during validation. When set to True, default values
        are checked during the validation process. We opt to set this to True, as we are attempting to make the sure
        that the data is valid prior to running / executing any Step.
        Pydantic default is False, which means that default values are not validated during validation.

    7. *frozen=False*\n
        This setting determines whether the model is immutable. If set to True, once a model is created, its fields
        cannot be changed.
        Pydantic default is also False, which means that the model is mutable.

    8. *coerce_numbers_to_str=True*\n
        This setting determines whether to convert number fields to strings. When set to True, enables automatic
        coercion of any `Number` type to `str`.
        Pydantic doesn't allow number types (`int`, `float`, `Decimal`) to be coerced as type `str` by default.

    9. *use_enum_values=True*\n
        This setting determines whether to use the values of Enum fields. If set to True, the actual value of the Enum
        is used instead of the reference.
        Pydantic default is False, which means that the reference to the Enum is used.
    """

    model_config = ConfigDict(
        extra="allow",
        arbitrary_types_allowed=True,
        populate_by_name=True,
        validate_assignment=False,
        revalidate_instances="subclass-instances",
        validate_default=True,
        frozen=False,
        coerce_numbers_to_str=True,
        use_enum_values=True,
    )

    name: Optional[str] = Field(default=None, description="Name of the Model")
    description: Optional[str] = Field(default=None, description="Description of the Model")

    @model_validator(mode="after")
    def _validate_name_and_description(self):  # type: ignore[no-untyped-def]
        """
        Validates the 'name' and 'description' of the Model according to the rules outlined in the class docstring.
        """
        self.name = str(self.name or self.__class__.__name__ or "")
        _description = (self.description or self.__doc__ or self.name).split("\n", maxsplit=2)
        self.description = next((line for line in _description if line.strip()), "").strip()

        # Limit the description to around 120 characters, cutting at the first whole word exceeding this limit
        if len(self.description) > 120:
            # Find the first space after the 115th character
            if (space_index := self.description.find(" ", 115)) == -1:
                space_index = 117  # 120 characters - 3 for the ellipsis

            self.description = self.description[:space_index] + "..."

        return self

    @property
    def log(self) -> Logger:
        """Returns a logger with the name of the class"""
        return LoggingFactory.get_logger(name=self.__class__.__name__, inherit_from_koheesio=True)

    @classmethod
    def partial(cls: type, **kwargs: dict) -> partial:
        """
        Create a partial function of the BaseModel.

        Partial allows you to alter or set defaults on an existing BaseModel without needing to create another class to
        use it. Newly provided defaults can always be overridden in a subsequent call.

        Examples
        --------
        ```python
        class SomeStep(BaseModel):
            foo: str
            bar: int


        # Create a partial BaseModel with a default value for 'foo'
        partial_step = SomeStep.partial(foo="default_foo")

        # Instantiate SomeStep with only 'bar' provided, 'foo' will use the default value provided above
        some_step = partial_step(bar=42)
        print(some_step.foo)  # prints 'default_foo'
        print(some_step.bar)  # prints 42

        # Instantiate SomeStep with both 'foo' and 'bar' provided, overriding the default value for 'foo'
        another_step = partial_step(foo="custom_foo", bar=100)
        print(another_step.foo)  # prints 'custom_foo'
        print(another_step.bar)  # prints 100
        ```

        This is advantageous because it allows you to create variations of a model with preset values without having to
        redefine the entire model. It simplifies the instantiation process when you have common default values that
        need to be reused across different instances.

        Parameters
        ----------
        cls : type
            The class type to create a partial function for.
        **kwargs : dict
            Keyword arguments to be passed to the partial function.

        Returns
        -------
        partial
            A partial object with the specified class and keyword arguments.
        """
        return partial(cls, **kwargs)

    @classmethod
    def from_basemodel(cls, basemodel: BaseModel, **kwargs) -> InstanceOf[BaseModel]:  # type: ignore[no-untyped-def]
        """Returns a new BaseModel instance based on the data of another BaseModel"""
        kwargs = {**basemodel.model_dump(), **kwargs}
        return cls(**kwargs)

    @classmethod
    def from_context(cls, context: Context) -> BaseModel:
        """Creates BaseModel instance from a given Context

        You have to make sure that the Context object has the necessary attributes to create the model.

        Examples
        --------
        ```python
        class SomeStep(BaseModel):
            foo: str


        context = Context(foo="bar")
        some_step = SomeStep.from_context(context)
        print(some_step.foo)  # prints 'bar'
        ```

        Parameters
        ----------
        context: Context

        Returns
        -------
        BaseModel
        """
        return cls(**context)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> BaseModel:
        """Creates BaseModel instance from a given dictionary

        Parameters
        ----------
        data: Dict[str, Any]

        Returns
        -------
        BaseModel
        """
        return cls(**data)

    @classmethod
    def from_json(cls, json_file_or_str: Union[str, Path]) -> BaseModel:
        """Creates BaseModel instance from a given JSON string

        BaseModel offloads the serialization and deserialization of the JSON string to Context class. Context uses
        jsonpickle library to serialize and deserialize the JSON string. This is done to allow for objects to be stored
        in the BaseModel object, which is not possible with the standard json library.

        See Also
        --------
        Context.from_json : Deserializes a JSON string to a Context object

        Parameters
        ----------
        json_file_or_str : Union[str, Path]
            Pathlike string or Path that points to the json file or string containing json

        Returns
        -------
        BaseModel
        """
        _context = Context.from_json(json_file_or_str)
        return cls.from_context(_context)

    @classmethod
    def from_toml(cls, toml_file_or_str: Union[str, Path]) -> BaseModel:
        """Creates BaseModel object from a given toml file

        Note: BaseModel offloads the serialization and deserialization of the TOML string to Context class.

        Parameters
        ----------
        toml_file_or_str: str or Path
            Pathlike string or Path that points to the toml file, or string containing toml

        Returns
        -------
        BaseModel
        """
        _context = Context.from_toml(toml_file_or_str)
        return cls.from_context(_context)

    @classmethod
    def from_yaml(cls, yaml_file_or_str: str) -> BaseModel:
        """Creates BaseModel object from a given yaml file

        Note: BaseModel offloads the serialization and deserialization of the YAML string to Context class.

        Parameters
        ----------
        yaml_file_or_str: str or Path
            Pathlike string or Path that points to the yaml file, or string containing yaml

        Returns
        -------
        BaseModel
        """
        _context = Context.from_yaml(yaml_file_or_str)
        return cls.from_context(_context)

    @classmethod
    def lazy(cls):  # type: ignore[no-untyped-def]
        """Constructs the model without doing validation

        Essentially an alias to BaseModel.construct()
        """
        return cls.model_construct()

    def __add__(self, other: Union[Dict, BaseModel]) -> BaseModel:
        """Allows to add two BaseModel instances together

        Essentially a shorthand for running .merge against a second BaseModel object

        Examples
        --------
        ```python
        step_output_1 = StepOutput(foo="bar")
        step_output_2 = StepOutput(lorem="ipsum")
        (
            step_output_1 + step_output_2
        )  # step_output_1 will now contain {'foo': 'bar', 'lorem': 'ipsum'}
        ```

        Parameters
        ----------
        other: Union[Dict, BaseModel]
            Dict or another instance of a BaseModel class that will be added to self

        Returns
        -------
        None
            Add is additive and modifies the object in place, hence there is no return value
        """
        return self.merge(other)

    def __enter__(self):  # type: ignore[no-untyped-def]
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):  # type: ignore[no-untyped-def]
        if exc_type is not None:
            # An exception occurred. We log it and raise it again.
            self.log.exception(f"An exception occurred: {exc_val}")
            return False  # this will re-raise the exception

        # No exception occurred. We validate the final state of the BaseModel.
        self.validate()
        return True

    def __getitem__(self, name) -> Any:  # type: ignore[no-untyped-def]
        """Get Item dunder method for BaseModel

        Allows for subscriptable (`class[key]`) type of access to the data.

        Examples
        --------
        ```python
        step_output = StepOutput(foo="bar")
        step_output["foo"]  # returns 'bar'
        ```

        Parameters
        ----------
        name: str
            The name of the attribute

        Returns
        -------
        Any
            The value of the attribute
        """
        return self.__getattribute__(name)

    def __setitem__(self, key: str, value: Any):  # type: ignore[no-untyped-def]
        """Set Item dunder method for BaseModel

        Allows for subscribing / assigning to `class[key]`

        Examples
        --------
        ```python
        step_output = StepOutput(foo="bar")
        step_output["foo"] = "baz"  # overwrites 'foo' to be 'baz'
        ```

        Parameters
        ----------
        key: str
            The key of the attribute to assign to
        value: Any
            Value that should be assigned to the given key
        """
        self.__setattr__(key, value)

    def hasattr(self, key: str) -> bool:
        """Check if given key is present in the model

        Parameters
        ----------
        key: str

        Returns
        -------
        bool
        """
        return hasattr(self, key)

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        """Get an attribute of the model, but don't fail if not present

        Similar to dict.get()

        Examples
        --------
        ```python
        step_output = StepOutput(foo="bar")
        step_output.get("foo")  # returns 'bar'
        step_output.get("non_existent_key", "oops")  # returns 'oops'
        ```

        Parameters
        ----------
        key: str
            name of the key to get
        default: Optional[Any]
            Default value in case the attribute does not exist

        Returns
        -------
        Any
            The value of the attribute
        """
        if self.hasattr(key):
            return self.__getitem__(key)
        return default

    def merge(self, other: Union[Dict, BaseModel]) -> BaseModel:
        """Merge key,value map with self

        Functionally similar to adding two dicts together; like running `{**dict_a, **dict_b}`.

        Examples
        --------
        ```python
        step_output = StepOutput(foo="bar")
        step_output.merge(
            {"lorem": "ipsum"}
        )  # step_output will now contain {'foo': 'bar', 'lorem': 'ipsum'}
        ```

        Parameters
        ----------
        other: Union[Dict, BaseModel]
            Dict or another instance of a BaseModel class that will be added to self
        """
        if isinstance(other, BaseModel):
            other = other.model_dump()  # ensures we really have a dict

        for k, v in other.items():
            self.set(k, v)

        return self

    def set(self, key: str, value: Any) -> None:
        """Allows for subscribing / assigning to `class[key]`.

        Examples
        --------
        ```python
        step_output = StepOutput(foo="bar")
        step_output.set(foo", "baz")  # overwrites 'foo' to be 'baz'
        ```

        Parameters
        ----------
        key: str
            The key of the attribute to assign to
        value: Any
            Value that should be assigned to the given key
        """
        self.__setitem__(key, value)

    def to_context(self) -> Context:
        """Converts the BaseModel instance to a Context object

        Returns
        -------
        Context
        """
        return Context(**self.to_dict())

    def to_dict(self) -> Dict[str, Any]:
        """Converts the BaseModel instance to a dictionary

        Returns
        -------
        Dict[str, Any]
        """
        return self.model_dump()

    def to_json(self, pretty: bool = False) -> str:
        """Converts the BaseModel instance to a JSON string

        BaseModel offloads the serialization and deserialization of the JSON string to Context class. Context uses
        jsonpickle library to serialize and deserialize the JSON string. This is done to allow for objects to be stored
        in the BaseModel object, which is not possible with the standard json library.

        See Also
        --------
        Context.to_json : Serializes a Context object to a JSON string

        Parameters
        ----------
        pretty : bool, optional, default=False
            Toggles whether to return a pretty json string or not

        Returns
        -------
        str
            containing all parameters of the BaseModel instance
        """
        _context = self.to_context()
        return _context.to_json(pretty=pretty)

    def to_yaml(self, clean: bool = False) -> str:
        """Converts the BaseModel instance to a YAML string

        BaseModel offloads the serialization and deserialization of the YAML string to Context class.

        Parameters
        ----------
        clean: bool
            Toggles whether to remove `!!python/object:...` from yaml or not.
            Default: False

        Returns
        -------
        str
            containing all parameters of the BaseModel instance
        """
        _context = self.to_context()
        return _context.to_yaml(clean=clean)

    # noinspection PyMethodOverriding
    def validate(self) -> BaseModel:
        """Validate the BaseModel instance

        This method is used to validate the BaseModel instance. It is used in conjunction with the lazy method to
        validate the instance after all the attributes have been set.

        This method is intended to be used with the `lazy` method. The `lazy` method is used to create an instance of
        the BaseModel without immediate validation. The `validate` method is then used to validate the instance after.

        > Note: in the Pydantic BaseModel, the `validate` method throws a deprecated warning. This is because Pydantic
        recommends using the `validate_model` method instead. However, we are using the `validate` method here in a
        different context and a slightly different way.

        Examples
        --------
        ```python
        class FooModel(BaseModel):
            foo: str
            lorem: str


        foo_model = FooModel.lazy()
        foo_model.foo = "bar"
        foo_model.lorem = "ipsum"
        foo_model.validate()
        ```
        In this example, the `foo_model` instance is created without immediate validation. The attributes foo and lorem
        are set afterward. The `validate` method is then called to validate the instance.

        Returns
        -------
        BaseModel
            The BaseModel instance
        """
        return self.model_validate(self.model_dump())


# pylint: enable=function-redefined


class ExtraParamsMixin(PydanticBaseModel):
    """
    Mixin class that adds support for arbitrary keyword arguments to Pydantic models.

    The keyword arguments are extracted from the model's `values` and moved to a `params` dictionary.
    """

    params: Dict[str, Any] = Field(default_factory=dict)

    @cached_property
    def extra_params(self) -> Optional[Dict[str, Any]]:
        """Extract params (passed as arbitrary kwargs) from values and move them to params dict"""
        # noinspection PyUnresolvedReferences
        return self.model_extra

    @model_validator(mode="after")
    def _move_extra_params_to_params(self):  # type: ignore[no-untyped-def]
        """Move extra_params to params dict"""
        self.params = {**self.params, **self.extra_params}  # type: ignore[assignment]
        return self


def _list_of_columns_validation(columns_value: Union[str, list]) -> list:
    """
    Performs validation for ListOfColumns type. Will ensure that there are no duplicate columns, empty strings, etc.
    In case an individual column is passed, it will coerce it to a list.
    """
    columns = [columns_value] if isinstance(columns_value, str) else [*columns_value]
    columns = [col for col in columns if col]  # remove empty strings, None, etc.

    return list(dict.fromkeys(columns))  # dict.fromkeys is used to dedup while maintaining order


ListOfColumns = Annotated[Union[str, List[str]], BeforeValidator(_list_of_columns_validation)]
""" Annotated type for a list of column names. 
Will ensure that there are no duplicate columns, empty strings, etc.
In case an individual column is passed, the value will be coerced to a list.
"""


class _SecretMixin:
    """Mixin class that provides additional functionality to Pydantic's SecretStr and SecretBytes classes."""

    def __add__(self, other: Union[str, bytes, "_SecretMixin"]) -> "_SecretMixin":
        """Support concatenation when the SecretMixin instance is on the left side of the + operator.

        Raises
        ------
        TypeError
            If concatenation fails.
        """
        left = self.get_secret_value()
        right = other.get_secret_value() if isinstance(other, _SecretMixin) else other
        return self.__class__(left + right)  # type: ignore

    def __radd__(self, other: Union[str, bytes, "_SecretMixin"]) -> "_SecretMixin":
        """Support concatenation when the SecretMixin instance is on the right side of the + operator.

        Raises
        ------
        TypeError
            If concatenation fails.
        """
        right = self.get_secret_value()
        left = other.get_secret_value() if isinstance(other, _SecretMixin) else other
        return self.__class__(left + right)  # type: ignore

    def __mul__(self, n: int) -> "_SecretMixin":
        """Support multiplication when the SecretMixin instance is on the left side of the * operator.

        Raises
        ------
        TypeError
            If multiplication fails.
        """
        if isinstance(n, int):
            return self.__class__(self.get_secret_value() * n)  # type: ignore
        return NotImplemented

    def __rmul__(self, n: int) -> "_SecretMixin":
        """Support multiplication when the SecretMixin instance is on the right side of the * operator.

        Raises
        ------
        TypeError
            If multiplication fails.
        """
        return self.__mul__(n)


class SecretStr(PydanticSecretStr, _SecretMixin):
    """A string type that ensures the secrecy of its value, extending Pydantic's SecretStr.

    This class provides additional functionality over Pydantic's SecretStr, including:
    - Support for concatenation with other strings and SecretStr instances.
    - Advanced f-string formatting support to ensure the secret value is only revealed in secure contexts.

    For more information on Pydantic's SecretStr, see: https://docs.pydantic.dev/latest/usage/types/#secret-types

    Examples
    --------
    ### Basic Usage
    ```python
    secret = SecretStr("my_secret")
    ```

    ### String representations of the secrets are masked
    ```python
    str(secret)
    # '**********'
    repr(secret)
    # "SecretStr('**********')"
    ```

    ### Concatenations are supported with other strings and SecretStr instances
    ```python
    secret + "suffix"
    # SecretStr('my_secretsuffix')
    "prefix" + secret
    # SecretStr('prefixmy_secret')
    ```

    ### f-string formatting is supported
    If the f-string is called from within a SecretStr, the secret value is returned as we are in a secure context.
    ```python
    new_secret = f"{SecretStr(f'prefix{secret}suffix')}"
    new_secret.get_secret_value()
    ### 'prefixmy_secretsuffix'
    ```

    Otherwise, we consider the context 'unsafe': the SecretStr instance is returned, and the secret value is masked.
    ```python
    f"{secret}"
    "**********"
    ```

    Parameters
    ----------
    secret : str
        The secret value to be stored.

    Methods
    -------
    get_secret_value()
        Returns the actual secret value.
    """

    def __format__(self, format_spec: str) -> str:
        """Advanced f-string formatting support.
        If the f-string is called from within a SecretStr, the secret value is returned as we are in a secure context.
        Otherwise, we consider the context 'unsafe' and we let pydantic take care of the formatting.

        !!! warning "Experimental Feature"\n'
            This method is experimental and may change or be removed in future versions if deemed unstable.
            Use with caution!
        """

        warnings.warn(
            "`SecretStr.__format__`: "
            "This method is experimental and may change or be removed in future versions if deemed unstable. "
            "Use with caution!",
            category=UserWarning,
            stacklevel=2,
        )

        # Inspect the call stack to determine if the string is being passed through SecretStr
        stack = inspect.stack(context=1)
        caller_context = stack[1].code_context[0]  # type: ignore

        # f-string behavior is different from Python 3.12 onwards, so we need to handle it separately
        if sys.version_info >= (3, 12):
            caller_frame = stack[1].frame

            # Check if we are in a multiline f-string
            if not any(substring in caller_context for substring in ("f'", 'f"', ".format")):
                # Multiline f-strings do not show the outer code context, we'll need to extract it from the source code
                source_lines = inspect.getsourcelines(caller_frame)[0]
                lineno = stack[1].positions.lineno  # the line of code that called this function

                # Find the start of the multiline string f""" or f'''
                starting_index = next(
                    (
                        lineno - i - 1
                        for i, line in enumerate(reversed(source_lines[:lineno]))
                        if any(marker in line for marker in ('f"""', "f'''"))
                    ),
                    None,
                )

                # Extract the multiline string
                multiline_string = "".join(source_lines[starting_index:lineno])

                # Remove the code context from the multiline string
                caller_context = multiline_string.replace(caller_context, "").strip()

        # Remove comments from the caller context
        caller_context = re.sub(r"#.*", "", caller_context).strip()

        # Remove the entire string that the format method was called on:
        # 1. matches any string enclosed in single or double quotes that contains 'format('.
        caller_context = re.sub(r'["\'].*?format\(.*?\)["\']', "", caller_context, flags=re.DOTALL).strip()
        # 2. matches any string enclosed in single or double quotes, optionally prefixed with 'f' for f-strings.
        caller_context = re.sub(r'f?["\'].*?["\']', "", caller_context, flags=re.DOTALL).strip()

        # safe context: the secret value is returned
        if "SecretStr(" in caller_context:
            return self.get_secret_value()

        # unsafe context: we let pydantic handle the formatting
        return super().__format__(format_spec)


class SecretBytes(PydanticSecretBytes, _SecretMixin):
    """A bytes type that ensures the secrecy of its value, extending Pydantic's SecretBytes.

    This class provides additional functionality over Pydantic's SecretBytes, including:
    - Support for concatenation with other bytes and SecretBytes instances.
    - Advanced f-string formatting support to ensure the secret value is only revealed in secure contexts.

    For more information on Pydantic's SecretBytes, see: https://docs.pydantic.dev/latest/usage/types/#secret-types

    Examples
    --------
    ### Basic Usage
    ```python
    secret = SecretBytes(b"my_secret")
    ```

    ### String representations of the secrets are masked
    ```python
    str(secret)
    # '**********'
    repr(secret)
    # "SecretBytes('**********')"
    ```

    ### Concatenations are supported with other bytes and SecretBytes instances
    ```python
    secret + b"suffix"
    # SecretBytes(b'my_secretsuffix')
    b"prefix" + secret
    # SecretBytes(b'prefixmy_secret')
    ```

    ### Multiplications are supported with integers
    ```python
    SecretBytes(b"foo") * 2
    # SecretBytes(b'foofoo')
    ```

    Note
    ----
    The pydantic `SecretBytes` class is quite permissive with the types of data it accepts. Hence, you have to ensure
    that the data you pass to the `SecretBytes` '+' operator is of the right type to be concatenated - the `bytes` type
    of the data is not guaranteed.

    For example, python can 'add' two lists together like this:

    ```python
    [1, 2] + [3, 4]  # [1, 2, 3, 4]
    ```

    If one of the list is wrapped in a SecretBytes instance, the '+' operator will work, just like the example above:

    ```python
    list1 = SecretBytes([1, 2, 3])
    list2 = [4, 5, 6]
    list1 + list2  # SecretBytes([1, 2, 3, 4, 5, 6])
    ```

    If however you try to add any other type of data to the `list1` from the example above, you will get a `TypeError`.
    You can use the `secret_data_type` method to check the type of the secret data stored in the `SecretBytes` instance.

    Parameters
    ----------
    secret : bytes
        The secret value to be stored.

    Methods
    -------
    get_secret_value()
        Returns the actual secret value.
    """

    def secret_data_type(self) -> type:
        """Return the type of the secret data."""
        return type(self.get_secret_value())


def _list_of_strings_validation(strings_value: Union[str, list]) -> list:
    """
    Performs validation for ListOfStrings type. Will ensure that whether one string is provided or a list of strings,
    a list is returned.
    """
    strings = [strings_value] if isinstance(strings_value, str) else [*strings_value]
    strings = [string for string in strings if string]  # remove empty strings, None, etc.

    return strings


ListOfStrings = Annotated[Union[str, List[str]], BeforeValidator(_list_of_strings_validation)]
""" Annotated type for a list of strings. Ensures that there are no empty strings, None etc.
In case an individual string is passed, the value will be coerced to a list. """
