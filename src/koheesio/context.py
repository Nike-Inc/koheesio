"""
The Context module is a part of the Koheesio framework and is primarily used for managing the environment configuration
where a Task or Step runs. It helps in adapting the behavior of a Task/Step based on the environment it operates in,
thereby avoiding the repetition of configuration values across different tasks.

The Context class, which is a key component of this module, functions similarly to a dictionary but with additional
features. It supports operations like handling nested keys, recursive merging of contexts, and
serialization/deserialization to and from various formats like JSON, YAML, and TOML.

For a comprehensive guide on the usage, examples, and additional features of the Context class, please refer to the
[reference/concepts/context](../reference/concepts/context.md) section of the Koheesio documentation.
"""

from __future__ import annotations

from typing import Any, Dict, Iterator, Union
from collections.abc import Mapping
from pathlib import Path
import re

import jsonpickle  # type: ignore[import-untyped]
import tomli
import yaml

__all__ = ["Context"]


class Context(Mapping):
    """
    The Context class is a key component of the Koheesio framework, designed to manage configuration data and shared
    variables across tasks and steps in your application. It behaves much like a dictionary, but with added
    functionalities.

    Key Features
    ------------
    - _Nested keys_: Supports accessing and adding nested keys similar to dictionary keys.
    - _Recursive merging_: Merges two Contexts together, with the incoming Context having priority.
    - _Serialization/Deserialization_: Easily created from a yaml, toml, or json file, or a dictionary, and can be
        converted back to a dictionary.
    - _Handling complex Python objects_: Uses jsonpickle for serialization and deserialization of complex Python objects
        to and from JSON.

    For a comprehensive guide on the usage, examples, and additional features of the Context class, please refer to the
    [reference/concepts/context](../reference/concepts/context.md) section of the Koheesio documentation.

    Methods
    -------
    add(key: str, value: Any) -> Context
        Add a key/value pair to the context.
    get(key: str, default: Any = None, safe: bool = True) -> Any`
        Get value of a given key.
    get_item(key: str, default: Any = None, safe: bool = True) -> Dict[str, Any]
        Acts just like `.get`, except that it returns the key also.
    contains(key: str) -> bool
        Check if the context contains a given key.
    merge(context: Context, recursive: bool = False) -> Context
        Merge this context with the context of another, where the incoming context has priority.
    to_dict() -> dict
        Returns all parameters of the context as a dict.
    from_dict(kwargs: dict) -> Context
        Creates Context object from the given dict.
    from_yaml(yaml_file: str | Path) -> Context
        Creates Context object from a given yaml file.
    from_json(json_file: str | Path) -> Context
        Creates Context object from a given json file.

    Dunder methods
    --------------

    - _`_iter__()`: Allows for iteration across a Context.
    - `__len__()`: Returns the length of the Context.
    - `__getitem__(item)`: Makes class subscriptable.

    Inherited from Mapping
    ----------------------

    - `items()`: Returns all items of the Context.
    - `keys()`: Returns all keys of the Context.
    - `values()`: Returns all values of the Context.
    """

    def __init__(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        """Initializes the Context object with given arguments."""
        for arg in args:
            if isinstance(arg, dict):
                kwargs.update(arg)
            if isinstance(arg, Context):
                kwargs.update(arg.to_dict())

        if kwargs:
            for key, value in kwargs.items():
                self.__dict__[key] = self.process_value(value)

    def __str__(self) -> str:
        """Returns a string representation of the Context."""
        return str(dict(self.__dict__))

    def __repr__(self) -> str:
        """Returns a string representation of the Context."""
        return self.__str__()

    def __iter__(self) -> Iterator[str]:
        """Allows for iteration across a Context"""
        return self.to_dict().__iter__()

    def __len__(self) -> int:
        """Returns the length of the Context"""
        return self.to_dict().__len__()

    def __getattr__(self, item: str) -> Any:
        try:
            return self.get(item, safe=False)
        except KeyError as e:
            raise AttributeError(item) from e

    def __getitem__(self, item: str) -> Any:
        """Makes class subscriptable"""
        return self.get(item, safe=False)

    @classmethod
    def _recursive_merge(cls, target_context: Context, merge_context: Context) -> Context:
        """
        Recursively merge two dictionaries to an arbitrary depth.

        Parameters
        ----------
        target_context: Context
            Koheesio Context to merge into
        merge_context: Context
            Koheesio Context that is being merged

        Returns
        -------
        Context

        Example
        --------
        ```python
        context_1 = {
            "key_1": "val_1",
            "key_2": {
                "sub_key_1": "sub_val_1",
                "sub_key_2": "sub_val_2",
            },
            "key_3": ["item_1"]
        }

        context_2 = {
            "key_2": {
                "sub_key_2": "sub_val_2.1",
                "sub_key_3": "sub_val_3",
            },
            "key_3": ["item_2"]
        }

        result = {
            "key_1": "val_1",
            "key_2": {
                "sub_key_1": "sub_val_1",
                "sub_key_2": "sub_val_2.1",
                "sub_key_3": "sub_val_3",
            }
            "key_3": ["item_1","item_2]
        }
        ```
        """
        for k, v in merge_context.items():
            if k in target_context and isinstance(target_context[k], Context) and isinstance(v, Context):
                cls._recursive_merge(target_context[k], merge_context[k])
            elif k in target_context and isinstance(target_context[k], list) and isinstance(v, list):
                # merge list items
                target_context[k].extend(merge_context[k])
            else:
                target_context.add(k, v)
        return target_context

    @classmethod
    def from_dict(cls, kwargs: dict) -> Context:
        """Creates Context object from the given dict

        Parameters
        ----------
        kwargs: dict

        Returns
        -------
        Context
        """
        return cls(kwargs)

    @classmethod
    def from_json(cls, json_file_or_str: Union[str, Path]) -> Context:
        """Creates Context object from a given json file

        Note: jsonpickle is used to serialize/deserialize the Context object. This is done to allow for objects to be
        stored in the Context object, which is not possible with the standard json library.

        Why jsonpickle?
        ---------------
        (from https://jsonpickle.github.io/)

        > Data serialized with python’s pickle (or cPickle or dill) is not easily readable outside of python. Using the
        json format, jsonpickle allows simple data types to be stored in a human-readable format, and more complex
        data types such as numpy arrays and pandas dataframes, to be machine-readable on any platform that supports
        json.

        Security
        --------
        (from https://jsonpickle.github.io/)

        > jsonpickle should be treated the same as the Python stdlib pickle module from a security perspective.

        ### ! Warning !
        > The jsonpickle module is not secure. Only unpickle data you trust.
        It is possible to construct malicious pickle data which will execute arbitrary code during unpickling.
        Never unpickle data that could have come from an untrusted source, or that could have been tampered with.
        Consider signing data with an HMAC if you need to ensure that it has not been tampered with.
        Safer deserialization approaches, such as reading JSON directly, may be more appropriate if you are processing
        untrusted data.

        Parameters
        ----------
        json_file_or_str : Union[str, Path]
            Pathlike string or Path that points to the json file or string containing json

        Returns
        -------
        Context
        """
        json_str = json_file_or_str

        # check if json_str is pathlike
        if (json_file := Path(json_file_or_str)).exists():
            json_str = json_file.read_text(encoding="utf-8")

        json_dict = jsonpickle.loads(json_str)
        return cls.from_dict(json_dict)

    @classmethod
    def from_toml(cls, toml_file_or_str: Union[str, Path]) -> Context:
        """Creates Context object from a given toml file

        Parameters
        ----------
        toml_file_or_str: Union[str, Path]
            Pathlike string or Path that points to the toml file or string containing toml

        Returns
        -------
        Context
        """

        # check if toml_str is pathlike
        if (toml_file := Path(toml_file_or_str)).exists():
            toml_str = toml_file.read_text(encoding="utf-8")
        else:
            toml_str = str(toml_file_or_str)

        toml_dict = tomli.loads(toml_str)
        return cls.from_dict(toml_dict)

    @classmethod
    def from_yaml(cls, yaml_file_or_str: str) -> Context:
        """Creates Context object from a given yaml file

        Parameters
        ----------
        yaml_file_or_str: str or Path
            Pathlike string or Path that points to the yaml file, or string containing yaml

        Returns
        -------
        Context
        """
        yaml_str = yaml_file_or_str

        # check if yaml_str is pathlike
        if (yaml_file := Path(yaml_file_or_str)).exists():
            yaml_str = yaml_file.read_text(encoding="utf-8")

        # Bandit: disable yaml.load warning
        yaml_dict = yaml.load(yaml_str, Loader=yaml.Loader)  # nosec B506: yaml_load

        return cls.from_dict(yaml_dict)

    def add(self, key: str, value: Any) -> Context:
        """Add a key/value pair to the context"""
        self.__dict__[key] = value
        return self

    def contains(self, key: str) -> bool:
        """Check if the context contains a given key

        Parameters
        ----------
        key: str

        Returns
        -------
        bool
        """
        try:
            self.get(key, safe=False)
            return True
        except KeyError:
            return False

    def get(self, key: str, default: Any = None, safe: bool = True) -> Any:
        """Get value of a given key

        The key can either be an actual key (top level) or the key of a nested value.
        Behaves a lot like a dict's `.get()` method otherwise.

        Parameters
        ----------
        key:
            Can be a real key, or can be a dotted notation of a nested key
        default:
            Default value to return
        safe:
            Toggles whether to fail or not when item cannot be found

        Returns
        -------
        Any
            Value of the requested item

        Example
        -------
        Example of a nested call:

        ```python
        context = Context({"a": {"b": "c", "d": "e"}, "f": "g"})
        context.get("a.b")
        ```

        Returns `c`
        """
        try:
            # in case key is directly available, or is written in dotted notation
            try:
                return self.__dict__[key]
            except KeyError:
                pass
            if "." in key:
                # handle nested keys
                nested_keys = key.split(".")
                value = self  # parent object
                for k in nested_keys:
                    value = value[k]  # iterate through nested values
                return value

            raise KeyError

        except (AttributeError, KeyError, TypeError) as e:
            if not safe:
                raise KeyError(f"requested key '{key}' does not exist in {self}") from e
            return default

    def get_item(self, key: str, default: Any = None, safe: bool = True) -> Dict[str, Any]:
        """Acts just like `.get`, except that it returns the key also

        Returns
        -------
        Dict[str, Any]
            key/value-pair of the requested item

        Example
        -------
        Example of a nested call:

        ```python
        context = Context({"a": {"b": "c", "d": "e"}, "f": "g"})
        context.get_item("a.b")
        ```

        Returns `{'a.b': 'c'}`
        """
        value = self.get(key, default, safe)
        return {key: value}

    def get_all(self) -> dict:
        """alias to to_dict()"""
        return self.to_dict()

    def merge(self, context: Context, recursive: bool = False) -> Context:
        """Merge this context with the context of another, where the incoming context has priority.

        Parameters
        ----------
        context: Context
            Another Context class
        recursive: bool
            Recursively merge two dictionaries to an arbitrary depth

        Returns
        -------
        Context
            updated context
        """
        if recursive:
            return Context.from_dict(self._recursive_merge(target_context=self, merge_context=context).to_dict())

        # just merge on the top level keys
        return Context.from_dict({**self.to_dict(), **context.to_dict()})

    def process_value(self, value: Any) -> Any:
        """Processes the given value, converting dictionaries to Context objects as needed."""
        if isinstance(value, dict):
            return self.from_dict(value)

        if isinstance(value, (list, set)):
            return [self.from_dict(v) if isinstance(v, dict) else v for v in value]

        return value

    def to_dict(self) -> Dict[str, Any]:
        """Returns all parameters of the context as a dict

        Returns
        -------
        dict
            containing all parameters of the context
        """
        result = {}

        for key, value in self.__dict__.items():
            if isinstance(value, Context):
                result[key] = value.to_dict()
            elif isinstance(value, list):
                result[key] = [e.to_dict() if isinstance(e, Context) else e for e in value]  # type: ignore[assignment]
            else:
                result[key] = value

        return result

    def to_json(self, pretty: bool = False) -> str:
        """Returns all parameters of the context as a json string

        Note: jsonpickle is used to serialize/deserialize the Context object. This is done to allow for objects to be
        stored in the Context object, which is not possible with the standard json library.

        Why jsonpickle?
        ---------------
        (from https://jsonpickle.github.io/)

        > Data serialized with python's pickle (or cPickle or dill) is not easily readable outside of python. Using the
        json format, jsonpickle allows simple data types to be stored in a human-readable format, and more complex
        data types such as numpy arrays and pandas dataframes, to be machine-readable on any platform that supports
        json.

        Parameters
        ----------
        pretty : bool, optional, default=False
            Toggles whether to return a pretty json string or not

        Returns
        -------
        str
            containing all parameters of the context
        """
        d = self.to_dict()
        return jsonpickle.dumps(d, indent=4) if pretty else jsonpickle.dumps(d)

    def to_yaml(self, clean: bool = False) -> str:
        """Returns all parameters of the context as a yaml string

        Parameters
        ----------
        clean: bool
            Toggles whether to remove `!!python/object:...` from yaml or not.
            Default: False

        Returns
        -------
        str
            containing all parameters of the context
        """
        # sort_keys=False to preserve order of keys
        yaml_str = yaml.dump(self.to_dict(), sort_keys=False)

        # remove `!!python/object:...` from yaml
        if clean:
            remove_pattern = re.compile(r"!!python/object:.*?\n")
            yaml_str = re.sub(remove_pattern, "\n", yaml_str)

        return yaml_str
