"""Test suite for Koheesio's extended BaseModel class"""

from typing import Optional
import json
from textwrap import dedent

import pytest
import yaml

from pydantic import SecretStr as PydanticSecretStr

from koheesio.context import Context
from koheesio.models import BaseModel, ExtraParamsMixin, SecretStr


class TestBaseModel:
    """Test suite for BaseModel class"""
    class SimpleModel(BaseModel):
        a: int
        b: str = "default"

    class FooModel(BaseModel):
        foo: Optional[str] = None
        baz: Optional[int] = None

    def test_partial(self) -> None:
        """Test BaseModel's partial method"""
        # Arrange
        partial_model = self.SimpleModel.partial(a=42, b="baz")
        # Act
        model_standard = partial_model()
        model_with_overwrite = partial_model(b="bla")
        # Assert
        assert model_standard.a == 42
        assert model_standard.b == "baz"
        assert model_with_overwrite.a == 42
        assert model_with_overwrite.b == "bla"

    def test_a_simple_model(self) -> None:
        """Test a simple model."""
        model = self.SimpleModel(a=1)
        assert model.model_dump() == {"a": 1, "b": "default", "description": "SimpleModel", "name": "SimpleModel"}

    def test_context_management_no_exception(self) -> None:
        """Test that with-statement works without throwing exceptions"""
        with self.SimpleModel.lazy() as m:
            m.a = 1
            m.b = "test"
        assert m.a == 1
        assert m.b == "test"

    def test_context_management_with_exception(self) -> None:
        """The context manager should raise the original exception after exiting the context."""
        with pytest.raises(ValueError):
            with self.SimpleModel.lazy() as m:
                m.a = 1
                m.b = "test"
                raise ValueError("Test exception")
        # The fields should still be set even if an exception was raised
        assert m.a == 1
        assert m.b == "test"

    @pytest.fixture(params=[{"foo": "bar"}, {"baz": 123}, {"foo": "bar", "baz": 123}])
    def context_data(self, request: pytest.FixtureRequest) -> dict:
        """Fixture for context data"""
        return request.param

    def test_add(self) -> None:
        """Test BaseModel's add method"""
        # Arrange
        model1 = self.SimpleModel(a=1)
        model2 = self.SimpleModel(a=2)
        # Act
        model = model1 + model2
        # Assert
        assert isinstance(model, BaseModel)
        assert model.a == 2
        assert model.b == "default"

    def test_getitem(self) -> None:
        """Test BaseModel's __getitem__ method"""
        model = self.SimpleModel(a=1)
        assert model["a"] == 1

    def test_setitem(self) -> None:
        """Test BaseModel's __setitem__ method"""
        model = self.SimpleModel(a=1)
        model["a"] = 2
        assert model.a == 2

    def test_hasattr(self) -> None:
        """Test BaseModel's hasattr method"""
        model = self.SimpleModel(a=1)
        assert model.hasattr("a")
        assert not model.hasattr("non_existent_key")

    def test_from_context(self, context_data: pytest.FixtureRequest) -> None:
        """Test BaseModel's from_context method"""
        context = Context(context_data)
        model = self.FooModel.from_context(context)
        assert isinstance(model, BaseModel)
        for key, value in context_data.items():
            assert getattr(model, key) == value

    def test_from_dict(self, context_data: pytest.FixtureRequest) ->  None:
        """Test BaseModel's from_dict method"""
        model = self.FooModel.from_dict(context_data)
        assert isinstance(model, BaseModel)
        for key, value in context_data.items():
            assert getattr(model, key) == value

    def test_from_json(self, context_data: pytest.FixtureRequest) -> None:
        """Test BaseModel's from_json method"""
        json_data = json.dumps(context_data)
        model = self.FooModel.from_json(json_data)
        assert isinstance(model, BaseModel)
        for key, value in context_data.items():
            assert getattr(model, key) == value

    def test_from_toml(self) -> None:
        """Test BaseModel's from_toml method"""
        # Arrange
        toml_data = dedent(
            """
            a = 1
            b = "default"
            """
        )
        # Act
        model = self.SimpleModel.from_toml(toml_data)
        # Assert
        assert isinstance(model, BaseModel)
        assert model.a == 1
        assert model.b == "default"

    def test_from_yaml(self, context_data: pytest.FixtureRequest) -> None:
        """Test BaseModel's from_yaml method"""
        # Arrange
        yaml_data = yaml.dump(context_data)
        # Act
        model = self.FooModel.from_yaml(yaml_data)
        # Assert
        assert isinstance(model, BaseModel)
        for key, value in context_data.items():
            assert getattr(model, key) == value

    def test_to_context(self) -> None:
        """Test BaseModel's to_context method"""
        # Arrange
        model = self.SimpleModel(a=1)
        # Act
        context = model.to_context()
        # Assert
        assert isinstance(context, Context)
        assert context.a == 1
        assert context.b == "default"

    def test_to_dict(self) -> None:
        """Test BaseModel's to_dict method"""
        # Arrange
        model = self.SimpleModel(a=1)
        # Act
        dict_model = model.to_dict()
        # Assert
        assert isinstance(dict_model, dict)
        assert dict_model["a"] == 1
        assert dict_model["b"] == "default"

    def test_to_json(self) -> None:
        """Test BaseModel's to_json method"""
        # Arrange
        model = self.SimpleModel(a=1)
        # Act
        json_model = model.to_json()
        # Assert
        assert isinstance(json_model, str)
        assert '"a": 1' in json_model
        assert '"b": "default"' in json_model

    def test_to_yaml(self) -> None:
        """Test BaseModel's to_yaml method"""
        # Arrange
        model = self.SimpleModel(a=1)
        # Act
        yaml_model = model.to_yaml()
        # Assert
        assert isinstance(yaml_model, str)
        assert "a: 1" in yaml_model
        assert "b: default" in yaml_model

    class ModelWithDescription(BaseModel):
        a: int = 42
        description: str = "This is a\nmultiline description"

    class ModelWithDocstring(BaseModel):
        """Docstring should be used as description
        when no explicit description is provided.
        """

        a: int = 42

    class EmptyLinesShouldBeRemoved(BaseModel):
        """
        Ignore the empty line
        """

        a: int = 42

    class ModelWithNoDescription(BaseModel):
        a: int = 42

    class IgnoreDocstringIfDescriptionIsProvided(BaseModel):
        """This is a docstring"""

        a: int = 42
        description: str = "This is a description"

    @pytest.mark.parametrize(
        "model_class, instance_arg, expected",
        [
            (ModelWithDescription, {"a": 1}, {"a": 1, "description": "This is a", "name": "ModelWithDescription"}),
            (
                ModelWithDocstring,
                {"a": 2},
                {"a": 2, "description": "Docstring should be used as description", "name": "ModelWithDocstring"},
            ),
            (
                EmptyLinesShouldBeRemoved,
                {"a": 3},
                {"a": 3, "description": "Ignore the empty line", "name": "EmptyLinesShouldBeRemoved"},
            ),
            (
                ModelWithNoDescription,
                {"a": 4},
                {"a": 4, "name": "ModelWithNoDescription", "description": "ModelWithNoDescription"},
            ),
            (
                IgnoreDocstringIfDescriptionIsProvided,
                {"a": 5},
                {"a": 5, "description": "This is a description", "name": "IgnoreDocstringIfDescriptionIsProvided"},
            ),
        ],
    )
    def test_name_and_multiline_description(self, model_class: type[BaseModel], instance_arg: dict, expected: dict) -> None:
        """Test that the name and description are correctly set."""
        instance = model_class(**instance_arg)
        assert instance.model_dump() == expected

    class ModelWithLongDescription(BaseModel):
        a: int = 42
        description: str = "This is a very long description. " * 42

    class ModelWithLongDescriptionAndNoSpaces(BaseModel):
        a: int = 42
        description: str = "ThisIsAVeryLongDescription" * 42

    @pytest.mark.parametrize(
        "model_class, expected_length, expected_description",
        [
            (ModelWithLongDescription, 121, "This is a very long description. " * 3 + "This is a very long..."),
            (ModelWithLongDescriptionAndNoSpaces, 120, "ThisIsAVeryLongDescription" * 4 + "ThisIsAVeryLo..."),
        ],
    )
    def test_extremely_long_description(self, model_class: type[BaseModel], expected_length: int, expected_description: str) -> None:
        """Test that the description is truncated if it is too long."""
        model = model_class()
        assert len(model.description) == expected_length
        assert model.description == expected_description
        assert model.description.endswith("...")


class TestExtraParamsMixin:
    """Test suite for ExtraParamsMixin class"""
    def test_extra_params_mixin(self) -> None:
        """Test ExtraParamsMixin class."""
        class SimpleModelWithExtraParams(BaseModel, ExtraParamsMixin):
            a: int
            b: str = "default"

        bar = SimpleModelWithExtraParams(a=1, c=3)
        assert bar.extra_params == {"c": 3}
        assert bar.model_dump() == {
            "a": 1,
            "b": "default",
            "c": 3,
            "description": "SimpleModelWithExtraParams",
            "params": {"c": 3},
            "name": "SimpleModelWithExtraParams",
        }


class TestSecretStr:
    """Test suite for SecretStr class"""
    # reference values
    secret_value = "foobarbazbladibla"
    secret = PydanticSecretStr(secret_value)
    prefix = "prefix"
    suffix = "suffix"
    
    def test_secret_str_str(self) -> None:
        """check that the integrity of the str method in SecretStr is preserved"""
        original = str(self.secret)
        actual = str(SecretStr(self.secret_value))
        expected = "**********"
        assert original == actual == expected
    
    def test_secret_str_repr(self) -> None:
        """check that the integrity of the repr method in SecretStr is preserved"""
        original = repr(self.secret)
        actual = repr(SecretStr(self.secret_value))
        expected = "SecretStr('**********')"
        assert original == actual == expected

    def test_concatenate(self) -> None:
        """check that concatenating a SecretStr with a non-str object raises an exception"""
        # arrange: a class that does not implement __str__
        class StrMethodRaisesException:
            def __str__(self):  # type: ignore
                raise TypeError("Cannot convert to string")

        # act/assert
        secret = SecretStr(self.secret_value)
        with pytest.raises(TypeError):
            _ = secret + StrMethodRaisesException()

        # should not raise any exceptions
        secret + "test"
        secret + 42  # int 
        secret + 3.14  # float
        secret + True  # bool
        secret + None  # None
        secret + [1, 2, 3]  # list
        secret + {"key": "value"}  # dict
        secret + (1, 2)  # tuple
        secret + {1, 2, 3}  # set
        secret + bytes("byte_string", "utf-8")  # bytes

    def test_secret_str_with_f_string_secretstr(self) -> None:
        """check that a str and SecretStr can be combined with one another using f-strings
        Test through using f-string with a SecretStr. Here we expect that the secret gets properly processed.
        """
        # arrange
        secret = SecretStr(self.secret_value)
        # act
        actual_secret = SecretStr(f"{self.prefix}{secret}{self.suffix}")
        # assert
        expected = PydanticSecretStr(f"{self.prefix}{self.secret_value}{self.suffix}")
        assert actual_secret.get_secret_value() == expected.get_secret_value()

    def test_secret_str_with_f_string_str(self) -> None:
        """check that a str and SecretStr can be combined with one another using f-strings
        Test through using f-string with a str. Here we expect the secret to remain hidden.
        """
        # arrange
        secret = SecretStr(self.secret_value)
        # act
        actual_str = f"{self.prefix}{secret}{self.suffix}"
        # assert
        expected = f"{self.prefix}**********{self.suffix}"
        assert actual_str == expected

    def test_secret_str_add(self) -> None:
        """check that a SecretStr and a str can be combined with one another using concatenation"""
        # arrange
        secret = SecretStr(self.secret_value)
        # act
        actual = secret + self.suffix
        # assert
        expected = PydanticSecretStr(self.secret_value + self.suffix)
        assert actual.get_secret_value() == expected.get_secret_value()

    def test_secret_str_radd(self) -> None:
        """check that a str and SecretStr can be combined with one another using concatenation"""
        # arrange
        secret = SecretStr(self.secret_value)
        # act
        actual = self.prefix + secret
        # assert
        expected = PydanticSecretStr(self.prefix + self.secret_value)
        assert actual.get_secret_value() == expected.get_secret_value()
    
    def test_add_two_secret_str(self) -> None:
        """check that two SecretStr can be added together"""
        # arrange
        secret = SecretStr(self.secret_value)
        # act
        actual = secret + secret
        # assert
        expected = PydanticSecretStr(self.secret_value *2)
        assert actual.get_secret_value() == expected.get_secret_value()
