"""Test suite for Koheesio's extended BaseModel class"""

from typing import Any, Optional
import json
from textwrap import dedent

import pytest
import yaml

from pydantic import SecretBytes as PydanticSecretBytes
from pydantic import SecretStr as PydanticSecretStr

from koheesio.context import Context
from koheesio.models import BaseModel, ExtraParamsMixin, ListOfStrings, SecretBytes, SecretStr


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

    def test_from_dict(self, context_data: pytest.FixtureRequest) -> None:
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
    def test_name_and_multiline_description(
        self, model_class: type[BaseModel], instance_arg: dict, expected: dict
    ) -> None:
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
    def test_extremely_long_description(
        self, model_class: type[BaseModel], expected_length: int, expected_description: str
    ) -> None:
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
    pydantic_secret = PydanticSecretStr(secret_value)
    koheesio_secret = SecretStr(secret_value)
    prefix = "prefix"
    suffix = "suffix"

    class StrMethodRaisesException:
        """a class that does not implement __str__ raise a TypeError"""

        def __str__(self):  # type: ignore
            raise TypeError("Cannot convert to string")

    def test_secret_str_str(self) -> None:
        """check that the integrity of the str method in SecretStr is preserved
        by comparing Pydantic's SecretStr with Koheesio's SecretStr str method output"""
        pydantic_secret_str = str(self.pydantic_secret)
        actual = str(self.koheesio_secret)
        expected = "**********"
        assert pydantic_secret_str == actual == expected

    def test_secret_str_repr(self) -> None:
        """check that the integrity of the repr method in SecretStr is preserved
        by comparing Pydantic's SecretStr with Koheesio's SecretStr repr method output"""
        pydantic_secret_str = repr(self.pydantic_secret)
        actual = repr(self.koheesio_secret)
        expected = "SecretStr('**********')"
        assert pydantic_secret_str == actual == expected

    @pytest.mark.parametrize(
        "other",
        [
            42,  # int
            3.14,  # float
            True,  # bool
            None,  # None
            [1, 2, 3],  # list
            {"key": "value"},  # dict
            (1, 2),  # tuple
            {1, 2, 3},  # set
            bytes("byte_string", "utf-8"),  # bytes
            StrMethodRaisesException(),  # custom class that raises TypeError in __str__
        ],
    )
    def test_concatenate_unhappy(self, other: Any) -> None:
        """check that concatenating a SecretStr with a non-stringable objects raises an exception"""
        with pytest.raises(TypeError):
            _ = self.koheesio_secret + other

    def test_secret_str_with_f_string_secretstr(self) -> None:
        """check that a str and SecretStr can be combined with one another using f-strings
        Test through using f-string with a SecretStr. Here we expect that the secret gets properly processed.
        """
        # arrange
        secret = self.koheesio_secret
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
        secret = self.koheesio_secret
        # act
        actual_str = f"{self.prefix}{secret}{self.suffix}"
        # assert
        expected = f"{self.prefix}**********{self.suffix}"
        assert actual_str == expected

    def test_secret_str_add(self) -> None:
        """check that a SecretStr and a str can be combined with one another using concatenation"""
        # arrange
        secret = self.koheesio_secret
        # act
        actual = secret + self.suffix
        # assert
        expected = PydanticSecretStr(self.secret_value + self.suffix)
        assert actual.get_secret_value() == expected.get_secret_value()

    def test_secret_str_radd(self) -> None:
        """check that a str and SecretStr can be combined with one another using concatenation"""
        # arrange
        secret = self.koheesio_secret
        # act
        actual = self.prefix + secret
        # assert
        expected = PydanticSecretStr(self.prefix + self.secret_value)
        assert actual.get_secret_value() == expected.get_secret_value()

    def test_add_two_secret_str(self) -> None:
        """check that two SecretStr can be added together"""
        # arrange
        secret = self.koheesio_secret
        # act
        actual = secret + secret
        # assert
        expected = PydanticSecretStr(self.secret_value * 2)
        assert actual.get_secret_value() == expected.get_secret_value()

    def test_secret_str_mul_and_rmul(self) -> None:
        """check that a SecretBytes can be multiplied by an integer"""
        # arrange
        secret = self.koheesio_secret
        # act
        actual_mul = secret * 3
        actual_rmul = 3 * secret
        # assert
        expected = PydanticSecretStr(self.secret_value * 3)
        assert actual_mul.get_secret_value() == actual_rmul.get_secret_value() == expected.get_secret_value()

    @pytest.mark.parametrize(
        "secret_value, format_spec, expected",
        [
            # Secure context with different format specifications
            pytest.param("my_secret", "", "my_secret", id="secure context"),
            pytest.param("my_secret", "s", "my_secret", id="secure context with format spec"),
            pytest.param("my_secret", "r", "my_secret", id="secure context with !r"),
            pytest.param("my_secret", "a", "my_secret", id="secure context with !a"),
            # Empty secret value
            pytest.param("", "", "", id="empty string"),
            pytest.param("", "s", "", id="empty string with format spec"),
            # Special characters in secret value
            pytest.param("special_chars!@#", "", "special_chars!@#", id="special characters"),
            pytest.param("special_chars!@#", "s", "special_chars!@#", id="special characters with format spec"),
            # Multiline secret value
            pytest.param("line1\nline2", "", "line1\nline2", id="multiline string"),
            pytest.param("line1\nline2", "s", "line1\nline2", id="multiline string with format spec"),
        ],
    )
    def test_secretstr_format_spec_secure_context(self, secret_value: str, format_spec: str, expected: str) -> None:
        """Test that SecretStr can be formatted with a format spec"""
        secret = SecretStr(secret_value)
        actual = SecretStr(f"{secret:{format_spec}}")
        assert actual.get_secret_value() == expected

    def test_secretstr_format_edge_cases(self) -> None:
        """Test that SecretStr remains secure in edge case situations
        Note that all tests in here are not parameterized as the inline comments are crucial to the test cases.
        """
        input_str = SecretStr("my_super_secret")

        # Test that SecretStr remains secure when an inline comment is added
        non_secure_context = "This is a secret: " + input_str  # This is a comment with SecretStr(
        assert str(non_secure_context) == "**********"

        # Test the same, but with using f-string
        non_secure_context = f"This is a secret: {input_str}"  # This is a comment with SecretStr(
        assert str(non_secure_context) == "This is a secret: **********"

        # Test string interpolation (non secure context)
        interpolated_str = "{}".format(input_str)
        assert interpolated_str == "**********"

        # Test string interpolation (secure context)
        interpolated_str = SecretStr("foo_{}".format(input_str))
        assert str(interpolated_str) == "**********"
        assert interpolated_str.get_secret_value() == "foo_my_super_secret"

        # Test multiline interpolation (non secure context)
        non_secure_context = """
        foo.SecretStr({input_str})
        """.format(input_str=input_str)
        assert non_secure_context == "\n        foo.SecretStr(**********)\n        "

        # Test multiline interpolation (secure context)
        secure_context = SecretStr(
            """
        foo.SecretStr({input_str})
        """.format(input_str=input_str)
        )
        assert str(secure_context) == "**********"

        # Ensure that we have no leakage of the secret value when using string interpolation
        interpolated_str = "foo.SecretStr({})".format(input_str)
        assert interpolated_str == "foo.SecretStr(**********)"

        # Check for """ notation
        non_secure_context = f"""foo.SecretStr({input_str})"""
        assert non_secure_context == "foo.SecretStr(**********)"

        # Check for ''' notation
        non_secure_context = f"""foo.SecretStr({input_str})"""
        assert non_secure_context == "foo.SecretStr(**********)"

        # Check multiline f-string - non secure context
        non_secure_context = f"""
        foo.SecretStr({input_str})
        """
        assert non_secure_context == "\n        foo.SecretStr(**********)\n        "

        # Check multiline f-string - secure context
        secure_context = SecretStr(f"""
        foo.{input_str}
        """)
        assert str(secure_context) == "**********"

        # Check with nested SecretStr - secure context
        secure_context = SecretStr(f"foo.{input_str}.{SecretStr('bar')}")
        assert str(secure_context) == "**********"
        assert secure_context.get_secret_value() == "foo.my_super_secret.bar"

        # Check with nested SecretStr - non secure context
        non_secure_context = f"foo.{input_str}.{SecretStr('bar')}"
        assert non_secure_context == "foo.**********.**********"

        # f-string with special characters - secure context
        special_chars_f_string = SecretStr(f"!@#$%^&*({input_str})")
        assert special_chars_f_string.get_secret_value() == "!@#$%^&*(my_super_secret)"

        # f-string with special characters - non secure context
        non_secure_context = f"!@#$%^&*({input_str})"
        assert non_secure_context == "!@#$%^&*(**********)"

        # f-string with escaped characters - secure context
        escaped_chars_f_string = SecretStr(f"\\n{input_str}\\t")
        assert escaped_chars_f_string.get_secret_value() == "\\nmy_super_secret\\t"

        # f-string with escaped characters - non secure context
        non_secure_context = f"\\n{input_str}\\t"
        assert non_secure_context == "\\n**********\\t"


class TestSecretBytes:
    """Test suite for SecretBytes class"""

    # reference values
    secret_value = b"foobarbazbladibla"
    pydantic_secret = PydanticSecretBytes(secret_value)
    koheesio_secret = SecretBytes(secret_value)
    prefix = b"prefix"
    suffix = b"suffix"

    def test_secret_bytes_str(self) -> None:
        """check that the str method in SecretBytes is preserved"""
        secret = self.koheesio_secret
        actual = str(secret)
        expected = "b'**********'"
        assert actual == expected

    def test_secret_bytes_repr(self) -> None:
        """check that the repr method in SecretBytes is preserve"""
        secret = self.koheesio_secret
        actual = repr(secret)
        expected = "SecretBytes(b'**********')"
        assert actual == expected

    def test_secret_bytes_add(self) -> None:
        """check that a SecretBytes and a bytes can be combined with one another using concatenation"""
        # arrange
        secret = self.koheesio_secret
        # act
        actual = secret + self.suffix
        # assert
        expected = PydanticSecretBytes(self.secret_value + self.suffix)
        assert actual.get_secret_value() == expected.get_secret_value()

    def test_secret_bytes_radd(self) -> None:
        """check that a bytes and SecretBytes can be combined with one another using concatenation"""
        # arrange
        secret = self.koheesio_secret
        # act
        actual = self.prefix + secret
        # assert
        expected = PydanticSecretBytes(self.prefix + self.secret_value)
        assert actual.get_secret_value() == expected.get_secret_value()

    def test_add_two_secret_bytes(self) -> None:
        """check that two SecretBytes can be added together"""
        # arrange
        secret = self.koheesio_secret
        # act
        actual = secret + secret
        # assert
        expected = PydanticSecretBytes(self.secret_value * 2)
        assert actual.get_secret_value() == expected.get_secret_value()

    def test_secret_bytes_mul_and_rmul(self) -> None:
        """check that a SecretBytes can be multiplied by an integer"""
        # arrange
        secret = self.koheesio_secret
        # act
        actual_mul = secret * 3
        actual_rmul = 3 * secret
        # assert
        expected = PydanticSecretBytes(self.secret_value * 3)
        assert actual_mul.get_secret_value() == actual_rmul.get_secret_value() == expected.get_secret_value()

    def test_secret_data_type(self) -> None:
        """check that the correct type is returned. Pydantic's SecretBytes maintains the data type passed to it"""
        # arrange
        secret = SecretBytes([1, 2, 3])
        # act and assert
        assert isinstance(secret.get_secret_value(), list)


class TestAnnotatedTypes:
    class SomeModelWithListOfStrings(BaseModel):
        a: ListOfStrings

    @pytest.mark.parametrize(
        "list_of_strings,expected_list_of_strings",
        [
            ("single_string", ["single_string"]),
            (["foo", "bar"], ["foo", "bar"]),
            (["some_strings_with_a", None, "in_between"], ["some_strings_with_a", "in_between"]),
            (["some_strings_with_an_empty", "", "in_between"], ["some_strings_with_an_empty", "in_between"]),
        ],
    )
    def test_list_of_strings(self, list_of_strings, expected_list_of_strings) -> None:
        model_with = TestAnnotatedTypes.SomeModelWithListOfStrings(a=list_of_strings)
        assert model_with.a == expected_list_of_strings
