import json
from typing import Optional
from textwrap import dedent

import pytest
import yaml

from koheesio.context import Context
from koheesio.models import BaseModel, ExtraParamsMixin


class TestBaseModel:
    class SimpleModel(BaseModel):
        a: int
        b: str = "default"

    class FooModel(BaseModel):
        foo: Optional[str] = None
        baz: Optional[int] = None

    def test_a_simple_model(self):
        """Test a simple model."""
        foo = self.SimpleModel(a=1)
        assert foo.model_dump() == {"a": 1, "b": "default", "description": "SimpleModel", "name": "SimpleModel"}

    def test_context_management_no_exception(self):
        with self.SimpleModel.lazy() as m:
            m.a = 1
            m.b = "test"
        assert m.a == 1
        assert m.b == "test"

    def test_context_management_with_exception(self):
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
    def context_data(self, request):
        return request.param

    def test_add(self):
        model1 = self.SimpleModel(a=1)
        model2 = self.SimpleModel(a=2)
        model = model1 + model2
        assert isinstance(model, BaseModel)
        assert model.a == 2
        assert model.b == "default"

    def test_getitem(self):
        model = self.SimpleModel(a=1)
        assert model["a"] == 1

    def test_setitem(self):
        model = self.SimpleModel(a=1)
        model["a"] = 2
        assert model.a == 2

    def test_hasattr(self):
        model = self.SimpleModel(a=1)
        assert model.hasattr("a")
        assert not model.hasattr("non_existent_key")

    def test_from_context(self, context_data):
        context = Context(context_data)
        model = self.FooModel.from_context(context)
        assert isinstance(model, BaseModel)
        for key, value in context_data.items():
            assert getattr(model, key) == value

    def test_from_dict(self, context_data):
        model = self.FooModel.from_dict(context_data)
        assert isinstance(model, BaseModel)
        for key, value in context_data.items():
            assert getattr(model, key) == value

    def test_from_json(self, context_data):
        json_data = json.dumps(context_data)
        model = self.FooModel.from_json(json_data)
        assert isinstance(model, BaseModel)
        for key, value in context_data.items():
            assert getattr(model, key) == value

    def test_from_toml(self):
        toml_data = dedent(
            """
            a = 1
            b = "default"
            """
        )
        model = self.SimpleModel.from_toml(toml_data)
        assert isinstance(model, BaseModel)
        assert model.a == 1
        assert model.b == "default"

    def test_from_yaml(self, context_data):
        yaml_data = yaml.dump(context_data)
        model = self.FooModel.from_yaml(yaml_data)
        assert isinstance(model, BaseModel)
        for key, value in context_data.items():
            assert getattr(model, key) == value

    def test_to_context(self):
        model = self.SimpleModel(a=1)
        context = model.to_context()
        assert isinstance(context, Context)
        assert context.a == 1
        assert context.b == "default"

    def test_to_dict(self):
        model = self.SimpleModel(a=1)
        dict_model = model.to_dict()
        assert isinstance(dict_model, dict)
        assert dict_model["a"] == 1
        assert dict_model["b"] == "default"

    def test_to_json(self):
        model = self.SimpleModel(a=1)
        json_model = model.to_json()
        assert isinstance(json_model, str)
        assert '"a": 1' in json_model
        assert '"b": "default"' in json_model

    def test_to_yaml(self):
        model = self.SimpleModel(a=1)
        yaml_model = model.to_yaml()
        assert isinstance(yaml_model, str)
        assert "a: 1" in yaml_model
        assert "b: default" in yaml_model

    import pytest

    class ModelWithDescription(BaseModel):
        a: int = "42"
        description: str = "This is a\nmultiline description"

    class ModelWithDocstring(BaseModel):
        """Docstring should be used as description
        when no explicit description is provided.
        """

        a: int = "42"

    class EmptyLinesShouldBeRemoved(BaseModel):
        """
        Ignore the empty line
        """

        a: int = "42"

    class ModelWithNoDescription(BaseModel):
        a: int = "42"

    class IgnoreDocstringIfDescriptionIsProvided(BaseModel):
        """This is a docstring"""

        a: int = "42"
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
    def test_name_and_multiline_description(self, model_class, instance_arg, expected):
        instance = model_class(**instance_arg)
        assert instance.model_dump() == expected

    class ModelWithLongDescription(BaseModel):
        a: int = "42"
        description: str = "This is a very long description. " * 42

    class ModelWithLongDescriptionAndNoSpaces(BaseModel):
        a: int = "42"
        description: str = "ThisIsAVeryLongDescription" * 42

    @pytest.mark.parametrize(
        "model_class, expected_length, expected_description",
        [
            (ModelWithLongDescription, 121, "This is a very long description. " * 3 + "This is a very long..."),
            (ModelWithLongDescriptionAndNoSpaces, 120, "ThisIsAVeryLongDescription" * 4 + "ThisIsAVeryLo..."),
        ],
    )
    def test_extremely_long_description(self, model_class, expected_length, expected_description):
        model = model_class()
        assert len(model.description) == expected_length
        assert model.description == expected_description
        assert model.description.endswith("...")


class TestExtraParamsMixin:
    def test_extra_params_mixin(self):
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
