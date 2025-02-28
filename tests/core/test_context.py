from textwrap import dedent

import pytest

from pydantic import SecretStr

from koheesio.context import Context
from koheesio.utils import get_project_root

test_dict = dict(foo="bar", nested=dict(bar="baz"), listed=["first_item", dict(deeply_nested="lorem ipsum")])
test_context = Context(test_dict)

PROJECT_ROOT = get_project_root()
CONTEXT_FOLDER = PROJECT_ROOT / "tests" / "_data" / "context"
SAMPLE_YAML = CONTEXT_FOLDER / "sample.yaml"
SAMPLE_JSON = CONTEXT_FOLDER / "sample.json"


def test_add():
    context = Context(test_dict)
    context.add("unit", "tests")
    assert context.unit == "tests"


def test_get():
    # test nested values
    actual = test_context.get("nested.bar")
    assert actual == "baz"
    assert actual == test_context.nested.bar

    # test that Context object is subscriptable
    assert actual == test_context["nested.bar"]

    # test deep nesting
    actual = test_context.get("listed")[1].get("deeply_nested")
    assert actual == "lorem ipsum"
    assert actual == test_context.listed[1].deeply_nested

    # test default value
    actual = test_context.get("non_existent_key", "default_value")
    assert actual == "default_value"

    # test that disabling safe mode raises a KeyError
    with pytest.raises(KeyError):
        test_context.get("non_existent_key", safe=False)

    # test that disabling safe mode for nested keys raises a KeyError
    with pytest.raises(KeyError):
        test_context.get("nested.foo", safe=False)

    # test that enabling safe mode for nested keys does not raise a KeyError
    actual = test_context.get("nested.foo", safe=True)
    assert actual is None


def test_get_item():
    actual = test_context.get_item("nested.bar")
    assert actual == {"nested.bar": "baz"}


def test_get_all():
    actual = test_context.get_all().items()
    # comparing two dicts with nested values is not ideal, hence the sort and string casting
    assert str(sorted(actual)) == str(sorted(test_dict.items()))


def test_contains():
    assert test_context.contains("foo")
    assert not test_context.contains("nonexistent_key")


@pytest.mark.parametrize(
    "left_context,right_context,expected,recursive",
    [
        (
            # Test simple merge
            # ---
            # left_context
            test_context,
            # right_context
            Context({"common": {"key": "value"}}),
            # expected
            {**test_dict, **{"common": {"key": "value"}}},
            # recursive
            None,
        ),
        (
            # Test nested merge without recursive
            # ---
            # left_context
            Context({"nested": {"bar": "baz"}}),
            # right_context
            Context({"nested": {"key": "value"}}),
            # expected
            {"nested": {"key": "value"}},
            # recursive
            False,
        ),
        (
            # Test recursive merge with nested values
            # ---
            # left_context
            Context({"nested": {"bar": "baz"}}),
            # right_context
            Context({"nested": {"key": "value"}}),
            # expected
            {"nested": {"key": "value", "bar": "baz"}},
            # recursive
            True,
        ),
        (
            # Test recursive merge with nested values and lists
            # ---
            # left_context
            Context({"nested": {"bar": "baz"}, "listed": ["first_item", {"deeply_nested": "lorem ipsum"}]}),
            # right_context
            Context({"nested": {"key": "value"}, "listed": ["other_item"]}),
            # expected
            {
                "nested": {"key": "value", "bar": "baz"},
                "listed": ["first_item", {"deeply_nested": "lorem ipsum"}, "other_item"],
            },
            # recursive
            True,
        ),
        (
            # Test recursive merge with yaml files
            # ---
            # left_context
            Context.from_yaml(CONTEXT_FOLDER / "common.yml"),
            # right_context
            Context.from_yaml(CONTEXT_FOLDER / "dev.yml"),
            # expected
            {
                "env": "dev",
                "top_level": {
                    "sources": {
                        "foo_table": {"database": "foo_db_dev", "table_name": "foo_table"},
                        "bar_table": {"database": "bar_db_dev", "table_name": "bar_table"},
                        "baz_table": {"database": "baz_db", "table_name": "baz_table"},
                    }
                },
            },
            # recursive
            True,
        ),
        (
            # Test recursive merge
            # ---
            # left_context
            Context({"k1": "v1", "k2": {"sk1": "sv1", "sk2": {"ssk1": "ssv1"}}, "k3": ["i1"]}),
            # right_context
            Context({"k2": {"sk2": {"ssk1": "ssv1.1", "ssk2": "ssv2"}, "sk3": "sv3"}, "k3": ["i2"], "k4": {}}),
            # expected
            {
                "k1": "v1",
                "k2": {
                    "sk1": "sv1",
                    "sk2": {
                        "ssk1": "ssv1.1",
                        "ssk2": "ssv2",
                    },
                    "sk3": "sv3",
                },
                "k3": ["i1", "i2"],
                "k4": {},
            },
            # recursive
            True,
        ),
        (
            # Test Context initialization with another Context object
            # ---
            # left_context
            Context({"foo": "bar"}),
            # right_context
            Context(Context({"baz": "qux"})),
            # expected
            {"foo": "bar", "baz": "qux"},
            # recursive
            None,
        ),
    ],
)
def test_merge(left_context, right_context, expected, recursive):
    if recursive is not None:
        actual = left_context.merge(right_context, recursive=recursive).to_dict()
    else:
        actual = left_context.merge(right_context).to_dict()

    assert actual == expected


def test_from_dict():
    other_context = Context.from_dict(test_dict)
    assert isinstance(other_context, Context)
    assert other_context["foo"] == "bar"


@pytest.mark.parametrize(
    "context,expected",
    [
        (
            Context({"a": 1, "b": "test", "c": Context({"d": 2, "e": "nested"})}),
            {"a": 1, "b": "test", "c": {"d": 2, "e": "nested"}},
        ),
        (
            Context(
                {
                    "a": 1.25,
                    "b": True,
                    "c": [
                        Context({"d": 2, "e": "nested"}),
                        1,
                        "test2",
                        False,
                        Context({"f": 2, "g": [Context({"d": 2, "e": "nested"}), 1, "test3"]}),
                    ],
                }
            ),
            {
                "a": 1.25,
                "b": True,
                "c": [{"d": 2, "e": "nested"}, 1, "test2", False, {"f": 2, "g": [{"d": 2, "e": "nested"}, 1, "test3"]}],
            },
        ),
        (
            # Test Context initialization with another Context object
            Context(Context({"baz": "qux"})),
            {"baz": "qux"},
        ),
        (
            # Test with just kwargs
            Context(foo="bar", baz="qux"),
            {"foo": "bar", "baz": "qux"},
        ),
        (
            # Test with dotted keys
            Context({"a.b.c": 1}),
            {"a.b.c": 1},
        ),
    ],
)
def test_to_dict(context, expected):
    # Call the method to be tested
    result = context.to_dict()
    # Check that the result is a dictionary with the correct values
    assert isinstance(result, dict)
    assert result == expected


@pytest.mark.parametrize(
    "json_path_or_str",
    [
        # all test will check that the output contains the expected key-value pair "foo": "bar"
        # ---
        # 1. test with json file
        SAMPLE_JSON,
        # 2. test with pathlike string
        str(SAMPLE_JSON.as_posix()),
        # 3. test with json string
        '{"foo": "bar"}',
        # 4. check that a json str generated by to_json can be used to create a Context object
        Context({"foo": "bar", "secret": SecretStr("super_secret")}).to_json(),
    ],
)
def test_from_json(json_path_or_str):
    """Test that from_json returns a Context object"""
    actual = Context.from_json(json_path_or_str)
    assert isinstance(actual, Context)
    assert actual["foo"] == "bar"

    if "secret" in actual:
        assert isinstance(actual["secret"], SecretStr)


@pytest.mark.parametrize(
    "yaml_path_or_str",
    [
        # all test will check that the output contains the expected key-value pair "foo": "bar"
        # ---
        # 1. test with yaml file
        SAMPLE_YAML,
        # 2. test with pathlike string
        str(SAMPLE_YAML.as_posix()),
        # 3. test with yaml string
        dedent(
            """
            foo: bar
            nested:
                deeply_nested: lorem ipsum
            """
        ),
    ],
)
def test_from_yaml(yaml_path_or_str):
    """Test that from_yaml returns a Context object"""
    actual = Context.from_yaml(yaml_path_or_str)
    assert isinstance(actual, Context)
    assert actual["foo"] == "bar"


def test_to_yaml():
    """Test that to_yaml returns a string and that the string can be used to create a new Context object"""
    context = Context.from_yaml(SAMPLE_YAML)
    actual_yaml = context.to_yaml(clean=True)
    actual_dict = Context.from_yaml(actual_yaml).to_dict()
    expected = context.to_dict()

    assert isinstance(actual_yaml, str)
    assert actual_dict == expected


def test_to_json():
    """Test that to_json works as intended when given data that is not serializable by standard json library"""

    import json

    # given an input that is not JSON serializable by standard library, a TypeError will be raised
    inp_data: dict = {"attr": SecretStr("val")}
    with pytest.raises(TypeError):
        _ = json.dumps(inp_data)

    # but, Context should be able to serialize this anyway
    to_json_context = Context(inp_data)
    result: str = to_json_context.to_json()
    assert result == '{"attr": {"py/object": "pydantic.types.SecretStr", "_secret_value": "val"}}'


def test_yaml_json_roundtrip():
    """Test that to_yaml and to_json are inverse operations"""
    # given an input that is not JSON serializable by standard library
    inp_data: dict = {"attr": SecretStr("val")}
    context = Context(inp_data)

    # generate json and yaml strings
    j = context.to_json()
    y = context.to_yaml()

    # convert back to Context objects
    j_context = Context.from_json(j)
    y_context = Context.from_yaml(y)

    # check that the resulting Context objects are equal (roundtrip)
    assert j_context.to_yaml() == y_context.to_yaml()
    assert j_context.to_json() == y_context.to_json()


def test_from_toml():
    """Test that from_toml returns a Context object"""
    toml_file = CONTEXT_FOLDER / "sample.toml"

    # Test with pathlike string
    toml_path_str = str(toml_file.as_posix())
    actual = Context.from_toml(toml_path_str)
    assert isinstance(actual, Context)
    assert actual["foo"] == "bar"

    # Test with toml file
    actual = Context.from_toml(toml_file)
    assert isinstance(actual, Context)
    assert actual["foo"] == "bar"

    # Test deeply nested value from toml file
    assert actual["nested"]["listed"]["deeply_nested"] == "lorem ipsum"

    # Test with toml string
    toml_str = """
    foo = "bar"
    nested = { deeply_nested = "lorem ipsum" }
    """
    actual = Context.from_toml(toml_str)
    assert isinstance(actual, Context)
    assert actual["foo"] == "bar"
