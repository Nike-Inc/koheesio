from __future__ import annotations

from typing import Any
from copy import deepcopy
from functools import wraps
import io
from logging import Logger
import os
from unittest import mock
from unittest.mock import call, patch
import warnings

import pytest

from pydantic import ValidationError

from koheesio.models import Field
from koheesio.steps import Step, StepMetaClass, StepOutput
from koheesio.steps.dummy import DummyOutput, DummyStep
from koheesio.utils import get_project_root

output_dict_1 = dict(a="foo", b=42)
test_output_1 = DummyOutput(**output_dict_1)

output_dict_2 = dict(a="bar", b=69)
test_output_2 = DummyOutput(**output_dict_2)

lazy_output = DummyOutput.lazy()

# we put the newline in the description to test that the newline is removed
test_step = DummyStep(a="foo", b=2, description="Dummy step for testing purposes.\nwith a newline")

PROJECT_ROOT = get_project_root()


class TestStepOutput:
    @pytest.mark.parametrize(
        "output_dict, expected",
        [(output_dict_1, output_dict_1), (output_dict_2, output_dict_2)],
    )
    def test_stepoutput_validate_output(self, output_dict, expected):
        """Tests that validate_output returns the expected output dict"""
        test_output = DummyOutput(**output_dict)
        actual = test_output.validate_output().model_dump()

        # we don't care about these fields
        actual.pop("name")
        actual.pop("description")

        # assert that the output is as expected
        assert actual == expected

    @pytest.mark.parametrize("output_dict", [dict(), dict(a="foo"), dict(b=42)])
    def test_stepoutput_unhappy_flow(self, output_dict):
        """Tests that validate_output raises an error when the output is not valid"""
        with pytest.raises(ValidationError):
            DummyOutput(**output_dict)

    @pytest.mark.parametrize(
        "a, b, expected",
        [
            # unhappy flow
            (None, None, ValidationError),
            # happy flow
            (
                "foo",
                42,
                {
                    "a": "foo",
                    "b": 42,
                    "name": "DummyOutput",
                    "description": "Dummy output for testing purposes.",
                },
            ),
            # test wrong type assigned
            ("foo", "invalid type", ValidationError),
        ],
    )
    def test_stepoutput_lazy(self, a, b, expected):
        """Tests that lazy output works as expected, also tests validate_output"""
        lazy_output = DummyOutput.lazy()
        lazy_output.a = a
        lazy_output.b = b

        with warnings.catch_warnings(record=True):
            warnings.filterwarnings(
                "ignore",
                message=r"^'Pydantic serializer warnings:\n  Expected `int` but got `str` - serialized value may not be as expected'",
                category=UserWarning,
            )
            if expected == ValidationError:
                with pytest.raises(expected):
                    lazy_output.validate_output()
            else:
                actual = lazy_output.validate_output().model_dump()
                assert actual == expected

    @pytest.mark.parametrize("attribute, expected", [("a", True), ("d", False)])
    def test_stepoutput_hasattr(self, attribute, expected):
        """Tests that hasattr works as expected"""
        assert hasattr(test_output_1, attribute) == expected

    def test_stepoutput_getters_and_setters(self):
        """Tests that getters and setters work as expected"""
        test_value = "bar"

        # __setitem__ and __getitem__
        test_a = deepcopy(test_output_1)
        test_a.a = test_value
        assert test_a.a == test_value

        # test subscriptable getter and setter
        test_b = deepcopy(test_output_1)
        test_b["a"] = test_value
        assert test_b["a"] == test_value

        # test get and set methods
        test_c = deepcopy(test_output_1)
        test_c.set("a", test_value)
        assert test_c.get("a") == test_value

        # test get with default value
        test_d = deepcopy(test_output_1)
        expected = "default is working"
        actual = test_d.get("non_existent", expected)
        assert actual == expected

    def test_stepoutput_merge_and_add(self):
        """Tests that merge and add work as expected"""
        expected = {**output_dict_1, **output_dict_2}

        # test merge
        test_a = deepcopy(test_output_1)
        test_a.merge(test_output_2)
        actual = test_a.model_dump()
        actual.pop("name")
        actual.pop("description")
        assert actual == expected

        # test add (through += notation)
        test_b = deepcopy(test_output_1)
        test_b += test_output_2
        actual = test_b.model_dump()
        actual.pop("name")
        actual.pop("description")
        assert actual == expected

        # test add (through + notation)
        test_c = deepcopy(test_output_1)
        test_c + test_output_2
        actual = test_c.model_dump()
        actual.pop("name")
        actual.pop("description")
        assert actual == expected


class TestStep:
    def test_simple_step(self):
        class SimpleStep(Step):
            a: str

            class Output(StepOutput):
                b: str

            def execute(self) -> Output:
                self.output.b = f"{self.a}-some-suffix"

        step = SimpleStep(a="foo")

        assert step.model_dump() == dict(a="foo", description="SimpleStep", name="SimpleStep")
        assert step.execute().model_dump() == dict(
            b="foo-some-suffix",
            name="SimpleStep.Output",
            description="Output for SimpleStep",
        )

        # as long as the following doesn't raise an error, we're good
        lazy_step_output = SimpleStep.Output.lazy()
        lazy_step_output.b = "bar"
        lazy_step_output.validate_output()

    def test_step_execute_and_run(self):
        """test that execute returns implicitly and that output is accessible
        note: this ensures that the metaclass wrapper is working as expected
        """
        actual_execute = test_step.execute()
        actual_run = test_step.run()
        expected = test_step.Output(
            name=test_step.name + ".Output",
            description=f"Output for {test_step.name}",
            a="foo",
            b=2,
            c="foofoo",
        )

        assert actual_execute == actual_run == expected

        # 3 ways to retrieve output
        actual = actual_execute
        assert actual.get("a") == actual.a == actual["a"] == expected["a"]
        assert actual.get("b") == actual.b == actual["b"] == expected["b"]
        assert actual.get("c") == actual.c == actual["c"] == expected["c"]

    def test_step_to_yaml_description_and_name(self):
        """Tests that to_yaml works as expected"""
        # test that description and name are set correctly prior to executing
        assert test_step.name == "DummyStep"
        assert test_step.description == "Dummy step for testing purposes."

        # execute the step
        test_step.execute()
        expected_output_path = PROJECT_ROOT / "tests" / "_data" / "steps"

        # using .strip() to avoid the test failing on leading or trailing whitespaces
        expected = (expected_output_path / "expected_step_output.yaml").read_text().strip()
        actual = test_step.repr_yaml().strip()
        assert actual == expected

        # testing with simple=True
        expected_simple = (expected_output_path / "expected_step_output_simple.yaml").read_text().strip()
        actual_simple = test_step.repr_yaml(simple=True).strip()
        assert actual != actual_simple == expected_simple

    def test_get_description_and_name(self):
        assert test_step.name == "DummyStep"
        assert test_step.description == "Dummy step for testing purposes."

    def test_repr_str_dunder(self):
        # using .strip() to avoid the test failing on leading or trailing whitespaces
        actual = str(DummyStep(a="foo", b=2)).strip()
        expected = "DummyStep\n=========\ninput:\n  a: foo\n  b: 2"
        assert actual == expected

    def test_step_with_super_call(self):
        class MyCustomParentStep(Step):
            foo: str = Field(default=..., description="Foo")
            bar: str = Field(default=..., description="Bar")

            class Output(Step.Output):
                baz: str = Field(default=..., description="Baz")

            def execute(self) -> Output:
                self.output.baz = self.foo + self.bar

        class MyCustomChildStepSuperCallFirst(MyCustomParentStep):
            class Output(MyCustomParentStep.Output):
                qux: str = Field(default=..., description="Qux")

            def execute(self) -> Output:
                super().execute()
                self.output.qux = self.foo + "qux"

        class MyCustomChildStepSuperCallAfter(MyCustomParentStep):
            class Output(MyCustomParentStep.Output):
                qux: str = Field(default=..., description="Qux")

            def execute(self) -> Output:
                self.output.qux = self.foo + "qux"
                super().execute()

        super_call_after_output = MyCustomChildStepSuperCallAfter(foo="foo", bar="bar", qux="qux")
        assert super_call_after_output.execute() is not None

        super_call_first_output = MyCustomChildStepSuperCallFirst(foo="foo", bar="bar", qux="qux")
        assert super_call_first_output.execute() is not None

    def test_execute_wrapper_only_called_once_when_nested(self):
        """
        Tests that _execute_wrapper is only called once when nested multiple times

        This can happen when a step is inherited from another step, and the child step has a _execute_wrapper decorator
        applied to it. In this case, the _execute_wrapper decorator could be called multiple times, which is not what we want.
        This test checks that Koheesio is not calling _execute_wrapper multiple times.
        """

        class FooStep(Step):
            foo: str

            class Output(Step.Output):
                bar: str

            @wraps(StepMetaClass._execute_wrapper)
            @wraps(StepMetaClass._execute_wrapper)
            @wraps(StepMetaClass._execute_wrapper)
            @wraps(StepMetaClass._execute_wrapper)
            @wraps(StepMetaClass._execute_wrapper)
            @wraps(StepMetaClass._execute_wrapper)
            def execute(self):
                self.output.bar = self.foo

        obj = FooStep(foo="bar")
        obj.execute()

        # Check that do_execute was not called multiple times
        assert (
            getattr(
                getattr(obj.execute, "_partialmethod", None),
                "_step_execute_wrapper_sentinel",
                None,
            )
            is StepMetaClass._step_execute_wrapper_sentinel
        )
        assert getattr(getattr(obj.execute, "_partialmethod", None), "_wrap_count", 0) == 1

    class YourClass(Step):
        def execute(self):
            self.log.info("This is from the execute method of YourClass")

    class YourClass2(YourClass):
        def execute(self):
            self.log.info("This is from the execute method of YourClass2")

    class YourClass3(YourClass2):
        def execute(self):
            self.log.info("This is from the execute method of YourClass3")

    class YourClassNoExecute(YourClass): ...

    class YourClass4(YourClassNoExecute):
        def execute(self):
            super().execute()
            self.log.info("This is from the execute method of YourClass4")

    class MyMetaClass(StepMetaClass):
        @classmethod
        def _log_end_message(cls, step: Step, skip_logging: bool = False, *args, **kwargs):
            print("It's me from custom meta class")
            super()._log_end_message(step, skip_logging, *args, **kwargs)

    class MyMetaClass2(StepMetaClass):
        @classmethod
        def _validate_output(cls, step: Step, skip_validating: bool = False, *args, **kwargs):
            step.output.dummy_value = "dummy"

    class YourClassWithCustomMeta(Step, metaclass=MyMetaClass):
        def execute(self):
            self.log.info(f"This is from the execute method of {self.__class__.__name__}")

    class YourClassWithCustomMeta2(Step, metaclass=MyMetaClass2):
        def execute(self):
            self.log.info(f"This is from the execute method of {self.__class__.__name__}")

    @pytest.mark.parametrize("test_class", [YourClassWithCustomMeta])
    def test_custom_metaclass_log(self, test_class):
        with (
            patch.object(test_class, "log", autospec=True) as mock_log,
            patch("sys.stdout", new=io.StringIO()) as mock_stdout,
        ):
            obj = test_class()
            obj.execute()

            # Get the output of print statements
            print_output = mock_stdout.getvalue()

            name = test_class.__name__

            # Check that logs were called once (and only once) with the correct messages, and in the correct order
            calls = [
                call.info("Start running step"),
                call.debug(f"Step Input: name='{name}' description='{name}'"),
                call.info(f"This is from the execute method of {name}"),
                call.debug(f"Step Output: name='{name}.Output' description='Output for {name}'"),
                call.info("Finished running step"),
            ]
            mock_log.assert_has_calls(calls, any_order=False)

            assert "It's me from custom meta class" in print_output

    def test_log_and_wrapper_duplication(self):
        """
        Test that when a step is inherited multiple times, the log and wrapper are only called once when subclasses do
        not have explicit execute methods set.
        """

        class MyCustomParentStep(Step):
            foo: str = Field(default=..., description="Foo")
            bar: str = Field(default=..., description="Bar")

            class Output(Step.Output):
                baz: str = Field(default=..., description="Baz")

            def execute(self) -> Output:
                self.log.error("This should not be logged")
                self.output.baz = self.foo + self.bar

        class MyCustomChildStep(MyCustomParentStep): ...

        class MyCustomGrandChildStep(MyCustomChildStep):
            def execute(self) -> MyCustomChildStep.Output:
                self.output.baz = self.foo + self.bar

        class MyCustomGreatGrandChildStep(MyCustomGrandChildStep): ...

        with (
            patch.object(MyCustomGreatGrandChildStep, "log", autospec=True) as mock_log,
        ):
            obj = MyCustomGreatGrandChildStep(foo="foo", bar="bar", qux="qux")
            obj.execute()

            name = MyCustomGreatGrandChildStep.__name__

            # Check that logs were called once (and only once) with the correct messages, and in the correct order
            calls = [
                call.info("Start running step"),
                call.debug(f"Step Input: name='{name}' description='{name}' foo='foo' bar='bar' qux='qux'"),
                call.debug(f"Step Output: name='{name}.Output' description='Output for {name}' baz='foobar'"),
                call.info("Finished running step"),
            ]
            mock_log.assert_has_calls(calls, any_order=False)

            # Check that the execute method is only wrapped once
            assert getattr(getattr(obj.execute, "_partialmethod", None), "_wrap_count", 0) == 1

    @pytest.mark.parametrize("test_class", [YourClassWithCustomMeta2])
    def test_custom_metaclass_output(self, test_class):
        with patch.object(test_class, "log", autospec=True) as mock_log:
            obj = test_class()
            obj.execute()

            name = test_class.__name__

            # Check that logs were called once (and only once) with the correct messages, and in the correct order
            calls = [
                call.info("Start running step"),
                call.debug(f"Step Input: name='{name}' description='{name}'"),
                call.info(f"This is from the execute method of {name}"),
                call.debug(f"Step Output: name='{name}.Output' description='Output for {name}' dummy_value='dummy'"),
                call.info("Finished running step"),
            ]
            mock_log.assert_has_calls(calls, any_order=False)

            assert obj.output.dummy_value == "dummy"

    @pytest.mark.parametrize(
        "test_class, log_entry",
        [
            pytest.param(YourClass, [call.info("This is from the execute method of YourClass")], id="No inheritance"),
            pytest.param(
                YourClass2, [call.info("This is from the execute method of YourClass2")], id="1 level of inheritance"
            ),
            pytest.param(
                YourClass3, [call.info("This is from the execute method of YourClass3")], id="2 levels of inheritance"
            ),
            pytest.param(
                YourClass4,
                [
                    call.info("This is from the execute method of YourClass"),
                    call.info("This is from the execute method of YourClass4"),
                ],
                id="Multiple levels of inheritance with super call",
            ),
        ],
    )
    def test_log_called_once(self, test_class: Step, log_entry: list) -> None:
        """
        Test that logs are correctly generated when a step is inherited multiple times.
        """
        # Arrange
        with mock.patch.object(test_class, "log", autospec=True) as mock_log:
            # Act
            obj = test_class()
            obj.execute()

            # Assert: Check that logs were called once (and only once) with the correct messages, and in the correct order
            name = test_class.__name__
            calls = [
                call.info("Start running step"),
                call.debug(f"Step Input: name='{name}' description='{name}'"),
                *log_entry,
                call.debug(f"Step Output: name='{name}.Output' description='Output for {name}'"),
                call.info("Finished running step"),
            ]
            mock_log.assert_has_calls(calls, any_order=False)

    def test_output_validation(self):
        class MyStep(Step):
            a: str
            b: int

            class Output(Step.Output):
                c: str
                d: int

            def execute(self):
                self.output.c = self.a
                self.output.d = self.b

        with mock.patch.object(Step.Output, "validate_output", autospec=True) as mock_validator:
            step = MyStep(a="foo", b=42)
            step.execute()

            mock_validator.assert_called_once()


class TestStepMetaClass:
    """Tests that target the StepMetaClass specifically"""

    def test_with_deeply_nested_super_call(self) -> None:
        """
        Test to ensure that logs are not duplicated for a step with a deeply nested super() call in the execute method.

        Only one instance of each log is expected where logs are called in the StepMetaClass.

        See [issue #167](https://github.com/Nike-Inc/koheesio/issues/167) for more details.
        """
        os.environ["KOHEESIO_LOGGING_LEVEL"] = "DEBUG"

        # Arrange: simulate a deeply nested inheritance structure

        class GrandParentStep(Step):
            """GrandParent step class"""

            def execute(self):
                self.log.info("GrandParentStep execute called")

        class ParentStep(GrandParentStep):
            """Parent step class"""

            foo: str = "bar"

            def execute(self) -> Step.Output:
                self.log.info("ParentStep execute called")
                self.log.info(f"ParentStep foo: {self.foo}")
                super().execute()

        class ChildStep(ParentStep):
            """Child step class"""

            bar: str = "baz"
            ...

        class GrandChildStep(ChildStep):
            """Grandchild step class"""

            def execute(self) -> Step.Output:
                super().execute()
                self.log.info("GrandChildStep execute called")

        class GreatGrandChildStep(GrandChildStep):
            """Great grandchild step class"""

            ...

        with (
            patch.object(ParentStep, "log", autospec=True) as mock_log,
        ):
            # Act
            obj = GreatGrandChildStep(foo="42", bar="Thanks for all the fish")
            obj.execute()

            # Assert: Check that logs were called once (and only once) with the correct messages, and in the correct order
            calls = [
                call.info("Start running step"),
                call.debug(
                    f"Step Input: name='{obj.name}' description='{obj.description}' foo='42' bar='Thanks for all the fish'"
                ),
                call.info("ParentStep execute called"),
                call.info("ParentStep foo: 42"),
                call.info("GrandParentStep execute called"),
                call.info("GrandChildStep execute called"),
                call.debug(f"Step Output: name='{obj.name}.Output' description='Output for {obj.name}'"),
                call.info("Finished running step"),
            ]
            mock_log.assert_has_calls(calls, any_order=False)
