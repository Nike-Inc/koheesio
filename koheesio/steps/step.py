"""
See Also: step.py
"""

from __future__ import annotations

import inspect
import json
import sys
import warnings
from abc import abstractmethod
from functools import partialmethod, wraps
from typing import Any

import yaml

from koheesio.models import BaseModel, ConfigDict, ModelMetaclass


class StepOutput(BaseModel):
    """Class for the StepOutput model

    Usage
    -----
    Setting up the StepOutputs class is done like this:
    ```python
    class YourOwnOutput(StepOutput):
        a: str
        b: int
    ```
    """

    model_config = ConfigDict(
        validate_default=False,
        defer_build=True,
    )

    # pylint: disable=no-value-for-parameter
    def validate_output(self) -> StepOutput:
        """Validate the output of the Step

        Essentially, this method is a wrapper around the validate method of the BaseModel class
        """
        validated_model = self.validate()
        return StepOutput.from_basemodel(validated_model)


class StepMetaClass(ModelMetaclass):
    """
    StepMetaClass has to be set up as a Metaclass extending ModelMetaclass to allow Pydantic to be unaffected while
    allowing for the execute method to be auto-decorated with do_execute
    """

    # Unique object to mark a function as wrapped
    _step_execute_wrapper_sentinel = object()

    # pylint: disable=signature-differs
    def __new__(
        mcs,
        cls_name: str,
        bases: tuple[type[Any], ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> type:
        """
        Create a new class instance.

        Parameters
        ----------
        mcs : type
            The metaclass of the class being created.
        cls_name : str
            The name of the class being created.
        bases : tuple[type[Any], ...]
            The base classes of the class being created.
        namespace : dict[str, Any]
            The namespace of the class being created.
        kwargs : Any
            Additional keyword arguments.

        Returns
        -------
        type
            The newly created class instance.

        Notes
        -----
        This method is called before the class is created. It allows customization of the class creation process.

        The method wraps the `execute` method of the class with a partial method if it is not already wrapped.
        The wrapped method is then set as the new `execute` method of the class.

        If the `execute` method is already wrapped, the method is not modified.

        The method also keeps track of the number of times the `execute` method has been wrapped.

        """
        cls = super().__new__(
            mcs,
            cls_name,
            bases,
            namespace,
            **kwargs,
        )

        # Extract execute method present in the class
        execute_method = getattr(cls, "execute")

        # check if function is already wrapped with do_execute
        # Here we are trying to get the attribute "_partialmethod__step_execute_wrapper_sentinel"
        # from the execute_method function.
        # If the function is already wrapped, this attribute should exist.
        sentinel = getattr(execute_method, "_partialmethod__step_execute_wrapper_sentinel", None)

        # Check if the sentinel is the same as the class's sentinel. If they are the same,
        # it means the function is already wrapped.
        is_already_wrapped = sentinel is cls._step_execute_wrapper_sentinel

        # Get the wrap count of the function. If the function is not wrapped yet, the default value is 0.
        wrap_count = getattr(execute_method, "_partialmethod_wrap_count", 0)

        # prevent multiple wrapping
        # If the function is not already wrapped, we proceed to wrap it.
        if not is_already_wrapped:
            # Create a partial method with the execute_method as one of the arguments.
            # This is the new function that will be called instead of the original execute_method.
            wrapper = mcs._partialmethod_impl(cls=cls, execute_method=execute_method)

            # Updating the attributes of the wrapping function to those of the original function.
            wraps(execute_method)(wrapper)  # type: ignore

            # Set the sentinel attribute to the wrapper. This is done so that we can check
            # if the function is already wrapped.
            setattr(wrapper, "_step_execute_wrapper_sentinel", cls._step_execute_wrapper_sentinel)

            # Increase the wrap count of the function. This is done to keep track of
            # how many times the function has been wrapped.
            setattr(wrapper, "_wrap_count", wrap_count + 1)
            setattr(cls, "execute", wrapper)

        return cls

    @staticmethod
    def _is_called_through_super(caller_self: Any, caller_name: str, *_args, **_kwargs) -> bool:
        """
        Check if the method is called through super() in the immediate parent class.

        Parameters
        ----------
        caller_self : Any
            The instance of the class.
        caller_name : str
            The name of the caller method.
        *args : Any
            Additional positional arguments.
        **kwargs : Any
            Additional keyword arguments.

        Returns
        -------
        bool
            True if the method is called through super(), False otherwise.

        """

        base_class = caller_self.__class__.__bases__[0]
        return caller_name in base_class.__dict__

    @classmethod
    def _partialmethod_impl(mcs, cls: type, execute_method) -> partialmethod:
        """
        This method creates a partial method implementation for a given class and execute method.
        It handles a specific issue with python>=3.11 where partialmethod forgets that _execute_wrapper
        is a method of wrapper, and it needs to pass that in as the first argument.

        Args:
            mcs: The metaclass instance.
            cls (type): The class for which the partial method is being created.
            execute_method: The method to be executed.

        Returns:
            wrapper: The partial method implementation.
        """

        # Solution to overcome issue with python>=3.11,
        # When partialmethod is forgetting that _execute_wrapper
        # is a method of wrapper, and it needs to pass that in as the first arg.
        # https://github.com/python/cpython/issues/99152
        class _partialmethod_with_self(partialmethod):
            """
            This class is a workaround for the issue with python>=3.11 where partialmethod forgets that
            _execute_wrapper is a method of wrapper, and it needs to pass that in as the first argument.
            """

            def __get__(self, obj, cls=None):
                """
                This method returns the unbound method for the given object and class.

                Args:
                    obj: The object instance.
                    cls (Optional): The class for which the method is being retrieved. Defaults to None.

                Returns:
                    The unbound method.
                """
                return self._make_unbound_method().__get__(obj, cls)

        _partialmethod_impl = partialmethod if sys.version_info < (3, 11) else _partialmethod_with_self
        wrapper = _partialmethod_impl(cls._execute_wrapper, execute_method=execute_method)

        return wrapper

    @classmethod
    def _execute_wrapper(mcs, step: Step, execute_method, *args, **kwargs) -> StepOutput:
        """
        Method that wraps some common functionalities on Steps
        Ensures proper logging and makes it so that a Steps execute method always returns the StepOutput

        Parameters
        ----------
        step : Step
            The step instance.
        execute_method : Callable
            The execute method of the step.
        *args : Any
            Additional positional arguments.
        **kwargs : Any
            Additional keyword arguments.

        Returns
        -------
        StepOutput
            The output of the step.

        """

        # check if the method is called through super() in the immediate parent class
        caller_name = inspect.currentframe().f_back.f_back.f_code.co_name
        is_called_through_super_ = mcs._is_called_through_super(step, caller_name)

        mcs._log_start_message(step=step, skip_logging=is_called_through_super_)
        return_value = mcs._run_execute(step=step, execute_method=execute_method, *args, **kwargs)
        mcs._configure_step_output(step=step, return_value=return_value)
        mcs._validate_output(step=step, skip_validating=is_called_through_super_)
        mcs._log_end_message(step=step, skip_logging=is_called_through_super_)

        return step.output

    @classmethod
    def _log_start_message(mcs, step: Step, *_args, skip_logging: bool = False, **_kwargs):
        """
        Log the start message of the step execution

        Parameters
        ----------
        step : Step
            The step instance.
        skip_logging : bool, optional
            Whether to skip logging, by default False.
        *args : Any
            Additional positional arguments.
        **kwargs : Any
            Additional keyword arguments.

        """

        if not skip_logging:
            step.log.info("Start running step")
            step.log.debug(f"Step Input: {step.__repr_str__(' ')}")

    @classmethod
    def _log_end_message(mcs, step: Step, *_args, skip_logging: bool = False, **_kwargs):
        """
        Log the end message of the step execution

        Parameters
        ----------
        step : Step
            The step instance.
        skip_logging : bool, optional
            Whether to skip logging, by default False.
        *args : Any
            Additional positional arguments.
        **kwargs : Any
            Additional keyword arguments.

        """

        if not skip_logging:
            step.log.debug(f"Step Output: {step.output.__repr_str__(' ')}")
            step.log.info("Finished running step")

    @classmethod
    def _validate_output(mcs, step: Step, *_args, skip_validating: bool = False, **_kwargs):
        """
        Validate the output of the step

        Parameters
        ----------
        step : Step
            The step instance.
        skip_validating : bool, optional
            Whether to skip validating, by default False.
        *args : Any
            Additional positional arguments.
        **kwargs : Any
            Additional keyword arguments.

        """

        if not skip_validating:
            step.output.validate_output()

    @classmethod
    def _configure_step_output(mcs, step, return_value: Any, *_args, **_kwargs):
        """
        Configure the output of the step.
        If the execute method returns a value, and it is not the output, set the output to the return value

        Parameters
        ----------
        step : Step
            The step instance.
        return_value : Any
            The return value of the execute method.
        *args : Any
            Additional positional arguments.
        **kwargs : Any
            Additional keyword arguments.

        """

        output: StepOutput = step.output

        if return_value:
            if not isinstance(return_value, StepOutput):
                msg = (
                    f"execute() did not produce output of type {output.name}, returns of the wrong type will be ignored"
                )
                warnings.warn(msg)
                step.log.warning(msg)

            if return_value != output:
                step.output.merge(return_value)

        step.output = output

    @classmethod
    def _run_execute(mcs, execute_method, step, *args, **kwargs) -> Any:
        """
        Run the execute method of the step, and catch any errors

        Parameters
        ----------
        execute_method : Callable
            The execute method of the step.
        step : Step
            The step instance.
        *args : Any
            Additional positional arguments.
        **kwargs : Any
            Additional keyword arguments.

        Returns
        -------
        Any
            The return value of the execute method.

        """

        try:
            return_value = execute_method(step, *args, **kwargs)
        except Exception as e:
            step.log.error(f"Error while running step: \n{step.__repr_str__(join_str=' ')}")
            raise e

        return return_value


class Step(BaseModel, metaclass=StepMetaClass):
    """Base class for a step

    A custom unit of logic that can be executed.

    The Step class is designed to be subclassed. To create a new step, one would subclass Step and implement the
    `def execute(self)` method, specifying the expected inputs and outputs.

    > Note: since the Step class is meta classed, the execute method is wrapped with the `do_execute` function making
    > it always return the Step's output. Hence, an explicit return is not needed when implementing execute.

    Methods and Attributes
    ----------------------
    The Step class has several attributes and methods.

    #### INPUT
    The following fields are available by default on the Step class:
    - `name`: Name of the Step. If not set, the name of the class will be used.
    - `description`: Description of the Step. If not set, the docstring of the class will be used. If the docstring
        contains multiple lines, only the first line will be used.

    When subclassing a Step, any additional pydantic field will be treated as `input` to the Step. See also the
    explanation on the `.execute()` method below.


    #### OUTPUT
    Every Step has an `Output` class, which is a subclass of `StepOutput`. This class is used to validate the output of
    the Step. The `Output` class is defined as an inner class of the Step class. The `Output` class can be accessed
    through the `Step.Output` attribute. The `Output` class can be extended to add additional fields to the output of
    the Step. See also the explanation on the `.execute()`.

    - `Output`: A nested class representing the output of the Step used to validate the output of the
        Step and based on the StepOutput class.
    - `output`: Allows you to interact with the Output of the Step lazily (see above and StepOutput)

    When subclassing a Step, any additional pydantic field added to the nested `Output` class will be treated as
    `output` of the Step. See also the description of `StepOutput` for more information.


    #### Methods:
    - `execute`: Abstract method to implement for new steps.
        - The Inputs of the step can be accessed, using `self.input_name`.
        - The output of the step can be accessed, using `self.output.output_name`.
    - `run`: Alias to .execute() method. You can use this to run the step, but execute is preferred.
    - `to_yaml`: YAML dump the step
    - `get_description`: Get the description of the Step

    When subclassing a Step, `execute` is the only method that __needs__ to be implemented. Any additional method added
    to the class will be treated as a method of the Step.

    Note: since the Step class is meta-classed, the execute method is automatically wrapped with the `do_execute`
    function making it always return a StepOutput. See also the explanation on the `do_execute` function.


    #### class methods:
    - `from_step`: Returns a new Step instance based on the data of another Step instance.
        for example: `MyStep.from_step(other_step, a="foo")`
    - `get_description`: Get the description of the Step


    #### dunder methods:
    - `__getattr__`: Allows input to be accessed through `self.input_name`
    - `__repr__` and `__str__`: String representation of a step


    Background
    ----------
    A Step is an atomic operation and serves as the building block of data pipelines built with the framework.
    Tasks typically consist of a series of Steps.

    A step can be seen as an operation on a set of inputs, that returns a set of outputs. This however does not imply
    that steps are stateless (e.g. data writes)!

    The diagram serves to illustrate the concept of a Step:

    ```text
    ┌─────────┐        ┌──────────────────┐        ┌─────────┐
    │ Input 1 │───────▶│                  ├───────▶│Output 1 │
    └─────────┘        │                  │        └─────────┘
                       │                  │
    ┌─────────┐        │                  │        ┌─────────┐
    │ Input 2 │───────▶│       Step       │───────▶│Output 2 │
    └─────────┘        │                  │        └─────────┘
                       │                  │
    ┌─────────┐        │                  │        ┌─────────┐
    │ Input 3 │───────▶│                  ├───────▶│Output 3 │
    └─────────┘        └──────────────────┘        └─────────┘
    ```

    Steps are built on top of Pydantic, which is a data validation and settings management using python type
    annotations. This allows for the automatic validation of the inputs and outputs of a Step.

    - Step inherits from BaseModel, which is a Pydantic class used to define data models. This allows Step to
      automatically validate data against the defined fields and their types.
    - Step is metaclassed by StepMetaClass, which is a custom metaclass that wraps the `execute` method of the Step
      class with the `_execute_wrapper` function. This ensures that the `execute` method always returns the output of
      the Step along with providing logging and validation of the output.
    - Step has an `Output` class, which is a subclass of `StepOutput`. This class is used to validate the output
      of the Step. The `Output` class is defined as an inner class of the Step class. The `Output` class can be
      accessed through the `Step.Output` attribute.
    - The `Output` class can be extended to add additional fields to the output of the Step.

    Examples
    --------
    ```python
    class MyStep(Step):
        a: str  # input

        class Output(StepOutput):  # output
            b: str

        def execute(self) -> MyStep.Output:
            self.output.b = f"{self.a}-some-suffix"
    ```
    """

    class Output(StepOutput):
        """Output class for Step"""

    __output__: Output

    @property
    def output(self) -> Output:
        """Interact with the output of the Step"""
        if not self.__output__:
            self.__output__ = self.Output.lazy()
            self.__output__.name = self.name + ".Output"
            self.__output__.description = "Output for " + self.name
        return self.__output__

    @output.setter
    def output(self, value: Output):
        """Set the output of the Step"""
        self.__output__ = value

    @abstractmethod
    def execute(self):
        """Abstract method to implement for new steps.

        The Inputs of the step can be accessed, using `self.input_name`

        Note: since the Step class is meta-classed, the execute method is wrapped with the `do_execute` function making
          it always return the Steps output
        """
        raise NotImplementedError

    def run(self):
        """Alias to .execute()"""
        return self.execute()

    def __repr__(self) -> str:
        """String representation of a step"""
        class_name = self.__class__.__name__
        underline = "=" * len(class_name)
        yaml_output = self.repr_yaml(simple=True)
        return f"{class_name}\n{underline}\n{yaml_output}"

    def __str__(self) -> str:
        """String representation of a step"""
        return self.__repr__()

    def repr_json(self, simple=False) -> str:
        """dump the step to json, meant for representation

        Note: use to_json if you want to dump the step to json for serialization
        This method is meant for representation purposes only!

        Examples
        --------
        ```python
        >>> step = MyStep(a="foo")
        >>> print(step.repr_json())
        {"input": {"a": "foo"}}
        ```

        Parameters
        ----------
        simple: bool
            When toggled to True, a briefer output will be produced. This is friendlier for logging purposes

        Returns
        -------
        str
            A string, which is valid json
        """
        model_dump_options = dict(warnings="none", exclude_unset=True)

        _result = {}

        # extract input
        _input = self.model_dump(**model_dump_options)

        # remove name and description from input and add to result if simple is not set
        name = _input.pop("name", None)
        description = _input.pop("description", None)
        if not simple:
            if name:
                _result["name"] = name
            if description:
                _result["description"] = description
        else:
            model_dump_options["exclude"] = {"name", "description"}

        # extract output
        _output = self.output.model_dump(**model_dump_options)

        # add output to result
        if _output:
            _result["output"] = _output

        # add input to result
        _result["input"] = _input

        class MyEncoder(json.JSONEncoder):
            """Custom JSON Encoder to handle non-serializable types"""

            def default(self, o: Any) -> Any:
                try:
                    return super().default(o)
                except TypeError:
                    return o.__class__.__name__

        # Use MyEncoder when converting the dictionary to a JSON string
        json_str = json.dumps(_result, cls=MyEncoder)

        return json_str

    def repr_yaml(self, simple=False) -> str:
        """dump the step to yaml, meant for representation

        Note: use to_yaml if you want to dump the step to yaml for serialization
        This method is meant for representation purposes only!

        Examples
        --------
        ```python
        >>> step = MyStep(a="foo")
        >>> print(step.repr_yaml())
        input:
          a: foo
        ```

        Parameters
        ----------
        simple: bool
            When toggled to True, a briefer output will be produced. This is friendlier for logging purposes

        Returns
        -------
        str
            A string, which is valid yaml
        """
        json_str = self.repr_json(simple=simple)

        # Parse the JSON string back into a dictionary
        _result = json.loads(json_str)

        return yaml.dump(_result)

    def __getattr__(self, key: str):
        """__getattr__ dunder

        Allows input to be accessed through `self.input_name`

        Parameters
        ----------
        key: str
            Name of the attribute to return the value of

        Returns
        -------
        Any
            The value of the attribute
        """
        return self.model_dump().get(key)

    @classmethod
    def from_step(cls, step: Step, **kwargs):
        """Returns a new Step instance based on the data of another Step or BaseModel instance"""
        return cls.from_basemodel(step, **kwargs)
