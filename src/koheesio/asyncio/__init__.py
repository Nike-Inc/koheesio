"""
This module provides classes for asynchronous steps in the koheesio package.
"""

from typing import Dict, Optional, Union
from abc import ABC
from asyncio import iscoroutine

from pydantic import PrivateAttr

from koheesio.steps import Step, StepMetaClass, StepOutput


class AsyncStepMetaClass(StepMetaClass):
    """Metaclass for asynchronous steps.

    This metaclass is used to define asynchronous steps in the Koheesio framework.
    It inherits from the StepMetaClass and provides additional functionality for
    executing asynchronous steps.

    Methods
    -------
    _execute_wrapper: Wrapper method for executing asynchronous steps.

    """

    def _execute_wrapper(cls, *args, **kwargs):  # type: ignore[no-untyped-def]
        """Wrapper method for executing asynchronous steps.

        This method is called when an asynchronous step is executed. It wraps the
        execution of the step with additional functionality.

        Parameters
        ----------
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns
        -------
            The result of executing the asynchronous step.

        """

        return super()._execute_wrapper(*args, **kwargs)


class AsyncStepOutput(Step.Output):
    """
    Represents the output of an asynchronous step.

    This class extends the base `Step.Output` class and provides additional functionality for merging key-value maps.

    Attributes
    ----------
    ...

    Methods
    -------
    merge(other: Union[Dict, StepOutput])
        Merge key-value map with self.
    """

    def merge(self, other: Union[Dict, StepOutput]) -> "AsyncStepOutput":
        """Merge key,value map with self

        Examples
        --------
        ```python
        step_output = StepOutput(foo="bar")
        step_output.merge(
            {"lorem": "ipsum"}
        )  # step_output will now contain {'foo': 'bar', 'lorem': 'ipsum'}
        ```

        Functionally similar to adding two dicts together; like running `{**dict_a, **dict_b}`.

        Parameters
        ----------
        other: Union[Dict, StepOutput]
            Dict or another instance of a StepOutputs class that will be added to self
        """
        if isinstance(other, StepOutput):
            other = other.model_dump()  # ensures we really have a dict

        if not iscoroutine(other):
            for k, v in other.items():
                self.set(k, v)

        return self


# noinspection PyUnresolvedReferences
class AsyncStep(Step, ABC, metaclass=AsyncStepMetaClass):
    """
    Asynchronous step class that inherits from Step and uses the AsyncStepMetaClass metaclass.

    Attributes
    ----------
    Output : AsyncStepOutput
        The output class for the asynchronous step.
    """

    class Output(AsyncStepOutput):
        """
        Output class for asyncio step.

        This class represents the output of the asyncio step. It inherits from the AsyncStepOutput class.
        """

    _output: Optional[Output] = PrivateAttr(default=None)
