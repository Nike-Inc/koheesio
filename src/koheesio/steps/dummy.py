"""Dummy step for testing purposes.

This module contains a dummy step for testing purposes. It is used to test the Koheesio framework or to provide a simple
example of how to create a new step.

Example
-------
```python
s = DummyStep(a="a", b=2)
s.execute()
```
In this case, `s.output` will be equivalent to the following dictionary:
```python
{"a": "a", "b": 2, "c": "aa"}
```
"""

from koheesio import Step, StepOutput


class DummyOutput(StepOutput):
    """Dummy output for testing purposes."""

    a: str
    b: int


class DummyStep(Step):
    """Dummy step for testing purposes."""

    a: str
    b: int

    class Output(DummyOutput):
        """Dummy output for testing purposes."""

        c: str

    def execute(self) -> None:
        """Dummy execute for testing purposes."""
        self.output.a = self.a
        self.output.b = self.b
        self.output.c = self.a * self.b
