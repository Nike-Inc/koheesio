"""Task Module

A Task is the unit of work of one execution of the framework. An execution usually consists of an Extract -> Transform
-> Load approach of one data object.

Tasks generally are made up of Steps chained one after another.

For a comprehensive guide on the usage, examples, and additional features of the Task class, please refer to the
[reference/concepts/tasks](../../reference/concepts/tasks.md) section of the Koheesio documentation.
"""

from abc import ABC

from koheesio.steps import Step


class Task(Step, ABC):
    """Base class for a Task
    Based on the Step class, but typically consist of a series of Steps that are executed one after another.
    """
