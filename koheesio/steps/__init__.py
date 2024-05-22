"""Steps Module

This module contains the definition of the `Step` class, which serves as the base class for custom units of logic that
can be executed. It also includes the `StepOutput` class, which defines the output data model for a `Step`.

The `Step` class is designed to be subclassed for creating new steps in a data pipeline. Each subclass should implement
the `execute` method, specifying the expected inputs and outputs.

This module also exports the `SparkStep` class for steps that interact with Spark

Classes:
--------
- *Step*: Base class for a custom unit of logic that can be executed.
- *StepOutput*: Defines the output data model for a `Step`.
- *SparkStep*: Class for steps that interact with Spark. Note: This class is only available if `pyspark` is installed.
"""

from koheesio.steps.step import Step, StepOutput

__all__ = ["StepOutput", "Step"]

# import SparkStep if pyspark is available
try:
    from koheesio.steps.spark import SparkStep

    __all__.append("SparkStep")
except ImportError:
    pass
