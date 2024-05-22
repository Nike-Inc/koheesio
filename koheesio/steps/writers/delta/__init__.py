"""
This module is the entry point for the koheesio.steps.writers.delta package.

It imports and exposes the DeltaTableWriter and DeltaTableStreamWriter classes for external use.

Classes:
    DeltaTableWriter: Class to write data in batch mode to a Delta table.
    DeltaTableStreamWriter: Class to write data in streaming mode to a Delta table.
"""

from koheesio.steps.writers.delta.batch import DeltaTableWriter
from koheesio.steps.writers.delta.stream import DeltaTableStreamWriter

__all__ = ["DeltaTableWriter", "DeltaTableStreamWriter"]
