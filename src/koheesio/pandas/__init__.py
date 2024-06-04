"""Base class for a Pandas step

Extends the Step class with Pandas DataFrame support. The following:
- Pandas steps are expected to return a Pandas DataFrame as output.
"""

from typing import Optional
from abc import ABC

from pandas import DataFrame

from koheesio import Step, StepOutput
from koheesio.models import Field


class PandasStep(Step, ABC):
    """Base class for a Pandas step

    Extends the Step class with Pandas DataFrame support. The following:
    - Pandas steps are expected to return a Pandas DataFrame as output.
    """

    class Output(StepOutput):
        """Output class for PandasStep"""

        df: Optional[DataFrame] = Field(default=None, description="The Pandas DataFrame")
