"""Base class for a Pandas step

Extends the Step class with Pandas DataFrame support. The following:
- Pandas steps are expected to return a Pandas DataFrame as output.
"""

from typing import Optional
from abc import ABC
from types import ModuleType

from koheesio import Step, StepOutput
from koheesio.models import Field
from koheesio.spark.utils import import_pandas_based_on_pyspark_version

pandas: ModuleType = import_pandas_based_on_pyspark_version()


class PandasStep(Step, ABC):
    """Base class for a Pandas step

    Extends the Step class with Pandas DataFrame support. The following:
    - Pandas steps are expected to return a Pandas DataFrame as output.
    """

    class Output(StepOutput):
        """Output class for PandasStep"""

        df: Optional[pandas.DataFrame] = Field(default=None, description="The Pandas DataFrame")  # type: ignore
