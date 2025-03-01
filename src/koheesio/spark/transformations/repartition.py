"""Repartition Transformation"""

from typing import Optional

from koheesio.models import Field, ListOfColumns, model_validator
from koheesio.spark.transformations import ColumnsTransformation


class Repartition(ColumnsTransformation):
    """
    Wrapper around DataFrame.repartition

    With repartition, the number of partitions can be given as an optional value. If this is not provided, a default
    value is used. The default number of partitions is defined by the spark config 'spark.sql.shuffle.partitions', for
    which the default value is 200 and will never exceed the number of rows in the DataFrame (whichever is value is
    lower).

    If columns are omitted, the entire DataFrame is repartitioned without considering the particular values in the
    columns.

    Parameters
    ----------
    columns : Optional[Union[str, List[str]]], optional, default=None
        Name of the source column(s). If omitted, the entire DataFrame is repartitioned without considering the
        particular values in the columns. Alias: column
    num_partitions : Optional[int], optional, default=None
        The number of partitions to repartition to. If omitted, the default number of partitions is used as defined by
        the spark config 'spark.sql.shuffle.partitions'.

    Example
    -------
    ```python
    Repartition(
        column=["c1", "c2"], num_partitions=3
    )  # results in 3 partitions
    Repartition(column="c1", num_partitions=2)  # results in 2 partitions
    Repartition(column=["c1", "c2"])  # results in <= 200 partitions
    Repartition(num_partitions=5)  # results in 5 partitions
    ```
    """

    columns: Optional[ListOfColumns] = Field(default="", alias="column", description="Name of the source column(s)")
    num_partitions: Optional[int] = Field(
        default=None,
        alias="numPartitions",
        description="The number of partitions to repartition to. If omitted, the default number of partitions is used "
        "as defined by the spark config 'spark.sql.shuffle.partitions'.",
    )

    @model_validator(mode="before")
    def _validate_field_and_num_partitions(cls, values: dict) -> dict:
        """Ensure that at least one of the fields 'columns' and 'num_partitions' is provided."""
        columns_value = values.get("columns") or values.get("column")
        num_partitions_value = values.get("numPartitions") or values.get("num_partitions")

        if not columns_value and not num_partitions_value:
            raise ValueError("The fields 'columns' and 'num_partitions' cannot both be empty!")

        values["numPartitions"] = num_partitions_value
        return values

    def execute(self) -> ColumnsTransformation.Output:
        # Prepare columns input:
        columns = self.df.columns if self.columns == ["*"] else self.columns
        # Prepare repartition input:
        #  num_partitions comes first, but if it is not provided it should not be included as None.
        repartition_inputs = [i for i in [self.num_partitions, *columns] if i]  # type: ignore
        self.output.df = self.df.repartition(*repartition_inputs)
