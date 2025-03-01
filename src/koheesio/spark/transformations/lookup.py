"""Lookup transformation for joining two dataframes together

Classes
-------
JoinMapping
TargetColumn
JoinType
JoinHint
DataframeLookup
"""

from typing import List, Optional, Union
from enum import Enum

from pyspark.sql import Column
from pyspark.sql import functions as f

from koheesio.models import BaseModel, Field, field_validator
from koheesio.spark import DataFrame
from koheesio.spark.transformations import Transformation

__all__ = [
    "JoinMapping",
    "TargetColumn",
    "JoinType",
    "JoinHint",
    "DataframeLookup",
]


class JoinMapping(BaseModel):
    """Mapping for joining two dataframes together"""

    source_column: str
    other_column: str

    @property
    def column(self) -> Column:
        """Get the join mapping as a pyspark.sql.Column object"""
        return f.col(self.other_column).alias(self.source_column)


class TargetColumn(BaseModel):
    """Target column for the joined dataframe"""

    target_column: str
    target_column_alias: str

    @property
    def column(self) -> Column:
        """Get the target column as a pyspark.sql.Column object"""
        return f.col(self.target_column).alias(self.target_column_alias)


class JoinType(str, Enum):
    """Supported join types"""

    INNER = "inner"
    FULL = "full"
    LEFT = "left"
    RIGHT = "right"
    CROSS = "cross"
    SEMI = "semi"
    ANTI = "anti"


class JoinHint(str, Enum):
    """Supported join hints"""

    BROADCAST = "broadcast"
    MERGE = "merge"


class DataframeLookup(Transformation):
    """Lookup transformation for joining two dataframes together

    Parameters
    ----------
    df : DataFrame
        The left Spark DataFrame
    other : DataFrame
        The right Spark DataFrame
    on : List[JoinMapping] | JoinMapping
        List of join mappings. If only one mapping is passed, it can be passed as a single object.
    targets : List[TargetColumn] | TargetColumn
        List of target columns. If only one target is passed, it can be passed as a single object.
    how : JoinType
        What type of join to perform. Defaults to left. See JoinType for more information.
    hint : JoinHint
        What type of join hint to use. Defaults to None. See JoinHint for more information.

    Example
    -------
    ```python
    from pyspark.sql import SparkSession
    from koheesio.spark.transformations.lookup import (
        DataframeLookup,
        JoinMapping,
        TargetColumn,
        JoinType,
    )

    spark = SparkSession.builder.getOrCreate()

    # create the dataframes
    left_df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])
    right_df = spark.createDataFrame(
        [(1, "A"), (3, "C")], ["id", "value"]
    )

    # perform the lookup
    lookup = DataframeLookup(
        df=left_df,
        other=right_df,
        on=JoinMapping(source_column="id", joined_column="id"),
        targets=TargetColumn(
            target_column="value", target_column_alias="right_value"
        ),
        how=JoinType.LEFT,
    )

    output_df = lookup.transform()
    ```

    __output_df__:

    | id|value|right_value|
    |---|----|------------|
    |  1|    A|          A|
    |  2|    B|       null|

    In this example, the `left_df` and `right_df` dataframes are joined together using the `id` column. The `value`
    column from the `right_df` is aliased as `right_value` in the output dataframe.
    """

    df: Optional[DataFrame] = Field(default=None, description="The left Spark DataFrame")
    other: Optional[DataFrame] = Field(default=None, description="The right Spark DataFrame")
    on: Union[List[JoinMapping], JoinMapping] = Field(
        default=...,
        alias="join_mapping",
        description="List of join mappings. If only one mapping is passed, it can be passed as a single object.",
    )
    targets: Union[List[TargetColumn], TargetColumn] = Field(
        default=...,
        alias="target_columns",
        description="List of target columns. If only one target is passed, it can be passed as a single object.",
    )
    how: Optional[JoinType] = Field(
        default=JoinType.LEFT, description="What type of join to perform. Defaults to left. " + str(JoinType.__doc__)
    )
    hint: Optional[JoinHint] = Field(
        default=None, description="What type of join hint to use. Defaults to None. " + str(JoinHint.__doc__)
    )

    @field_validator("on", "targets")
    def set_list(cls, value: Union[List[JoinMapping], JoinMapping, List[TargetColumn], TargetColumn]) -> List:
        """Ensure that we can pass either a single object, or a list of objects"""
        return [value] if not isinstance(value, list) else value

    class Output(Transformation.Output):
        """Output for the lookup transformation"""

        left_df: DataFrame = Field(default=..., description="The left Spark DataFrame")
        right_df: DataFrame = Field(default=..., description="The right Spark DataFrame")

    def get_right_df(self) -> DataFrame:
        """Get the right side dataframe"""
        return self.other

    def execute(self) -> Output:
        """Execute the lookup transformation"""
        # prepare the right dataframe
        prepared_right_df = self.get_right_df().select(
            *[join_mapping.column for join_mapping in self.on],  # type: ignore
            *[target.column for target in self.targets],  # type: ignore
        )
        if self.hint:
            prepared_right_df = prepared_right_df.hint(self.hint)

        # generate the output
        self.output.left_df = self.df
        self.output.right_df = prepared_right_df
        self.output.df = self.df.join(
            prepared_right_df,  # type: ignore
            on=[join_mapping.source_column for join_mapping in self.on],
            how=self.how,
        )
