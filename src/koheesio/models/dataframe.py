"""
Module for the BaseReader and BaseTransformation classes
"""

from typing import Optional, TypeVar
from abc import ABC, abstractmethod

from koheesio import Step
from koheesio.models import Field

DataFrameType = TypeVar("DataFrameType")
"""Defines a type variable that can be any type of DataFrame"""


class BaseReader(Step, ABC):
    """Base class for all Readers

    Concept
    -------
    A Reader is a Step that reads data from a source based on the input parameters
    and stores the result in self.output.df (DataFrame).

    When implementing a Reader, the execute() method should be implemented.
    The execute() method should read from the source and store the result in self.output.df.

    The Reader class implements a standard read() method that calls the execute() method and returns the result. This
    method can be used to read data from a Reader without having to call the execute() method directly. Read method
    does not need to be implemented in the child class.

    The Reader class also implements a shorthand for accessing the output Dataframe through the df-property. If the
    output.df is None, .execute() will be run first.
    """

    @property
    def to_df(self) -> Optional[DataFrameType]:
        """Shorthand for accessing self.output.df
        If the output.df is None, .execute() will be run first

        aliases:
        - toDF, mimics the Delta API
        - df
        """
        if not self.output.df:
            self.execute()
        return self.output.df

    toDF = to_df
    df = to_df

    @abstractmethod
    def execute(self) -> Step.Output:
        """Execute on a Reader should handle self.output.df (output) as a minimum
        Read from whichever source -> store result in self.output.df
        """
        pass

    def read(self) -> DataFrameType:
        """Read from a Reader without having to call the execute() method directly"""
        self.execute()
        return self.output.df


class BaseTransformation(Step, ABC):
    """Base class for all Transformations

    Concept
    -------
    A Transformation is a Step that takes a DataFrame as input and returns a DataFrame as output. The DataFrame is
    transformed based on the logic implemented in the `execute` method. Any additional parameters that are needed for
    the transformation can be passed to the constructor.

    When implementing a Transformation, the `execute` method should be implemented. The `execute` method should take the
    input DataFrame, transform it, and store the result in `self.output.df`.

    The Transformation class implements a standard `transform` method that calls the `execute` method and returns the
    result. This method can be used to transform a DataFrame without having to call the `execute` method directly. The
    `transform` method does not need to be implemented in the child class.

    The Transformation class also implements a shorthand for accessing the output DataFrame through the
    `to-df`-property (alias: `toDF`). If the `output.df` is `None`, `.execute()` will be run first.
    """

    df: Optional[DataFrameType] = Field(default=None, description="The input DataFrame")

    @abstractmethod
    def execute(self) -> Step.Output:
        """Execute on a Transformation should handle self.df (input) and set self.output.df (output)

        This method should be implemented in the child class. The input DataFrame is available as `self.df` and the
        output DataFrame should be stored in `self.output.df`.

        For example:
        ```python
        # pyspark example
        def execute(self):
            self.output.df = self.df.withColumn(
                "new_column", f.col("old_column") + 1
            )
        ```

        The transform method will call this method and return the output DataFrame.
        """
        # self.df  # input DataFrame
        # self.output.df # output DataFrame
        self.output.df = ...  # implement the transformation logic
        raise NotImplementedError

    @property
    def to_df(self) -> Optional[DataFrameType]:
        """Shorthand for accessing self.output.df
        If the output.df is None, .execute() will be run first
        """
        if not self.output.df:
            self.execute()
        return self.output.df

    toDF = to_df
    """Alias for the to_df property - mimics the Delta API"""

    def transform(self, df: Optional[DataFrameType] = None) -> DataFrameType:
        """Execute the transformation and return the output DataFrame

        Note: when creating a child from this, don't implement this transform method. Instead, implement `execute`.

        See Also
        --------
        `Transformation.execute`

        Parameters
        ----------
        df: Optional[DataFrameType]
            The DataFrame to apply the transformation to. If not provided, the DataFrame passed to the constructor
            will be used.

        Returns
        -------
        DataFrameType
            The transformed DataFrame
        """
        if df is not None:
            self.df = df
        if self.df is None:
            raise RuntimeError("No valid Dataframe was passed")
        self.execute()
        return self.output.df

    def __call__(self, *args, **kwargs):
        """Allow the class to be called as a function.
        This is especially useful when using a Pyspark DataFrame's transform method.

        Example
        -------
        Using pyspark's DataFrame transform method:
        ```python
        input_df = spark.range(3)

        output_df = input_df.transform(AddOne(target_column="foo")).transform(
            AddOne(target_column="bar")
        )
        ```

        In the above example, the `AddOne` transformation is applied to the `input_df` DataFrame using the `transform`
        method. The `output_df` will now contain the original DataFrame with an additional columns called `foo` and
        `bar', each with the values of `id` + 1.
        """
        return self.transform(*args, **kwargs)
