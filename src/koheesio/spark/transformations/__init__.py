"""
This module contains the base classes for all transformations.

See class docstrings for more information.

References
----------
For a comprehensive guide on the usage, examples, and additional features of Transformation classes, please refer to the
[reference/concepts/spark/transformations](../../../reference/spark/transformations.md) section of the Koheesio
documentation.

Classes
-------
Transformation
    Base class for all transformations

ColumnsTransformation
    Extended Transformation class with a preset validator for handling column(s) data

ColumnsTransformationWithTarget
    Extended ColumnsTransformation class with an additional `target_column` field
"""

from typing import Iterator, List, Optional, Union
from abc import ABC, abstractmethod

from pyspark.sql import functions as f
from pyspark.sql.types import DataType

from koheesio.models import Field, ListOfColumns, model_validator
from koheesio.spark import Column, DataFrame, SparkStep
from koheesio.spark.utils import SparkDatatype, get_column_name


class Transformation(SparkStep, ABC):
    """Base class for all transformations

    Concept
    -------
    A Transformation is a Step that takes a DataFrame as input and returns a DataFrame as output. The DataFrame is
    transformed based on the logic implemented in the `execute` method. Any additional parameters that are needed for
    the transformation can be passed to the constructor.

    Parameters
    ----------
    df : Optional[DataFrame]
        The DataFrame to apply the transformation to. If not provided, the DataFrame has to be passed to the
        transform-method.

    Example
    -------
    ### Implementing a transformation using the Transformation class:
    ```python
    from koheesio.steps.transformations import Transformation
    from pyspark.sql import functions as f


    class AddOne(Transformation):
        target_column: str = "new_column"

        def execute(self):
            self.output.df = self.df.withColumn(
                self.target_column, f.col("old_column") + 1
            )
    ```

    In the example above, the `execute` method is implemented to add 1 to the values of the `old_column` and store the
    result in a new column called `new_column`.

    ### Using the transformation:
    In order to use this transformation, we can call the `transform` method:

    ```python
    from pyspark.sql import SparkSession

    # create a DataFrame with 3 rows
    df = SparkSession.builder.getOrCreate().range(3)

    output_df = AddOne().transform(df)
    ```

    The `output_df` will now contain the original DataFrame with an additional column called `new_column` with the
    values of `old_column` + 1.

    __output_df:__

    |id|new_column|
    |--|----------|
    | 0|         1|
    | 1|         2|
    | 2|         3|
    ...

    ### Alternative ways to use the transformation:
    Alternatively, we can pass the DataFrame to the constructor and call the `execute` or `transform` method without
    any arguments:

    ```python
    output_df = AddOne(df).transform()
    # or
    output_df = AddOne(df).execute().output.df
    ```

    > Note: that the transform method was not implemented explicitly in the AddOne class. This is because the `transform`
    method is already implemented in the `Transformation` class. This means that all classes that inherit from the
    Transformation class will have the `transform` method available. Only the execute method needs to be implemented.

    ### Using the transformation as a function:
    The transformation can also be used as a function as part of a DataFrame's `transform` method:

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

    df: Optional[DataFrame] = Field(default=None, description="The Spark DataFrame")

    @abstractmethod
    def execute(self) -> SparkStep.Output:
        """Execute on a Transformation should handle self.df (input) and set self.output.df (output)

        This method should be implemented in the child class. The input DataFrame is available as `self.df` and the
        output DataFrame should be stored in `self.output.df`.

        For example:
        ```python
        def execute(self):
            self.output.df = self.df.withColumn(
                "new_column", f.col("old_column") + 1
            )
        ```

        The transform method will call this method and return the output DataFrame.
        """
        # self.df  # input dataframe
        # self.output.df # output dataframe
        self.output.df = ...  # implement the transformation logic
        raise NotImplementedError

    def transform(self, df: Optional[DataFrame] = None) -> DataFrame:
        """Execute the transformation and return the output DataFrame

        Note: when creating a child from this, don't implement this transform method. Instead, implement execute!

        See Also
        --------
        `Transformation.execute`

        Parameters
        ----------
        df: Optional[DataFrame]
            The DataFrame to apply the transformation to. If not provided, the DataFrame passed to the constructor
            will be used.

        Returns
        -------
        DataFrame
            The transformed DataFrame
        """
        self.df = df or self.df
        if not self.df:
            raise RuntimeError("No valid Dataframe was passed")
        self.execute()
        return self.output.df

    def __call__(self, *args, **kwargs):
        """Allow the class to be called as a function.
        This is especially useful when using a DataFrame's transform method.

        Example
        -------
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


class ColumnsTransformation(Transformation, ABC):
    """Extended Transformation class with a preset validator for handling column(s) data with a standardized input
    for a single column or multiple columns.

    Concept
    -------
    A ColumnsTransformation is a Transformation with a standardized input for column or columns.

    - `columns` are stored as a list
    - either a single string, or a list of strings can be passed to enter the `columns`
    - `column` and `columns` are aliases to one another - internally the name `columns` should be used though.

    If more than one column is passed, the behavior of the Class changes this way:

    - the transformation will be run in a loop against all the given columns

    Configuring the ColumnsTransformation
    -------------------------------------
    [ColumnConfig]: ./index.md#koheesio.spark.transformations.ColumnsTransformation.ColumnConfig
    [SparkDatatype]: ../utils.md#koheesio.spark.utils.SparkDatatype

    The ColumnsTransformation class has a [ColumnConfig] class that can be used to configure the behavior of the class.
    Users should not have to interact with the [ColumnConfig] class directly.

    This class has the following fields:

    - `run_for_all_data_type`
        allows to run the transformation for all columns of a given type.

    - `limit_data_type`
        allows to limit the transformation to a specific data type.

    - `data_type_strict_mode`
        Toggles strict mode for data type validation. Will only work if `limit_data_type` is set.

    Data types need to be specified as a [SparkDatatype] enum.

    ---

    <small>
    - See the docstrings of the [ColumnConfig] class for more information.<br>
    - See the [SparkDatatype] enum for a list of available data types.<br>
    </small>

    Example
    -------
    Implementing a transformation using the `ColumnsTransformation` class:

    ```python
    from pyspark.sql import functions as f
    from koheesio.steps.transformations import ColumnsTransformation


    class AddOne(ColumnsTransformation):
        def execute(self):
            for column in self.get_columns():
                self.output.df = self.df.withColumn(
                    column, f.col(column) + 1
                )
    ```

    In the above example, the `execute` method is implemented to add 1 to the values of a given column.

    Parameters
    ----------
    columns : ListOfColumns
        The column (or list of columns) to apply the transformation to. Alias: column

    """

    columns: ListOfColumns = Field(
        default="",
        alias="column",
        description="The column (or list of columns) to apply the transformation to. Alias: column",
    )

    class ColumnConfig:
        """
        Koheesio ColumnsTransformation specific Config

        Parameters
        ----------
        run_for_all_data_type : Optional[List[SparkDatatype]]
            allows to run the transformation for all columns of a given type.
            A user can trigger this behavior by either omitting the `columns` parameter or by passing a single `*` as a
            column name. In both cases, the `run_for_all_data_type` will be used to determine the data type.
            Value should be passed as a SparkDatatype enum.
            (default: [None])

        limit_data_type : Optional[List[SparkDatatype]]
            allows to limit the transformation to a specific data type.
            Value should be passed as a SparkDatatype enum.
            (default: [None])

        data_type_strict_mode : bool
            Toggles strict mode for data type validation. Will only work if `limit_data_type` is set.
            - when True, a ValueError will be raised if any column does not adhere to the `limit_data_type`
            - when False, a warning will be thrown and the column will be skipped instead
            (default: False)
        """

        run_for_all_data_type: Optional[List[SparkDatatype]] = None
        limit_data_type: Optional[List[SparkDatatype]] = None
        data_type_strict_mode: bool = False

    @model_validator(mode="after")
    def set_columns(self) -> "ColumnsTransformation":
        """Validate columns through the columns configuration provided"""
        columns = self.columns

        if len(columns) == 0:
            if self.run_for_all_is_set:
                columns = ["*"]
        else:
            if columns[0] == "*" and not self.run_for_all_is_set:
                raise ValueError("Cannot use '*' as a column name when no run_for_all_data_type is set")

        self.columns = columns
        return self

    @classmethod
    @property
    def run_for_all_is_set(cls) -> bool:
        """Returns True if the transformation should be run for all columns of a given type"""
        rfadt = cls.ColumnConfig.run_for_all_data_type  # shorthand
        return bool(rfadt and isinstance(rfadt, list) and rfadt[0] is not None)

    @classmethod
    @property
    def limit_data_type_is_set(cls) -> bool:
        """Returns True if limit_data_type is set"""
        if (limit_data_type := cls.ColumnConfig.limit_data_type) is not None:
            return limit_data_type[0] is not None  # type: ignore
        return False

    @property
    def data_type_strict_mode_is_set(self) -> bool:
        """Returns True if data_type_strict_mode is set"""
        return self.ColumnConfig.data_type_strict_mode

    def column_type_of_col(
        self,
        col: Union[Column, str],
        df: Optional[DataFrame] = None,
        simple_return_mode: bool = True,
    ) -> Union[DataType, str]:
        """
        Returns the dataType of a Column object as a string.

        The Column object does not have a type attribute, so we have to ask the DataFrame its schema and find the type
        based on the column name. We retrieve the name of the column from the Column object by calling toString() from
        the JVM.

        Examples
        --------
        __input_df:__
        | str_column | int_column |
        |------------|------------|
        | hello      | 1          |
        | world      | 2          |

        ```python
        # using the AddOne transformation from the example above
        add_one = AddOne(
            columns=["str_column", "int_column"],
            df=input_df,
        )
        add_one.column_type_of_col("str_column")  # returns "string"
        add_one.column_type_of_col("int_column")  # returns "integer"
        # returns IntegerType
        add_one.column_type_of_col("int_column", simple_return_mode=False)
        ```

        Parameters
        ----------
        col : Union[str, Column]
            The column to check the type of

        df : Optional[DataFrame]
            The DataFrame belonging to the column. If not provided, the DataFrame passed to the constructor will be
            used.

        simple_return_mode : bool, default=True
            If True, the return value will be a simple string. If False, the return value will be a SparkDatatype enum.

        Returns
        -------
        datatype: str
            The type of the column as a string
        """
        df = df or self.df
        if not df:
            raise RuntimeError("No valid Dataframe was passed")

        # ensure that the column is a Column object
        if not isinstance(col, Column):  # type:ignore[misc, arg-type]
            col = f.col(col)  # type:ignore[arg-type]
        col_name = get_column_name(col)

        # In order to check the datatype of the column, we have to ask the DataFrame its schema
        df_col = next((c for c in df.schema if c.name == col_name), None)

        # Check if the column exists in the DataFrame schema
        if df_col is None:
            raise ValueError(f"Column '{col_name}' does not exist in the DataFrame schema")

        if simple_return_mode:
            return SparkDatatype(df_col.dataType.typeName()).value

        return df_col.dataType

    def get_all_columns_of_specific_type(self, data_type: Union[str, SparkDatatype]) -> List[str]:
        """Get all columns from the dataframe of a given type

        A DataFrame needs to be available in order to get the columns. If no DataFrame is available, a ValueError will
        be raised.

        Note: only one data type can be passed to this method. If you want to get columns of multiple data types, you
        have to call this method multiple times.

        Parameters
        ----------
        data_type : Union[str, SparkDatatype]
            The data type to get the columns for

        Returns
        -------
        List[str]
            A list of column names of the given data type
        """
        if not self.df:
            raise ValueError("No dataframe available - cannot get columns")

        expected_data_type = (SparkDatatype.from_string(data_type) if isinstance(data_type, str) else data_type).value

        columns_of_given_type: List[str] = [
            col for col in self.df.columns if self.df.schema[col].dataType.typeName() == expected_data_type
        ]

        if not columns_of_given_type:
            self.log.warning(f"No columns of type '{expected_data_type}' found in the DataFrame")

        return columns_of_given_type

    def is_column_type_correct(self, column: Union[Column, str]) -> bool:
        """Check if column type is correct and handle it if not, when limit_data_type is set"""
        if not self.limit_data_type_is_set:
            return True

        if self.column_type_of_col(column) in (limit_data_types := self.get_limit_data_types()):
            return True

        # Raises a ValueError if the Column object is not of a given type and data_type_strict_mode is set
        if self.data_type_strict_mode_is_set:
            raise ValueError(
                f"Critical error: {column} is not of type {limit_data_types}. Exception is raised because "
                f"`data_type_strict_mode` is set to True for {self.name}."
            )

        # Otherwise, throws a warning that the Column object is not of a given type
        self.log.warning(f"Column `{column}` is not of type `{limit_data_types}` and will be skipped.")
        return False

    def get_limit_data_types(self) -> list:
        """Get the limit_data_type as a list of strings"""
        return [dt.value for dt in self.ColumnConfig.limit_data_type]  # type: ignore

    def get_columns(self) -> Iterator[str]:
        """Return an iterator of the columns"""
        columns = self.columns or self.df.columns

        # If `run_for_all_is_set` to True, we want to run the transformation for all columns of a given type,
        # unless the user has specified specific columns
        if self.run_for_all_is_set and (not columns or columns[0] == "*"):
            columns = [
                col
                for data_type in self.ColumnConfig.run_for_all_data_type  # type: ignore
                for col in self.get_all_columns_of_specific_type(data_type)
            ]

        for column in columns:
            if self.is_column_type_correct(column):
                yield column


class ColumnsTransformationWithTarget(ColumnsTransformation, ABC):
    """Extended ColumnsTransformation class with an additional `target_column` field

    Using this class makes implementing Transformations significantly easier.

    Concept
    -------
    A `ColumnsTransformationWithTarget` is a `ColumnsTransformation` with an additional `target_column` field. This
    field can be used to store the result of the transformation in a new column.

    If the `target_column` is not provided, the result will be stored in the source column.

    If more than one column is passed, the behavior of the Class changes this way:

    - the transformation will be run in a loop against all the given columns
    - automatically handles the renaming of the columns when more than one column is passed
    - the `target_column` will be used as a suffix. Leaving this blank will result in the original columns being renamed

    The `func` method should be implemented in the child class. This method should return the transformation that will
    be applied to the column(s). The execute method (already preset) will use the `get_columns_with_target` method to
    loop over all the columns and apply this function to transform the DataFrame.

    Parameters
    ----------
    columns : ListOfColumns, optional, default=*
        The column (or list of columns) to apply the transformation to. Alias: column. If not provided, the
        `run_for_all_data_type` will be used to determine the data type. If `run_for_all_data_type` is not set, the
        transformation will be run for all columns of a given type.
    target_column : Optional[str], optional, default=None
        The name of the column to store the result in. If not provided, the result will be stored in the source column.
        Alias: target_suffix - if multiple columns are given as source, this input will be used as a suffix instead.

    Example
    -------
    Writing your own transformation using the `ColumnsTransformationWithTarget` class:

    ```python
    from pyspark.sql import Column
    from koheesio.steps.transformations import (
        ColumnsTransformationWithTarget,
    )


    class AddOneWithTarget(ColumnsTransformationWithTarget):
        def func(self, col: Column):
            return col + 1
    ```

    In the above example, the `func` method is implemented to add 1 to the values of a given column.

    In order to use this transformation, we can call the `transform` method:
    ```python
    from pyspark.sql import SparkSession

    # create a DataFrame with 3 rows
    df = SparkSession.builder.getOrCreate().range(3)

    output_df = AddOneWithTarget(
        column="id", target_column="new_id"
    ).transform(df)
    ```

    The `output_df` will now contain the original DataFrame with an additional column called `new_id` with the values of
    `id` + 1.

    __output_df:__

    |id|new_id|
    |--|------|
    | 0|     1|
    | 1|     2|
    | 2|     3|


    > Note: The `target_column` will be used as a suffix when more than one column is given as source. Leaving this
    blank will result in the original columns being renamed.
    """

    target_column: Optional[str] = Field(
        default=None,
        alias="target_suffix",
        description="The column to store the result in. If not provided, the result will be stored in the source"
        "column. Alias: target_suffix - if multiple columns are given as source, this will be used as a suffix",
    )

    @abstractmethod
    def func(self, column: Column) -> Column:
        """The function that will be run on a single Column of the DataFrame

        The `func` method should be implemented in the child class. This method should return the transformation that
        will be applied to the column(s). The execute method (already preset) will use the `get_columns_with_target`
        method to loop over all the columns and apply this function to transform the DataFrame.

        Parameters
        ----------
        column : Column
            The column to apply the transformation to

        Returns
        -------
        Column
            The transformed column
        """
        raise NotImplementedError

    def get_columns_with_target(self) -> Iterator[tuple[str, str]]:
        """Return an iterator of the columns

        Works just like in get_columns from the  ColumnsTransformation class except that it handles the `target_column`
        as well.

        If more than one column is passed, the behavior of the Class changes this way:
        - the transformation will be run in a loop against all the given columns
        - the target_column will be used as a suffix. Leaving this blank will result in the original columns being
            renamed.

        Returns
        -------
        iter
            An iterator of tuples containing the target column name and the original column name
        """
        columns = [*self.get_columns()]

        for column in columns:
            # ensures that we at least use the original column name
            target_column = self.target_column or column

            if len(columns) > 1:  # target_column becomes a suffix when more than 1 column is given
                # dict.fromkeys is used to avoid duplicates in the name while maintaining order
                _cols = [column, target_column]
                target_column = "_".join(list(dict.fromkeys(_cols)))

            yield target_column, column

    def execute(self) -> None:
        """Execute on a ColumnsTransformationWithTarget handles self.df (input) and set self.output.df (output)
        This can be left unchanged, and hence should not be implemented in the child class.
        """
        df = self.df

        for target_column, column in self.get_columns_with_target():
            func = self.func  # select the applicable function
            df = df.withColumn(
                target_column,
                func(f.col(column)),  # type:ignore[arg-type]
            )

        self.output.df = df
