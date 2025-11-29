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

from typing import (
    Any,
    Callable,
    Concatenate,
    Dict,
    Iterator,
    List,
    Optional,
    ParamSpec,
    Type,
    Union,
    get_args,
    get_type_hints,
    overload,
)
from abc import ABC, abstractmethod
import inspect

from pyspark.sql import functions as f
from pyspark.sql.types import DataType

from koheesio.models import (
    ExtraParamsMixin,
    Field,
    PrivateAttr,
    model_validator,
)
from koheesio.spark import Column, DataFrame, SparkStep
from koheesio.spark.utils import ListOfColumns, SparkDatatype, get_column_name
from koheesio.utils import get_args_for_func


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

    @classmethod
    def from_func(
        cls, func: Callable, **kwargs: Dict[str, Any]
    ) -> Callable[..., "Transformation"]:
        """Create a Transformation from a function.

        This method enables function-based transformations for DataFrame operations, providing a simpler alternative
        to subclassing for simple transformations.

        Parameters
        ----------
        func : Callable
            Function that takes a DataFrame as first parameter and returns a DataFrame.
            Signature: func(df: DataFrame, **params) -> DataFrame
        **kwargs : dict
            Default parameters to pass to the function. Can be overridden when instantiating.

        Returns
        -------
        Callable[..., Transformation]
            A partial function that creates Transformation instances.

        Example
        -------
        ### Simple DataFrame transformation
        ```python
        from koheesio.spark.transformations import Transformation


        def lowercase_columns(df: DataFrame) -> DataFrame:
            return df.withColumnsRenamed(
                {col: col.lower() for col in df.columns}
            )


        LowercaseColumns = Transformation.from_func(lowercase_columns)

        # Usage
        output_df = LowercaseColumns().transform(input_df)
        ```

        ### Parameterized transformation
        ```python
        def add_prefix(
            df: DataFrame, prefix: str, columns: List[str]
        ) -> DataFrame:
            for col_name in columns:
                df = df.withColumn(
                    col_name, f.concat(f.lit(prefix), f.col(col_name))
                )
            return df


        AddPrefix = Transformation.from_func(add_prefix)

        # Usage with parameters
        output_df = AddPrefix(
            prefix="test_", columns=["name", "id"]
        ).transform(input_df)
        ```

        ### Using .partial() for specialization
        ```python
        AddTestPrefix = AddPrefix.partial(prefix="test_")
        output_df = AddTestPrefix(columns=["name"]).transform(input_df)
        ```

        Notes
        -----
        - This is the recommended approach for simple DataFrame transformations in v0.11+
        - For complex transformations with validation, computed properties, or multiple methods,
          continue using subclassing
        - The Transform class is deprecated in favor of this method

        See Also
        --------
        Transform.from_func : Legacy implementation (deprecated)
        ColumnsTransformation.from_func : For column-level transformations
        """
        # Capture the function in a local variable
        _func = func
        _default_kwargs = kwargs

        # Create a concrete implementation class
        class FunctionBasedTransformation(Transformation, ExtraParamsMixin):
            """Transformation created from a function."""

            func: callable = Field(default=_func, exclude=True)

            @model_validator(mode="before")
            def _merge_default_kwargs(cls, values):
                """Merge default kwargs from from_func with instance params."""
                if isinstance(values, dict):
                    # Merge default kwargs with any params provided
                    params = values.get("params", {})
                    merged_params = {**_default_kwargs, **params}
                    if merged_params:
                        values["params"] = merged_params
                return values

            def execute(self) -> SparkStep.Output:
                """Execute the function-based transformation."""
                func_with_args, func_kwargs = get_args_for_func(self.func, self.params)
                self.output.df = func_with_args(self.df, **func_kwargs)

        # Set a meaningful name for the class based on the function name
        func_name = getattr(_func, "__name__", "Transformation")
        FunctionBasedTransformation.__name__ = (
            func_name if func_name != "<lambda>" else "FunctionBasedTransformation"
        )
        FunctionBasedTransformation.__qualname__ = FunctionBasedTransformation.__name__

        # Return the class itself (can be used with .partial() for further customization)
        return FunctionBasedTransformation


# ParamSpec for capturing function parameters (excluding df)
P = ParamSpec("P")


@overload
def transformation(
    func: Callable[Concatenate[DataFrame, P], DataFrame],
    *,
    validate: bool = True,
    **kwargs,
) -> Type["Transformation"]: ...


@overload
def transformation(
    func: None = None, *, validate: bool = True, **kwargs
) -> Callable[
    [Callable[Concatenate[DataFrame, P], DataFrame]], Type["Transformation"]
]: ...


def transformation(
    func: Optional[Callable] = None, *, validate: bool = True, **kwargs
) -> Union[Type["Transformation"], Callable[[Callable], Type["Transformation"]]]:
    """Decorator to create a Transformation from a function with runtime type validation.

    This is syntactic sugar over Transformation.from_func() that adds Pydantic runtime
    validation for enhanced type safety.

    Parameters
    ----------
    func : Callable, optional
        Function to decorate. If None, returns a decorator (for @transformation(...) syntax).
    validate : bool, default=True
        Whether to apply Pydantic runtime validation to the function parameters.
        Note: Validation occurs at transformation instantiation, not execution.
    **kwargs : dict
        Default parameters to pass to the transformation (same as .from_func()).

    Returns
    -------
    Callable
        Either a decorator or a Transformation class.

    Examples
    --------
    ### Simple transformation
    ```python
    @transformation
    def lowercase_columns(df: DataFrame) -> DataFrame:
        return df.select([f.lower(f.col(c)).alias(c) for c in df.columns])


    # Usage
    output_df = lowercase_columns().transform(input_df)
    ```

    ### With parameters
    ```python
    @transformation
    def filter_by_threshold(
        df: DataFrame, column: str, threshold: float
    ) -> DataFrame:
        return df.filter(f.col(column) > threshold)


    # Usage - type validation ensures threshold is float
    output_df = filter_by_threshold(
        column="age", threshold=18.0
    ).transform(input_df)
    ```

    ### With default parameters
    ```python
    @transformation(threshold=18.0)
    def filter_adults(
        df: DataFrame, column: str, threshold: float
    ) -> DataFrame:
        return df.filter(f.col(column) > threshold)


    # Usage
    output_df = filter_adults(column="age").transform(input_df)
    ```

    Notes
    -----
    - Internally calls Transformation.from_func()
    - Adds Pydantic validation for function parameters (excluding DataFrame)
    - Type mismatches raise ValidationError at instantiation time
    - Validation can be disabled with validate=False for performance
    """

    def decorator(_func: Callable) -> Type["Transformation"]:
        # Simply use from_func - validation happens via Pydantic BaseModel
        # The ExtraParamsMixin already provides parameter validation
        return Transformation.from_func(_func, **kwargs)

    # Support both @transformation and @transformation(...) syntax
    if func is None:
        # Called with arguments: @transformation(...)
        return decorator
    else:
        # Called without arguments: @transformation
        return decorator(func)


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

    # ColumnConfig fields as actual Pydantic fields (for function-based transformations)
    run_for_all_data_type: Optional[List[SparkDatatype]] = Field(
        default=None,
        description="Data types to automatically select when no columns specified",
    )
    limit_data_type: Optional[List[SparkDatatype]] = Field(
        default=None,
        description="Data types to restrict transformation to",
    )
    data_type_strict_mode: bool = Field(
        default=False,
        description="Whether to raise error on incompatible types (vs. warning)",
    )

    class ColumnConfig:
        """
        Koheesio `ColumnsTransformation` specific config.

        Deprecated: use the field-based configuration on `ColumnsTransformation` instead:
        `run_for_all_data_type`, `limit_data_type`, and `data_type_strict_mode`.

        This nested `ColumnConfig` class is kept for backward compatibility and will be removed in v1.0.

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
                raise ValueError(
                    "Cannot use '*' as a column name when no run_for_all_data_type is set"
                )

        self.columns = columns
        return self

    @property
    def run_for_all_is_set(self) -> bool:
        """Returns True if the transformation should be run for all columns of a given type"""
        # Check instance field first (for function-based transformations), then fall back to ColumnConfig
        if self.run_for_all_data_type is not None:
            rfadt = self.run_for_all_data_type
        else:
            rfadt = self.ColumnConfig.run_for_all_data_type
        return bool(rfadt and isinstance(rfadt, list) and rfadt[0] is not None)

    @property
    def limit_data_type_is_set(self) -> bool:
        """Returns True if limit_data_type is set"""
        # Check instance field first (for function-based transformations), then fall back to ColumnConfig
        if self.limit_data_type is not None:
            limit_data_type = self.limit_data_type
        else:
            limit_data_type = self.ColumnConfig.limit_data_type
        if limit_data_type is not None:
            return limit_data_type[0] is not None  # type: ignore
        return False

    @property
    def data_type_strict_mode_is_set(self) -> bool:
        """Returns True if data_type_strict_mode is set"""
        # Check instance field first (for function-based transformations), then fall back to ColumnConfig
        if self.data_type_strict_mode is not False:
            return self.data_type_strict_mode
        else:
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
            raise ValueError(
                f"Column '{col_name}' does not exist in the DataFrame schema"
            )

        if simple_return_mode:
            return SparkDatatype(df_col.dataType.typeName()).value

        return df_col.dataType

    def get_all_columns_of_specific_type(
        self, data_type: Union[str, SparkDatatype]
    ) -> List[str]:
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

        expected_data_type = (
            SparkDatatype.from_string(data_type)
            if isinstance(data_type, str)
            else data_type
        ).value

        columns_of_given_type: List[str] = [
            col
            for col in self.df.columns
            if self.df.schema[col].dataType.typeName() == expected_data_type
        ]

        if not columns_of_given_type:
            self.log.warning(
                f"No columns of type '{expected_data_type}' found in the DataFrame"
            )

        return columns_of_given_type

    def is_column_type_correct(self, column: Union[Column, str]) -> bool:
        """Check if column type is correct and handle it if not, when limit_data_type is set"""
        if not self.limit_data_type_is_set:
            return True

        if self.column_type_of_col(column) in (
            limit_data_types := self.get_limit_data_types()
        ):
            return True

        # Raises a ValueError if the Column object is not of a given type and data_type_strict_mode is set
        if self.data_type_strict_mode_is_set:
            raise ValueError(
                f"Critical error: {column} is not of type {limit_data_types}. Exception is raised because "
                f"`data_type_strict_mode` is set to True for {self.name}."
            )

        # Otherwise, throws a warning that the Column object is not of a given type
        self.log.warning(
            f"Column `{column}` is not of type `{limit_data_types}` and will be skipped."
        )
        return False

    def get_limit_data_types(self) -> list:
        """Get the limit_data_type as a list of strings"""
        # Check instance field first (for function-based transformations), then fall back to ColumnConfig
        limit_data_type = (
            self.limit_data_type
            if self.limit_data_type is not None
            else self.ColumnConfig.limit_data_type
        )
        # Handle both SparkDatatype enums and string values
        return [dt.value if hasattr(dt, "value") else dt for dt in limit_data_type]  # type: ignore

    def get_columns(self) -> Iterator[str]:
        """Return an iterator of the columns"""
        columns = self.columns or self.df.columns

        # If `run_for_all_is_set` to True, we want to run the transformation for all columns of a given type,
        # unless the user has specified specific columns
        if self.run_for_all_is_set and (not columns or columns[0] == "*"):
            # Check instance field first (for function-based transformations), then fall back to ColumnConfig
            run_for_all_types = (
                self.run_for_all_data_type
                if self.run_for_all_data_type is not None
                else self.ColumnConfig.run_for_all_data_type
            )
            columns = [
                col
                for data_type in run_for_all_types  # type: ignore
                for col in self.get_all_columns_of_specific_type(data_type)
            ]

        for column in columns:
            if self.is_column_type_correct(column):
                yield column

    @staticmethod
    def _detect_for_each(func: Callable, default_for_each: bool) -> bool:
        """Detect whether to apply function column-wise or multi-column based on type hints.

        Parameters
        ----------
        func : Callable
            The function to inspect
        default_for_each : bool
            The default value to return if detection is inconclusive

        Returns
        -------
        bool
            True for column-wise (apply to each column), False for multi-column (pass all columns)
        """
        # TODO: improve readability of this method along with reducing nestednes and better docs/comments
        try:
            sig = inspect.signature(func)

            # Check for variadic *args
            for param in sig.parameters.values():
                if param.kind == inspect.Parameter.VAR_POSITIONAL:
                    # *args detected → multi-column
                    return False

            # Get type hints - need to handle cases where Column type might not be available.
            # Pass Column in localns so it can be resolved.
            try:
                hints = get_type_hints(func, localns={"Column": Column})
            except (NameError, TypeError, AttributeError, ValueError):
                # If get_type_hints fails, fall back to checking annotations directly.
                hints = getattr(func, "__annotations__", {})

            # Check for ListOfColumns parameter
            if any(t == ListOfColumns for t in hints.values()):
                return False  # ListOfColumns → multi-column

            # Count Column-typed parameters (exclude 'return' type hint)
            column_params = []
            # TODO: refactor to use case matching instead of if-else statements
            for name, type_ in hints.items():
                if name == "return":
                    continue  # Skip return type hint
                # Direct Column type - check both by identity and by name
                # (Column might be a string annotation or actual type)
                if type_ == Column or (
                    isinstance(type_, type) and type_.__name__ == "Column"
                ):
                    column_params.append(name)
                # Handle string annotations
                elif isinstance(type_, str) and type_ == "Column":
                    column_params.append(name)
                # list[Column], tuple[Column], Sequence[Column]
                elif hasattr(type_, "__origin__"):
                    if type_.__origin__ in (list, tuple):
                        args = get_args(type_)
                        if args and (
                            args[0] == Column
                            or (
                                isinstance(args[0], type)
                                and args[0].__name__ == "Column"
                            )
                        ):
                            return False  # Sequence of Columns → multi-column

            if len(column_params) > 1:
                return False  # Multiple Column params → multi-column
            elif len(column_params) == 1:
                return True  # Single Column param → column-wise
            else:
                return default_for_each  # No Column params, use method default
        except (TypeError, ValueError):
            # If introspection fails in an expected way, fall back to the provided default.
            return default_for_each

    @classmethod
    def from_func(
        cls,
        func,
        run_for_all_data_type: Optional[List[SparkDatatype]] = None,
        limit_data_type: Optional[List[SparkDatatype]] = None,
        data_type_strict_mode: bool = False,
        for_each: Optional[bool] = None,
        **kwargs,
    ):
        """Create a ColumnsTransformation from a function.

        This method enables function-based column transformations with optional ColumnConfig parameters
        for data type validation and automatic column selection.

        Parameters
        ----------
        func : Callable
            Function that operates on Column objects.
            Signature: func(col: Column, **params) -> Column
        run_for_all_data_type : Optional[List[SparkDatatype]]
            Data types to automatically select when no columns specified.
            Equivalent to ColumnConfig.run_for_all_data_type
        limit_data_type : Optional[List[SparkDatatype]]
            Data types to restrict transformation to.
            Equivalent to ColumnConfig.limit_data_type
        data_type_strict_mode : bool
            Whether to raise error on incompatible types (vs. warning).
            Equivalent to ColumnConfig.data_type_strict_mode
        for_each : Optional[bool]
            Explicitly control column-wise vs multi-column behavior.
            - True: Apply function to each column separately (column-wise)
            - False: Pass all columns to function at once (multi-column)
            - None: Auto-detect from type hints (default: True if ambiguous)
        **kwargs : dict
            Default parameters to pass to the function.

        Returns
        -------
        Callable[..., ColumnsTransformation]
            A partial function that creates ColumnsTransformation instances.

        Example
        -------
        ### Simple column transformation
        ```python
        from pyspark.sql import Column
        from pyspark.sql import functions as f
        from koheesio.spark.transformations import ColumnsTransformation
        from koheesio.spark.utils import SparkDatatype


        def to_lower(col: Column) -> Column:
            return f.lower(col)


        LowerCase = ColumnsTransformation.from_func(
            to_lower,
            run_for_all_data_type=[SparkDatatype.STRING],
            limit_data_type=[SparkDatatype.STRING],
        )

        # Usage (same as class-based)
        output_df = LowerCase(
            columns=["name"], target_column="name_lower"
        ).transform(input_df)
        ```

        ### Parameterized transformation
        ```python
        def map_value(col: Column, from_val: str, to_val: str) -> Column:
            return f.when(col == from_val, to_val).otherwise(col)


        MapValue = ColumnsTransformation.from_func(map_value)
        UkToGb = MapValue.partial(from_val="uk", to_val="gb")

        # Usage
        output_df = UkToGb(columns=["country_code"]).transform(input_df)
        ```

        ### Factory pattern for related transformations
        ```python
        def string_transform(func):
            return ColumnsTransformation.from_func(
                func,
                run_for_all_data_type=[SparkDatatype.STRING],
                limit_data_type=[SparkDatatype.STRING],
            )


        LowerCase = string_transform(lambda col: f.lower(col))
        UpperCase = string_transform(lambda col: f.upper(col))
        TitleCase = string_transform(lambda col: f.initcap(col))
        ```

        Notes
        -----
        - The `target_column` parameter works automatically (in-place if not provided)
        - ColumnConfig parameters are optional (no restrictions if omitted)
        - Use `.partial()` to create specialized versions with preset parameters
        - This is the recommended approach for simple column transformations in v0.11+

        See Also
        --------
        BaseModel.partial : For creating specialized transformations
        ColumnsTransformationWithTarget : Current class-based approach (deprecated in v1.0)
        Transformation.from_func : For DataFrame-level transformations
        """
        # Capture parameters in local variables to avoid linter warnings
        _func = func
        _kwargs = kwargs

        # Detect for_each behavior (column-wise vs multi-column)
        if for_each is None:
            # Auto-detect from type hints, default to True (column-wise) if ambiguous
            _for_each = cls._detect_for_each(func, default_for_each=True)
        else:
            _for_each = for_each

        # Create a dynamic class that wraps the function
        class FunctionBasedColumnsTransformation(cls, ExtraParamsMixin):
            """Dynamically created column transformation from function."""

            wrapped_func: Callable = Field(default=_func, exclude=True)
            _for_each_mode: bool = PrivateAttr(default=_for_each)
            target_column: Optional[str] = Field(
                default=None,
                alias="target_suffix",
                description="The column to store the result in. If not provided, the result will be stored in the source column.",
            )

            @model_validator(mode="before")
            def _merge_default_kwargs(cls, values):
                """Merge default kwargs from from_func with instance params and set ColumnConfig defaults."""
                if isinstance(values, dict):
                    # Merge default kwargs with any params provided
                    params = values.get("params", {})
                    merged_params = {**_kwargs, **params}
                    if merged_params:
                        values["params"] = merged_params

                    # Set ColumnConfig defaults if not provided
                    if (
                        "run_for_all_data_type" not in values
                        and run_for_all_data_type is not None
                    ):
                        values["run_for_all_data_type"] = run_for_all_data_type
                    if "limit_data_type" not in values and limit_data_type is not None:
                        values["limit_data_type"] = limit_data_type
                    if (
                        "data_type_strict_mode" not in values
                        and data_type_strict_mode is not False
                    ):
                        values["data_type_strict_mode"] = data_type_strict_mode
                return values

            def func(self, column: Column) -> Column:
                """Apply the function to a column."""
                # Get the function with bound parameters
                func_with_args, func_kwargs = get_args_for_func(
                    self.wrapped_func, self.params
                )

                # If the function accepts a column parameter, pass it
                # Otherwise, call the partial function with the column
                try:
                    return func_with_args(column, **func_kwargs)
                except TypeError:
                    # Function might be a partial that already has column bound
                    return func_with_args(**func_kwargs)

            def execute(self) -> None:
                """Execute the column transformation with target column support."""
                from typing import get_type_hints
                import inspect

                df = self.df
                columns = [*self.get_columns()]

                # Use the for_each mode determined at class creation time
                if not self._for_each_mode:
                    # Multi-column: Pass ALL columns to function at once
                    target_col = self.target_column or "_".join(columns)

                    func_with_args, func_kwargs = get_args_for_func(
                        self.wrapped_func, self.params
                    )

                    # Inspect function signature to determine how to pass columns
                    sig = inspect.signature(self.wrapped_func)
                    params = list(sig.parameters.values())

                    # Check for variadic *args
                    has_var_positional = any(
                        p.kind == inspect.Parameter.VAR_POSITIONAL for p in params
                    )

                    # Check for ListOfColumns parameter
                    try:
                        hints = get_type_hints(self.wrapped_func)
                        has_list_of_columns = any(
                            t == ListOfColumns for t in hints.values()
                        )
                    except (NameError, TypeError, AttributeError, ValueError):
                        has_list_of_columns = False

                    if has_var_positional:
                        # Variadic *args: pass columns as separate arguments
                        column_objs = [f.col(c) for c in columns]
                        result_col = func_with_args(*column_objs, **func_kwargs)
                    elif has_list_of_columns:
                        # ListOfColumns parameter: pass as list
                        column_objs = [f.col(c) for c in columns]
                        result_col = func_with_args(column_objs, **func_kwargs)
                    else:
                        # Multiple positional params: pass as separate arguments
                        positional_params = [
                            p
                            for p in params
                            if p.kind in (p.POSITIONAL_OR_KEYWORD, p.POSITIONAL_ONLY)
                        ]
                        column_objs = [
                            f.col(c) for c in columns[: len(positional_params)]
                        ]
                        result_col = func_with_args(*column_objs, **func_kwargs)

                    df = df.withColumn(target_col, result_col)

                else:
                    # Column-wise: Apply function to EACH column separately
                    for idx, column in enumerate(columns):
                        target_col = self.target_column or column

                        if len(columns) > 1 and self.target_column:
                            _cols = [column, target_col]
                            target_col = "_".join(list(dict.fromkeys(_cols)))

                        func_with_args, func_kwargs = get_args_for_func(
                            self.wrapped_func, self.params
                        )

                        # Handle paired parameters (strict zip matching)
                        indexed_kwargs = {}
                        for key, value in func_kwargs.items():
                            if isinstance(value, (list, tuple)):
                                if len(value) != len(columns):
                                    raise ValueError(
                                        f"Parameter '{key}' has {len(value)} values but "
                                        f"{len(columns)} columns provided. "
                                        f"Lists must match column count exactly "
                                        f"(like Python's zip(..., strict=True))."
                                    )
                                indexed_kwargs[key] = value[idx]
                            else:
                                indexed_kwargs[key] = value

                        df = df.withColumn(
                            target_col, func_with_args(f.col(column), **indexed_kwargs)
                        )

                self.output.df = df

        # Set a meaningful name for the dynamic class based on the function name
        func_name = getattr(_func, "__name__", "ColumnsTransformation")
        if func_name != "<lambda>":
            # Use the actual function name if it's not a lambda
            FunctionBasedColumnsTransformation.__name__ = func_name
        else:
            # For lambdas, use a generic name
            FunctionBasedColumnsTransformation.__name__ = (
                "FunctionBasedColumnsTransformation"
            )
        FunctionBasedColumnsTransformation.__qualname__ = (
            FunctionBasedColumnsTransformation.__name__
        )

        # Set default values by modifying the FieldInfo objects directly
        # Pydantic will use these when instantiating the class
        if run_for_all_data_type is not None:
            FunctionBasedColumnsTransformation.model_fields[
                "run_for_all_data_type"
            ].default = run_for_all_data_type
        if limit_data_type is not None:
            FunctionBasedColumnsTransformation.model_fields[
                "limit_data_type"
            ].default = limit_data_type
        if data_type_strict_mode is not False:
            FunctionBasedColumnsTransformation.model_fields[
                "data_type_strict_mode"
            ].default = data_type_strict_mode

        return FunctionBasedColumnsTransformation

    @classmethod
    def from_multi_column_func(
        cls,
        func,
        run_for_all_data_type: Optional[List[SparkDatatype]] = None,
        limit_data_type: Optional[List[SparkDatatype]] = None,
        data_type_strict_mode: bool = False,
        for_each: Optional[bool] = None,
        **kwargs,
    ):
        """Create a multi-column ColumnsTransformation from a function.

        This method is similar to from_func() but defaults to multi-column behavior
        (passing all columns to the function at once) when type hints are ambiguous.

        Parameters
        ----------
        func : Callable
            Function that operates on multiple Column objects.
            Signature: func(col1: Column, col2: Column, ..., **params) -> Column
            Or: func(*cols: Column, **params) -> Column
            Or: func(cols: ListOfColumns, **params) -> Column
        run_for_all_data_type : Optional[List[SparkDatatype]]
            Data types to automatically select when no columns specified.
            Equivalent to ColumnConfig.run_for_all_data_type
        limit_data_type : Optional[List[SparkDatatype]]
            Data types to restrict transformation to.
            Equivalent to ColumnConfig.limit_data_type
        data_type_strict_mode : bool
            Whether to raise error on incompatible types (vs. warning).
            Equivalent to ColumnConfig.data_type_strict_mode
        for_each : Optional[bool]
            Explicitly control column-wise vs multi-column behavior.
            - True: Apply function to each column separately (column-wise)
            - False: Pass all columns to function at once (multi-column)
            - None: Auto-detect from type hints (default: False if ambiguous)
        **kwargs : dict
            Default parameters to pass to the function.

        Returns
        -------
        Callable[..., ColumnsTransformation]
            A class that creates ColumnsTransformation instances.

        Example
        -------
        ### Multi-column aggregation
        ```python
        from pyspark.sql import Column
        from pyspark.sql import functions as f
        from koheesio.spark.transformations import ColumnsTransformation


        def sum_quarters(
            q1: Column, q2: Column, q3: Column, q4: Column
        ) -> Column:
            return q1 + q2 + q3 + q4


        SumQuarters = ColumnsTransformation.from_multi_column_func(
            sum_quarters
        )
        output_df = SumQuarters(
            columns=["sales_q1", "sales_q2", "sales_q3", "sales_q4"],
            target_column="total_sales",
        ).transform(input_df)
        ```

        ### Variadic columns
        ```python
        from functools import reduce


        def sum_all(*cols: Column) -> Column:
            return reduce(lambda a, b: a + b, cols)


        SumAll = ColumnsTransformation.from_multi_column_func(sum_all)
        output_df = SumAll(
            columns=["jan", "feb", "mar"], target_column="q1_total"
        ).transform(input_df)
        ```

        Notes
        -----
        - This method defaults to multi-column behavior (for_each=False) when ambiguous
        - Use from_func() for column-wise operations (default: for_each=True)
        - Auto-detection works the same way, but fallback default differs

        See Also
        --------
        from_func : For column-wise transformations (default)
        """
        # Detect for_each behavior, but default to False (multi-column) if ambiguous
        if for_each is None:
            _for_each = cls._detect_for_each(func, default_for_each=False)
        else:
            _for_each = for_each

        # Use the same implementation as from_func, just with different default
        return cls.from_func(
            func,
            run_for_all_data_type=run_for_all_data_type,
            limit_data_type=limit_data_type,
            data_type_strict_mode=data_type_strict_mode,
            for_each=_for_each,
            **kwargs,
        )


# ParamSpec for column transformation parameters (excluding Column)
PC = ParamSpec("PC")


@overload
def column_transformation(
    func: Callable[Concatenate[Column, PC], Column],
    *,
    validate: bool = True,
    run_for_all_data_type: Optional[List[SparkDatatype]] = None,
    limit_data_type: Optional[List[SparkDatatype]] = None,
    data_type_strict_mode: bool = False,
    for_each: Optional[bool] = None,
    **kwargs,
) -> Type["ColumnsTransformation"]: ...


@overload
def column_transformation(
    func: None = None,
    *,
    validate: bool = True,
    run_for_all_data_type: Optional[List[SparkDatatype]] = None,
    limit_data_type: Optional[List[SparkDatatype]] = None,
    data_type_strict_mode: bool = False,
    for_each: Optional[bool] = None,
    **kwargs,
) -> Callable[
    [Callable[Concatenate[Column, PC], Column]], Type["ColumnsTransformation"]
]: ...


def column_transformation(
    func: Optional[Callable] = None,
    *,
    validate: bool = True,
    run_for_all_data_type: Optional[List[SparkDatatype]] = None,
    limit_data_type: Optional[List[SparkDatatype]] = None,
    data_type_strict_mode: bool = False,
    for_each: Optional[bool] = None,
    **kwargs,
) -> Union[
    Type["ColumnsTransformation"], Callable[[Callable], Type["ColumnsTransformation"]]
]:
    """Decorator to create a ColumnsTransformation with runtime type validation.

    This is syntactic sugar over ColumnsTransformation.from_func() that adds Pydantic
    runtime validation and explicit ColumnConfig parameters.

    Parameters
    ----------
    func : Callable, optional
        Function to decorate. If None, returns a decorator.
    validate : bool, default=True
        Whether to apply Pydantic runtime validation.
    run_for_all_data_type : Optional[List[SparkDatatype]]
        Data types to automatically select when no columns specified.
    limit_data_type : Optional[List[SparkDatatype]]
        Data types to restrict transformation to.
    data_type_strict_mode : bool, default=False
        Whether to raise error on incompatible types (vs. warning).
    for_each : Optional[bool]
        Explicitly set column-wise (True) or multi-column (False) mode.
        If None, auto-detects based on function signature.
    **kwargs : dict
        Default parameters to pass to the transformation.

    Returns
    -------
    Callable
        Either a decorator or a ColumnsTransformation class.

    Examples
    --------
    ### Simple column transformation
    ```python
    @column_transformation
    def to_upper(col: Column) -> Column:
        return f.upper(col)


    # Usage
    output_df = to_upper(columns=["name", "city"]).transform(input_df)
    ```

    ### With ColumnConfig
    ```python
    @column_transformation(run_for_all_data_type=[SparkDatatype.STRING])
    def trim_strings(col: Column) -> Column:
        return f.trim(col)


    # Usage - automatically applies to all string columns
    output_df = trim_strings().transform(input_df)
    ```

    ### With parameters and paired values
    ```python
    @column_transformation
    def add_tax(amount: Column, rate: float = 0.08) -> Column:
        return amount * (1 + rate)


    # Single rate (broadcast)
    output_df = add_tax(columns=["price"], rate=0.10).transform(input_df)

    # Paired rates (strict zip)
    output_df = add_tax(
        columns=["food_price", "non_food_price"], rate=[0.08, 0.13]
    ).transform(input_df)
    ```

    ### Type validation in action
    ```python
    @column_transformation
    def scale(col: Column, factor: int) -> Column:
        return col * factor


    # This raises ValidationError - factor must be int
    output_df = scale(columns=["value"], factor="2").transform(input_df)
    ```

    Notes
    -----
    - Internally calls ColumnsTransformation.from_func()
    - Adds Pydantic validate_call for runtime type checking
    - Supports all from_func() features: paired parameters, variadic, multi-column
    """

    def decorator(f: Callable) -> Type["ColumnsTransformation"]:
        # Simply use from_func - validation happens via Pydantic BaseModel
        # The ExtraParamsMixin already provides parameter validation
        return ColumnsTransformation.from_func(
            f,
            run_for_all_data_type=run_for_all_data_type,
            limit_data_type=limit_data_type,
            data_type_strict_mode=data_type_strict_mode,
            for_each=for_each,
            **kwargs,
        )

    # Support both @column_transformation and @column_transformation(...) syntax
    if func is None:
        return decorator
    else:
        return decorator(func)


# ParamSpec for multi-column transformation parameters
PM = ParamSpec("PM")


@overload
def multi_column_transformation(
    func: Callable[PM, Column],
    *,
    validate: bool = True,
    run_for_all_data_type: Optional[List[SparkDatatype]] = None,
    limit_data_type: Optional[List[SparkDatatype]] = None,
    data_type_strict_mode: bool = False,
    **kwargs,
) -> Type["ColumnsTransformation"]: ...


@overload
def multi_column_transformation(
    func: None = None,
    *,
    validate: bool = True,
    run_for_all_data_type: Optional[List[SparkDatatype]] = None,
    limit_data_type: Optional[List[SparkDatatype]] = None,
    data_type_strict_mode: bool = False,
    **kwargs,
) -> Callable[[Callable[PM, Column]], Type["ColumnsTransformation"]]: ...


def multi_column_transformation(
    func: Optional[Callable] = None,
    *,
    validate: bool = True,
    run_for_all_data_type: Optional[List[SparkDatatype]] = None,
    limit_data_type: Optional[List[SparkDatatype]] = None,
    data_type_strict_mode: bool = False,
    **kwargs,
) -> Union[
    Type["ColumnsTransformation"], Callable[[Callable], Type["ColumnsTransformation"]]
]:
    """Decorator for multi-column transformations (N columns → 1 result).

    This is syntactic sugar over ColumnsTransformation.from_multi_column_func()
    that adds Pydantic runtime validation. Explicitly sets for_each=False.

    Parameters
    ----------
    func : Callable, optional
        Function to decorate. Must accept multiple columns.
    validate : bool, default=True
        Whether to apply Pydantic runtime validation.
    run_for_all_data_type : Optional[List[SparkDatatype]]
        Data types to automatically select.
    limit_data_type : Optional[List[SparkDatatype]]
        Data types to restrict to.
    data_type_strict_mode : bool, default=False
        Whether to raise error on incompatible types.
    **kwargs : dict
        Default parameters.

    Returns
    -------
    Callable
        Either a decorator or a ColumnsTransformation class.

    Examples
    --------
    ### Fixed arity aggregation
    ```python
    @multi_column_transformation
    def sum_quarters(
        q1: Column, q2: Column, q3: Column, q4: Column
    ) -> Column:
        return q1 + q2 + q3 + q4


    # Usage
    output_df = sum_quarters(
        columns=["sales_q1", "sales_q2", "sales_q3", "sales_q4"],
        target_column="total_sales",
    ).transform(input_df)
    ```

    ### Variadic aggregation
    ```python
    @multi_column_transformation
    def sum_all(*cols: Column) -> Column:
        from functools import reduce

        return reduce(lambda a, b: a + b, cols)


    # Usage - works with any number of columns
    output_df = sum_all(
        columns=["jan", "feb", "mar"], target_column="q1"
    ).transform(input_df)
    ```

    ### With parameters
    ```python
    @multi_column_transformation
    def weighted_sum(
        q1: Column, q2: Column, q3: Column, q4: Column, weights: list
    ) -> Column:
        return (
            q1 * weights[0]
            + q2 * weights[1]
            + q3 * weights[2]
            + q4 * weights[3]
        )


    # Usage
    output_df = weighted_sum(
        columns=["q1", "q2", "q3", "q4"],
        weights=[0.1, 0.2, 0.3, 0.4],
        target_column="weighted_total",
    ).transform(input_df)
    ```

    Notes
    -----
    - Internally calls ColumnsTransformation.from_multi_column_func()
    - Explicitly sets for_each=False (multi-column mode)
    - Adds Pydantic validate_call for runtime type checking
    """

    def decorator(f: Callable) -> Type["ColumnsTransformation"]:
        # Simply use from_multi_column_func - validation happens via Pydantic BaseModel
        # The ExtraParamsMixin already provides parameter validation
        return ColumnsTransformation.from_multi_column_func(
            f,
            run_for_all_data_type=run_for_all_data_type,
            limit_data_type=limit_data_type,
            data_type_strict_mode=data_type_strict_mode,
            **kwargs,
        )

    # Support both syntaxes
    if func is None:
        return decorator
    else:
        return decorator(func)


class ColumnsTransformationWithTarget(ColumnsTransformation, ABC):
    """Extended ColumnsTransformation class with an additional `target_column` field

    .. deprecated:: 0.11
        This class is deprecated and will be removed in v1.0.
        Use :meth:`ColumnsTransformation.from_func` instead.
        See https://github.com/Nike-Inc/koheesio/discussions/225

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

            if (
                len(columns) > 1
            ):  # target_column becomes a suffix when more than 1 column is given
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


__all__ = [
    "Transformation",
    "ColumnsTransformation",
    "ColumnsTransformationWithTarget",
    "transformation",
    "column_transformation",
    "multi_column_transformation",
]
