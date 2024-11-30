"""
A collection of classes for performing various transformations on arrays in PySpark.

These transformations include operations such as removing duplicates, exploding arrays into separate rows, reversing
the order of elements, sorting elements, removing certain values, and calculating aggregate statistics like minimum,
maximum, sum, mean, and median.

Concept
-------
* Every transformation in this module is implemented as a class that inherits from the `ArrayTransformation` class.
* The `ArrayTransformation` class is a subclass of `ColumnsTransformationWithTarget`
* The `ArrayTransformation` class implements the `func` method, which is used to define the transformation logic.
* The `func` method takes a `column` as input and returns a `Column` object.
* The `Column` object is a PySpark column that can be used to perform transformations on a DataFrame column.
* The `ArrayTransformation` limits the data type of the transformation to array by setting the `ColumnConfig` class to
    `run_for_all_data_type = [SparkDatatype.ARRAY]` and `limit_data_type = [SparkDatatype.ARRAY]`.

See Also
--------
* [koheesio.spark.transformations](index.md)
    Module containing all transformation classes.
* [koheesio.spark.transformations.ColumnsTransformationWithTarget](index.md#koheesio.spark.transformations.ColumnsTransformationWithTarget)
    Base class for all transformations that operate on columns and have a target column.
"""

from typing import Any
from abc import ABC
from functools import reduce

from pyspark.sql import Column
from pyspark.sql import functions as F

from koheesio.models import Field
from koheesio.spark.transformations import ColumnsTransformationWithTarget
from koheesio.spark.utils import (
    SparkDatatype,
    spark_data_type_is_numeric,
    spark_minor_version,
)

__all__ = [
    "ArrayDistinct",
    "Explode",
    "ExplodeDistinct",
    "ArrayReverse",
    "ArraySort",
    "ArraySortAsc",
    "ArraySortDesc",
    "ArrayRemove",
    "ArrayMin",
    "ArrayMax",
    "ArraySum",
    "ArrayMean",
    "ArrayMedian",
]


class ArrayTransformation(ColumnsTransformationWithTarget, ABC):
    """Base class for array transformations"""

    # pylint: disable=R0903
    class ColumnConfig(ColumnsTransformationWithTarget.ColumnConfig):
        """Set the data type of the Transformation to array"""

        run_for_all_data_type = [SparkDatatype.ARRAY]
        limit_data_type = [SparkDatatype.ARRAY]

    # pylint: enable=R0903

    def func(self, column: Column) -> Column:
        raise NotImplementedError("This is an abstract class")


class ArrayDistinct(ArrayTransformation):
    """
    Remove duplicates from array

    Example
    -------
    ```python
    ArrayDistinct(column="array_column")
    ```
    """

    filter_empty: bool = Field(
        default=True, description="Remove null, nan, and empty values from array. Default is True."
    )

    def func(self, column: Column) -> Column:
        _fn = F.array_distinct(column)

        # noinspection PyUnresolvedReferences
        element_type = self.column_type_of_col(column, None, False).elementType
        is_numeric = spark_data_type_is_numeric(element_type)

        if self.filter_empty:
            # Remove null values from array
            if spark_minor_version >= 3.4:
                # Run array_compact if spark version is 3.4 or higher
                # https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.array_compact.html
                # pylint: disable=E0611
                from pyspark.sql.functions import array_compact as _array_compact

                _fn = _array_compact(_fn)
                # pylint: enable=E0611
            else:
                # Otherwise, remove null from array using array_except
                _fn = F.array_except(_fn, F.array(F.lit(None)))

            # Remove nan or empty values from array (depends on the type of the elements in array)
            if is_numeric:
                # Remove nan from array (float/int/numbers)
                _fn = F.array_except(_fn, F.array(F.lit(float("nan")).cast(element_type)))
            else:
                # Remove empty values from array (string/text)
                _fn = F.array_except(_fn, F.array(F.lit(""), F.lit(" ")))

        return _fn


class Explode(ArrayTransformation):
    """
    Explode the array into separate rows

    Example
    -------
    ```python
    Explode(column="array_column")
    ```
    """

    distinct: bool = Field(False, description="Remove duplicates from the exploded array. Default is False.")
    preserve_nulls: bool = Field(
        True,
        description="Preserve rows with null values in the exploded array"
        " by using explode_outer instead of explode.Default is True.",
    )

    def func(self, column: Column) -> Column:
        if self.distinct:
            column = ArrayDistinct.from_step(self).func(column)
        return F.explode_outer(column) if self.preserve_nulls else F.explode(column)


class ExplodeDistinct(Explode):
    """
    Explode the array into separate rows while removing duplicates and empty values

    Example
    -------
    ```python
    ExplodeDistinct(column="array_column")
    ```
    """

    distinct: bool = True


class ArrayReverse(ArrayTransformation):
    """
    Reverse the order of elements in the array

    Example
    -------
    ```python
    ArrayReverse(column="array_column")
    ```
    """

    def func(self, column: Column) -> Column:
        return F.reverse(column)


class ArraySort(ArrayTransformation):
    """
    Sort the elements in the array

    By default, the elements are sorted in ascending order. To sort the elements in descending order, set the `reverse`
    parameter to True.

    Example
    -------
    ```python
    ArraySort(column="array_column")
    ```
    """

    reverse: bool = Field(
        default=False, description="Sort the elements in the array in a descending order. Default is False."
    )

    def func(self, column: Column) -> Column:
        column = F.array_sort(column)
        if self.reverse:
            # Reverse the order of elements in the array
            column = ArrayReverse.from_step(self).func(column)
        return column


ArraySortAsc = ArraySort


class ArraySortDesc(ArraySort):
    """Sort the elements in the array in descending order"""

    reverse: bool = True


class ArrayNullNanProcess(ArrayTransformation):
    """
    Process an array by removing NaN and/or NULL values from elements.

    Parameters
    ----------
    keep_nan : bool, default False
        Whether to keep NaN values in the array. If set to True, the NaN values will be kept in the array.

    keep_null : bool, default False
        Whether to keep NULL values in the array. If set to True, the NULL values will be kept in the array.

    Returns
    -------
    column : Column
        The processed column with NaN and/or NULL values removed from elements.

    Examples
    --------
    ```python
    >>> input_data = [(1, [1.1, 2.1, 4.1, float("nan")])]
    >>> input_schema = StructType([StructField("id", IntegerType(), True),
        StructField("array_float", ArrayType(FloatType()), True),
    ])
    >>> spark = SparkSession.builder.getOrCreate()
    >>> df = spark.createDataFrame(input_data, schema=input_schema)
    >>> transformer = ArrayNumericNanProcess(column="array_float", keep_nan=False)
    >>> transformer.transform(df)
    >>> print(transformer.output.df.collect()[0].asDict()["array_float"])
    [1.1, 2.1, 4.1]

    >>> input_data = [(1, [1.1, 2.2, 4.1, float("nan")])]
    >>> input_schema = StructType([StructField("id", IntegerType(), True),
        StructField("array_float", ArrayType(FloatType()), True),
    ])
    >>> spark = SparkSession.builder.getOrCreate()
    >>> df = spark.createDataFrame(input_data, schema=input_schema)
    >>> transformer = ArrayNumericNanProcess(column="array_float", keep_nan=True)
    >>> transformer.transform(df)
    >>> print(transformer.output.df.collect()[0].asDict()["array_float"])
    [1.1, 2.1, 4.1, nan]
    ```
    """

    keep_nan: bool = Field(
        False,
        description="Whether to keep nan values in the array. Default is False. "
        "If set to True, the nan values will be kept in the array.",
    )

    keep_null: bool = Field(
        False,
        description="Whether to keep null values in the array. Default is False. "
        "If set to True, the null values will be kept in the array.",
    )

    def func(self, column: Column) -> Column:
        """
        Process the given column by removing NaN and/or NULL values from elements.

        Parameters:
        -----------
        column : Column
            The column to be processed.

        Returns:
        --------
        column : Column
            The processed column with NaN and/or NULL values removed from elements.
        """

        def apply_logic(x: Column):
            if self.keep_nan is False and self.keep_null is False:
                logic = x.isNotNull() & ~F.isnan(x)
            elif self.keep_nan is False:
                logic = ~F.isnan(x)
            elif self.keep_null is False:
                logic = x.isNotNull()

            return logic

        if self.keep_nan is False or self.keep_null is False:
            column = F.filter(column, apply_logic)

        return column


class ArrayRemove(ArrayNullNanProcess):
    """
    Remove a certain value from the array

    Parameters
    ----------
    keep_nan : bool, default False
        Whether to keep NaN values in the array. If set to True, the NaN values will be kept in the array.

    keep_null : bool, default False
        Whether to keep NULL values in the array. If set to True, the NULL values will be kept in the array.

    Example
    -------
    ```python
    ArrayRemove(column="array_column", value="value_to_remove")
    ```
    """

    value: Any = Field(default=None, description="The value to remove from the array.")
    make_distinct: bool = Field(default=False, description="Whether to remove duplicates from the array.")

    def func(self, column: Column) -> Column:
        value = self.value

        column = super().func(column)

        def filter_logic(x: Column, _val: Any):
            if self.keep_null and self.keep_nan:
                logic = (x != F.lit(_val)) | x.isNull() | F.isnan(x)
            elif self.keep_null:
                logic = (x != F.lit(_val)) | x.isNull()
            elif self.keep_nan:
                logic = (x != F.lit(_val)) | F.isnan(x)
            else:
                logic = x != F.lit(_val)

            return logic

        # Check if the value is iterable (i.e., a list, tuple, or set)
        if isinstance(value, (list, tuple, set)):
            result = reduce(lambda res, val: F.filter(res, lambda x: filter_logic(x, val)), value, column)
        else:
            # If the value is not iterable, simply remove the value from the array
            result = F.filter(column, lambda x: filter_logic(x, value))

        if self.make_distinct:
            result = F.array_distinct(result)

        return result


class ArrayMin(ArrayTransformation):
    """
    Return the minimum value in the array

    Example
    -------
    ```python
    ArrayMin(column="array_column")
    ```
    """

    def func(self, column: Column) -> Column:
        return F.array_min(column)


class ArrayMax(ArrayNullNanProcess):
    """
    Return the maximum value in the array

    Example
    -------
    ```python
    ArrayMax(column="array_column")
    ```
    """

    def func(self, column: Column) -> Column:
        # Call for processing of nan values
        column = super().func(column)

        return F.array_max(column)


class ArraySum(ArrayNullNanProcess):
    """
    Return the sum of the values in the array

    Parameters
    ----------
    keep_nan : bool, default False
        Whether to keep NaN values in the array. If set to True, the NaN values will be kept in the array.

    keep_null : bool, default False
        Whether to keep NULL values in the array. If set to True, the NULL values will be kept in the array.

    Example
    -------
    ```python
    ArraySum(column="array_column")
    ```
    """

    def func(self, column: Column) -> Column:
        """Using the `aggregate` function to sum the values in the array"""
        # raise an error if the array contains non-numeric elements
        element_type = self.column_type_of_col(column, None, False).elementType
        if not spark_data_type_is_numeric(element_type):
            raise ValueError(
                f"{column = } contains non-numeric values. The array type is {element_type}. "
                f"Only numeric values are supported for summing."
            )

        # remove na values from array.
        column = super().func(column)

        # Using the `aggregate` function to sum the values in the array by providing the initial value as 0.0 and the
        # lambda function to add the elements together. Pyspark will automatically infer the type of the initial value
        # making 0.0 valid for both integer and float types.
        initial_value = F.lit(0.0)
        return F.aggregate(column, initial_value, lambda accumulator, x: accumulator + x)


class ArrayMean(ArrayNullNanProcess):
    """
    Return the mean of the values in the array.

    Note: Only numeric values are supported for calculating the mean.

    Example
    -------
    ```python
    ArrayMean(column="array_column", target_column="average")
    ```
    """

    def func(self, column: Column) -> Column:
        """Calculate the mean of the values in the array"""
        # raise an error if the array contains non-numeric elements
        element_type = self.column_type_of_col(col=column, df=None, simple_return_mode=False).elementType

        if not spark_data_type_is_numeric(element_type):
            raise ValueError(
                f"{column = } contains non-numeric values. The array type is {element_type}. "
                f"Only numeric values are supported for calculating a mean."
            )

        _sum = ArraySum.from_step(self).func(column)
        # Call for processing of nan values
        column = super().func(column)
        _size = F.size(column)
        # return 0 if the size of the array is 0 to avoid division by zero
        return F.when(_size == 0, F.lit(0)).otherwise(_sum / _size)


class ArrayMedian(ArrayNullNanProcess):
    """
    Return the median of the values in the array.

    The median is the middle value in a sorted, ascending or descending, list of numbers.

    - If the size of the array is even, the median is the average of the two middle numbers.
    - If the size of the array is odd, the median is the middle number.

    Note: Only numeric values are supported for calculating the median.

    Example
    -------
    ```python
    ArrayMedian(column="array_column", target_column="median")
    ```
    """

    def func(self, column: Column) -> Column:
        """Calculate the median of the values in the array"""
        # Call for processing of nan values
        column = super().func(column)

        sorted_array = ArraySort.from_step(self).func(column)
        _size: Column = F.size(sorted_array)

        # Calculate the middle index. If the size is odd, PySpark discards the fractional part.
        # Use floor function to ensure the result is an integer
        middle: Column = F.floor((_size + 1) / 2).cast("int")

        # Define conditions
        is_size_zero: Column = _size == 0
        is_column_null: Column = column.isNull()
        is_size_even: Column = _size % 2 == 0

        # Define actions / responses
        # For even-sized arrays, calculate the average of the two middle elements
        average_of_middle_elements = (F.element_at(sorted_array, middle) + F.element_at(sorted_array, middle + 1)) / 2
        # For odd-sized arrays, select the middle element
        middle_element = F.element_at(sorted_array, middle)
        # In case the array is empty, return either None or 0
        none_value = F.lit(None)
        zero_value = F.lit(0)

        median = (
            # Check if the size of the array is 0
            F.when(
                is_size_zero,
                # If the size of the array is 0 and the column is null, return None
                # If the size of the array is 0 and the column is not null, return 0
                F.when(is_column_null, none_value).otherwise(zero_value),
            ).otherwise(
                # If the size of the array is not 0, calculate the median
                F.when(is_size_even, average_of_middle_elements).otherwise(middle_element)
            )
        )

        return median
