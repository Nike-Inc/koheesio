# Transformation Module

The `Transformation` module in Koheesio provides a set of classes for transforming data within a DataFrame. A
`Transformation` is a type of `SparkStep` that takes a DataFrame as input, applies a transformation, and returns a
DataFrame as output. The transformation logic is implemented in the `execute` method of each `Transformation` subclass.

## What is a Transformation?

A `Transformation` is a subclass of `SparkStep` that applies a transformation to a DataFrame and stores the result. The
transformation could be any operation that modifies the data or structure of the DataFrame, such as adding a new column,
filtering rows, or aggregating data.

Using `Transformation` classes ensures that data is transformed in a consistent manner across different parts of your 
pipeline. This can help avoid errors and make your code easier to understand and maintain.

## API Reference

See [API Reference](../../api_reference/spark/transformations/index.md) for a detailed description of the 
`Transformation` classes and their methods.

## Types of Transformations

There are three main types of transformations in Koheesio:

1. `Transformation`: This is the base class for all transformations. It takes a DataFrame as input and returns a
    DataFrame as output. The transformation logic is implemented in the `execute` method.

2. `ColumnsTransformation`: This is an extended `Transformation` class with a preset validator for handling column(s)
    data. It standardizes the input for a single column or multiple columns. If more than one column is passed, the 
    transformation will be run in a loop against all the given columns.

3. `ColumnsTransformationWithTarget`: This is an extended `ColumnsTransformation` class with an additional
    `target_column` field. This field can be used to store the result of the transformation in a new column. If the 
    `target_column` is not provided, the result will be stored in the source column.

Each type of transformation has its own use cases and advantages. The right one to use depends on the specific
requirements of your data pipeline.


## How to Define a Transformation

To define a `Transformation`, you create a subclass of the `Transformation` class and implement the `execute` method.
The `execute` method should take a DataFrame from `self.input.df`, apply a transformation, and store the result in
`self.output.df`.

`Transformation` classes abstract away some of the details of transforming data, allowing you to focus on the logic of
your pipeline. This can make your code cleaner and easier to read.

Here's an example of a `Transformation`:

```python
class MyTransformation(Transformation):
    def execute(self):
        # get data from self.input.df
        data = self.input.df
        # apply transformation
        transformed_data = apply_transformation(data)
        # store result in self.output.df
        self.output.df = transformed_data
```

In this example, `MyTransformation` is a subclass of `Transformation` that you've defined. The `execute` method gets
the data from `self.input.df`, applies a transformation called `apply_transformation` (undefined in this example), and
stores the result in `self.output.df`.


## How to Define a ColumnsTransformation

To define a `ColumnsTransformation`, you create a subclass of the `ColumnsTransformation` class and implement the 
`execute` method. The `execute` method should apply a transformation to the specified columns of the DataFrame.

`ColumnsTransformation` classes can be easily swapped out for different data transformations without changing the rest 
of your pipeline. This can make your pipeline more flexible and easier to modify or extend.

Here's an example of a `ColumnsTransformation`:

```python
from pyspark.sql import functions as f
from koheesio.spark.transformations import ColumnsTransformation

class AddOne(ColumnsTransformation):
    def execute(self):
        for column in self.get_columns():
            self.output.df = self.df.withColumn(column, f.col(column) + 1)
```

In this example, `AddOne` is a subclass of `ColumnsTransformation` that you've defined. The `execute` method adds 1 to
each column in `self.get_columns()`.

The `ColumnsTransformation` class has a `ColumnConfig` class that can be used to configure the behavior of the class.
This class has the following fields:

- `run_for_all_data_type`: Allows to run the transformation for all columns of a given type.
- `limit_data_type`: Allows to limit the transformation to a specific data type.
- `data_type_strict_mode`: Toggles strict mode for data type validation. Will only work if `limit_data_type` is set.

Note that data types need to be specified as a `SparkDatatype` enum. Users should not have to interact with the
`ColumnConfig` class directly.


## How to Define a ColumnsTransformationWithTarget

To define a `ColumnsTransformationWithTarget`, you create a subclass of the `ColumnsTransformationWithTarget` class and
implement the `func` method. The `func` method should return the transformation that will be applied to the column(s).
The `execute` method, which is already preset, will use the `get_columns_with_target` method to loop over all the
columns and apply this function to transform the DataFrame.

Here's an example of a `ColumnsTransformationWithTarget`:

```python
from pyspark.sql import Column
from koheesio.spark.transformations import ColumnsTransformationWithTarget

class AddOneWithTarget(ColumnsTransformationWithTarget):
    def func(self, col: Column):
        return col + 1
```

In this example, `AddOneWithTarget` is a subclass of `ColumnsTransformationWithTarget` that you've defined. The `func`
method adds 1 to the values of a given column.

The `ColumnsTransformationWithTarget` class has an additional `target_column` field. This field can be used to store the
result of the transformation in a new column. If the `target_column` is not provided, the result will be stored in the
source column. If more than one column is passed, the `target_column` will be used as a suffix. Leaving this blank will
result in the original columns being renamed.

The `ColumnsTransformationWithTarget` class also has a `get_columns_with_target` method. This method returns an iterator
of the columns and handles the `target_column` as well.


## Key Features of a Transformation

1. **Execute Method**: The `Transformation` class provides an `execute` method to implement in your subclass.
    This method should take a DataFrame from `self.input.df`, apply a transformation, and store the result in 
    `self.output.df`.
    
    For `ColumnsTransformation` and `ColumnsTransformationWithTarget`, the `execute` method is already implemented in
    the base class. Instead of overriding `execute`, you implement a `func` method in your subclass. This `func` method
    should return the transformation to be applied to each column. The `execute` method will then apply this
    func to each column in a loop.

2. **DataFrame Property**: The `Transformation` class provides a `df` property as a shorthand for accessing
    `self.input.df`. This property ensures that the data is ready to be transformed, even if the `execute` method
    hasn't been explicitly called. This is useful for 'early validation' of the input data.

3. **SparkSession**: Every `Transformation` has a `SparkSession` available as `self.spark`. This is the currently active
    `SparkSession`, which can be used to perform distributed data processing tasks.

4. **Columns Property**: The `ColumnsTransformation` and `ColumnsTransformationWithTarget` classes provide a `columns`
    property. This property standardizes the input for a single column or multiple columns. If more than one column is
    passed, the transformation will be run in a loop against all the given columns.

5. **Target Column Property**: The `ColumnsTransformationWithTarget` class provides a `target_column` property. This
    field can be used to store the result of the transformation in a new column. If the `target_column` is not provided,
    the result will be stored in the source column.


## Examples of Transformation Classes in Koheesio

Koheesio provides a variety of `Transformation` subclasses for transforming data in different ways. Here are some
examples:

- `DataframeLookup`: This transformation joins two dataframes together based on a list of join mappings. It allows you
  to specify the join type and join hint, and it supports selecting specific target columns from the right dataframe.

    Here's an example of how to use the `DataframeLookup` transformation:

    ```python
    from pyspark.sql import SparkSession
    from koheesio.spark.transformations.lookup import DataframeLookup, JoinMapping, TargetColumn, JoinType

    spark = SparkSession.builder.getOrCreate()
    left_df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])
    right_df = spark.createDataFrame([(1, "A"), (3, "C")], ["id", "value"])

    lookup = DataframeLookup(
        df=left_df,
        other=right_df,
        on=JoinMapping(source_column="id", other_column="id"),
        targets=TargetColumn(target_column="value", target_column_alias="right_value"),
        how=JoinType.LEFT,
    )

    output_df = lookup.execute().df
    ```

- `HashUUID5`: This transformation is a subclass of `Transformation` and provides an interface to generate a UUID5
   hash for each row in the DataFrame. The hash is generated based on the values of the specified source columns.

    Here's an example of how to use the `HashUUID5` transformation:

    ```python
    from pyspark.sql import SparkSession
    from koheesio.spark.transformations.uuid5 import HashUUID5

    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])

    hash_transform = HashUUID5(
        df=df,
        source_columns=["id", "value"],
        target_column="hash"
    )

    output_df = hash_transform.execute().df
    ```

In this example, `HashUUID5` is a subclass of `Transformation`. After creating an instance of `HashUUID5`, you call
the `execute` method to apply the transformation. The `execute` method generates a UUID5 hash for each row in the
DataFrame based on the values of the `id` and `value` columns and stores the result in a new column named `hash`.

## Benefits of using Koheesio Transformations

Using a Koheesio `Transformation` over plain Spark provides several benefits:

1. **Consistency**: By using `Transformation` classes, you ensure that data is transformed in a consistent manner
    across different parts of your pipeline. This can help avoid errors and make your code easier to understand and
    maintain.

2. **Abstraction**: `Transformation` classes abstract away the details of transforming data, allowing you to focus on
    the logic of your pipeline. This can make your code cleaner and easier to read.

3. **Flexibility**: `Transformation` classes can be easily swapped out for different data transformations without
    changing the rest of your pipeline. This can make your pipeline more flexible and easier to modify or extend.

4. **Early Input Validation**: As a `Transformation` is a type of `SparkStep`, which in turn is a `Step` and a type of
    Pydantic `BaseModel`, all inputs are validated when an instance of a `Transformation` class is created. This early
    validation helps catch errors related to invalid input, such as an invalid column name, before the PySpark pipeline
    starts executing. This can help avoid unnecessary computation and make your data pipelines more robust and reliable.

5. **Ease of Testing**: `Transformation` classes are designed to be easily testable. This can make it easier to write
    unit tests for your data pipeline, helping to ensure its correctness and reliability.

6. **Robustness**: Koheesio has been extensively tested with hundreds of unit tests, ensuring that the `Transformation`
    classes work as expected under a wide range of conditions. This makes your data pipelines more robust and less
    likely to fail due to unexpected inputs or edge cases.

By using the concept of a `Transformation`, you can create data pipelines that are simple, consistent, flexible,
efficient, and reliable.

## Advanced Usage of Transformations

Transformations can be combined and chained together to create complex data processing pipelines. Here's an example of
how to chain transformations:

```python
from pyspark.sql import SparkSession
from koheesio.spark.transformations.uuid5 import HashUUID5
from koheesio.spark.transformations.lookup import DataframeLookup, JoinMapping, TargetColumn, JoinType

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Define two DataFrames
df1 = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])
df2 = spark.createDataFrame([(1, "C"), (3, "D")], ["id", "value"])

# Define the first transformation
lookup = DataframeLookup(
    other=df2,
    on=JoinMapping(source_column="id", other_column="id"),
    targets=TargetColumn(target_column="value", target_column_alias="right_value"),
    how=JoinType.LEFT,
)

# Apply the first transformation
output_df = lookup.transform(df1)

# Define the second transformation
hash_transform = HashUUID5(
    source_columns=["id", "value", "right_value"],
    target_column="hash"
)

# Apply the second transformation
output_df2 = hash_transform.transform(output_df)
```

In this example, `DataframeLookup` is a subclass of `ColumnsTransformation` and `HashUUID5` is a subclass of 
`Transformation`. After creating instances of `DataframeLookup` and `HashUUID5`, you call the `transform` method to 
apply each transformation. The `transform` method of `DataframeLookup` performs a left join with `df2` on the `id`
column and adds the `value` column from `df2` to the result DataFrame as `right_value`. The `transform` method of
`HashUUID5` generates a UUID5 hash for each row in the DataFrame based on the values of the `id`, `value`, and
`right_value` columns and stores the result in a new column named `hash`.

## Troubleshooting Transformations

If you encounter an error when using a transformation, here are some steps you can take to troubleshoot:

1. **Check the Input Data**: Make sure the input DataFrame to the transformation is correct. You can use the `show`
    method of the DataFrame to print the first few rows of the DataFrame.

2. **Check the Transformation Parameters**: Make sure the parameters passed to the transformation are correct. For
    example, if you're using a `DataframeLookup`, make sure the join mappings and target columns are correctly
    specified.

3. **Check the Transformation Logic**: If the input data and parameters are correct, there might be an issue with the
    transformation logic. You can use PySpark's logging utilities to log intermediate results and debug the
    transformation logic.

4. **Check the Output Data**: If the transformation executes without errors but the output data is not as expected, you
    can use the `show` method of the DataFrame to print the first few rows of the output DataFrame. This can help you
    identify any issues with the transformation logic.

## Conclusion

The `Transformation` module in Koheesio provides a powerful and flexible way to transform data in a DataFrame. By
using `Transformation` classes, you can create data pipelines that are simple, consistent, flexible, efficient, and
reliable. Whether you're performing simple transformations like adding a new column, or complex transformations like
joining multiple DataFrames, the `Transformation` module has you covered.
