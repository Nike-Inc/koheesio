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

There are two ways to define a transformation: **subclassing** (traditional) and **function-based** (simpler for basic cases).

### Function-Based Transformations

For simple transformations, you can use `Transformation.from_func()` to create a transformation from a function without subclassing:

```python
from koheesio.spark.transformations import Transformation
from pyspark.sql import functions as f

# Define a function that transforms a DataFrame
def lowercase_columns(df):
    return df.select([f.col(c).alias(c.lower()) for c in df.columns])

# Create a transformation from the function
LowercaseColumns = Transformation.from_func(lowercase_columns)

# Use it like any other transformation
input_df = spark.createDataFrame([("John", 25)], ["NAME", "AGE"])
output_df = LowercaseColumns().transform(input_df)
# Result: columns are now ["name", "age"]
```

**Parameterized Transformations:**

```python
def add_audit_columns(df, user: str, system: str = "koheesio"):
    return df.withColumn("created_by", f.lit(user)).withColumn("system", f.lit(system))

AddAudit = Transformation.from_func(add_audit_columns)

# Use with parameters
output_df = AddAudit(user="pipeline_user", system="production").transform(input_df)
```

**Specialized Transformations with `.partial()`:**

```python
# Create a specialized version with preset parameters
AddMyAudit = AddAudit.partial(user="my_pipeline", system="production")

# Use without needing to pass parameters
output_df = AddMyAudit().transform(input_df)
```

Function-based transformations are ideal for:
- Simple DataFrame operations
- Quick prototyping
- When you don't need complex validation or computed properties

### Decorator-Based Transformations

For even more concise syntax with added type safety, you can use decorators to create transformations. Decorators are syntactic sugar over `.from_func()` that add Pydantic runtime type validation:

```python
from koheesio.spark.transformations import transformation, column_transformation, multi_column_transformation
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as f
from koheesio.spark.utils import SparkDatatype

# DataFrame transformation with type validation
@transformation
def filter_adults(df: DataFrame, min_age: int = 18) -> DataFrame:
    return df.filter(f.col("age") >= min_age)

# Type validation ensures min_age is int
output_df = filter_adults(min_age=21).transform(input_df)

# This raises ValidationError - type mismatch
try:
    output_df = filter_adults(min_age="21").transform(input_df)  # ❌ Error!
except ValidationError:
    print("Type validation caught the error!")

# Column transformation with ColumnConfig
@column_transformation(run_for_all_data_type=[SparkDatatype.STRING])
def trim_strings(col: Column) -> Column:
    return f.trim(col)

# Automatically applies to all string columns
output_df = trim_strings().transform(input_df)

# Multi-column aggregation
@multi_column_transformation
def sum_quarters(q1: Column, q2: Column, q3: Column, q4: Column) -> Column:
    return q1 + q2 + q3 + q4

output_df = sum_quarters(
    columns=["sales_q1", "sales_q2", "sales_q3", "sales_q4"],
    target_column="total_sales"
).transform(input_df)
```

**Available Decorators:**

1. **`@transformation`** - For DataFrame-level transformations
   - Adds type validation to function parameters
   - Equivalent to `Transformation.from_func()` with `validate_call`

2. **`@column_transformation`** - For column-level transformations
   - Supports all ColumnConfig parameters
   - Auto-detects column-wise vs multi-column patterns
   - Adds type validation

3. **`@multi_column_transformation`** - For multi-column aggregations
   - Explicitly sets multi-column mode (for_each=False)
   - Ideal for N→1 aggregations
   - Adds type validation

**Type Validation Benefits:**

```python
@column_transformation
def add_tax(amount: Column, rate: float) -> Column:
    return amount * (1 + rate)

# Valid: rate is float
output_df = add_tax(columns=["price"], rate=0.08).transform(input_df)

# Valid: list of floats (paired parameters)
output_df = add_tax(
    columns=["food", "non_food"],
    rate=[0.08, 0.13]
).transform(input_df)

# Invalid: rate must be float or list of floats
try:
    output_df = add_tax(columns=["price"], rate="0.08").transform(input_df)
except ValidationError as e:
    print(f"Validation error: {e}")
```

**Disabling Validation:**

For performance-critical code:

```python
@column_transformation(validate=False)
def fast_transform(col: Column) -> Column:
    return f.upper(col)
```

**Comparison: Decorator vs .from_func():**

```python
# Decorator syntax (with type safety)
@column_transformation(run_for_all_data_type=[SparkDatatype.STRING])
def to_upper(col: Column) -> Column:
    return f.upper(col)

# Equivalent .from_func() syntax (no automatic type safety)
def to_upper_func(col: Column) -> Column:
    return f.upper(col)

ToUpper = ColumnsTransformation.from_func(
    to_upper_func,
    run_for_all_data_type=[SparkDatatype.STRING]
)
```

Decorator-based transformations are ideal for:

- When you want runtime type safety
- When writing new transformations from scratch
- When team consistency and clear intent are important
- When you want IDE autocomplete and type hints

### Subclassing Transformations

For more complex transformations, you create a subclass of the `Transformation` class and implement the `execute` method.
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

Subclassing is ideal for:

- Complex validation logic
- Computed properties or multiple methods
- Orchestration of other transformations


## How to Define a ColumnsTransformation

There are two ways to define a column transformation: **function-based** (simpler) and **subclassing** (for complex cases).

### Function-Based Column Transformations

For simple column operations, you can use `ColumnsTransformation.from_func()` to create a transformation from a function:

```python
from pyspark.sql import functions as f
from koheesio.spark.transformations import ColumnsTransformation
from koheesio.spark.utils import SparkDatatype

# Create a transformation that lowercases string columns
LowerCase = ColumnsTransformation.from_func(
    lambda col: f.lower(col),
    run_for_all_data_type=[SparkDatatype.STRING],
    limit_data_type=[SparkDatatype.STRING]
)

# Example data
input_df = spark.createDataFrame([("JOHN", "ENGINEER"), ("JANE", "MANAGER")], ["name", "title"])

# Apply to specific column
output_df = LowerCase(columns=["name"]).transform(input_df)
# Result: name="john", title="ENGINEER" (unchanged)

# Apply to all string columns automatically
output_df = LowerCase().transform(input_df)
# Result: name="john", title="engineer" (both lowercased)

# With a target column (creates new column)
output_df = LowerCase(columns=["name"], target_column="name_lower").transform(input_df)
# Result: name="JOHN" (original), name_lower="john" (new)
```

**Parameterized Column Transformations:**

```python
def add_prefix(col, prefix: str):
    return f.concat(f.lit(prefix), col)

AddPrefix = ColumnsTransformation.from_func(add_prefix)

# Use with parameter
output_df = AddPrefix(columns=["name"], prefix="Mr. ").transform(input_df)
# Result: name="Mr. JOHN"

# Create a specialized version
AddMrPrefix = AddPrefix.partial(prefix="Mr. ")
output_df = AddMrPrefix(columns=["name"]).transform(input_df)
# Same result, but don't need to pass prefix each time
```

**Factory Pattern for Related Transformations:**

```python
def string_transform(func):
    """Factory for creating string column transformations."""
    return ColumnsTransformation.from_func(
        func,
        run_for_all_data_type=[SparkDatatype.STRING],
        limit_data_type=[SparkDatatype.STRING]
    )

# Create multiple related transformations
LowerCase = string_transform(lambda col: f.lower(col))
UpperCase = string_transform(lambda col: f.upper(col))
TitleCase = string_transform(lambda col: f.initcap(col))

# Use them
output_df = (
    input_df
    .transform(LowerCase(columns=["name"], target_column="name_lower"))
    .transform(UpperCase(columns=["name"], target_column="name_upper"))
    .transform(TitleCase(columns=["name"], target_column="name_title"))
)
# Result: original name + name_lower, name_upper, name_title columns
```

**Multi-Column Aggregation (Pass All Columns at Once):**

```python
from pyspark.sql import Column

# Sum multiple columns into one
def sum_quarters(q1: Column, q2: Column, q3: Column, q4: Column) -> Column:
    return q1 + q2 + q3 + q4

SumQuarters = ColumnsTransformation.from_func(sum_quarters)

input_df = spark.createDataFrame([(100, 200, 150, 250)], ["q1", "q2", "q3", "q4"])
output_df = SumQuarters(
    columns=["q1", "q2", "q3", "q4"],
    target_column="yearly_total"
).transform(input_df)
# Result: yearly_total = 700 (all columns passed to function at once)

# Concatenate columns
def concat_names(first: Column, last: Column) -> Column:
    return f.concat(first, f.lit(" "), last)

ConcatNames = ColumnsTransformation.from_func(concat_names)
output_df = ConcatNames(
    columns=["first_name", "last_name"],
    target_column="full_name"
).transform(input_df)
# Result: full_name = "John Doe"

# Weighted sum with parameters
def weighted_sum(q1: Column, q2: Column, q3: Column, q4: Column, weights: list) -> Column:
    return q1*weights[0] + q2*weights[1] + q3*weights[2] + q4*weights[3]

WeightedSum = ColumnsTransformation.from_func(weighted_sum)
output_df = WeightedSum(
    columns=["q1", "q2", "q3", "q4"],
    weights=[0.1, 0.2, 0.3, 0.4],
    target_column="weighted_total"
).transform(input_df)
```

**Paired Parameters (Different Values Per Column):**

```python
# Apply different tax rates to different columns
def add_tax(amount: Column, rate: float = 0.08) -> Column:
    return amount * (1 + rate)

AddTax = ColumnsTransformation.from_func(add_tax)

input_df = spark.createDataFrame([(100.0, 200.0)], ["food_price", "non_food_price"])

# Single rate for all columns (broadcast)
output_df = AddTax(columns=["food_price", "non_food_price"], rate=0.08).transform(input_df)
# Result: Both columns get 8% tax

# Different rate per column (paired parameters)
output_df = AddTax(
    columns=["food_price", "non_food_price"],
    rate=[0.08, 0.13]  # Must match column count (strict=True)
).transform(input_df)
# Result: food_price gets 8% tax (108.0), non_food_price gets 13% tax (226.0)

# Multiple paired parameters
def scale_and_offset(col: Column, scale: float, offset: float) -> Column:
    return col * scale + offset

ScaleOffset = ColumnsTransformation.from_func(scale_and_offset)
output_df = ScaleOffset(
    columns=["temp_c", "pressure_kpa"],
    scale=[1.8, 0.145],      # Celsius to Fahrenheit, kPa to PSI
    offset=[32.0, 0.0]
).transform(input_df)
```

**Important:** List parameters must exactly match the number of columns (like Python's `zip(..., strict=True)`). If the list length doesn't match, a `ValueError` is raised.

```python
# ❌ This will raise ValueError
AddTax(columns=["a", "b", "c"], rate=[0.08, 0.13])
# ValueError: Parameter 'rate' has 2 values but 3 columns provided.

# ✅ Fix: match the count
AddTax(columns=["a", "b", "c"], rate=[0.08, 0.13, 0.13])

# ✅ Or use single value
AddTax(columns=["a", "b", "c"], rate=0.08)
```

**Variadic Support (Any Number of Columns):**

```python
from functools import reduce

# Variadic *args - any number of columns
def sum_columns(*cols: Column) -> Column:
    return reduce(lambda a, b: a + b, cols)

SumColumns = ColumnsTransformation.from_func(sum_columns)

# Works with any number of columns
output_df = SumColumns(columns=["jan", "feb", "mar"], target_column="q1_total").transform(input_df)
output_df = SumColumns(columns=["q1", "q2", "q3", "q4"], target_column="yearly_total").transform(input_df)
```

**Two Methods for Different Use Cases:**

Koheesio provides two methods for creating column transformations:

1. **`from_func()`** - Defaults to column-wise behavior when ambiguous
   - Best for: Simple column operations, string transformations, element-wise math
   - Auto-detects multi-column when type hints indicate multiple Column parameters

2. **`from_multi_column_func()`** - Defaults to multi-column behavior when ambiguous
   - Best for: Aggregations, concatenations, combining multiple columns
   - Auto-detects column-wise when type hints indicate single Column parameter

**Explicit Override:**
```python
# Force column-wise
Transform = ColumnsTransformation.from_func(some_func, for_each=True)

# Force multi-column
Transform = ColumnsTransformation.from_func(some_func, for_each=False)
```

**Pattern Detection:**

| Function Signature | Auto-Detected | from_func() default | from_multi_column_func() default |
|-------------------|---------------|---------------------|----------------------------------|
| `func(col: Column)` | Column-wise | ✅ Column-wise | ✅ Column-wise (overrides) |
| `func(c1: Column, c2: Column)` | Multi-column | ✅ Multi-column (overrides) | ✅ Multi-column |
| `func(*cols: Column)` | Multi-column | ✅ Multi-column | ✅ Multi-column |
| `func(col: Column, rate: float)` | Column-wise | ✅ Column-wise | ✅ Column-wise (overrides) |
| `lambda col: ...` | Ambiguous | ⚠️ Column-wise | ⚠️ Multi-column |
| `lambda a, b: ...` | Ambiguous | ⚠️ Column-wise | ⚠️ Multi-column |

### Subclassing ColumnsTransformation

For more complex column transformations, you create a subclass of the `ColumnsTransformation` class and implement the 
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

### ColumnConfig Parameters

The `ColumnsTransformation` class supports configuration parameters for controlling column selection and validation:

- `run_for_all_data_type`: Automatically select all columns of specified data types when no columns are provided
- `limit_data_type`: Restrict the transformation to specific data types
- `data_type_strict_mode`: When True, raises an error if a column doesn't match `limit_data_type`; when False, shows a warning and skips the column

Data types are specified using the `SparkDatatype` enum.

For function-based transformations, pass these as parameters to `from_func()`:

```python
LowerCase = ColumnsTransformation.from_func(
    lambda col: f.lower(col),
    run_for_all_data_type=[SparkDatatype.STRING],
    limit_data_type=[SparkDatatype.STRING],
    data_type_strict_mode=True
)
```

For subclasses, define them in a nested `ColumnConfig` class:

```python
class LowerCase(ColumnsTransformationWithTarget):
    class ColumnConfig:
        run_for_all_data_type = [SparkDatatype.STRING]
        limit_data_type = [SparkDatatype.STRING]
        data_type_strict_mode = False
    
    def func(self, column):
        return f.lower(column)
```


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

## Why Choose Koheesio Over Raw PySpark?

Using Koheesio `Transformation` classes over raw PySpark provides compelling benefits that become especially apparent in team environments and production pipelines:

### 1. **Reusability & Team Libraries**

**Raw PySpark** requires copying transformation logic across files:

```python
# Repeated everywhere you need to uppercase strings
df = df.withColumn("name", f.upper(f.col("name")))
df = df.withColumn("city", f.upper(f.col("city")))
```

**Koheesio** lets you build reusable transformation libraries:

```python
# Define once
@column_transformation(run_for_all_data_type=[SparkDatatype.STRING])
def to_upper(col: Column) -> Column:
    return f.upper(col)

# Reuse everywhere - applies to ALL string columns automatically
outp1_df = to_upper().transform(input_df)
# While still being able to use it on select columns as well
outp2_df = to_upper(column="foo").transform(input_df)
```

**Value:** Build a library of transformations your entire team can reuse. No more copy-pasting.

### 2. **Automatic Column Type Filtering (ColumnConfig)**

**Raw PySpark** requires manual type checking:

```python
# Manual iteration and type checking
for col_name, dtype in df.dtypes:
    if dtype == 'string':
        df = df.withColumn(col_name, f.trim(f.col(col_name)))
```

**Koheesio** automates type-based operations:

```python
@column_transformation(run_for_all_data_type=[SparkDatatype.STRING])
def trim_strings(col: Column) -> Column:
    return f.trim(col)

trim_strings().transform(df)  # Automatically finds and processes all string columns
```

**Value:** Eliminate boilerplate for type-based column operations.

### 3. **Parameter Validation & Fail-Fast Errors**

**Raw PySpark** allows silent data corruption:

```python
def add_tax(df, columns, rates):
    # Silent truncation if lengths don't match!
    for col, rate in zip(columns, rates):
        df = df.withColumn(col, f.col(col) * (1 + rate))
    return df

# This silently ignores the third column - no error!
add_tax(df, ["food", "non_food", "beverage"], [0.08, 0.13])
```

**Koheesio** enforces strict validation:

```python
@column_transformation
def add_tax(amount: Column, rate: float) -> Column:
    return amount * (1 + rate)

# Clear error with strict zip semantics
AddTax(columns=["food", "non_food", "beverage"], rate=[0.08, 0.13])
# ValueError: Parameter 'rate' has 2 values but 3 columns provided.
# Lists must match column count exactly (like Python's zip(..., strict=True)).
```

**Value:** Fail fast with clear errors instead of silent data corruption.

### 4. **Consistent, Focused Unit Tests**

Both raw PySpark code and Koheesio transformations ultimately operate on Spark `DataFrame`s and `Column`s, so you still
need a Spark session and test data either way. The difference is *how easy it is to isolate and reuse the logic you
want to test*.

```python
# Raw PySpark: logic often lives inside larger pipelines
def complex_pipeline(df):
    df = df.withColumn("x", f.col("a") + f.col("b"))
    df = df.withColumn("y", f.col("x") * 2)
    df = df.filter(f.col("y") > 10)
    return df
```

With Koheesio, you are encouraged to put reusable logic behind a small, well‑named transformation:

```python
@multi_column_transformation
def add_columns(a: Column, b: Column) -> Column:
    return a + b

def test_add_columns(spark):
    df = spark.createDataFrame([(1, 2)], ["a", "b"])
    result = add_columns(columns=["a", "b"], target_column="sum").transform(df)
    assert result.select("sum").first()[0] == 3
```

**Value:** Transformations share a common interface (`.transform(df)`), making it natural to write small, consistent
tests that focus on a single piece of logic, even though Spark is still involved.

### 5. **Declarative Intent & Self-Documenting Code**

**Raw PySpark** requires reading implementation to understand intent:

```python
# What does this do? You have to read the code
def process_data(df):
    return df.withColumn("total", 
        f.col("q1") + f.col("q2") + f.col("q3") + f.col("q4"))
```

**Koheesio** makes intent crystal clear:

```python
@multi_column_transformation
def sum_quarters(q1: Column, q2: Column, q3: Column, q4: Column) -> Column:
    """Sum quarterly sales into annual total."""
    return q1 + q2 + q3 + q4

# Self-documenting - intent is obvious
SumQuarters(
    columns=["sales_q1", "sales_q2", "sales_q3", "sales_q4"],
    target_column="annual_sales"
).transform(df)
```

**Value:** Code documents itself. The decorator and class name tell you what it does.

Optionally, the `name` and `description` fields can be used to further clarify your code:

```python
SumQuarters(
    name="annual_sales_sum",
    description="Sum Q1–Q4 into annual_sales for reporting",
    columns=["sales_q1", "sales_q2", "sales_q3", "sales_q4"],
    target_column="annual_sales",
).transform(df)
```

All Koheesio transformation instances have a `name` field: by default it is derived from the class name (which, for
`.from_func()` and decorator-based transformations, comes from the original function name), and you can override it
per-instance via `name="..."` when needed.

### 6. **Factory Patterns for Related Transformations**

You *can* build DRY helpers in raw PySpark, but they tend to remain ad‑hoc functions:

```python
def string_transform(func):
    def apply(df, columns):
        for col in columns:
            df = df.withColumn(col, func(f.col(col)))
        return df

    return apply

to_lower = string_transform(lambda c: f.lower(c))
to_upper = string_transform(lambda c: f.upper(c))
title_case = string_transform(lambda c: f.initcap(c))
```

With Koheesio, the same idea produces **first-class transformations** that plug into the rest of the framework:

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

df = spark.createDataFrame([(" Alice ", "Smith ")], ["first_name", "last_name"])

# Because these are ColumnsTransformation classes, you get:
# - ColumnConfig behaviour (run_for_all_data_type / limit_data_type)
# - A uniform .transform(df) interface
# - Optional name/description metadata
cleaned = LowerCase(
    name="normalize_names",
    description="Lowercase all string columns",
).transform(df)
```

**Value:** Reusable factories create properly-typed `ColumnsTransformation` classes that participate fully in Koheesio
pipelines (ColumnConfig, validation, metadata, composition) instead of remaining one-off helper functions.

### 7. **Team Consistency & Standardization**

**Raw PySpark** leads to inconsistent styles:

```python
# Engineer A's style
df.withColumn("result", f.col("a") + f.col("b"))

# Engineer B's style  
df.select("*", (f.col("a") + f.col("b")).alias("result"))

# Engineer C's style
def add_cols(a: Column, b: Column) -> Column:
    return a + b
df.withColumn("result", add_cols(f.col("a"), f.col("b")))
```

**Koheesio** enforces consistent patterns:

```python
# Everyone uses the same pattern
@column_transformation
def add_columns(a: Column, b: Column) -> Column:
    return a + b

# Consistent usage across the team
AddColumns(columns=["a", "b"], target_column="result").transform(df)
```

**Value:** Standardized patterns make code reviews easier and onboarding faster.

### 8. **Early Input Validation**

As a `Transformation` is a type of `SparkStep`, which in turn is a `Step` and a type of Pydantic `BaseModel`, all inputs are validated when an instance of a `Transformation` class is created. This early validation helps catch errors related to invalid input, such as an invalid column name, before the PySpark pipeline starts executing. This can help avoid unnecessary computation and make your data pipelines more robust and reliable.

### 9. **Production-Grade Robustness**

Koheesio has been extensively tested with hundreds of unit tests. The `Transformation` classes are validated under a wide range of conditions, making your data pipelines more robust and less likely to fail due to unexpected inputs or edge cases. You don't need to re‑test Koheesio's core behavior in every project (except for your own business logic) so you can iterate faster.

### The Decorator Advantage

With v0.11's decorator-based transformations, **the barrier to entry is now as low as raw PySpark**:

```python
# Before: Had to write a class
class AddTax(ColumnsTransformationWithTarget):
    rate: float = 0.08
    def func(self, col):
        return col * (1 + self.rate)

# After: Just decorate a function
@column_transformation
def add_tax(col: Column, rate: float = 0.08) -> Column:
    return col * (1 + rate)
```

**Key Insight:** Decorators make Koheesio **as easy to write as raw PySpark**, but with enterprise-grade benefits (reusability, type safety, ColumnConfig, testability, team consistency) for free.

### When to Use Raw PySpark

Raw PySpark is better for:

- **One-off scripts** where you'll never reuse the logic
- **Extremely simple operations** (single `withColumn` call)
- **Performance-critical hot paths** where abstraction overhead matters
- **Solo development** where team consistency isn't needed

### Summary

By using Koheesio `Transformation` classes, you can create data pipelines that are:

- **Reusable** - Build libraries, not copy-paste code
- **Type-Safe** - Fail fast with clear errors
- **Testable** - Unit test transformations in isolation
- **Declarative** - Self-documenting code
- **Consistent** - Standardized patterns across teams
- **Robust** - Production-tested and reliable

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
