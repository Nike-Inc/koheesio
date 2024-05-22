# Writer Module

The `Writer` module in Koheesio provides a set of classes for writing data to various destinations. A `Writer` is a
type of `SparkStep` that takes data from `self.input.df` and writes it to a destination based on the output parameters.

## What is a Writer?

A `Writer` is a subclass of `SparkStep` that writes data to a destination. The data to be written is taken from a
DataFrame, which is accessible through the `df` property of the `Writer`.

## How to Define a Writer?

To define a `Writer`, you create a subclass of the `Writer` class and implement the `execute` method. The `execute`
method should take data from `self.input.df` and write it to the destination.

Here's an example of a `Writer`:

```python
class MyWriter(Writer):
    def execute(self):
        # get data from self.input.df
        data = self.input.df
        # write data to destination
        write_to_destination(data)
```

## Key Features of a Writer

1. **Write Method**: The `Writer` class provides a `write` method that calls the `execute` method and writes the data
    to the destination. Essentially, calling `.write()` is a shorthand for calling `.execute().output.df`. This allows
    you to write data to a `Writer` without having to call the `execute` method directly. This is a convenience method
    that simplifies the usage of a `Writer`.

    Here's an example of how to use the `.write()` method:

    ```python
    # Instantiate the Writer
    my_writer = MyWriter()

    # Use the .write() method to write the data
    my_writer.write()

    # The data from MyWriter's DataFrame is now written to the destination
    ```

    In this example, `MyWriter` is a subclass of `Writer` that you've defined. After creating an instance of `MyWriter`,
    you call the `.write()` method to write the data to the destination. The data from `MyWriter`'s DataFrame is now
    written to the destination.

2. **DataFrame Property**: The `Writer` class provides a `df` property as a shorthand for accessing `self.input.df`.
    This property ensures that the data is ready to be written, even if the `execute` method hasn't been explicitly
    called.

    Here's an example of how to use the `df` property:

    ```python
    # Instantiate the Writer
    my_writer = MyWriter()

    # Use the df property to get the data as a DataFrame
    df = my_writer.df

    # Now df is a DataFrame with the data that will be written by MyWriter
    ```

    In this example, `MyWriter` is a subclass of `Writer` that you've defined. After creating an instance of `MyWriter`,
    you access the `df` property to get the data as a DataFrame. The DataFrame `df` now contains the data that will be
    written by `MyWriter`.

3. **SparkSession**: Every `Writer` has a `SparkSession` available as `self.spark`. This is the currently active
    `SparkSession`, which can be used to perform distributed data processing tasks.

    Here's an example of how to use the `spark` property:

    ```python
    # Instantiate the Writer
    my_writer = MyWriter()

    # Use the spark property to get the SparkSession
    spark = my_writer.spark

    # Now spark is the SparkSession associated with MyWriter
    ```

    In this example, `MyWriter` is a subclass of `Writer` that you've defined. After creating an instance of `MyWriter`,
    you access the `spark` property to get the `SparkSession`. The `SparkSession` `spark` can now be used to perform
    distributed data processing tasks.

## Understanding Inheritance in Writers

Just like a `Step`, a `Writer` is defined as a subclass that inherits from the base `Writer` class. This means it
inherits all the properties and methods from the `Writer` class and can add or override them as needed. The main method
that needs to be overridden is the `execute` method, which should implement the logic for writing data from
`self.input.df` to the destination.

## Examples of Writer Classes in Koheesio

Koheesio provides a variety of `Writer` subclasses for writing data to different destinations. Here are just a few
examples:

- `BoxFileWriter`
- `DeltaTableStreamWriter`
- `DeltaTableWriter`
- `DummyWriter`
- `ForEachBatchStreamWriter`
- `KafkaWriter`
- `SnowflakeWriter`
- `StreamWriter`

Please note that this is not an exhaustive list. Koheesio provides many more `Writer` subclasses for a wide range of
data destinations. For a complete list, please refer to the Koheesio documentation or the source code.

## Benefits of Using Writers in Data Pipelines

Using `Writer` classes in your data pipelines has several benefits:

1. **Simplicity**: Writers abstract away the details of writing data to various destinations, allowing you to focus on
    the logic of your pipeline.
2. **Consistency**: By using Writers, you ensure that data is written in a consistent manner across different parts of
    your pipeline.
3. **Flexibility**: Writers can be easily swapped out for different data destinations without changing the rest of your
    pipeline.
4. **Efficiency**: Writers automatically manage resources like connections and file handles, ensuring efficient use of
    resources.
5. **Early Input Validation**: As a `Writer` is a type of `SparkStep`, which in turn is a `Step` and a type of Pydantic
    `BaseModel`, all inputs are validated when an instance of a `Writer` class is created. This early validation helps
    catch errors related to invalid input, such as an invalid URL for a database, before the PySpark pipeline starts
    executing. This can help avoid unnecessary computation and make your data pipelines more robust and reliable.

By using the concept of a `Writer`, you can create data pipelines that are simple, consistent, flexible, efficient, and
reliable.
