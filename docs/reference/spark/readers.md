# Reader Module

The `Reader` module in Koheesio provides a set of classes for reading data from various sources. A `Reader` is a type 
of `SparkStep` that reads data from a source based on the input parameters and stores the result in `self.output.df` 
for subsequent steps.

## What is a Reader?

A `Reader` is a subclass of `SparkStep` that reads data from a source and stores the result. The source could be a 
file, a database, a web API, or any other data source. The result is stored in a DataFrame, which is accessible through 
the `df` property of the `Reader`.

## API Reference

See [API Reference](../../api_reference/spark/readers/index.md) for a detailed description of the `Reader` class and its methods.

## Key Features of a Reader

1. **Read Method**: The `Reader` class provides a `read` method that calls the `execute` method and returns the result. 
  Essentially, calling `.read()` is a shorthand for calling `.execute().output.df`. This allows you to read data from 
  a `Reader` without having to call the `execute` method directly. This is a convenience method that simplifies the 
  usage of a `Reader`.

  Here's an example of how to use the `.read()` method:

  ```python
  # Instantiate the Reader
  my_reader = MyReader()

  # Use the .read() method to get the data as a DataFrame
  df = my_reader.read()

  # Now df is a DataFrame with the data read by MyReader
  ```

  In this example, `MyReader` is a subclass of `Reader` that you've defined. After creating an instance of `MyReader`, 
  you call the `.read()` method to read the data and get it back as a DataFrame. The DataFrame `df` now contains the 
  data read by `MyReader`.

2. **DataFrame Property**: The `Reader` class provides a `df` property as a shorthand for accessing `self.output.df`. 
  If `self.output.df` is `None`, the `execute` method is run first. This property ensures that the data is loaded and 
  ready to be used, even if the `execute` method hasn't been explicitly called.

  Here's an example of how to use the `df` property:

  ```python
  # Instantiate the Reader
  my_reader = MyReader()

  # Use the df property to get the data as a DataFrame
  df = my_reader.df

  # Now df is a DataFrame with the data read by MyReader
  ```

  In this example, `MyReader` is a subclass of `Reader` that you've defined. After creating an instance of `MyReader`, 
  you access the `df` property to get the data as a DataFrame. The DataFrame `df` now contains the data read by 
  `MyReader`.

3. **SparkSession**: Every `Reader` has a `SparkSession` available as `self.spark`. This is the currently active 
  `SparkSession`, which can be used to perform distributed data processing tasks.

  Here's an example of how to use the `spark` property:

  ```python
  # Instantiate the Reader
  my_reader = MyReader()

  # Use the spark property to get the SparkSession
  spark = my_reader.spark

  # Now spark is the SparkSession associated with MyReader
  ```

  In this example, `MyReader` is a subclass of `Reader` that you've defined. After creating an instance of `MyReader`, 
  you access the `spark` property to get the `SparkSession`. The `SparkSession` `spark` can now be used to perform 
  distributed data processing tasks.

## How to Define a Reader?

To define a `Reader`, you create a subclass of the `Reader` class and implement the `execute` method. The `execute` 
method should read from the source and store the result in `self.output.df`. This is an abstract method, which means it
must be implemented in any subclass of `Reader`.

Here's an example of a `Reader`:

```python
class MyReader(Reader):
  def execute(self):
    # read data from source
    data = read_from_source()
    # store result in self.output.df
    self.output.df = data
```

## Understanding Inheritance in Readers

Just like a `Step`, a `Reader` is defined as a subclass that inherits from the base `Reader` class. This means it 
inherits all the properties and methods from the `Reader` class and can add or override them as needed. The main method
that needs to be overridden is the `execute` method, which should implement the logic for reading data from the source
and storing it in `self.output.df`.

## Benefits of Using Readers in Data Pipelines

Using `Reader` classes in your data pipelines has several benefits:

1. **Simplicity**: Readers abstract away the details of reading data from various sources, allowing you to focus on the
  logic of your pipeline.

2. **Consistency**: By using Readers, you ensure that data is read in a consistent manner across different parts of your
  pipeline.

3. **Flexibility**: Readers can be easily swapped out for different data sources without changing the rest of your
  pipeline.

4. **Efficiency**: Readers automatically manage resources like connections and file handles, ensuring efficient use of
  resources.

By using the concept of a `Reader`, you can create data pipelines that are simple, consistent, flexible, and efficient.


## Examples of Reader Classes in Koheesio

Koheesio provides a variety of `Reader` subclasses for reading data from different sources. Here are just a few
examples:

1. **Teradata Reader**: A `Reader` subclass for reading data from Teradata databases. 
  It's defined in the [`koheesio/steps/readers/teradata.py`]("koheesio/steps/readers/teradata.py") file.

2. **Snowflake Reader**: A `Reader` subclass for reading data from Snowflake databases. 
  It's defined in the [`koheesio/steps/readers/snowflake.py`]("koheesio/steps/readers/snowflake.py") file.

3. **Box Reader**: A `Reader` subclass for reading data from Box.
  It's defined in the [`koheesio/steps/integrations/box.py`]("koheesio/steps/integrations/box.py") file.

These are just a few examples of the many `Reader` subclasses available in Koheesio. Each `Reader` subclass is designed
to read data from a specific source. They all inherit from the base `Reader` class and implement the `execute` method to
read data from their respective sources and store it in `self.output.df`.

Please note that this is not an exhaustive list. Koheesio provides many more `Reader` subclasses for a wide range of
data sources. For a complete list, please refer to the Koheesio documentation or the source code.

More readers can be found in the `koheesio/steps/readers` module.
