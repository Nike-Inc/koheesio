# Onboarding to Koheesio

Koheesio is a Python library that simplifies the development of data engineering pipelines. It provides a structured 
way to define and execute data processing tasks, making it easier to build, test, and maintain complex data workflows. 

This guide will walk you through the process of transforming a traditional Spark application into a Koheesio pipeline 
along with explaining the advantages of using Koheesio over raw Spark.

## Traditional Spark Application

First let's create a simple Spark application that you might use to process data.

The following Spark application reads a CSV file, performs a transformation, and writes the result to a Delta table. 
The transformation includes filtering data where age is greater than 18 and performing an aggregation to calculate the 
average salary per country. The result is then written to a Delta table partitioned by country.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder.getOrCreate()

# Read data from CSV file
df = spark.read.csv("input.csv", header=True, inferSchema=True)

# Filter data where age is greater than 18
df = df.filter(col("age") > 18)

# Perform aggregation
df = df.groupBy("country").agg(avg("salary").alias("average_salary"))

# Write data to Delta table with partitioning
df.write.format("delta").partitionBy("country").save("/path/to/delta_table")
```

## Transforming to Koheesio

The same pipeline can be rewritten using Koheesio's `EtlTask`. In this version, each step (reading, transformations, 
writing) is encapsulated in its own class, making the code easier to understand and maintain.  

First, a `CsvReader` is defined to read the input CSV file. Then, a `DeltaTableWriter` is defined to write the result 
to a Delta table partitioned by country. 

Two transformations are defined: 
1. one to filter data where age is greater than 18
2. and, another to calculate the average salary per country. 

These transformations are then passed to an `EtlTask` along with the reader and writer. Finally, the `EtlTask` is 
executed to run the pipeline.

```python
from koheesio.spark.etl_task import EtlTask
from koheesio.spark.readers.file_loader import CsvReader
from koheesio.spark.writers.delta.batch import DeltaTableWriter
from koheesio.spark.transformations.transform import Transform
from pyspark.sql.functions import col, avg

# Define reader
reader = CsvReader(path="input.csv", header=True, inferSchema=True)

# Define writer
writer = DeltaTableWriter(table="delta_table", partition_by=["country"])

# Define transformations
age_transformation = Transform(
    func=lambda df: df.filter(col("age") > 18)
)
avg_salary_per_country = Transform(
    func=lambda df: df.groupBy("country").agg(avg("salary").alias("average_salary"))
)

# Define and execute EtlTask
task = EtlTask(
    source=reader, 
    target=writer, 
    transformations=[
        age_transformation,
        avg_salary_per_country
    ]
)
task.execute()
```
This approach with Koheesio provides several advantages. It makes the code more modular and easier to test. Each step
can be tested independently and reused across different tasks. It also makes the pipeline more readable and easier to
maintain.

## Advantages of Koheesio

Using Koheesio instead of raw Spark has several advantages:

- **Modularity**: Each step in the pipeline (reading, transformation, writing) is encapsulated in its own class, 
    making the code easier to understand and maintain.
- **Reusability**: Steps can be reused across different tasks, reducing code duplication.
- **Testability**: Each step can be tested independently, making it easier to write unit tests.
- **Flexibility**: The behavior of a task can be customized using a `Context` class.
- **Consistency**: Koheesio enforces a consistent structure for data processing tasks, making it easier for new 
    developers to understand the codebase.
- **Error Handling**: Koheesio provides a consistent way to handle errors and exceptions in data processing tasks.
- **Logging**: Koheesio provides a consistent way to log information and errors in data processing tasks.

In contrast, using the plain PySpark API for transformations can lead to more verbose and less structured code, which 
can be harder to understand, maintain, and test. It also doesn't provide the same level of error handling, logging, and
flexibility as the Koheesio Transform class.

## Using a Context Class

Here's a simple example of how to use a `Context` class to customize the behavior of a task.
The Context class in Koheesio is designed to behave like a dictionary, but with added features. 

```python
from koheesio import Context
from koheesio.spark.etl_task import EtlTask
from koheesio.spark.readers.file_loader import CsvReader
from koheesio.spark.writers.delta import DeltaTableWriter
from koheesio.spark.transformations.transform import Transform

context = Context({  # this could be stored in a JSON or YAML
    "age_threshold": 18,
    "reader_options": {
        "path": "input.csv",
        "header": True,
        "inferSchema": True
    },
    "writer_options": {
        "table": "delta_table",
        "partition_by": ["country"]
    }
})

task = EtlTask(
    source = CsvReader(**context.reader_options),
    target = DeltaTableWriter(**context.writer_options),
    transformations = [
        Transform(func=lambda df: df.filter(df["age"] > context.age_threshold))
    ]
)

task.execute()
```

In this example, we're using `CsvReader` to read the input data, `DeltaTableWriter` to write the output data, and a 
`Transform` step to filter the data based on the age threshold. The options for the reader and writer are stored in a
`Context` object, which can be easily updated or loaded from a JSON or YAML file.
