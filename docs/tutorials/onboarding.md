# Onboarding to Koheesio

Koheesio is a Python library that simplifies the development of data engineering pipelines. It provides a structured 
way to define and execute data processing tasks, making it easier to build, test, and maintain complex data workflows. 

This guide will walk you through the process of transforming a traditional Spark application into a Koheesio pipeline.

## Traditional Spark Application

Let's start with a simple Spark application that reads a CSV file, performs a transformation, and writes the result to 
another CSV file.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv("input.csv", header=True, inferSchema=True)
df = df.filter(df["age"] > 18)
df.write.csv("output.csv")
```

## Transforming to Koheesio

Now, let's rewrite this pipeline using Koheesio's `EtlTask`.

```python
from koheesio import EtlTask, CsvReader, CsvWriter, Filter

reader = CsvReader(path="input.csv", header=True, inferSchema=True)
writer = CsvWriter(path="output.csv)

task = EtlTask(
    source = reader,
    target = writer,
    transformations = [
        Filter("age > 18")
    ]
)

task.execute()
```

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

## Using a Context Class

Here's a simple example of how to use a `Context` class to customize the behavior of a task.
The Context class in Koheesio is designed to behave like a dictionary, but with added features. 

```python
from koheesio.tasks import EtlTask
from koheesio.steps.readers import CsvReader
from koheesio.steps.writers import CsvWriter
from koheesio.context import Context
from koheesio.steps.transformations import Filter

context = Context({  # this could be stored in a JSON or YAML
    "age_threshold": 18,
    "reader_options": {
        "path": "input.csv",
        "header": True,
        "inferSchema": True
    },
    "writer_options": {
        "path": "output.csv"
    }
})

task = EtlTask(
    source = CsvReader(**context.reader_options),
    target = CsvWriter(**context.writer_options),
    transformations = [
        Filter(expr=f"age > {context.age_threshold}")
    ]
)

task.execute()
```

In this example, the `age_threshold` is passed to the `Filter` class through the `context` attribute, allowing the 
filter condition to be customized at runtime. `context` is also used to hold the options that are provided to the 
Reader and Writer classes.
