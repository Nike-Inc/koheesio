# Simple Examples

## Bring your own SparkSession

The Koheesio Spark module does not set up a SparkSession for you. You need to create a SparkSession before using 
Koheesio spark classes. This is the entry point for any Spark functionality, allowing the step to interact with the 
Spark cluster.

- Every `SparkStep` has a `spark` attribute, which is the active SparkSession.
- Koheesio supports both local and remote (connect) Spark Sessions
- The SparkSession you created can be explicitly passed to the `SparkStep` constructor (this is optional)

To create a simple SparkSession, you can use the following code:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
```

## Creating a Custom Step

This example demonstrates how to use the `SparkStep` class from the `koheesio` library to create a custom step named 
`HelloWorldStep`.

### Code

```python
from koheesio.spark import SparkStep

class HelloWorldStep(SparkStep):
    message: str

    def execute(self) -> SparkStep.Output:
        # create a DataFrame with a single row containing the message
        self.output.df = self.spark.createDataFrame([(1, self.message)], ["id", "message"])
```

### Usage

```python
hello_world_step = HelloWorldStep(message="Hello, World!", spark=spark)  # optionally pass the spark session
hello_world_step.execute()

hello_world_step.output.df.show()
```

### Understanding the Code

The `HelloWorldStep` class is a `SparkStep` in Koheesio, designed to generate a DataFrame with a single row containing a custom message. Here's a more detailed overview:

- `HelloWorldStep` inherits from `SparkStep`, a fundamental building block in Koheesio for creating data processing steps with Apache Spark.
- It has a `message` attribute. When creating an instance of `HelloWorldStep`, you can pass a custom message that will be used in the DataFrame.
- `SparkStep` also includes an `Output` class, used to store the output of the step. In this case, `Output` has a `df` attribute to store the output DataFrame.
- The `execute` method creates a DataFrame with the custom message and stores it in `output.df`. It doesn't return a value explicitly; instead, the output DataFrame can be accessed via `output.df`.
- Koheesio uses pydantic for automatic validation of the step's input and output, ensuring they are correctly defined and of the correct types.
- The `spark` attribute can be optionally passed to the constructor when creating an instance of `HelloWorldStep`. This allows you to use an existing SparkSession or create a new one specifically for the step.
- If no `SparkSession` is passed to a `SparkStep`, Koheesio will use the `SparkSession.getActiveSession()` method to attempt retrieving an active SparkSession. If no active session is found, your code will not work.

Note: Pydantic is a data validation library that provides a way to validate that the data (in this case, the input and output of the step) conforms to the expected format.


## Creating a Custom Task

This example demonstrates how to use the `EtlTask` from the `koheesio` library to create a custom task named `MyFavoriteMovieTask`.

### Code

```python
from typing import Any
from pyspark.sql import functions as f
from koheesio.spark import DataFrame
from koheesio.spark.transformations.transform import Transform
from koheesio.spark.etl_task import EtlTask


def add_column(df: DataFrame, target_column: str, value: Any):
    return df.withColumn(target_column, f.lit(value))


class MyFavoriteMovieTask(EtlTask):
    my_favorite_movie: str

    def transform(self, df: Optional[DataFrame] = None) -> DataFrame:
        df = df or self.extract()

        # pre-transformations specific to this class
        pre_transformations = [
            Transform(add_column, target_column="myFavoriteMovie", value=self.my_favorite_movie)
        ]

        # execute transformations one by one
        for t in pre_transformations:
            df = t.transform(df)

        self.output.transform_df = df
        return df
```

### Configuration

Here is the `sample.yaml` configuration file used in this example:

```yaml
raw_layer:
  catalog: development
  schema: my_favorite_team
  table: some_random_table
movies:
  favorite: Office Space
hash_settings:
  source_columns:
  - id
  - foo
  target_column: hash_uuid5
source:
  range: 4
```

### Usage

```python
from pyspark.sql import SparkSession
from koheesio.context import Context
from koheesio.spark.readers.dummy import DummyReader
from koheesio.spark.writers.dummy import DummyWriter

context = Context.from_yaml("sample.yaml")

SparkSession.builder.getOrCreate()

my_fav_mov_task = MyFavoriteMovieTask(
    source=DummyReader(**context.raw_layer),
    target=DummyWriter(truncate=False),
    my_favorite_movie=context.movies.favorite,
)
my_fav_mov_task.execute()
```

### Understanding the Code

This example creates a `MyFavoriteMovieTask` that adds a column named `myFavoriteMovie` to the DataFrame. The value for this column is provided when the task is instantiated.

The `MyFavoriteMovieTask` class is a custom task that extends the `EtlTask` from the `koheesio` library. It demonstrates how to add a custom transformation to a DataFrame. Here's a detailed breakdown:

- `MyFavoriteMovieTask` inherits from `EtlTask`, a base class in Koheesio for creating Extract-Transform-Load (ETL) tasks with Apache Spark.

- It has a `my_favorite_movie` attribute. When creating an instance of `MyFavoriteMovieTask`, you can pass a custom movie title that will be used in the DataFrame.

- The `transform` method is where the main logic of the task is implemented. It first extracts the data (if not already provided), then applies a series of transformations to the DataFrame.

- In this case, the transformation is adding a new column to the DataFrame named `myFavoriteMovie`, with the value set to the `my_favorite_movie` attribute. This is done using the `add_column` function and the `Transform` class from Koheesio.

- The transformed DataFrame is then stored in `self.output.transform_df`.

- The `sample.yaml` configuration file is used to provide the context for the task, including the source data and the favorite movie title.

- In the usage example, an instance of `MyFavoriteMovieTask` is created with a `DummyReader` as the source, a `DummyWriter` as the target, and the favorite movie title from the context. The task is then executed, which runs the transformations and stores the result in `self.output.transform_df`.

