# Getting Started with Koheesio

## Requirements

- [Python 3.9+](https://docs.python.org/3/whatsnew/3.9.html)

## Installation

<details>
    <summary>hatch / hatchling</summary>

    If you're using hatch (or hatchling), simply add `koheesio` to the `dependencies` or section in your 
    `pyproject.toml` file:
    
    ```toml title="pyproject.toml"
    dependencies = [
        "koheesio",
    ]
    ```
</details>

<details>
    <summary>pip</summary>
    
    If you're using pip, run the following command to install Koheesio:
    
    Requires [pip](https://pip.pypa.io/en/stable/).
    
    ```bash
    pip install koheesio
    ```
</details>

## Basic Usage

Once you've installed Koheesio, you can start using it in your Python scripts. Here's a basic example:

```python title="my_first_step.py"
from koheesio import Step

# Define a step
class MyStep(Step):
    def execute(self):
        # Your step logic here

# Create an instance of the step
step = MyStep()

# Run the step
step.execute()
```

## Advanced Usage

```python title="my_first_etl.py"
from pyspark.sql.functions import lit
from pyspark.sql import DataFrame, SparkSession

# Step 1: import Koheesio dependencies
from koheesio.context import Context
from koheesio.spark.readers.dummy import DummyReader
from koheesio.spark.transformations.camel_to_snake import CamelToSnakeTransformation
from koheesio.spark.writers.dummy import DummyWriter
from koheesio.spark.etl_task import EtlTask

# Step 2: Set up a SparkSession
spark = SparkSession.builder.getOrCreate()

# Step 3: Configure your Context
context = Context({
    "source": DummyReader(),
    "transformations": [CamelToSnakeTransformation()],
    "target": DummyWriter(),
    "my_favorite_movie": "inception",
})

# Step 4: Create a Task
class MyFavoriteMovieTask(EtlTask):
    my_favorite_movie: str

    def transform(self, df: DataFrame = None) -> DataFrame:
        df = df.withColumn("MyFavoriteMovie", lit(self.my_favorite_movie))
        return super().transform(df)

# Step 5: Run your Task
task = MyFavoriteMovieTask(**context)
task.run()
```

### Contributing
If you want to contribute to Koheesio, check out the `CONTRIBUTING.md` file in this repository. It contains guidelines
for contributing, including how to submit issues and pull requests.

### Testing
To run the tests for Koheesio, use the following command:

```bash
make dev-test
```

This will run all the tests in the `tests` directory.
