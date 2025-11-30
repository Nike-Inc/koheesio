# Testing Koheesio Steps

Testing is a crucial part of developing robust data processing pipelines. This guide will show you how to effectively test your Koheesio steps and transformations across the layered architecture.

## Testing Strategy by Layer

Koheesio's layered architecture allows for targeted testing:

- **Core Layer**: Test step logic, validation, configuration
- **Pandas Layer**: Test DataFrame transformations in isolation  
- **ML Layer**: Test feature engineering and ML integrations
- **Spark Layer**: Test distributed processing and integrations
- **Integration Layers**: Test external system connections

## Test Environment Setup

### Running Tests by Layer

Koheesio provides test markers to run specific test suites:

```bash
# Test only core functionality (minimal dependencies)
hatch test -i version=core

# Test pandas functionality (no Spark required)
hatch test -i version=pandas  

# Test ML functionality
hatch test -i version=ml

# Test Spark functionality (requires PySpark)
hatch test -i version=pyspark35

# Run all non-Spark tests
pytest -m "not spark"

# Run only pandas tests
pytest -m pandas

# Run ML + pandas tests  
pytest -m "ml or pandas"
```

### Available Test Markers

```python
# Core framework tests
@pytest.mark.core
def test_step_functionality():
    pass

# Pandas-specific tests  
@pytest.mark.pandas
def test_pandas_transformation():
    pass

# ML-specific tests
@pytest.mark.ml  
def test_ml_preprocessing():
    pass

# Spark-specific tests
@pytest.mark.spark
def test_spark_transformation():
    pass

# Integration tests
@pytest.mark.snowflake
@pytest.mark.box
@pytest.mark.sftp
def test_integrations():
    pass
```

## Unit Testing

Unit testing involves testing individual components of the software in isolation. In the context of Koheesio, this means testing individual tasks or steps.

Here's an example of how to unit test a Koheesio task:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from koheesio.spark import DataFrame
from koheesio.spark.etl_task import EtlTask
from koheesio.spark.readers.dummy import DummyReader
from koheesio.spark.writers.dummy import DummyWriter
from koheesio.spark.transformations.transform import Transform


def filter_age(df: DataFrame) -> DataFrame:
    return df.filter(col("Age") > 18)


def test_etl_task():
    # Initialize SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Create a DataFrame for testing
    data = [("John", 19), ("Anna", 20), ("Tom", 18)]
    df = spark.createDataFrame(data, ["Name", "Age"])

    # Define the task
    task = EtlTask(
        source=DummyReader(df=df),
        target=DummyWriter(),
        transformations=[
            Transform(filter_age)
        ]
    )

    # Execute the task
    task.execute()

    # Assert the result
    result_df = task.output.df
    assert result_df.count() == 2
    assert result_df.filter("Name == 'Tom'").count() == 0
```

In this example, we're testing an EtlTask that reads data from a DataFrame, applies a filter transformation, and writes 
the result to another DataFrame. The test asserts that the task correctly filters out rows where the age is less than or
equal to 18.

## Integration Testing

Integration testing involves testing the interactions between different components of the software. In the context of 
Koheesio, this means testing the entirety of data flowing through one or more tasks.

We'll create a simple test for a hypothetical EtlTask that uses DeltaReader and DeltaWriter. We'll use pytest and unittest.mock to mock the responses of the reader and writer.  First, let's assume that you have an EtlTask defined in a module named my_module. This task reads data from a Delta table, applies some transformations, and writes the result to another Delta table.

Here's an example of how to write an integration test for this task:

```python
# my_module.py
from pyspark.sql.functions import col
from koheesio.spark.etl_task import EtlTask
from koheesio.spark.readers.delta import DeltaTableReader
from koheesio.spark.writers.delta import DeltaTableWriter
from koheesio.spark.transformations.transform import Transform
from koheesio.context import Context


def filter_age(df):
    return df.filter(col("Age") > 18)


context = Context({
    "reader_options": {
        "table": "input_table"
    },
    "writer_options": {
        "table": "output_table"
    }
})

task = EtlTask(
    source=DeltaTableReader(**context.reader_options),
    target=DeltaTableWriter(**context.writer_options),
    transformations=[
        Transform(filter_age)
    ]
)
```

Now, let's create a test for this task. We'll use pytest and unittest.mock to mock the responses of the reader and writer. We'll also use a pytest fixture to create a test context and a test DataFrame.

```python
# test_my_module.py
import pytest
from unittest.mock import patch
from pyspark.sql import SparkSession
from koheesio.context import Context
from koheesio.spark.readers import Reader
from koheesio.spark.writers import Writer

from my_module import task

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.getOrCreate()

@pytest.fixture(scope="module")
def test_context():
    return Context({
        "reader_options": {
            "table": "test_input_table"
        },
        "writer_options": {
            "table": "test_output_table"
        }
    })

@pytest.fixture(scope="module")
def test_df(spark):
    data = [("John", 19), ("Anna", 20), ("Tom", 18)]
    return spark.createDataFrame(data, ["Name", "Age"])

def test_etl_task(spark, test_context, test_df):
    # Mock the read method of the Reader class
    with patch.object(Reader, "read", return_value=test_df):
        # Mock the write method of the Writer class
        with patch.object(Writer, "write") as mock_write:
            # Execute the task
            task.execute()

            # Assert the result
            result_df = task.output.df
            assert result_df.count() == 2
            assert result_df.filter("Name == 'Tom'").count() == 0

            # Assert that the reader and writer were called with the correct arguments
            Reader.read.assert_called_once_with(**test_context.reader_options)
            mock_write.assert_called_once_with(**test_context.writer_options)
```

In this test, we're mocking the DeltaReader and DeltaWriter to return a test DataFrame and check that they're called 
with the correct arguments. We're also asserting that the task correctly filters out rows where the age is less than 
or equal to 18.