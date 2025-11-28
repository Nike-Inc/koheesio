# Pandas Module

The `koheesio.pandas` module provides pandas DataFrame operations that work independently of Spark, making them perfect for data science workflows, Jupyter notebooks, and environments where Spark is not available or needed.

## Key Features

- **Spark Independent**: Works without PySpark installation
- **DBR Compatible**: Runs in Databricks notebooks without Spark cluster  
- **Consistent API**: Same patterns as Spark transformations
- **ML Integration**: Seamless integration with scikit-learn, numpy, scipy

## Installation

```bash
# Pandas layer only
pip install "koheesio[pandas]"

# Pandas + ML capabilities  
pip install "koheesio[pandas,ml]"
```

## Modules

- [Transformations](transformations.md) - DataFrame transformation classes
- [Readers](readers.md) - Data input operations
- [Utils](utils.md) - Pandas utility functions

## Quick Start

```python
from koheesio.pandas.transformations import Transformation
import pandas as pd

class AddCalculatedColumn(Transformation):
    multiplier: int = 2
    
    def execute(self):
        self.output.df = self.df.copy()
        self.output.df['calculated'] = self.df['value'] * self.multiplier

# Usage
df = pd.DataFrame({'value': [1, 2, 3, 4, 5]})
result = AddCalculatedColumn(multiplier=3).transform(df)
```

## Architecture

The pandas module follows the same architectural patterns as the Spark module:

```python
from koheesio.pandas import PandasStep
from koheesio.pandas.transformations import Transformation

# Base class for pandas operations
class MyPandasStep(PandasStep):
    def execute(self):
        # Your pandas logic here
        pass

# DataFrame transformations
class MyTransformation(Transformation):
    def execute(self):
        # Transform self.df and assign to self.output.df
        self.output.df = self.df.pipe(some_operation)
```

## Testing

Tests for pandas functionality are marked with `@pytest.mark.pandas`:

```python
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

@pytest.mark.pandas
def test_pandas_transformation():
    # Test pandas transformations in isolation
    pass
```

Run pandas tests independently:

```bash
# Run only pandas tests (no Spark required)
hatch test -i version=pandas

# Or using pytest markers
pytest -m pandas
```

## Databricks Integration

Pandas transformations work perfectly in Databricks notebooks without requiring a Spark cluster:

```python
# In Databricks notebook (no cluster needed)
from koheesio.pandas.transformations import Transformation

# Read data using pandas
df = pd.read_csv("/dbfs/path/to/data.csv")

# Apply transformations
result = MyTransformation().transform(df)

# Save result  
result.to_parquet("/dbfs/path/to/output.parquet")
```

This is particularly useful for:
- Data exploration and analysis
- ML model development and training
- Feature engineering workflows
- Preprocessing pipelines that don't require distributed computing

