# Testing Guide

This guide explains how to run and write tests for Koheesio's layered architecture.

## Architecture & Test Strategy

Koheesio uses a layered architecture that enables targeted testing:

```
Core Layer (koheesio)
├── Pandas Layer (koheesio[pandas])  
├── ML Layer (koheesio[ml])
└── Spark Layer (koheesio[pyspark])
    └── Integration Layers (snowflake, box, etc.)
```

Each layer can be tested independently, allowing for faster feedback and better isolation.

## Running Tests

### By Layer

```bash
# Core layer only (minimal dependencies)
hatch test -i version=core

# Pandas layer (independent of Spark)  
hatch test -i version=pandas

# ML layer (numpy, scikit-learn, scipy)
hatch test -i version=ml

# Spark layer (full PySpark functionality)
hatch test -i version=pyspark35  # or pyspark34, pyspark352, etc.
```

### By Test Markers

```bash
# Run only core tests
pytest -m core

# Run pandas and ML tests together
pytest -m "pandas or ml"

# Run everything except Spark tests
pytest -m "not spark"

# Run integration tests
pytest -m "snowflake or box or sftp"

# Run all non-integration tests
pytest -m "not (snowflake or box or sftp)"
```

### Development Testing

```bash
# Run all tests (requires all dependencies)
hatch test

# Run tests with coverage
hatch test --cov

# Run specific test file
hatch test tests/pandas/test_transformations.py

# Run with verbose output
hatch test --verbose

# Run in parallel
hatch test -n auto
```

## Test Markers Reference

### Core Markers
- `@pytest.mark.core` - Core framework functionality
- `@pytest.mark.slow` - Tests that take longer to run
- `@pytest.mark.integration` - Integration tests requiring external systems

### Layer Markers  
- `@pytest.mark.pandas` - Pandas DataFrame operations
- `@pytest.mark.ml` - Machine learning functionality
- `@pytest.mark.spark` - Apache Spark functionality

### Integration Markers
- `@pytest.mark.snowflake` - Snowflake data warehouse tests
- `@pytest.mark.box` - Box cloud storage tests  
- `@pytest.mark.sftp` - SFTP file transfer tests
- `@pytest.mark.tableau` - Tableau integration tests

## Writing Tests

### Core Layer Tests

```python
import pytest
from koheesio import Step

@pytest.mark.core
class TestStep:
    def test_step_execution(self):
        class MyStep(Step):
            def execute(self):
                self.output.result = "success"
        
        step = MyStep()
        step.execute()
        assert step.output.result == "success"
    
    def test_step_validation(self):
        with pytest.raises(ValidationError):
            Step(invalid_param="invalid")
```

### Pandas Layer Tests

```python
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
from koheesio.pandas.transformations import Transformation

@pytest.mark.pandas
class TestPandasTransformation:
    def test_transformation_logic(self):
        class AddColumn(Transformation):
            def execute(self):
                self.output.df = self.df.copy()
                self.output.df['new_col'] = 'test'
        
        input_df = pd.DataFrame({'col1': [1, 2, 3]})
        expected_df = pd.DataFrame({'col1': [1, 2, 3], 'new_col': ['test', 'test', 'test']})
        
        result = AddColumn().transform(input_df)
        assert_frame_equal(result, expected_df)
    
    def test_immutability(self):
        """Test that input DataFrame is not modified."""
        input_df = pd.DataFrame({'col1': [1, 2, 3]})
        original_df = input_df.copy()
        
        class ModifyDF(Transformation):
            def execute(self):
                self.output.df = self.df.copy()
                self.output.df['new_col'] = 'added'
        
        ModifyDF().transform(input_df)
        assert_frame_equal(input_df, original_df)
```

### ML Layer Tests

```python
import pytest
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

@pytest.mark.ml
class TestMLIntegration:
    def test_feature_engineering(self):
        from koheesio.pandas.transformations import Transformation
        
        class CreateFeatures(Transformation):
            def execute(self):
                df = self.df.copy()
                df['feature_squared'] = df['feature'] ** 2
                df['feature_log'] = np.log(df['feature'] + 1)
                self.output.df = df
        
        input_df = pd.DataFrame({'feature': [1, 2, 3, 4, 5]})
        result = CreateFeatures().transform(input_df)
        
        assert 'feature_squared' in result.columns
        assert 'feature_log' in result.columns
        assert result['feature_squared'].tolist() == [1, 4, 9, 16, 25]
    
    def test_sklearn_integration(self):
        """Test integration with scikit-learn models."""
        X = np.random.rand(100, 4)
        y = np.random.randint(0, 2, 100)
        
        model = RandomForestClassifier(n_estimators=10, random_state=42)
        model.fit(X, y)
        
        predictions = model.predict(X[:5])
        assert len(predictions) == 5
        assert all(pred in [0, 1] for pred in predictions)
```

### Spark Layer Tests

```python
import pytest
from koheesio.spark.transformations import Transformation

@pytest.mark.spark  
class TestSparkTransformation:
    def test_spark_transformation(self, spark_session):
        class AddSparkColumn(Transformation):
            def execute(self):
                from pyspark.sql.functions import lit
                self.output.df = self.df.withColumn("new_col", lit("test"))
        
        # Create test DataFrame
        input_df = spark_session.createDataFrame([(1,), (2,), (3,)], ["col1"])
        
        result = AddSparkColumn().transform(input_df)
        
        assert "new_col" in result.columns
        assert result.collect()[0]["new_col"] == "test"
    
    def test_spark_sql(self, spark_session):
        """Test SQL transformations."""
        input_df = spark_session.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
        input_df.createOrReplaceTempView("test_table")
        
        result = spark_session.sql("SELECT id, value, id * 2 as doubled FROM test_table")
        
        assert result.count() == 2
        assert "doubled" in result.columns
```

### Integration Tests

```python
import pytest
from unittest.mock import Mock, patch

@pytest.mark.snowflake
class TestSnowflakeIntegration:
    @patch('koheesio.integrations.snowflake.snowflake_connector')
    def test_snowflake_connection(self, mock_connector):
        """Test Snowflake integration (mocked)."""
        mock_connector.connect.return_value = Mock()
        
        from koheesio.integrations.snowflake import SnowflakeReader
        
        reader = SnowflakeReader(
            url="test.snowflakecomputing.com",
            user="test_user",
            password="test_password",
            query="SELECT * FROM test_table"
        )
        
        # Test that connection is attempted
        mock_connector.connect.assert_called_once()

@pytest.mark.box
class TestBoxIntegration:
    def test_box_client_creation(self):
        """Test Box client creation."""
        # Implementation depends on Box integration
        pass
```

## Test Configuration

### Pytest Configuration

```ini
# pytest.ini or pyproject.toml [tool.pytest.ini_options]
markers =
    core: Core framework tests
    pandas: Pandas DataFrame tests  
    ml: Machine learning tests
    spark: Apache Spark tests
    snowflake: Snowflake integration tests
    box: Box cloud storage tests
    sftp: SFTP file transfer tests
    tableau: Tableau integration tests
    slow: Tests that take longer to run
    integration: Tests requiring external systems

testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*

# Filter warnings
filterwarnings =
    ignore::DeprecationWarning
    ignore::PendingDeprecationWarning
```

### Hatch Test Matrix

The test matrix is configured in `pyproject.toml`:

```toml
[tool.hatch.envs.hatch-test]
matrix-name-format = "{variable}_{value}"

[[tool.hatch.envs.hatch-test.matrix]]
python = ["3.9", "3.10", "3.11", "3.12"]
version = ["core", "pandas", "ml", "pyspark35"]
```

## Continuous Integration

### GitHub Actions

Tests run automatically on:
- **Layered Tests**: Core, pandas, and ML layers on key Python versions
- **Spark Tests**: Full matrix of Python versions and PySpark versions  
- **Integration Tests**: External system integrations (when secrets available)

### Local CI Simulation

```bash
# Simulate CI locally
hatch test --python=3.10 -i version=core
hatch test --python=3.10 -i version=pandas  
hatch test --python=3.10 -i version=ml
hatch test --python=3.10 -i version=pyspark35
```

## Best Practices

### Test Organization
- Group tests by layer using markers
- Keep integration tests separate from unit tests
- Use descriptive test names that explain the scenario
- Follow the AAA pattern (Arrange, Act, Assert)

### Data Testing
- Use small, representative test datasets
- Test edge cases (empty DataFrames, single rows, etc.)
- Verify immutability (input data unchanged)
- Test error conditions and validation

### Performance Testing
- Mark slow tests with `@pytest.mark.slow`
- Use appropriate test data sizes
- Consider memory usage in pandas tests
- Profile critical paths when needed

### Isolation Testing
- Mock external dependencies
- Use temporary files/directories  
- Clean up resources in teardown
- Avoid test interdependencies

## Troubleshooting

### Common Issues

**Import Errors in Core Tests**:
```bash
# Make sure you're using the right environment
hatch test -i version=core
# Not: hatch test (which includes all dependencies)
```

**Pandas Version Conflicts**:
```python
# Check pandas compatibility
from koheesio.utils.pandas import import_pandas_with_version_check
pandas = import_pandas_with_version_check()
```

**Spark Session Issues**:
```python
# Use the provided fixture
@pytest.mark.spark
def test_spark_feature(spark_session):
    # spark_session is automatically configured
    pass
```

**Marker Not Running**:
```bash
# List available markers
pytest --markers

# Run specific marker with verbose output
pytest -m pandas -v
```

For more specific testing scenarios, see the [API Reference](../api_reference/testing/index.md).

