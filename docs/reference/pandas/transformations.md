# Pandas Transformations

The `koheesio.pandas` module provides pandas DataFrame transformations that work independently of Spark, making them perfect for data science workflows, Jupyter notebooks, and environments where Spark is not available or needed.

## Architecture

Pandas transformations follow the same patterns as Spark transformations but operate on pandas DataFrames:

```python
from koheesio.pandas.transformations import Transformation
from koheesio.pandas import pandas as pd

class MyTransformation(Transformation):
    def execute(self):
        # Transform self.df (input pandas DataFrame)  
        self.output.df = self.df.copy()
        # Apply your transformation logic here
```

## Key Features

- **Spark Independent**: Works without PySpark installation
- **DBR Compatible**: Runs in Databricks notebooks without Spark cluster
- **Consistent API**: Same patterns as Spark transformations
- **Functional Style**: Can be used with pandas `.pipe()` method

## Basic Usage

### Simple Transformation

```python
from koheesio.pandas.transformations import Transformation
from koheesio.pandas import pandas as pd

class AddCalculatedColumn(Transformation):
    multiplier: int = 2
    target_column: str = "calculated"
    
    def execute(self):
        self.output.df = self.df.copy()
        self.output.df[self.target_column] = self.df["value"] * self.multiplier

# Usage
df = pd.DataFrame({"value": [1, 2, 3, 4, 5]})
result = AddCalculatedColumn(multiplier=3, target_column="tripled").transform(df)
print(result)
#    value  tripled
# 0      1        3
# 1      2        6
# 2      3        9
# 3      4       12
# 4      5       15
```

### Using with Pandas Pipe

Transformations can be used as functions in pandas `.pipe()` method:

```python
result = (
    df
    .pipe(AddCalculatedColumn(multiplier=2, target_column="doubled"))
    .pipe(AddCalculatedColumn(multiplier=3, target_column="tripled"))
)
```

### Alternative Execution Methods

```python
# Method 1: Pass DataFrame to transform()
result = AddCalculatedColumn().transform(df)

# Method 2: Set DataFrame in constructor
transformation = AddCalculatedColumn(df=df)
transformation.execute()
result = transformation.output.df

# Method 3: Use as callable (for .pipe())
add_calc = AddCalculatedColumn(multiplier=10)
result = df.pipe(add_calc)
```

## Advanced Examples

### Data Cleaning Transformation

```python
class CleanData(Transformation):
    def execute(self):
        df = self.df.copy()
        
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Fill missing values
        df = df.fillna(df.mean(numeric_only=True))
        
        # Standardize column names
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        
        self.output.df = df

# Usage in a pipeline
cleaned_df = (
    raw_df
    .pipe(CleanData())
    .pipe(AddCalculatedColumn(multiplier=1.1, target_column="adjusted"))
)
```

### Feature Engineering for ML

```python
from koheesio.pandas.transformations import Transformation
import numpy as np

class FeatureEngineering(Transformation):
    """Create features for machine learning."""
    
    def execute(self):
        df = self.df.copy()
        
        # Create polynomial features
        df['value_squared'] = df['value'] ** 2
        df['value_cubed'] = df['value'] ** 3
        
        # Create categorical features
        df['value_category'] = pd.cut(df['value'], bins=3, labels=['low', 'medium', 'high'])
        
        # Create interaction features
        if 'price' in df.columns and 'quantity' in df.columns:
            df['total_value'] = df['price'] * df['quantity']
        
        # Create time-based features if datetime column exists
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[f'{col}_year'] = df[col].dt.year
                df[f'{col}_month'] = df[col].dt.month
                df[f'{col}_dayofweek'] = df[col].dt.dayofweek
        
        self.output.df = df

# Usage with ML pipeline
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor

# Prepare features
features_df = FeatureEngineering().transform(raw_data)

# Split for ML
X = features_df.drop(['target'], axis=1)
y = features_df['target']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

# Train model
model = RandomForestRegressor()
model.fit(X_train, y_train)
```

## Integration with ML Workflows

Pandas transformations integrate seamlessly with machine learning libraries:

```python
# Install with ML support
# pip install "koheesio[ml]"

from koheesio.pandas.transformations import Transformation
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import numpy as np

class MLPreprocessing(Transformation):
    """Preprocessing pipeline for ML."""
    
    n_components: int = 5
    
    def execute(self):
        df = self.df.copy()
        
        # Separate numeric and categorical columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        categorical_cols = df.select_dtypes(include=['object']).columns
        
        # Scale numeric features
        if len(numeric_cols) > 0:
            scaler = StandardScaler()
            df[numeric_cols] = scaler.fit_transform(df[numeric_cols])
            
        # Apply PCA to numeric features
        if len(numeric_cols) > self.n_components:
            pca = PCA(n_components=self.n_components)
            pca_features = pca.fit_transform(df[numeric_cols])
            
            # Replace numeric columns with PCA components
            pca_df = pd.DataFrame(
                pca_features, 
                columns=[f'pca_component_{i}' for i in range(self.n_components)],
                index=df.index
            )
            df = pd.concat([df[categorical_cols], pca_df], axis=1)
        
        self.output.df = df

# Use in ML pipeline
processed_data = (
    raw_data
    .pipe(CleanData())
    .pipe(FeatureEngineering())  
    .pipe(MLPreprocessing(n_components=3))
)
```

## Databricks Integration

Pandas transformations work perfectly in Databricks notebooks without requiring a Spark cluster:

```python
# In Databricks notebook (no Spark cluster needed)
from koheesio.pandas.transformations import Transformation
import pandas as pd

# Read data (using pandas, not Spark)
df = pd.read_csv("/dbfs/path/to/data.csv")

# Apply transformations
result = (
    df
    .pipe(CleanData())
    .pipe(FeatureEngineering())
    .pipe(MLPreprocessing())
)

# Save result
result.to_parquet("/dbfs/path/to/output.parquet")

# Or continue with ML workflow
from sklearn.ensemble import RandomForestClassifier
# ... ML code continues
```

## Testing Pandas Transformations

```python
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

class TestAddCalculatedColumn:
    def test_multiplier_applied_correctly(self):
        # Arrange
        input_df = pd.DataFrame({"value": [1, 2, 3]})
        expected_df = pd.DataFrame({"value": [1, 2, 3], "doubled": [2, 4, 6]})
        transformation = AddCalculatedColumn(multiplier=2, target_column="doubled")
        
        # Act  
        result = transformation.transform(input_df)
        
        # Assert
        assert_frame_equal(result, expected_df)
    
    def test_original_dataframe_unchanged(self):
        # Arrange
        input_df = pd.DataFrame({"value": [1, 2, 3]})
        original_df = input_df.copy()
        transformation = AddCalculatedColumn()
        
        # Act
        result = transformation.transform(input_df)
        
        # Assert - original dataframe should be unchanged
        assert_frame_equal(input_df, original_df)
```

## Performance Considerations

- **Memory Efficiency**: Always use `.copy()` when modifying DataFrames to avoid unintended side effects
- **Large Datasets**: Consider chunking for datasets that don't fit in memory
- **Vectorization**: Leverage pandas vectorized operations instead of loops
- **Type Optimization**: Use appropriate data types (`category`, `int32` vs `int64`, etc.)

```python
class OptimizedTransformation(Transformation):
    def execute(self):
        df = self.df.copy()
        
        # Optimize memory usage
        for col in df.select_dtypes(include=['object']):
            if df[col].nunique() < 0.5 * len(df):
                df[col] = df[col].astype('category')
        
        # Use vectorized operations
        df['calculated'] = np.where(
            df['condition'], 
            df['value'] * 2, 
            df['value'] / 2
        )
        
        self.output.df = df
```

## API Reference

See the [API Reference](../../api_reference/pandas/transformations/index.md) for detailed documentation of all pandas transformation classes and methods.

