# ML Workflows Without Spark

This directory contains examples demonstrating how to use Koheesio for ML workflows **without requiring PySpark**.

## Key Value Proposition

Koheesio now supports lightweight ML pipelines using pandas transformations, allowing you to:

- **Build ML pipelines without Spark** - No need to install or configure PySpark
- **Develop locally** - Fast iteration on your local machine
- **Scale when needed** - Switch to Spark transformations when you need distributed processing
- **Stay type-safe** - Full Pydantic validation for all transformations

## Installation

For ML workflows without Spark:

```bash
pip install koheesio[ml]
```

For ML workflows with Spark support:

```bash
pip install koheesio[ml,pyspark]
```

## Examples

### Simple ML Pipeline (`simple_ml_pipeline.py`)

A complete feature engineering pipeline demonstrating:
- Age group categorization
- Loan-to-income ratio calculation
- Credit score normalization
- Risk score computation

**Run it:**

```bash
python simple_ml_pipeline.py
```

**Key Features:**
- No Spark required
- Uses pandas transformations
- Composable pipeline with `.pipe()`
- Type-safe with Pydantic validation

## When to Use ML Without Spark

Use `koheesio[ml]` (without Spark) when:

- Working with small to medium datasets (fits in memory)
- Developing and testing ML features locally
- Building lightweight ML services
- Running in environments without Spark
- Prototyping before scaling to Spark

Use `koheesio[ml,pyspark]` (with Spark) when:

- Processing large datasets that don't fit in memory
- Running on distributed clusters (Databricks, EMR, etc.)
- Need Spark's distributed computing capabilities
- Integrating with existing Spark workflows

## Interoperability

The best part? You can switch between pandas and Spark transformations seamlessly:

```python
# Start with pandas for local development
from koheesio.pandas.transformations import Transformation as PandasTransformation

# Scale to Spark when needed
from koheesio.spark.transformations import Transformation as SparkTransformation
```

Both share the same API and patterns!

## Learn More

- [Koheesio Documentation](https://github.com/Nike-Inc/koheesio)
- [Pandas Transformations API](../../docs/reference/pandas/)
- [Installation Guide](../../docs/tutorials/getting-started.md)
