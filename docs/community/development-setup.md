# Development Setup Guide

This guide covers setting up a development environment for Koheesio using our layered architecture approach.

## Prerequisites

- Python 3.9+
- [Hatch](https://hatch.pypa.io/latest/install/) package manager  
- Make (for managing development commands)

## Quick Setup

### Install Hatch
```bash
# On macOS (automatic)
make init

# Or manually install hatch
# See: https://hatch.pypa.io/latest/install/
```

### Clone and Setup
```bash
git clone https://github.com/Nike-Inc/koheesio.git
cd koheesio

# Full development environment (all dependencies)
make dev
```

## Layered Development Environments

Koheesio's layered architecture allows you to work with minimal dependencies based on your task:

### Core Development
**Use case:** Working on framework fundamentals (Step, BaseModel, Context, Logger)

```bash
make dev-core
```

**What's included:** Minimal dependencies, core testing tools
**Benefits:** Fast setup, lightweight environment

### Pandas Development  
**Use case:** Data science workflows, pandas transformations, Databricks notebooks

```bash
make dev-pandas
```

**What's included:** pandas + core dependencies
**Benefits:** Works without Spark, perfect for ML/data science development

### ML Development
**Use case:** Machine learning features, model development, feature engineering

```bash  
make dev-ml
```

**What's included:** numpy, scikit-learn, scipy, pandas + core
**Benefits:** DBR ML compatible, no Spark overhead

### Spark Development
**Use case:** Big data processing, Spark transformations, distributed computing

```bash
make dev-spark
```

**What's included:** Full PySpark + all dependencies
**Benefits:** Complete Spark functionality, all integrations

### Full Development
**Use case:** Working across multiple layers, integration work

```bash
make dev
```

**What's included:** All dependencies from all layers
**Benefits:** No limitations, supports all features

## Development Workflow

### 1. Environment Selection
Choose the appropriate environment based on your work:

```bash
# Working on core Step classes?
make dev-core

# Building pandas transformations?
make dev-pandas

# Developing ML features?
make dev-ml

# Working with Spark DataFrames?
make dev-spark
```

### 2. Code Quality
Run formatting and linting:

```bash
# Format code
make fmt

# Check code quality
make check
```

### 3. Testing
Run tests appropriate to your changes:

```bash
# Quick tests for non-Spark changes
make test-layered

# Test specific layer
make test-core      # Core functionality
make test-pandas    # Pandas operations  
make test-ml        # ML features
make test-integrations  # External systems

# Full test suite (before PR)
make all-tests
```

### 4. Documentation
Preview documentation changes:

```bash
make docs
```

## Environment Details

### Core Environment (`dev-core`)
```toml
# Minimal dependencies
dependencies = [
    "pydantic>=2.0.0",
    "pyyaml>=6.0", 
    "rich>=13.0.0",
    # ... core only
]
```

**Isolation verified:** No pandas, no PySpark, no ML libraries

### Pandas Environment (`dev-pandas`)  
```toml
# Core + pandas
features = ["pandas", "test"]
```

**Isolation verified:** Has pandas, no PySpark, works in Databricks without cluster

### ML Environment (`dev-ml`)
```toml  
# Core + ML stack
features = ["ml", "test"]
extra-dependencies = [
    "numpy>=1.21.0",
    "scikit-learn>=1.0.0", 
    "scipy>=1.7.0",
]
```

**DBR compatible:** Works with Databricks Runtime ML 13, 14, 15

### Spark Environment (`dev-spark`)
```toml
# Full feature set
features = ["pyspark", "async", "snowflake", "box", ...]
```

**Full functionality:** All Koheesio features available

## Troubleshooting

### Environment Issues
```bash
# Clean environments
hatch env prune

# Recreate specific environment
hatch env create hatch-test.py3.12-core

# Show environment info
make hatch-envs
```

### Dependency Conflicts
```bash
# Update dependencies
make sync

# Check installed packages
make hatch-show-dependencies
```

### Testing Issues
```bash
# Run tests with verbose output
hatch test -i version=core -v

# Check test markers
pytest --markers

# Run specific test file
hatch test tests/core/test_step.py
```

## IDE Integration

### VS Code
Add to `.vscode/settings.json`:

```json
{
  "python.defaultInterpreterPath": "./.venv/bin/python",
  "python.terminal.activateEnvironment": true,
  "python.testing.pytestEnabled": true,
  "python.testing.pytestArgs": ["tests/"],
  "python.linting.enabled": true,
  "python.linting.ruffEnabled": true
}
```

### PyCharm
1. Go to File → Settings → Project → Python Interpreter
2. Add interpreter → Existing environment  
3. Point to `.venv/bin/python`
4. Configure pytest as test runner

## Performance Tips

### Development Speed
- Use `dev-core` for core framework work (fastest startup)
- Use `dev-pandas` for data science without Spark overhead  
- Use `test-layered` during development (faster than `all-tests`)
- Only use `dev-spark` when actually working with Spark

### Memory Usage
- Core environment: ~50MB
- Pandas environment: ~200MB  
- ML environment: ~400MB
- Spark environment: ~1GB+

### Testing Speed
```bash
# Fastest: Core tests only
make test-core          # ~30 seconds

# Fast: All non-Spark tests  
make test-layered       # ~2 minutes

# Comprehensive: All tests
make all-tests          # ~20+ minutes
```

## Best Practices

### Environment Hygiene
- Use layer-specific environments during development
- Only use full environment (`make dev`) when necessary
- Clean environments regularly with `hatch env prune`

### Testing Strategy  
- Run layer-specific tests during development
- Use `make test-layered` for quick validation
- Run `make all-tests` before submitting PRs
- Add appropriate pytest markers to new tests

### Code Organization
- Keep core functionality independent of layers
- Place pandas code in `koheesio.pandas.*`
- Place ML code with `@pytest.mark.ml` markers
- Maintain clear separation between layers

## Additional Commands

```bash
# Environment management
make help               # Show all available commands
make hatch-version     # Show hatch version
make clean             # Clean build artifacts

# Code quality  
make black-check       # Check formatting
make ruff-check        # Check linting  
make mypy-check        # Check types
make pylint-check      # Check code quality

# Documentation
make docs              # Build and serve docs locally

# Package management
make build             # Build package
make requirements      # Export requirements
```

For more information, see:
- [Contribution Guide](contribute.md)
- [Testing Guide](testing-guide.md)  
- [Architecture Documentation](../reference/concepts/concepts.md)






