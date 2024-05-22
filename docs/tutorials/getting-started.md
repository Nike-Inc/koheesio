# Getting Started with Koheesio

## Requirements

- [Python 3.9+](https://docs.python.org/3/whatsnew/3.9.html)

## Installation

### Poetry

If you're using Poetry, add the following entry to the `pyproject.toml` file:

```toml title="pyproject.toml"
[[tool.poetry.source]]
name = "nike"
url = "https://artifactory.nike.com/artifactory/api/pypi/python-virtual/simple"
secondary = true
```

```bash
poetry add koheesio
```

### pip

If you're using pip, run the following command to install Koheesio:

Requires [pip](https://pip.pypa.io/en/stable/).

```bash
pip install koheesio --extra-index-url https://artifactory.nike.com/artifactory/api/pypi/python-virtual/simple
```

## Basic Usage

Once you've installed Koheesio, you can start using it in your Python scripts. Here's a basic example:

```python
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

### Advanced Usage
For more advanced usage, you can check out the examples in the `__notebooks__` directory of this repository. These examples show how to use Koheesio's features in more detail.

### Contributing
If you want to contribute to Koheesio, check out the `CONTRIBUTING.md` file in this repository. It contains guidelines for contributing, including how to submit issues and pull requests.

### Testing
To run the tests for Koheesio, use the following command:

```bash
make test
```

This will run all the tests in the `test` directory.
