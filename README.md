# Koheesio

<div align="center">
<img src="https://raw.githubusercontent.com/Nike-Inc/koheesio/main/docs/assets/logo_koheesio.svg" alt="Koheesio logo" width="500" role="img">
</div>

|         |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| CI/CD   | [![CI - Test](https://github.com/Nike-Inc/koheesio/actions/workflows/test.yml/badge.svg)](https://github.com/Nike-Inc/koheesio/actions/workflows/test.yml) [![CD - Build Koheesio](https://github.com/Nike-Inc/koheesio/actions/workflows/build_koheesio.yml/badge.svg)](https://github.com/Nike-Inc/koheesio/actions/workflows/release.yml)                                                                                                                                                                                                                                                                                                                                                                                                        |
| Package | [![PyPI - Version](https://img.shields.io/pypi/v/koheesio.svg?logo=pypi&label=PyPI&logoColor=gold)](https://pypi.org/project/koheesio/) [![PyPI - Python Version](https://img.shields.io/pypi/pyversions/koheesio.svg?logo=python&label=Python&logoColor=gold)](https://pypi.org/project/koheesio/) [![PyPI - Installs](https://img.shields.io/pypi/dm/koheesio.svg?color=blue&label=Installs&logo=pypi&logoColor=gold)](https://pypi.org/project/koheesio/) [![Release - Downloads](https://img.shields.io/github/downloads/Nike-Inc/koheesio/total?label=Downloads)](https://github.com/Nike-Inc/koheesio/releases)                                                                                                                               |
| Meta    | [![Hatch project](https://img.shields.io/badge/%F0%9F%A5%9A-Hatch-4051b5.svg)](https://github.com/pypa/hatch) [![linting - Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff) [![types - Mypy](https://img.shields.io/badge/types-Mypy-blue.svg)](https://github.com/python/mypy) [![docstring - numpydoc](https://img.shields.io/badge/docstring-numpydoc-blue)](https://numpydoc.readthedocs.io/en/latest/format.html) [![code style - black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black) [![License - Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-green.svg)](LICENSE.txt) |



Koheesio, named after the Finnish word for cohesion, is a robust Python framework for building efficient data pipelines. 
It promotes modularity and collaboration, enabling the creation of complex pipelines from simple, reusable components. 

The framework is versatile, aiming to support multiple implementations and working seamlessly with various data 
processing libraries or frameworks. This ensures that Koheesio can handle any data processing task, regardless of the
underlying technology or data scale.

Koheesio uses [Pydantic] for strong typing, data validation, and settings management, ensuring a high level of type 
safety and structured configurations within pipeline components.

[Pydantic]: docs/includes/glossary.md#pydantic

Koheesio's goal is to ensure predictable pipeline execution through a solid foundation of well-tested code and a rich
set of features, making it an excellent choice for developers and organizations seeking to build robust and adaptable
Data Pipelines.


### What sets Koheesio apart from other libraries?
Koheesio encapsulates years of data engineering expertise, fostering a collaborative and innovative community. While 
similar libraries exist, Koheesio's focus on data pipelines, integration with PySpark, and specific design for tasks 
like data transformation, ETL jobs, data validation, and large-scale data processing sets it apart.
  
Koheesio aims to provide a rich set of features including readers, writers, and transformations for any type of Data
processing. Koheesio is not in competition with other libraries. Its aim is to offer wide-ranging support and focus 
on utility in a multitude of scenarios. Our preference is for integration, not competition...

We invite contributions from all, promoting collaboration and innovation in the data engineering community.


## Koheesio Core Components

Here are the key components included in Koheesio:

- __Step__: This is the fundamental unit of work in Koheesio. It represents a single operation in a data pipeline,
  taking in inputs and producing outputs.
    ```text
    ┌─────────┐        ┌──────────────────┐        ┌──────────┐
    │ Input 1 │───────▶│                  ├───────▶│ Output 1 │
    └─────────┘        │                  │        └──────────┘
                       │                  │
    ┌─────────┐        │                  │        ┌──────────┐
    │ Input 2 │───────▶│       Step       │───────▶│ Output 2 │
    └─────────┘        │                  │        └──────────┘
                       │                  │
    ┌─────────┐        │                  │        ┌──────────┐
    │ Input 3 │───────▶│                  ├───────▶│ Output 3 │
    └─────────┘        └──────────────────┘        └──────────┘
    ```
- __Context__: This is a configuration class used to set up the environment for a Task. It can be used to share
variables across tasks and adapt the behavior of a Task based on its environment.
- __Logger__: This is a class for logging messages at different levels.


## Installation

You can install Koheesio using either pip or poetry.

### Using Pip

To install Koheesio using pip, run the following command in your terminal:

```bash
pip install koheesio
```

### Using Hatch

If you're using Hatch for package management, you can add Koheesio to your project by simply adding koheesio to your
`pyproject.toml`.


### Using Poetry

If you're using poetry for package management, you can add Koheesio to your project with the following command:

```bash
poetry add koheesio
```

or add the following line to your `pyproject.toml` (under `[tool.poetry.dependencies]`), making sure to replace
`...` with the version you want to have installed:

```toml
koheesio = {version = "..."}
```

### Extras / Integrations

Koheesio also provides some additional features that can be useful in certain scenarios. We call these 'integrations'.
With an integration we mean a module that requires additional dependencies to be installed.

Extras can be added by adding `extras=['name_of_the_extra']` (poetry) or `koheesio[name_of_the_extra]` (pip/hatch) to 
the pyproject.toml entry mentioned above.

- __Spark Expectations:__  Available through the `koheesio.steps.integration.spark.dq.spark_expectations` module; installable through the `se` extra.
    - SE Provides Data Quality checks for Spark DataFrames.
    - For more information, refer to the [Spark Expectations docs](https://engineering.nike.com/spark-expectations).

[//]: # (- **Brickflow:** Available through the `koheesio.steps.integration.workflow` module; installable through the `bf` extra.)
[//]: # (    - Brickflow is a workflow orchestration tool that allows you to define and execute workflows in a declarative way.)
[//]: # (    - For more information, refer to the [Brickflow docs]&#40;https://engineering.nike.com/brickflow&#41;)

- __Box__: Available through the `koheesio.integration.box` module; installable through the `box` extra.
    - Box is a cloud content management and file sharing service for businesses.


- __SFTP__: Available through the `koheesio.integration.spark.sftp` module; installable through the `sftp` extra.
    - SFTP is a network protocol used for secure file transfer over a secure shell.


## Contributing

### How to Contribute

We welcome contributions to our project! Here's a brief overview of our development process:

- __Code Standards__: We use `pylint`, `black`, and `mypy` to maintain code standards. Please ensure your code passes
  these checks by running `make check`. No errors or warnings should be reported by the linter before you submit a pull
  request.

- __Testing__: We use `pytest` for testing. Run the tests with `make test` and ensure all tests pass before submitting
  a pull request.

- __Release Process__: We aim for frequent releases. Typically when we have a new feature or bugfix, a developer with
  admin rights will create a new release on GitHub and publish the new version to PyPI.

For more detailed information, please refer to our [contribution guidelines](./docs/contribute.md). We also adhere to
[Nike's Code of Conduct](https://github.com/Nike-Inc/nike-inc.github.io/blob/master/CONDUCT.md) and []

### Additional Resources

- [General GitHub documentation](https://help.github.com/)
- [GitHub pull request documentation](https://help.github.com/send-pull-requests/)
- [Nike OSS](https://nike-inc.github.io/)
