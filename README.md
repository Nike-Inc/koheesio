# Koheesio

<div align="center">
<img src="https://raw.githubusercontent.com/Nike-Inc/koheesio/main/docs/assets/logo_koheesio.svg" alt="Koheesio logo" width="500" role="img">
</div>

|         |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| CI/CD   | [![CI - Test](https://github.com/Nike-Inc/koheesio/actions/workflows/test.yml/badge.svg)](https://github.com/Nike-Inc/koheesio/actions/workflows/test.yml) [![CD - Build Koheesio](https://github.com/Nike-Inc/koheesio/actions/workflows/build_koheesio.yml/badge.svg)](https://github.com/Nike-Inc/koheesio/actions/workflows/release.yml)                                                                                                                                                                                                                                                                                                                                                                                                        |
| Package | [![PyPI - Version](https://img.shields.io/pypi/v/koheesio.svg?logo=pypi&label=PyPI&logoColor=gold)](https://pypi.org/project/koheesio/) [![PyPI - Python Version](https://img.shields.io/pypi/pyversions/koheesio.svg?logo=python&label=Python&logoColor=gold)](https://pypi.org/project/koheesio/) [![PyPI - Installs](https://img.shields.io/pypi/dm/koheesio.svg?color=blue&label=Installs&logo=pypi&logoColor=gold)](https://pypi.org/project/koheesio/) [![Release - Downloads](https://img.shields.io/github/downloads/Nike-Inc/koheesio/total?label=Downloads)](https://github.com/Nike-Inc/koheesio/releases)                                                                                                                               |
| Meta    | [![Hatch project](https://img.shields.io/badge/%F0%9F%A5%9A-Hatch-4051b5.svg)](https://github.com/pypa/hatch) [![linting - Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff) [![types - Mypy](https://img.shields.io/badge/types-Mypy-blue.svg)](https://github.com/python/mypy) [![docstring - numpydoc](https://img.shields.io/badge/docstring-numpydoc-blue)](https://numpydoc.readthedocs.io/en/latest/format.html) [![code style - black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black) [![License - Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-green.svg)](LICENSE.txt) |

[//]: # (suggested edit: )
# Koheesio: A Python Framework for Efficient Data Pipelines


Koheesio - the Finnish word for cohesion - is a robust Python framework designed to build efficient data pipelines. It
encourages modularity and collaboration, allowing the creation of complex pipelines from simple, reusable components.


## What is Koheesio?

Koheesio is a versatile framework that supports multiple implementations and works seamlessly with various data 
processing libraries or frameworks. This ensures that Koheesio can handle any data processing task, regardless of the
underlying technology or data scale.

Koheesio uses [Pydantic] for strong typing, data validation, and settings management, ensuring a high level of type
safety and structured configurations within pipeline components.

[Pydantic]: docs/includes/glossary.md#pydantic

The goal of Koheesio is to ensure predictable pipeline execution through a solid foundation of well-tested code and a
rich set of features. This makes it an excellent choice for developers and organizations seeking to build robust and
adaptable data pipelines.


### What Koheesio is Not

Koheesio is **not** a workflow orchestration tool. It does not serve the same purpose as tools like Luigi,
Apache Airflow, or Databricks workflows, which are designed to manage complex computational workflows and generate 
DAGs (Directed Acyclic Graphs).

Instead, Koheesio is focused on providing a robust, modular, and testable framework for data tasks. It's designed to
make it easier to write, maintain, and test data processing code in Python, with a strong emphasis on modularity,
reusability, and error handling.

If you're looking for a tool to orchestrate complex workflows or manage dependencies between different tasks, you might
want to consider dedicated workflow orchestration tools.


### The Strength of Koheesio

The core strength of Koheesio lies in its **focus on the individual tasks within those workflows**. It's all about
making these tasks as robust, repeatable, and maintainable as possible. Koheesio aims to break down tasks into small,
manageable units of work that can be easily tested, reused, and composed into larger workflows orchestrated with other
tools or frameworks (such as Apache Airflow, Luigi, or Databricks Workflows).

By using Koheesio, you can ensure that your data tasks are resilient, observable, and repeatable, adhering to good
software engineering practices. This makes your data pipelines more reliable and easier to maintain, ultimately leading
to more efficient and effective data processing.


### Promoting Collaboration and Innovation

Koheesio encapsulates years of software and data engineering expertise. It fosters a collaborative and innovative
community, setting itself apart with its unique design and focus on data pipelines, data transformation, ETL jobs,
data validation, and large-scale data processing.

The core components of Koheesio are designed to bring strong software engineering principles to data engineering. 

'Steps' break down tasks and workflows into manageable, reusable, and testable units. Each 'Step' comes with built-in
logging, providing transparency and traceability. The 'Context' component allows for flexible customization of task 
behavior, making it adaptable to various data processing needs.

In essence, Koheesio is a comprehensive solution for data engineering challenges, designed with the principles of
modularity, reusability, testability, and transparency at its core. It aims to provide a rich set of features including
utilities, readers, writers, and transformations for any type of data processing. It is not in competition with other
libraries, but rather aims to offer wide-ranging support and focus on utility in a multitude of scenarios. Our
preference is for integration, not competition.

We invite contributions from all, promoting collaboration and innovation in the data engineering community.


### Comparison to other libraries

- [dbt](https://www.getdbt.com/): A SQL-first transformation workflow that enables teams to deploy analytics code quickly and collaboratively. It adheres to software engineering best practices such as modularity, portability, CI/CD, and documentation. This allows anyone on the data team to contribute safely to production-grade data pipelines. While dbt focuses on transforming data in warehouses using SQL, Koheesio is a more general data pipeline framework that can handle a variety of data processing tasks.- [Flyte](https://flyte.org/): A cloud-native platform for orchestrating ML and data processing workflows. Unlike Koheesio, it requires Kubernetes for deployment and has a strong focus on workflow orchestration.
- [Kubeflow](https://kubeflow.org/): A project dedicated to simplifying the deployment of ML workflows on Kubernetes. Unlike Koheesio, it is more specialized for ML workflows.
- [Snakemake](https://snakemake.readthedocs.io/en/stable/): A workflow management system that uses a Python-style language for defining workflows. While it's powerful for creating complex workflows, Koheesio's focus on modularity and reusability might make it easier to build, test, and maintain your data pipelines.
- [Broadway](https://elixir-broadway.org/): An Elixir library for building concurrent, multi-stage data ingestion and processing pipelines. If your team is more comfortable with Python or if you're looking for a framework that emphasizes modularity and collaboration, Koheesio could be a better fit.
- [Apache Airflow](https://airflow.apache.org/docs/): A platform to programmatically author, schedule and monitor workflows. Unlike Koheesio, it focuses on managing complex computational workflows.
- [Luigi](https://luigi.readthedocs.io/): A Python module that helps you build complex pipelines of batch jobs. It is more focused on workflow orchestration compared to Koheesio.
- [Databricks Workflows](https://docs.databricks.com/data-engineering/pipelines/index.html): A set of tools for building, debugging, deploying, and running Apache Spark workflows on Databricks.
- [Prefect](https://docs.prefect.io/): A new workflow management system, designed for modern infrastructure and powered by the open-source Prefect Core workflow engine. It is more focused on workflow orchestration and management compared to Koheesio.
- [Dagster](https://docs.dagster.io/): A data orchestrator for machine learning, analytics, and ETL. It's more focused on orchestrating and visualizing data workflows compared to Koheesio.
- [Kedro](https://kedro.readthedocs.io/): A Python framework that applies software engineering best-practice to data and machine-learning pipelines. It is similar to Koheesio but has a stronger emphasis on machine learning pipelines.
- [Metaflow](https://docs.metaflow.org/): A human-centric framework for data science that addresses the entire data science lifecycle. It is more focused on data science projects compared to Koheesio.
- [Pachyderm](https://docs.pachyderm.com/): A data versioning, data lineage, and workflow system running on Kubernetes. It is more focused on data versioning and lineage compared to Koheesio.
- [Argo](https://argoproj.github.io/argo/): An open source container-native workflow engine for orchestrating parallel jobs on Kubernetes. Unlike Koheesio, it requires Kubernetes for deployment.
- [MLflow](https://mlflow.org/docs/latest/index.html): An open source platform for managing the end-to-end machine learning lifecycle. It is more focused on machine learning projects compared to Koheesio.
- [Seldon Core](https://docs.seldon.io/projects/seldon-core/en/latest/): An open source platform for deploying machine learning models on Kubernetes. Unlike Koheesio, it is more focused on model deployment.
- [TFX](https://www.tensorflow.org/tfx/guide): An end-to-end platform for deploying production ML pipelines. It is more focused on TensorFlow-based machine learning pipelines compared to Koheesio.
- [Ray](https://docs.ray.io/en/latest/): A general-purpose distributed computing framework. Unlike Koheesio, it is more focused on distributed computing.
- [Ploomber](https://ploomber.readthedocs.io/): A Python library for building robust data pipelines. In some ways it is similar to Koheesio, but has a very different API design more focused on workflow orchestration.
- [Daggit](https://daggit.readthedocs.io/): A Python library for creating and executing Directed Acyclic Graphs (DAGs). It is more focused on DAG execution compared to Koheesio.
- [DataJoint](https://docs.datajoint.io/): A language for defining data relations and manipulating data. Unlike Koheesio, it is more focused on data relation definition and manipulation.
- [Dataform](https://docs.dataform.co/): A platform for managing data in BigQuery, Snowflake, Redshift, and other data warehouses. Unlike Koheesio, it is more focused on data warehouse management.


## Koheesio Core Components

Here are the 3 core components included in Koheesio:

- __Step__: This is the fundamental unit of work in Koheesio. It represents a single operation in a data pipeline,
  taking in inputs and producing outputs.
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

## Extras

Koheesio also provides some additional features that can be useful in certain scenarios. We call these 'integrations'.
With an integration we mean a module that requires additional dependencies to be installed.

Extras can be added by adding `extras=['name_of_the_extra']` (poetry) or `koheesio[name_of_the_extra]` (pip/hatch) to 
the `pyproject.toml` entry mentioned above or installing through pip.

### Integrations

- __Spark Expectations:__   
    Available through the `koheesio.steps.integration.spark.dq.spark_expectations` module; installable through the `se` extra.
    - SE Provides Data Quality checks for Spark DataFrames.
    - For more information, refer to the [Spark Expectations docs](https://engineering.nike.com/spark-expectations).

[//]: # (- **Brickflow:** Available through the `koheesio.steps.integration.workflow` module; installable through the `bf` extra.)
[//]: # (    - Brickflow is a workflow orchestration tool that allows you to define and execute workflows in a declarative way.)
[//]: # (    - For more information, refer to the [Brickflow docs]&#40;https://engineering.nike.com/brickflow&#41;)

- __Box__:  
    Available through the `koheesio.integration.box` module; installable through the `box` extra.
    - [Box](https://www.box.com) is a cloud content management and file sharing service for businesses.

- __SFTP__:  
    Available through the `koheesio.integration.spark.sftp` module; installable through the `sftp` extra.
    - SFTP is a network protocol used for secure file transfer over a secure shell.
    - The SFTP integration of Koheesio relies on [paramiko](https://www.paramiko.org/)

[//]: # (TODO: add implementations)
[//]: # (## Implementations)
[//]: # (TODO: add async extra)
[//]: # (TODO: add spark extra)
[//]: # (TODO: add pandas extra)


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
