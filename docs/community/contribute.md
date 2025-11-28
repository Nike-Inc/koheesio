<!-- TODO: add code of conduct -->

# How to contribute

There are a few guidelines that we need contributors to follow so that we are able to process requests as efficiently as possible. If you have any questions or concerns please feel free to contact us at [opensource@nike.com](mailto:opensource@nike.com).

## Getting Started

* Review our [Code of Conduct](https://github.com/Nike-Inc/nike-inc.github.io/blob/master/CONDUCT.md)
* Make sure you have a [GitHub account](https://github.com/signup/free)
* Submit a ticket for your issue, assuming one does not already exist.
    * Clearly describe the issue including steps to reproduce when it is a bug.
    * Make sure you fill in the earliest version that you know has the issue.
* Fork the repository on GitHub

## Making Changes

* Create a feature branch off of `main` before you start your work.
    * Please avoid working directly on the `main` branch.
* Setup the required package manager [hatch](#package-manager)
* Setup the dev environment [see below](#dev-environment-setup)
* Make commits of logical units.
    * You may be asked to squash unnecessary commits down to logical units.
* Check for unnecessary whitespace with `git diff --check` before committing.
* Write meaningful, descriptive commit messages.
* Please follow existing code conventions when working on a file
* Make sure to check the standards on the code, [see below](#linting-and-standards)
* Make sure to test the code before you push changes [see below](#testing)

## 🤝 Submitting Changes

* Push your changes to a topic branch in your fork of the repository.
* Submit a pull request to the repository in the Nike-Inc organization.
* After feedback has been given we expect responses within two weeks. After two weeks we may close the pull request 
if it isn't showing any activity.
* Bug fixes or features that lack appropriate tests may not be considered for merge.
* Changes that lower test coverage may not be considered for merge.

### 🔨 Make commands

We use `make` for managing different steps of setup and maintenance in the project. The Makefile is designed around Koheesio's layered architecture, providing commands for each layer.

You can install make by following the instructions [here](https://formulae.brew.sh/formula/make)

For a full list of available make commands, you can run:

```bash
make help
```

#### Command Categories

**Setup Commands:**
- `make dev`, `make dev-core`, `make dev-pandas`, `make dev-ml`, `make dev-spark`
- `make init` / `make hatch-install`

**Testing Commands:**  
- `make test-core`, `make test-pandas`, `make test-ml`, `make test-layered`
- `make spark-tests`, `make all-tests`, `make test-integrations`
- `make cov`, `make dev-test`

**Code Quality Commands:**
- `make check`, `make fmt`  
- `make black-check`, `make ruff-check`, `make mypy-check`

**Documentation Commands:**
- `make docs`

The layered approach allows you to work efficiently with only the dependencies and tests relevant to your changes.


### 📦 Package manager

We use `hatch` as our package manager.

> Note: Please DO NOT use pip or conda to install the dependencies. Instead, use hatch.

To install hatch, run the following command:
```console
make init
```

or,
```console
make hatch-install
```

This will install hatch using brew if you are on a Mac. 

If you are on a different OS, you can follow the instructions [here](https://hatch.pypa.io/latest/install/)


### 📌 Dev Environment Setup

Koheesio uses a layered architecture that allows you to set up different development environments based on what you're working on.

#### Full Development Environment
For general development with all dependencies:
```bash
make dev
```

#### Layered Development Environments  
For working on specific layers with minimal dependencies:

```bash
# Core layer only (minimal dependencies)
make dev-core

# Pandas layer (no Spark required)  
make dev-pandas

# ML layer (machine learning dependencies)
make dev-ml

# Spark layer (full Spark functionality)
make dev-spark
```

**When to use each environment:**
- **`dev-core`**: Working on core framework, Step classes, BaseModel, Context, Logger
- **`dev-pandas`**: Data science work, pandas transformations, notebooks (works in Databricks without Spark)
- **`dev-ml`**: Machine learning features, model training, feature engineering
- **`dev-spark`**: Big data processing, Spark transformations, distributed computing
- **`dev`**: Full development when working across multiple layers

### 🧹 Linting and Standards

We use `ruff`, `pylint`, `isort`, `black` and `mypy` to maintain standards in the codebase.

Run the following two commands to check the codebase for any issues:

```bash
make check
```
This will run all the checks including pylint and mypy.

```bash
make fmt
```
This will format the codebase using black, isort, and ruff.

Make sure that the linters and formatters do not report any errors or warnings before submitting a pull request.

### 🧪 Testing

Koheesio supports layered testing to match our architecture. You can run tests for specific layers or all tests.

#### Layer-Specific Testing
Run tests for individual layers with minimal dependencies:

```bash
# Core framework tests only (fastest, minimal deps)
make test-core

# Pandas layer tests (no Spark required)  
make test-pandas

# ML layer tests
make test-ml

# All layered tests (core + pandas + ml, no Spark)
make test-layered

# Spark layer tests (requires PySpark)
make spark-tests

# Integration tests (external systems)
make test-integrations
```

#### Full Test Suite
Run all tests across all environments:
```bash
make all-tests
```

#### Coverage Testing
Run tests with coverage reporting:
```bash
make cov
```

#### Development Testing
Quick testing in development environment:
```bash
make dev-test              # All tests in dev environment
make dev-test-spark        # Just Spark tests  
make dev-test-non-spark    # Everything except Spark tests
```

**Testing Strategy:**
- Use layer-specific tests during development for faster feedback
- Run `make test-layered` to test non-Spark functionality quickly
- Run `make all-tests` before submitting pull requests
- Use appropriate test commands based on what layers you're modifying

Make sure that all relevant tests pass and that you have adequate coverage before submitting a pull request.

# Additional Resources

* [General GitHub documentation](https://help.github.com/)
* [GitHub pull request documentation](https://help.github.com/send-pull-requests/)
* [Nike's Code of Conduct](https://github.com/Nike-Inc/nike-inc.github.io/blob/master/CONDUCT.md)
* [Nike OSS](https://nike-inc.github.io/)