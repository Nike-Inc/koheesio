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

We use `make` for managing different steps of setup and maintenance in the project. You can install make by following
the instructions [here](https://formulae.brew.sh/formula/make)

For a full list of available make commands, you can run:

```bash
make help
```


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

To ensure our standards, make sure to install the required packages.

```bash
make dev
```

This will install all the required packages for development in the project under the `.venv` directory.
Use this virtual environment to run the code and tests during local development.

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

We use `pytest` to test our code. 


You can run the tests by running one of the following commands:

```bash
make cov  # to run the tests and check the coverage
make all-tests  # to run all the tests
make spark-tests  # to run the spark tests
make non-spark-tests  # to run the non-spark tests
```

Make sure that all tests pass and that you have adequate coverage before submitting a pull request.

# Additional Resources

* [General GitHub documentation](https://help.github.com/)
* [GitHub pull request documentation](https://help.github.com/send-pull-requests/)
* [Nike's Code of Conduct](https://github.com/Nike-Inc/nike-inc.github.io/blob/master/CONDUCT.md)
* [Nike OSS](https://nike-inc.github.io/)