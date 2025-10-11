.PHONY: help  ## Display this message
help:
	@python src/koheesio/__about__.py
	@echo "\nAvailable \033[34m'make'\033[0m commands:"
	@echo "\n\033[1mSetup:\033[0m"
	@grep -E '^.PHONY: .*?## setup - .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ".PHONY: |## (setup|hatch) - "}; {printf " \033[36m%-22s\033[0m %s\n", $$2, $$3}'
	@echo "\n\033[1mCode Quality:\033[0m"
	@grep -E '^.PHONY: .*?## code quality - .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ".PHONY: |## code quality - "}; {printf " \033[36m%-22s\033[0m %s\n", $$2, $$3}'
	@echo "\n\033[1mTesting and Coverage:\033[0m"
	@grep -E '^.PHONY: .*?## testing - .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ".PHONY: |## testing - "}; {printf " \033[36m%-22s\033[0m %s\n", $$2, $$3}'
	@echo "\n\033[1mHatch Commands:\033[0m"
	@grep -E '^.PHONY: .*?## hatch - .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ".PHONY: |## hatch - "}; {printf " \033[36m%-22s\033[0m %s\n", $$2, $$3}'
	@echo "\n\033[1mDocsite:\033[0m"
	@grep -E '^.PHONY: .*?## docs - .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ".PHONY: |## docs - "}; {printf " \033[36m%-22s\033[0m %s\n", $$2, $$3}'
	@echo "\n\033[1mMiscellaneous:\033[0m"
	@grep -E '^.PHONY: .*?## misc - .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ".PHONY: |## misc - "}; {printf " \033[36m%-22s\033[0m %s\n", $$2, $$3}'

#  Setup
.PHONY: dev  ## setup - Install the environment and dependencies for local development
dev: hatch-version
	@echo "\033[1mSetting up development environment for\033[0;32m local development\033[0m\033[1m, using: \033[0;36m"
	@hatch env create dev
	@hatch run dev:python --version
	@hatch run dev:uv pip freeze | grep pyspark
	@echo "\033[0m"
	@echo "\033[0mAvailable commands:"
	@echo "\033[3m- Run\033[0m \033[0;36m'exit'\033[0m\033[3m to exit the shell\033[0m"
	@echo "\033[3m- Run\033[0m \033[0;36m'make help'\033[0m\033[3m to see all available commands\033[0m"
	@echo
	@echo "\033[3mThe virtual environment is available in the \033[0;95m'.venv'\033[0m\033[3m folder\033[0m"
	@echo "\033[0;34mHappy coding! 🚀"
	@echo "\033[0m"
	@hatch shell dev

.PHONY: init hatch-install  ## setup - Install Hatch (on Mac)
hatch-install:
	@if [ `uname -s` = "Darwin" ]; then \
		brew install hatch; \
	else \
		echo "Koheesio only supports automatic installation of hatch on Mac. Please install hatch manually, go to https://hatch.pypa.io/latest/install/ for instructions"; \
	fi
init: hatch-install

.PHONY: sync  ## hatch - Update dependencies if you changed project dependencies in pyproject.toml
.PHONY: update  ## hatch - alias for sync (if you are used to poetry, thi is similar to running `poetry update`)
sync:
	@hatch run dev:uv sync --all-extras
update: sync

#  Code Quality
# TODO: hatch fmt enviroment is needed to allow `hatch run fmt: ...` OR fix hatch-fmt env to allow doing it theretype commands
.PHONY: black black-fmt ## code quality - Use black to (re)format the codebase
black-fmt:
	@echo "\033[1mRunning black-fmt\033[0m:\n\033[35m Use black to (re)format the codebase\033[0m"
	@hatch run black-fmt
black: black-fmt
.PHONY: black-check ## code quality - Check the codebase for black compliance
black-check:
	@echo "\033[1mRunning black-check:\n\033[0m\033[35m Check the codebase for black compliance\033[0m"
	@hatch run black-check

.PHONY: isort isort-fmt  ## code quality - Sort imports using isort to keep them clean and readable, ensuring imports are sorted alphabetically and automatically separated into sections and by type
isort-fmt:
	@echo "\033[1mRunning isort-fmt:\n\033[0m\033[35m Sorts the imports in your Python files alphabetically and automatically separate them into sections and by type\033[0m"
	@hatch run isort-fmt
isort: isort-fmt
.PHONY: isort-check  ## code quality - Check the codebase for isort compliance
isort-check:
	@echo "\033[1mRunning isort-check:\n\033[0m\033[35m Check the codebase for isort compliance\033[0m"
	@hatch run isort-check

.PHONY: ruff ruff-fmt  ## code quality - Lint and format the codebase using ruff
ruff-fmt:
	@echo "\033[1mRunning ruff-fmt:\033[0m\n\033[35m Lint and format the codebase using ruff\033[0m"
	@hatch run ruff-fmt
ruff: ruff-fmt
.PHONY: ruff-check  ## code quality - Check the codebase for ruff compliance
ruff-check:
	@echo "\033[1mRunning ruff-check:\n\033[0m\033[35m Check the codebase for ruff compliance\033[0m"
	@hatch run ruff-check .

.PHONY: mypy mypy-check  ## code quality - Run mypy to check the codebase for type errors
mypy-check:
	@echo "\033[1mRunning mypy-check:\033[0m\n\033[35m Check the codebase for type errors using mypy\033[0m"
	@hatch run mypy-check
mypy: mypy-check

.PHONY: pylint pylint-check  ## code quality - Run pylint to check the codebase for errors
pylint-check:
	@echo "\033[1mRunning pylint-check:\033[0m\n\033[35m Check the codebase for errors using pylint\033[0m"
	@hatch run pylint-check
pylint: pylint-check

.PHONY: check  ## code quality - Run all checks
check:
	@echo "\033[1mRunning all checks:\033[0m\n\033[35m This will run all the checks including pylint and mypy\033[0m"
	@hatch run check

.PHONY: fmt  ## code quality - Format the codebase
fmt:
	@echo "\033[1mFormatting the codebase:\033[0m\n\033[35m This will format the codebase using black, isort, and ruff\033[0m"
	@hatch run dev:fmt


# Testing and Coverage
.PHONY: cov ## testing - Run pytest with coverage
cov:
	@echo "\033[1mRunning pytest with coverage:\033[0m\n\033[35m This will run the test suite with coverage enabled\033[0m"
	@hatch run dev:coverage
coverage: cov

.PHONY: all-tests  ## testing - Run ALL tests in ALL environments
all-tests:
	@echo "\033[1mRunning all tests:\033[0m\n\033[35m This will run the full test suite\033[0m"
	@echo "\033[1;31mWARNING:\033[0;33m This may take upward of 20-30 minutes to complete!\033[0m"
	@hatch test --all --no-header
.PHONY: spark-tests  ## testing - Run SPARK tests in ALL environments
spark-tests:
	@echo "\033[1mRunning Spark tests:\033[0m\n\033[35m This will run the Spark test suite against all specified environments\033[0m"
	@echo "\033[1;31mWARNING:\033[0;33m This may take upward of 20-30 minutes to complete!\033[0m"
	@hatch test --all -m spark --no-header
.PHONY: non-spark-tests  ## testing - Run non-spark tests in ALL environments
non-spark-tests:
	@echo "\033[1mRunning non-Spark tests:\033[0m\n\033[35m This will run the non-Spark test suite against all specified environments\033[0m"
	@hatch test --all -m "not spark" --no-header

.PHONY: dev-test ## testing - Run pytest, with all tests in the dev environment
dev-test:
	@echo "\033[1mRunning pytest:\033[0m\n\033[35m This will run the full test suite, but only once (in the dev environment)\033[0m"
	@hatch run dev:test -vv
.PHONY: dev-test-spark  ## testing - Run pytest (just spark) in the dev environment
dev-test-spark:
	@echo "\033[1mRunning pytest for Spark tests:\033[0m\n\033[35m This will run the Spark test suite\033[0m"
	@hatch run dev:spark-tests -vv
.PHONY: dev-test-non-spark  ## testing - Run pytest without spark in the dev environment
dev-test-non-spark:
	@echo "\033[1mRunning pytest for non-Spark tests:\033[0m\n\033[35m This will run the test suite, excluding all spark tests\033[0m"
	@hatch run dev:non-spark-tests -vv


# Hatch commands
.PHONY: requirements hatch-requirements  ## hatch - Export the requirements to requirements.txt
hatch-requirements:
	@hatch run uv pip freeze > requirements.txt
requirements: hatch-requirements

.PHONY: build hatch-build  ## hatch - Build the package
hatch-build:
	@hatch build
build: hatch-build

.PHONY: hatch-env-show hatch-envs ## hatch - Show the hatch environment info
hatch-env-show:
	@hatch env show
hatch-envs: hatch-env-show

.PHONY: hatch-version  ## hatch - Show the installed hatch version
hatch-version:
	@hatch --version

.PHONY: hatch-show-dependencies  ## hatch - Show the installed hatch dependencies
hatch-show-dependencies:
	@hatch run uv pip freeze


# Docsite
.PHONY: docs  ## docs - Build and serve the documentation site locally
docs:
	@hatch run docs:serve


#  Misc
.PHONY: clean  ## misc - Clear local caches and build artifacts
clean:
	@echo "\033[1mCleaning up:\033[0m\n\033[35m This will remove all local caches and build artifacts\033[0m"
	@rm -rf `find . -name __pycache__`
	@rm -f `find . -type f -name '*.py[co]'`
	@rm -f `find . -type f -name '*~'`
	@rm -f `find . -type f -name '.*~'`
	@rm -rf .run
	@rm -rf .venv
	@rm -rf .venvs
	@rm -rf .cache
	@rm -rf .pytest_cache
	@rm -rf .ruff_cache
	@rm -rf htmlcov
	@rm -rf *.egg-info
	@rm -f .coverage
	@rm -f .coverage.*
	@rm -rf build
	@rm -rf dist
	@rm -rf site
	@rm -rf docs/_build
	@rm -rf docs/.changelog.md docs/.version.md docs/.tmp_schema_mappings.html
	@rm -rf fastapi/test.db
	@rm -rf coverage.xml


# FIXME: everything under this line still needs further review

##
#  Release Tag & Docs Deploy
##
.PHONY: release-tag  ## Tag the main branch with hatch project version and push the tag
# Example Usage:
# 	make release-tag-main
release-tag-main:
	@git checkout main
	@git pull origin main
	@git tag $(hatch version -s)
	@git push origin $(hatch version -s)

.PHONY: docs-deploy-tag  ## Deploy the docs from a tag
# Example Usage:
# 	make docs-deploy-tag tag=1.0.0
docs-deploy-tag: .docs-clean-gh-pages
	@git checkout $(tag)
	@hatch run docs:build

.PHONY: docs-deploy-main  ## Deploy the docs from the main branch
# Example Usage:
# 	make docs-deploy-tag
docs-deploy-main: .docs-clean-gh-pages
	@git checkout main
	@git pull origin main
	@hatch run docs:build

##
#  Docs Maintenance
##
.docs-clean-gh-pages:
	# clean gh-pages branch
	@git branch -D gh-pages &>/dev/null || true
	@git fetch origin

.PHONY: docs-list-identifiers  ## List all identifiers
docs-list-identifiers:
	@hatch run docs:list

.PHONY: docs-set-identifier-alias  ## Set an alias for an identifier
# Example Usage:
#   make docs-set-identifier-alias identifier=main alias=latest
#   make docs-set-identifier-alias identifier=0.3.1 alias=0.3.0
docs-set-identifier-alias: .docs-clean-gh-pages
	@hatch run docs:alias $(identifier) $(alias)

.PHONY: docs-delete-identifier  ## Delete an identifier
# Example Usage:
#	make delete-identifier identifier=0.3.0
docs-delete-identifier: .docs-clean-gh-pages
	@hatch run docs:delete $(identifier)

.PHONY: docs-retitle-identifier  ## Retitle an identifier
# Example Usage:
# 	make retitle-identifier identifier=main title="main (latest)"
docs-retitle-identifier: .docs-clean-gh-pages
	@hatch run docs:retitle $(identifier) "$(title)"

