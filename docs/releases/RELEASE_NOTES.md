# Koheesio Release Notes

## 0.10.0

### (0.10) What's Changed

v0.10 brings several important features, security improvements, and bug fixes across different modules of Koheesio.
The overall API remains unchanged.

### (0.10) New Features / Refactors

The following new features are included with 0.10:

* [feature] **Core** Introduces Koheesio specific `SecretStr` and `SecretBytes` classes for handling secret strings and bytes with enhanced security by @dannymeijer in https://github.com/Nike-Inc/koheesio/pull/164
  * Introduces Koheesio specific `SecretStr` and `SecretBytes` along with a `SecretMixin` class (to reduce code duplication across)
  * New Secret classes are compatible with Pydantic's `SecretStr` and  `SecretBytes` and allow seamless integration with existing code
  * To use: Replace `from pydantic import SecretStr, SecretBytes` with `from koheesio.models import SecretStr, SecretBytes` to use the enhanced  secret handling
  * These classes expand support to allow usage with an f-string (or `.format`) and `"string" +  "other_string"` concatenation while remaining secure
* [feature] **Core > BaseModel** Added `partial` classmethod to `BaseModel` for enhanced customization and flexibility by @dannymeijer in https://github.com/Nike-Inc/koheesio/pull/150
  * Partial allows for creating a new instance of a model with only the specified fields updated (such as overwriting or setting a fields default values)
* [feature] **Box** Added a buffered version of `BoxFileWriter` by @riccamini in https://github.com/Nike-Inc/koheesio/pull/161 (#87, #148)
  * Added the `BoxBufferFileWriter` class for writing files to Box when physical storage isn't available. Data is instead buffered in memory before being written to Box.
  * Also improves `BoxCsvFileReader` logging output by providing the file name in addition to the file ID.
* [feature] **Dev Experience** Easier debugging and dev improvements by @dannymeijer in https://github.com/Nike-Inc/koheesio/pull/168
  * To make debugging easier, `pyproject.toml` was updated to allow for easier running `spark connect` in your local dev environment:
    * Added extra dependencies for `pyspark[connect]==3.5.4`.
    * Added environment variables for Spark Connect in the development environment.
    * Changed to verbose mode logging in the pytest output (also visible through Github Actions tests run output).
* [refactor] **Snowflake** Snowflake classes now use `params` over `options` by @dannymeijer in https://github.com/Nike-Inc/koheesio/pull/168
  * Snowflake classes now also bases `ExtraParamsMixin`
  * Renamed `options` field to `params` and added alias `options` for backwards compatibility.
  * Introduced `SF_DEFAULT_PARAMS`.
* [feature] **Delta** Support for Delta table history by @zarembat in https://github.com/Nike-Inc/koheesio/pull/163
  * Enables fetching Delta table history and checking data staleness based on defined intervals and refresh days.
  * Changes to `DeltaTableStep` class:
    * Added `describe_history()` method to `DeltaTableStep` for fetching Delta table history as a Spark DataFrame.
    * Added `is_date_stale()` method to `DeltaTableStep` to check data staleness based on time intervals or specific refresh days.
* [feature] **Http** Added support for authorization headers with proper masking for improved security by @dannymeijer in https://github.com/Nike-Inc/koheesio/pull/158 and https://github.com/Nike-Inc/koheesio/pull/170 (#157)
  * Addresses potential data leaks in authorization headers, ensuring secure handling of sensitive information.
  * Comprehensive unit tests added to prevent regressions and ensure expected behavior.
  * Changes to `HttpStep` class:
    * Added `decode_sensitive_headers` method to decode `SecretStr` values in headers.
    * Modified `get_headers` method to dump headers into JSON without `SecretStr` masking.
    * Added `auth_header` field to handle authorization headers.
    * Implemented masking for bearer tokens to maintain their 'secret' status.
* [feature] **Step & Spark** Add transformation to download file from url data through python or spark by @mikita-sakalouski and @dannymeijer in https://github.com/Nike-Inc/koheesio/pull/143 (#75)
  * Allow downloading files from a given URL
  * Added `DownloadFileStep` class in a new module `koheesio.steps.download_file`
  * Added `FileWriteMode` enum with supported wrtie modes: `OVERWRITE`, `APPEND`, `IGNORE`, `EXCLUSIVE`, `BACKUP`:
  * Also made available as a spark `Transformation` in `DownloadFileFromUrlTransformation` in a new module `koheesio.spark.transformations.download_files`
    * The spark implementation allows passing urls through a column in the a given DataFrame
    * All URLs are then downloaded by the Spark Driver to a given location
* [refactor] **Spark > Reader > JDBC** Updated JDBC behavior by @dannymeijer in https://github.com/Nike-Inc/koheesio/pull/168
  * `JDBCReader` class now also base `ExtraParamsMixin`.
  * Renamed `options` field to `params` and added alias `options` for backwards compatibility.
  * `dbtable` and `query` validation now handled upon initialization rather than at runtime.
  * Behavior now requires either `dbtable` or `query` to be submitted to be able to use JDBC.
* [refactor] **Spark > Reader > HanaReader** Updated `HanaReader` behavior by @dannymeijer in https://github.com/Nike-Inc/koheesio/pull/168
  * `HanaReader` class no longer has an `options` field.
  * Instead uses `params` and the alias `options` for backwards compatibility (see `JDBCReader` changes mentioned above).
* [refactor] **Spark > Reader > TeradataReader** Updated `TeradataReader` behavior by @dannymeijer in https://github.com/Nike-Inc/koheesio/pull/168
  * `TeradataReader` class no longer has an `options` field
  * Instead uses `params` and the alias `options` for backwards compatibility (see `JDBCReader` changes mentioned above).
* [feature] **Spark > Transformation > CamelToSnake**  added more efficient Spark 3.4+ supported operation for `CamelToSnakeTransformation` by @dannymeijer in https://github.com/Nike-Inc/koheesio/pull/142

### (0.10) Bug fixes

The following bugfixes are included with 0.9.0:

* [bugfix] **Core > Context** Fix Context initialization with another Context object and dotted notation by @dannymeijer in https://github.com/Nike-Inc/koheesio/pull/160 (#159)
  * The __init__ method of the Context class incorrectly updated the `kwargs` making it return `None`. Calls to Context containing another Context object, would previously fail.
  * Also fixed an issue with how Context handled get operations for nested keys when using dotted notation
* [bugfix] **Core > Step** Fixed duplicate logging issues in nested Step classes by @dannymeijer in https://github.com/Nike-Inc/koheesio/pull/168
  * We observed log duplication when using specific super call sequences in nested Step classes
  * Several changes were made to the `StepMetaClass` to address duplicate logs when using `super()` in the execute method of a Step class under specific circumstances.
  * Updated `_is_called_through_super` method to traverse the entire method resolution order (MRO) and correctly identify `super()` calls.
  * Ensured `_execute_wrapper` method triggers logging only once per execute call.
  * This change prevents duplicate logs and ensures accurate log entries. The `_is_called_through_super` method was also used for `Output` validation, ensuring it is called only once.
* [bugfix]: **Delta** Improve merge clause handling in `DeltaTableWriter` by @mikita-sakalouski in https://github.com/Nike-Inc/koheesio/pull/155 (#149)
  * Before, when using delta merge configuration (as dict) to provide merge condition to merge builder and having multiple calls for merge operation (e.g. for each batch processing in streaming), the original implementation was breaking due to a pop call on the used dictionary.
* [bugfix] **Spark** Pyspark Connect support fixes by @nogitting and @dannymeijer in https://github.com/Nike-Inc/koheesio/pull/154 (#153)
  * Connect support check previously excluded Spark 3.4 wrongfully
  * Fix gets rid of False positives in our spark connect check utility
* [bugfix] **Spark > ColumnsTransformation** `ColumnConfig` defaults in `ColumnsTransformation` not working correctly by @dannymeijer in https://github.com/Nike-Inc/koheesio/pull/142
  * `run_for_all_data_type` and `limit_data_type` were previously not working correctly
* [bugfix] **Spark > Transformation > Hash** Fix error handling missing columns in Spark Connect by @dannymeijer in https://github.com/Nike-Inc/koheesio/pull/168
  * Updated `sha2` function call to use named parameters.
  * Changes to `Sha2Hash` class:
    * Added check for missing columns.
    * Improved handling when no columns are provided.

### (0.10) New Contributors

Big shout out to all contributors and a heartfelt welcome to our new contributors:

* @nogitting made their first contribution in https://github.com/Nike-Inc/koheesio/pull/154
* @zarembat made their first contribution in https://github.com/Nike-Inc/koheesio/pull/163

**Full Changelog**: https://github.com/Nike-Inc/koheesio/compare/koheesio-v0.9.1...koheesio-v0.10.0

## 0.9.1

### (0.9.1) Bug fixes

* [bugfix] **Box > CSV Reader** Fix Box CSVReader behavior by @louis-paulvlx in https://github.com/Nike-Inc/koheesio/pull/147 (#144)
  * Fixed a bug on csv reader for box integration where all types were being cast to String.

**Full Changelog**: https://github.com/Nike-Inc/koheesio/compare/koheesio-v0.9.0...koheesio-v0.9.1

## 0.9.0

### (0.9) What's Changed

v0.9 brings many changes to the spark module, allowing support for pyspark connect along with a bunch of bug fixes and some new features. Additionally, the snowflake implementation is significantly reworked now relying on a pure python implementation for interacting with Snowflake outside of spark.

### (0.9) New features / Refactors

The following new features are included with 0.9:

* [feature] **Box** - Add overwrite functionality to the BoxFileWriterClass by @ToneVDB in https://github.com/Nike-Inc/koheesio/pull/103
* [feature] **Box** - allow setting file encoding by @louis-paulvlx in https://github.com/Nike-Inc/koheesio/pull/96
* [refactor] **Core** - change private attr and step getter by @mikita-sakalouski in https://github.com/Nike-Inc/koheesio/pull/82
* [feature] **DataBricks** - DataBricksSecret for getting secrets from DataBricks scope by @mikita-sakalouski in https://github.com/Nike-Inc/koheesio/pull/133
* [feature] **Delta** - Enable adding options to DeltaReader both streaming and batch by @mikita-sakalouski in https://github.com/Nike-Inc/koheesio/pull/111
* [feature] **SE** - SparkExpectations bump version to 2.2.0 by @dannymeijer in https://github.com/Nike-Inc/koheesio/pull/99
* [feature] **Snowflake** - Populate account from url if not provided in SnowflakeBaseModel by @mikita-sakalouski in https://github.com/Nike-Inc/koheesio/pull/117
* [feature] **Spark** - add support for Spark Connect by @mikita-sakalouski in https://github.com/Nike-Inc/koheesio/pull/63
* [feature] **Spark** - Make Transformations callable by @dannymeijer in https://github.com/Nike-Inc/koheesio/pull/126
* [feature] **Tableau** - Add support for HyperProcess parameters by @maxim-mityutko in https://github.com/Nike-Inc/koheesio/pull/112

### (0.9) Bug fixes

The following bugfixes are included with 0.9.0:

* [bugfix] **Core** - Accidental duplication of logs by @dannymeijer in https://github.com/Nike-Inc/koheesio/pull/105
* [bugfix] **Core** - Adjust branch fetching logic for forked repo for Github Actions by @mikita-sakalouski in https://github.com/Nike-Inc/koheesio/pull/106 and @mikita-sakalouski in https://github.com/Nike-Inc/koheesio/pull/109
* [bugfix] **Delta** - DeltaMergeBuilder instance type didn't check out by @dannymeijer in https://github.com/Nike-Inc/koheesio/pull/100
* [bugfix] **Delta** - fix merge builder instance check for connect + util fix by @dannymeijer in https://github.com/Nike-Inc/koheesio/pull/130
* [bugfix] **Docs** - broken import statements and updated hello-world.md by @dannymeijer in https://github.com/Nike-Inc/koheesio/pull/107
* [bugfix] **Snowflake** - python connector default config dir by @mikita-sakalouski in https://github.com/Nike-Inc/koheesio/pull/125
* [bugfix] **Snowflake** - Remove duplicated implementation by @mikita-sakalouski in https://github.com/Nike-Inc/koheesio/pull/116
* [bugfix] **Spark** - unused SparkSession being import from pyspark.sql in several tests by @dannymeijer in https://github.com/Nike-Inc/koheesio/pull/140
* [bugfix] **Spark/Docs** - Remove mention of non-existent class type in docs by @dannymeijer in https://github.com/Nike-Inc/koheesio/pull/138
* [bugfix] **Tableau** - Decimals conversion in HyperFileDataFrameWriter by @maxim-mityutko in https://github.com/Nike-Inc/koheesio/pull/77
* [bugfix] **Tableau** - small fix for Tableau Server path checking by @dannymeijer in https://github.com/Nike-Inc/koheesio/pull/134
* [bugfix] **Snowflake** - replace RunQuery with SnowflakeRunQueryPython by @mikita-sakalouski in https://github.com/Nike-Inc/koheesio/pull/121

## (0.9) New Contributors

Big shout out to all contributors and a heartfelt welcome to our new contributors:

* @louis-paulvlx made their first contribution in https://github.com/Nike-Inc/koheesio/pull/96
* @ToneVDB made their first contribution in https://github.com/Nike-Inc/koheesio/pull/103

## Migrating from v0.8

For users currently using v0.8, consider the following:

* Spark connect is now fully supported. For this to work we've had to introduce several replacement types for pyspark such as DataFrame (i.e. `pyspark.sql.DataFrame` vs `pyspark.sql.connect.DataFrame`) as well as the SparkSession. If you are using custom Step logic in which you reference spark types, take these types from the `koheesio.spark` module instead. This will allow you to use pyspark connect with your custom code also.

* Snowflake was extensively reworked. 
  * To be able to use snowflake, a new `extra` / `feature` was added to the `pyproject.toml` - install this using `koheesio[snowflake]` in order to have access to snowflake python
  * Code for snowflake support was moved to new primary modules:
    * `koheesio.integrations.spark.snowflake` hosts all spark related snowflake code 
    * `koheesio.integrations.snowflake`  hosts the non-spark / pure-python implementations
    * The original API was kept in place through pass-through imports; no immediate code changes should be needed

**Full Changelog**: https://github.com/Nike-Inc/koheesio/compare/koheesio-v0.8.1...koheesio-v0.9.0