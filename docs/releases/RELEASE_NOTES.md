## What's Changed

v0.9 brings many changes to the spark module, allowing support for pyspark connect along with a bunch of bug fixes and some new features. Additionally, the snowflake implementation is significantly reworked now relying on a pure python implementation for interacting with Snowflake outside of spark.

### New features / Refactors
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

### Bug fixes
The following bugfixes are included with 0.9:

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

## New Contributors
Big shout out to all contributors and a heartfelt welcome to our new contributors:
* @louis-paulvlx made their first contribution in https://github.com/Nike-Inc/koheesio/pull/96
* @ToneVDB made their first contribution in https://github.com/Nike-Inc/koheesio/pull/103

## Migrating from v0.8
For users currently using v0.8, consider the following:
- Spark connect is now fully supported. For this to work we've had to introduce several replacement types for pyspark such as DataFrame (i.e. `pyspark.sql.DataFrame` vs `pyspark.sql.connect.DataFrame`) as well as the SparkSession. If you are using custom Step logic in which you reference spark types, take these types from the `koheesio.spark` module instead. This will allow you to use pyspark connect with your custom code also.

- Snowflake was extensively reworked. 
    - To be able to use snowflake, a new `extra` / `feature` was added to the `pyproject.toml` - install this using `koheesio[snowflake]` in order to have access to snowflake python
    - Code for snowflake support was moved to new primary modules:
        - `koheesio.integrations.spark.snowflake` hosts all spark related snowflake code 
        - `koheesio.integrations.snowflake`  hosts the non-spark / pure-python implementations
        - The original API was kept in place through pass-through imports; no immediate code changes should be needed

**Full Changelog**: https://github.com/Nike-Inc/koheesio/compare/koheesio-v0.8.1...koheesio-v0.9.0