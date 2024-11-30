"""
Module containing Snowflake reader classes.

This module contains classes for reading data from Snowflake. The classes are used to create a Spark DataFrame from a
Snowflake table or a query.

Classes
-------
SnowflakeReader
    Reader for Snowflake tables.
Query
    Reader for Snowflake queries.
DbTableQuery
    Reader for Snowflake queries that return a single row.

Notes
-----
The classes are defined in the koheesio.steps.integrations.snowflake module; this module simply inherits from the
classes defined there.

See Also
--------
- [koheesio.spark.readers.Reader](index.md#koheesio.spark.readers.Reader)
    Base class for all Readers.
- [koheesio.spark.snowflake](../snowflake.md)
    Module containing Snowflake classes.

More detailed class descriptions can be found in the class docstrings.
"""

from koheesio.spark.snowflake import DbTableQuery, Query, SnowflakeReader

__all__ = ["SnowflakeReader", "Query", "DbTableQuery"]
