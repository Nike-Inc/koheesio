"""
Module for creating and managing Delta tables.
"""

import warnings
from typing import Dict, List, Optional, Union

from py4j.protocol import Py4JJavaError  # type: ignore

from pyspark.sql import DataFrame
from pyspark.sql.types import DataType

from koheesio.models import Field, field_validator, model_validator
from koheesio.spark import AnalysisException, SparkStep
from koheesio.spark.utils import on_databricks


class DeltaTableStep(SparkStep):
    """
    Class for creating and managing Delta tables.

    DeltaTable aims to provide a simple interface to create and manage Delta tables.
    It is a wrapper around the Spark SQL API for Delta tables.

    <!--- TODO: handle errors like this one:
    #     AnalysisException: You are trying to read a Delta table ... that does not have any columns.
    #     Write some new data with the option `mergeSchema = true` to be able to read the table.
    -->

    Example
    -------
    ```python
    from koheesio.steps import DeltaTableStep

    DeltaTableStep(
        table="my_table",
        database="my_database",
        catalog="my_catalog",
        create_if_not_exists=True,
        default_create_properties={
            "delta.randomizeFilePrefixes": "true",
            "delta.checkpoint.writeStatsAsStruct": "true",
            "delta.minReaderVersion": "2",
            "delta.minWriterVersion": "5",
        },
    )
    ```

    Methods
    -------
    get_persisted_properties()
        Get persisted properties of table.
    add_property(key: str, value: str, override: bool = False)
        Alter table and set table property.
    add_properties(properties: Dict[str, str], override: bool = False)
        Alter table and add properties.
    execute()
        Nothing to execute on a Table.
    max_version_ts_of_last_execution(query_predicate: str = None) -> datetime.datetime
        Max version timestamp of last execution. If no timestamp is found, returns 1900-01-01 00:00:00.
        Note: will raise an error if column `VERSION_TIMESTAMP` does not exist.
        <!---
        FIXME: This is not working (local or on Databricks) - asked for reference implementation through
            cop-data-engineering. Response from Sarath: New release of data common utils is coming soon in which this
            function will be fixed. He is indicating to use read_cdc instead.
        -->

    Properties
    ----------
    - name -> str
        Deprecated. Use `.table_name` instead.
    - table_name -> str
        Table name.
    - dataframe -> DataFrame
        Returns a DataFrame to be able to interact with this table.
    - columns -> Optional[List[str]]
        Returns all column names as a list.
    - has_change_type -> bool
        Checks if a column named `_change_type` is present in the table.
    - exists -> bool
        Check if table exists.

    Parameters
    ----------
    table : str
        Table name.
    database : str, optional, default=None
        Database or Schema name.
    catalog : str, optional, default=None
        Catalog name.
    create_if_not_exists : bool, optional, default=False
        Force table creation if it doesn't exist. Note: Default properties will be applied to the table during CREATION.
    default_create_properties : Dict[str, str], optional, default={"delta.randomizeFilePrefixes": "true", "delta.checkpoint.writeStatsAsStruct": "true", "delta.minReaderVersion": "2", "delta.minWriterVersion": "5"}
        Default table properties to be applied during CREATION if `force_creation` True.
    """

    # Order of inputs is reversed to ensure that TableName is always present
    table: str
    database: Optional[str] = Field(
        default=None,
        description="Database or Schema name.",
    )
    catalog: Optional[str] = Field(
        default=None,
        description="Catalog name. "
        "Note: Can be ignored if using a SparkCatalog that does not support catalog notation (e.g. Hive)",
    )

    # Table parameters
    create_if_not_exists: bool = Field(
        default=False,
        alias="force_creation",
        description="Force table creation if it doesn't exist."
        "Note: Default properties will be applied to the table during CREATION.",
    )
    default_create_properties: Dict[str, Union[str, bool, int]] = Field(
        default={
            "delta.randomizeFilePrefixes": "true",
            "delta.checkpoint.writeStatsAsStruct": "true",
            "delta.minReaderVersion": "2",
            "delta.minWriterVersion": "5",
        },
        description="Default table properties to be applied during CREATION if `create_if_not_exists` True",
    )

    @field_validator("default_create_properties")
    def _adjust_default_properties(cls, default_create_properties):
        """Adjust default properties based on environment."""
        if on_databricks():
            default_create_properties["delta.autoOptimize.autoCompact"] = True
            default_create_properties["delta.autoOptimize.optimizeWrite"] = True

        for k, v in default_create_properties.items():
            default_create_properties[k] = str(v) if not isinstance(v, bool) else str(v).lower()

        return default_create_properties

    @model_validator(mode="after")
    def _validate_catalog_database_table(self):
        """Validate that catalog, database/schema, and table are correctly set"""
        database, catalog, table = self.database, self.catalog, self.table

        try:
            self.log.debug(f"Value of `table` input parameter: {table}")
            catalog, database, table = table.split(".")
            self.log.debug("Catalog, database and table were given")
        except ValueError as e:
            if str(e) == "not enough values to unpack (expected 3, got 1)":
                self.log.debug("Only table name was given")
            elif str(e) == "not enough values to unpack (expected 3, got 2)":
                self.log.debug("Only table name and database name were given")
                database, table = table.split(".")
            else:
                raise ValueError(f"Unable to parse values for Table: {table}") from e

        self.database, self.catalog, self.table = database, catalog, table
        return self

    def get_persisted_properties(self) -> Dict[str, str]:
        """Get persisted properties of table.

        Returns
        -------
        Dict[str, str]
            Persisted properties as a dictionary.
        """
        persisted_properties = {}
        raw_options = self.spark.sql(f"SHOW TBLPROPERTIES {self.table_name}").collect()

        for ro in raw_options:
            key, value = ro.asDict().values()
            persisted_properties[key] = value

        return persisted_properties

    @property
    def is_cdf_active(self) -> bool:
        """Check if CDF property is set and activated

        Returns
        -------
        bool
            delta.enableChangeDataFeed property is set to 'true'

        """
        props = self.get_persisted_properties()
        return props.get("delta.enableChangeDataFeed", "false") == "true"

    def add_property(self, key: str, value: Union[str, int, bool], override: bool = False):
        """Alter table and set table property.

        Parameters
        ----------
        key: str
            Property key(name).
        value: Union[str, int, bool]
            Property value.
        override: bool
            Enable override of existing value for property in table.

        """
        persisted_properties = self.get_persisted_properties()
        v_str = str(value) if not isinstance(value, bool) else str(value).lower()

        def _alter_table() -> None:
            property_pair = f"'{key}'='{v_str}'"

            try:
                # noinspection SqlNoDataSourceInspection
                self.spark.sql(f"ALTER TABLE {self.table_name} SET TBLPROPERTIES ({property_pair})")
                self.log.debug(f"Table `{self.table_name}` has been altered. Property `{property_pair}` added.")
            except Py4JJavaError as e:
                msg = f"Property `{key}` can not be applied to table `{self.table_name}`. Exception: {e}"
                self.log.warning(msg)
                warnings.warn(msg)

        if self.exists:
            if key in persisted_properties and persisted_properties[key] != v_str:
                if override:
                    self.log.debug(
                        f"Property `{key}` presents in `{self.table_name}` and has value `{persisted_properties[key]}`."
                        f"Override is enabled.The value will be changed to `{v_str}`."
                    )
                    _alter_table()
                else:
                    self.log.debug(
                        f"Skipping adding property `{key}`, because it is already set "
                        f"for table `{self.table_name}` to `{v_str}`. To override it, provide override=True"
                    )
            else:
                _alter_table()
        else:
            self.default_create_properties[key] = v_str

    def add_properties(self, properties: Dict[str, Union[str, bool, int]], override: bool = False):
        """Alter table and add properties.

        Parameters
        ----------
        properties : Dict[str, Union[str, int, bool]]
            Properties to be added to table.
        override : bool, optional, default=False
            Enable override of existing value for property in table.

        """
        for k, v in properties.items():
            v_str = str(v) if not isinstance(v, bool) else str(v).lower()
            self.add_property(key=k, value=v_str, override=override)

    def execute(self):
        """Nothing to execute on a Table"""

    @property
    def table_name(self) -> str:
        """Fully qualified table name in the form of `catalog.database.table`"""
        return ".".join([n for n in [self.catalog, self.database, self.table] if n])

    @property
    def dataframe(self) -> DataFrame:
        """Returns a DataFrame to be able to interact with this table"""
        return self.spark.table(self.table_name)

    @property
    def columns(self) -> Optional[List[str]]:
        """Returns all column names as a list.

        Example
        -------
        ```python
        DeltaTableStep(...).columns
        ```
        Would for example return `['age', 'name']` if the table has columns `age` and `name`.
        """
        return self.dataframe.columns if self.exists else None

    def get_column_type(self, column: str) -> Optional[DataType]:
        """Get the type of a column in the table.

        Parameters
        ----------
        column : str
            Column name.

        Returns
        -------
        Optional[DataType]
            Column type.
        """
        return self.dataframe.schema[column].dataType if self.columns and column in self.columns else None

    @property
    def has_change_type(self) -> bool:
        """Checks if a column named `_change_type` is present in the table"""
        return "_change_type" in self.columns

    @property
    def exists(self) -> bool:
        """Check if table exists"""
        result = False

        try:
            self.spark.table(self.table_name)
            result = True
        except AnalysisException as e:
            err_msg = str(e).lower()
            if err_msg.startswith("[table_or_view_not_found]") or err_msg.startswith("table or view not found"):
                self.log.error(f"Table `{self.table}` doesn't exist.")
            else:
                raise e

        return result
