"""
Snowflake steps and tasks for Koheesio

Every class in this module is a subclass of `Step` or `Task` and is used to perform operations on Snowflake.

Notes
-----
Every Step in this module is based on [SnowflakeBaseModel](./snowflake.md#koheesio.spark.snowflake.SnowflakeBaseModel).
The following parameters are available for every Step.

Parameters
----------
url : str
    Hostname for the Snowflake account, e.g. <account>.snowflakecomputing.com.
    Alias for `sfURL`.
user : str
    Login name for the Snowflake user.
    Alias for `sfUser`.
password : SecretStr
    Password for the Snowflake user.
    Alias for `sfPassword`.
database : str
    The database to use for the session after connecting.
    Alias for `sfDatabase`.
sfSchema : str
    The schema to use for the session after connecting.
    Alias for `schema` ("schema" is a reserved name in Pydantic, so we use `sfSchema` as main name instead).
role : str
    The default security role to use for the session after connecting.
    Alias for `sfRole`.
warehouse : str
    The default virtual warehouse to use for the session after connecting.
    Alias for `sfWarehouse`.
authenticator : Optional[str], optional, default=None
    Authenticator for the Snowflake user. Example: "okta.com".
options : Optional[Dict[str, Any]], optional, default={"sfCompress": "on", "continue_on_error": "off"}
    Extra options to pass to the Snowflake connector.
format : str, optional, default="snowflake"
    The default `snowflake` format can be used natively in Databricks, use `net.snowflake.spark.snowflake` in other
    environments and make sure to install required JARs.
"""

from __future__ import annotations
import json
from collections.abc import Iterable
from logging import warn
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from abc import ABC
from textwrap import dedent

from koheesio import Step, StepOutput
from koheesio.models import (
    BaseModel,
    ExtraParamsMixin,
    Field,
    SecretStr,
    conlist,
    field_validator,
    model_validator,
)

__all__ = [
    "GrantPrivilegesOnFullyQualifiedObject",
    "GrantPrivilegesOnObject",
    "GrantPrivilegesOnTable",
    "GrantPrivilegesOnView",
    "RunQuery",
    "SnowflakeRunQueryPython",
    "SnowflakeBaseModel",
    "SnowflakeStep",
    "SnowflakeTableStep",
    "TableExists",
]

# pylint: disable=inconsistent-mro, too-many-lines
# Turning off inconsistent-mro because we are using ABCs and Pydantic models and Tasks together in the same class
# Turning off too-many-lines because we are defining a lot of classes in this file


class SnowflakeBaseModel(BaseModel, ExtraParamsMixin, ABC):
    """
    BaseModel for setting up Snowflake Driver options.

    Notes
    -----
    * Snowflake is supported natively in Databricks 4.2 and newer:
        https://docs.snowflake.com/en/user-guide/spark-connector-databricks
    * Refer to Snowflake docs for the installation instructions for non-Databricks environments:
        https://docs.snowflake.com/en/user-guide/spark-connector-install
    * Refer to Snowflake docs for connection options:
        https://docs.snowflake.com/en/user-guide/spark-connector-use#setting-configuration-options-for-the-connector

    Parameters
    ----------
    url : str
        Hostname for the Snowflake account, e.g. <account>.snowflakecomputing.com.
        Alias for `sfURL`.
    user : str
        Login name for the Snowflake user.
        Alias for `sfUser`.
    password : SecretStr
        Password for the Snowflake user.
        Alias for `sfPassword`.
    database : str
        The database to use for the session after connecting.
        Alias for `sfDatabase`.
    sfSchema : str
        The schema to use for the session after connecting.
        Alias for `schema` ("schema" is a reserved name in Pydantic, so we use `sfSchema` as main name instead).
    role : str
        The default security role to use for the session after connecting.
        Alias for `sfRole`.
    warehouse : str
        The default virtual warehouse to use for the session after connecting.
        Alias for `sfWarehouse`.
    authenticator : Optional[str], optional, default=None
        Authenticator for the Snowflake user. Example: "okta.com".
    options : Optional[Dict[str, Any]], optional, default={"sfCompress": "on", "continue_on_error": "off"}
        Extra options to pass to the Snowflake connector.
    format : str, optional, default="snowflake"
        The default `snowflake` format can be used natively in Databricks, use `net.snowflake.spark.snowflake` in other
        environments and make sure to install required JARs.
    """

    url: str = Field(
        default=...,
        alias="sfURL",
        description="Hostname for the Snowflake account, e.g. <account>.snowflakecomputing.com",
        examples=["example.snowflakecomputing.com"],
    )
    user: str = Field(default=..., alias="sfUser", description="Login name for the Snowflake user")
    password: SecretStr = Field(default=..., alias="sfPassword", description="Password for the Snowflake user")
    authenticator: Optional[str] = Field(
        default=None,
        description="Authenticator for the Snowflake user",
        examples=["okta.com"],
    )
    database: str = Field(
        default=..., alias="sfDatabase", description="The database to use for the session after connecting"
    )
    sfSchema: str = Field(default=..., alias="schema", description="The schema to use for the session after connecting")
    role: str = Field(
        default=..., alias="sfRole", description="The default security role to use for the session after connecting"
    )
    warehouse: str = Field(
        default=...,
        alias="sfWarehouse",
        description="The default virtual warehouse to use for the session after connecting",
    )
    options: Optional[Dict[str, Any]] = Field(
        default={"sfCompress": "on", "continue_on_error": "off"},
        description="Extra options to pass to the Snowflake connector",
    )
    format: str = Field(
        default="snowflake",
        description="The default `snowflake` format can be used natively in Databricks, use "
        "`net.snowflake.spark.snowflake` in other environments and make sure to install required JARs.",
    )

    def get_options(self, by_alias: bool = True, include: Optional[List[str]] = None) -> Dict[str, Any]:
        """Get the sfOptions as a dictionary.

        Parameters
        ----------
        by_alias : bool, optional, default=True
            Whether to use the alias names or not. E.g. `sfURL` instead of `url`
        include : List[str], optional
            List of keys to include in the output dictionary
        """
        _model_dump_options = {
            "by_alias": by_alias,
            "exclude_none": True,
            "exclude": {
                # Exclude koheesio specific fields
                "params", "name", "description", "format"
                # options should be specifically implemented
                "options",
                # schema and password have to be handled separately
                "sfSchema", "password",
            }
        }
        if include:
            _model_dump_options["include"] = {*include}

        options = self.model_dump(**_model_dump_options)

        # handle schema and password
        options.update(
            {
                "sfSchema" if by_alias else "schema": self.sfSchema,
                "sfPassword" if by_alias else "password": self.password.get_secret_value(),
            }
        )

        return {
            key: value
            for key, value in {
                **self.options,
                **options,
                **self.params,
            }.items()
            if value is not None
        }


class SnowflakeStep(SnowflakeBaseModel, Step, ABC):
    """Expands the SnowflakeBaseModel so that it can be used as a Step"""


class SnowflakeTableStep(SnowflakeStep, ABC):
    """Expands the SnowflakeStep, adding a 'table' parameter"""

    table: str = Field(default=..., description="The name of the table", alias="dbtable")

    @property
    def full_name(self):
        """
        Returns the fullname of snowflake table based on schema and database parameters.

        Returns
        -------
        str
            Snowflake Complete tablename (database.schema.table)
        """
        return f"{self.database}.{self.sfSchema}.{self.table}"


class SnowflakeRunQueryBase(SnowflakeStep, ABC):
    """Base class for RunQuery and RunQueryPython"""

    query: str = Field(default=..., description="The query to run", alias="sql")

    @field_validator("query")
    def validate_query(cls, query):
        """Replace escape characters"""
        return query.replace("\\n", "\n").replace("\\t", "\t").strip()


QueryResults = List[Tuple[Any]]
"""Type alias for the results of a query"""


class SnowflakeRunQueryPython(SnowflakeRunQueryBase):
    """
    Run a query on Snowflake using the Python connector

    Example
    -------
    ```python
    RunQueryPython(
        database="MY_DB",
        schema="MY_SCHEMA",
        warehouse="MY_WH",
        user="account",
        password="***",
        role="APPLICATION.SNOWFLAKE.ADMIN",
        query="CREATE TABLE test (col1 string)",
    ).execute()
    ```
    """
    snowflake_conn: Any = None

    @model_validator(mode="after")
    def validate_snowflake_connector(self):
        """Validate that the Snowflake connector is installed"""
        try:
            from snowflake import connector as snowflake_connector
            self.snowflake_conn = snowflake_connector
        except ImportError as e:
            warn(
                "You need to have the `snowflake-connector-python` package installed to use the Snowflake steps that "
                "are based around SnowflakeRunQueryPython. You can install this in Koheesio by adding "
                "`koheesio[snowflake]` to your package dependencies."
            )
        return self

    class Output(StepOutput):
        """Output class for RunQueryPython"""

        results: Optional[QueryResults] = Field(default=..., description="The results of the query")

    @property
    def conn(self):
        sf_options = dict(
            url=self.url,
            user=self.user,
            role=self.role,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.sfSchema,
            authenticator=self.authenticator,
        )
        return self.snowflake_conn.connect(**self.get_options(by_alias=False))

    @property
    def cursor(self):
        return self.conn.cursor()

    def execute(self) -> None:
        """Execute the query"""
        self.conn.cursor().execute(self.query)
        self.conn.close()


RunQuery = SnowflakeRunQueryPython
"""Added for backwards compatibility"""


class TableExists(SnowflakeTableStep):
    """
    Check if the table exists in Snowflake by using INFORMATION_SCHEMA.

    Example
    -------
    ```python
    k = TableExists(
        url="foo.snowflakecomputing.com",
        user="YOUR_USERNAME",
        password="***",
        database="db",
        schema="schema",
        table="table",
    )
    ```
    """

    class Output(StepOutput):
        """Output class for TableExists"""

        exists: bool = Field(default=..., description="Whether or not the table exists")

    def execute(self):
        query = (
            dedent(
                # Force upper case, due to case-sensitivity of where clause
                f"""
                SELECT *
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_CATALOG     = '{self.database}'
                  AND TABLE_SCHEMA      = '{self.sfSchema}'
                  AND TABLE_TYPE        = 'BASE TABLE'
                  AND upper(TABLE_NAME) = '{self.table.upper()}'
                """  # nosec B608: hardcoded_sql_expressions
            )
            .upper()
            .strip()
        )

        self.log.debug(f"Query that was executed to check if the table exists:\n{query}")

        df = Query(**self.get_options(), query=query).read()

        exists = df.count() > 0
        self.log.info(
            f"Table '{self.database}.{self.sfSchema}.{self.table}' {'exists' if exists else 'does not exist'}"
        )
        self.output.exists = exists


class GrantPrivilegesOnObject(SnowflakeStep):
    """
    A wrapper on Snowflake GRANT privileges

    With this Step, you can grant Snowflake privileges to a set of roles on a table, a view, or an object

    See Also
    --------
    https://docs.snowflake.com/en/sql-reference/sql/grant-privilege.html

    Parameters
    ----------
    warehouse : str
        The name of the warehouse. Alias for `sfWarehouse`
    user : str
        The username. Alias for `sfUser`
    password : SecretStr
        The password. Alias for `sfPassword`
    role : str
        The role name
    object : str
        The name of the object to grant privileges on
    type : str
        The type of object to grant privileges on, e.g. TABLE, VIEW
    privileges : Union[conlist(str, min_length=1), str]
        The Privilege/Permission or list of Privileges/Permissions to grant on the given object.
    roles : Union[conlist(str, min_length=1), str]
        The Role or list of Roles to grant the privileges to

    Example
    -------
    ```python
    GrantPermissionsOnTable(
        object="MY_TABLE",
        type="TABLE",
        warehouse="MY_WH",
        user="gid.account@nike.com",
        password=Secret("super-secret-password"),
        role="APPLICATION.SNOWFLAKE.ADMIN",
        permissions=["SELECT", "INSERT"],
    ).execute()
    ```

    In this example, the `APPLICATION.SNOWFLAKE.ADMIN` role will be granted `SELECT` and `INSERT` privileges on
    the `MY_TABLE` table using the `MY_WH` warehouse.
    """

    object: str = Field(default=..., description="The name of the object to grant privileges on")
    type: str = Field(default=..., description="The type of object to grant privileges on, e.g. TABLE, VIEW")

    privileges: Union[conlist(str, min_length=1), str] = Field(
        default=...,
        alias="permissions",
        description="The Privilege/Permission or list of Privileges/Permissions to grant on the given object. "
        "See https://docs.snowflake.com/en/sql-reference/sql/grant-privilege.html",
    )
    roles: Union[conlist(str, min_length=1), str] = Field(
        default=...,
        alias="role",
        validation_alias="roles",
        description="The Role or list of Roles to grant the privileges to",
    )

    class Output(SnowflakeStep.Output):
        """Output class for GrantPrivilegesOnObject"""

        query: conlist(str, min_length=1) = Field(
            default=..., description="Query that was executed to grant privileges", validate_default=False
        )

    @model_validator(mode="before")
    def set_roles_privileges(cls, values):
        """Coerce roles and privileges to be lists if they are not already."""
        roles_value = values.get("roles") or values.get("role")
        privileges_value = values.get("privileges")

        if not (roles_value and privileges_value):
            raise ValueError("You have to specify roles AND privileges when using 'GrantPrivilegesOnObject'.")

        # coerce values to be lists
        values["roles"] = [roles_value] if isinstance(roles_value, str) else roles_value
        values["role"] = values["roles"][0]  # hack to keep the validator happy
        values["privileges"] = [privileges_value] if isinstance(privileges_value, str) else privileges_value

        return values

    @model_validator(mode="after")
    def validate_object_and_object_type(self):
        """Validate that the object and type are set."""
        object_value = self.object
        if not object_value:
            raise ValueError("You must provide an `object`, this should be the name of the object. ")

        object_type = self.type
        if not object_type:
            raise ValueError(
                "You must provide a `type`, e.g. TABLE, VIEW, DATABASE. "
                "See https://docs.snowflake.com/en/sql-reference/sql/grant-privilege.html"
            )

        return self

    def get_query(self, role: str):
        """Build the GRANT query

        Parameters
        ----------
        role: str
            The role name

        Returns
        -------
        query : str
            The Query that performs the grant
        """
        query = f"GRANT {','.join(self.privileges)} ON {self.type} {self.object} TO ROLE {role}".upper()
        return query

    def execute(self):
        self.output.query = []
        roles = self.roles

        for role in roles:
            query = self.get_query(role)
            self.output.query.append(query)
            RunQuery(**self.get_options(), query=query).execute()


class GrantPrivilegesOnFullyQualifiedObject(GrantPrivilegesOnObject):
    """Grant Snowflake privileges to a set of roles on a fully qualified object, i.e. `database.schema.object_name`

    This class is a subclass of `GrantPrivilegesOnObject` and is used to grant privileges on a fully qualified object.
    The advantage of using this class is that it sets the object name to be fully qualified, i.e.
    `database.schema.object_name`.

    Meaning, you can set the `database`, `schema` and `object` separately and the object name will be set to be fully
    qualified, i.e. `database.schema.object_name`.

    Example
    -------
    ```python
    GrantPrivilegesOnFullyQualifiedObject(
        database="MY_DB",
        schema="MY_SCHEMA",
        warehouse="MY_WH",
        ...
        object="MY_TABLE",
        type="TABLE",
        ...
    )
    ```

    In this example, the object name will be set to be fully qualified, i.e. `MY_DB.MY_SCHEMA.MY_TABLE`.
    If you were to use `GrantPrivilegesOnObject` instead, you would have to set the object name to be fully qualified
    yourself.
    """

    @model_validator(mode="after")
    def set_object_name(self):
        """Set the object name to be fully qualified, i.e. database.schema.object_name"""
        # database, schema, obj_name
        db = self.database
        schema = self.model_dump()["sfSchema"]  # since "schema" is a reserved name
        obj_name = self.object

        self.object = f"{db}.{schema}.{obj_name}"

        return self


class GrantPrivilegesOnTable(GrantPrivilegesOnFullyQualifiedObject):
    """Grant Snowflake privileges to a set of roles on a table"""

    type: str = "TABLE"
    object: str = Field(
        default=...,
        alias="table",
        description="The name of the Table to grant Privileges on. This should be just the name of the table; so "
        "without Database and Schema, use sfDatabase/database and sfSchema/schema to set those instead.",
    )


class GrantPrivilegesOnView(GrantPrivilegesOnFullyQualifiedObject):
    """Grant Snowflake privileges to a set of roles on a view"""

    type: str = "VIEW"
    object: str = Field(
        default=...,
        alias="view",
        description="The name of the View to grant Privileges on. This should be just the name of the view; so "
        "without Database and Schema, use sfDatabase/database and sfSchema/schema to set those instead.",
    )


class TagSnowflakeQuery(Step, ExtraParamsMixin):
    """
    Provides Snowflake query tag pre-action that can be used to easily find queries through SF history search
    and further group them for debugging and cost tracking purposes.

    Takes in query tag attributes as kwargs and additional Snowflake options dict that can optionally contain
    other set of pre-actions to be applied to a query, in that case existing pre-action aren't dropped, query tag
    pre-action will be added to them.

    Passed Snowflake options dictionary is not modified in-place, instead anew dictionary containing updated pre-actions
    is returned.

    Notes
    -----
    See this article for explanation: https://select.dev/posts/snowflake-query-tags

    Arbitrary tags can be applied, such as team, dataset names, business capability, etc.

    Example
    -------
    #### Using `options` parameter
    ```python
    query_tag = AddQueryTag(
        options={"preactions": "ALTER SESSION"},
        task_name="cleanse_task",
        pipeline_name="ingestion-pipeline",
        etl_date="2022-01-01",
        pipeline_execution_time="2022-01-01T00:00:00",
        task_execution_time="2022-01-01T01:00:00",
        environment="dev",
        trace_id="e0fdec43-a045-46e5-9705-acd4f3f96045",
        span_id="cb89abea-1c12-471f-8b12-546d2d66f6cb",
        ),
    ).execute().options
    ```
    In this example, the query tag pre-action will be added to the Snowflake options.

    #### Using `preactions` parameter
    Instead of using `options` parameter, you can also use `preactions` parameter to provide existing preactions.
    ```python
    query_tag = AddQueryTag(
        preactions="ALTER SESSION"
        ...
    ).execute().options
    ```

    The result will be the same as in the previous example.

    #### Using `get_options` method
    The shorthand method `get_options` can be used to get the options dictionary.
    ```python
    query_tag = AddQueryTag(...).get_options()
    ```
    """

    options: Dict = Field(
        default_factory=dict, description="Additional Snowflake options, optionally containing additional preactions")

    preactions: Optional[str] = Field(
        default="", description="Existing preactions from Snowflake options"
    )

    class Output(StepOutput):
        """Output class for AddQueryTag"""

        options: Dict = Field(default=..., description="Snowflake options dictionary with added query tag preaction")

    def execute(self) -> TagSnowflakeQuery.Output:
        """Add query tag preaction to Snowflake options"""
        tag_json = json.dumps(self.extra_params, indent=4, sort_keys=True)
        tag_preaction = f"ALTER SESSION SET QUERY_TAG = '{tag_json}';"
        preactions = self.options.get("preactions", self.preactions)
        # update options with new preactions
        self.output.options = {**self.options, "preactions": f"{preactions}\n{tag_preaction}".strip()}

    def get_options(self) -> Dict:
        """shorthand method to get the options dictionary

        Functionally equivalent to running `execute().options`

        Returns
        -------
        Dict
            Snowflake options dictionary with added query tag preaction
        """
        return self.execute().options
