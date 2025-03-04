# noinspection PyUnresolvedReferences
"""
Snowflake steps and tasks for Koheesio

Every class in this module is a subclass of `Step` or `BaseModel` and is used to perform operations on Snowflake.

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

from typing import Any, Dict, Generator, List, Optional, Set, Union
from abc import ABC
from contextlib import contextmanager
from functools import partial
import os
import tempfile
from types import ModuleType
from urllib.parse import urlparse

from koheesio import Step
from koheesio.logger import warn
from koheesio.models import (
    BaseModel,
    ExtraParamsMixin,
    Field,
    PrivateAttr,
    SecretStr,
    conlist,
    field_validator,
    model_validator,
)
from koheesio.spark.utils.common import on_databricks

__all__ = [
    "GrantPrivilegesOnFullyQualifiedObject",
    "GrantPrivilegesOnObject",
    "GrantPrivilegesOnTable",
    "GrantPrivilegesOnView",
    "SnowflakeRunQueryPython",
    "SnowflakeBaseModel",
    "SnowflakeStep",
    "SnowflakeTableStep",
    "safe_import_snowflake_connector",
]

# pylint: disable=inconsistent-mro, too-many-lines
# Turning off inconsistent-mro because we are using ABCs and Pydantic models and Tasks together in the same class
# Turning off too-many-lines because we are defining a lot of classes in this file


def __check_access_snowflake_config_dir() -> bool:
    """Check if the Snowflake configuration directory is accessible

    Returns
    -------
    bool
        True if the Snowflake configuration directory is accessible, otherwise False

    Raises
    ------
    RuntimeError
        If `snowflake-connector-python` is not installed
    """
    check_result = False

    try:
        from snowflake.connector.sf_dirs import _resolve_platform_dirs  # noqa: F401

        _resolve_platform_dirs().user_config_path
        check_result = True
    except PermissionError as e:
        warn(f"Snowflake configuration directory is not accessible. Please check the permissions.Catched error: {e}")
    except (ImportError, ModuleNotFoundError) as e:
        raise RuntimeError(
            "You need to have the `snowflake-connector-python` package installed to use the Snowflake steps that are"
            "based around SnowflakeRunQueryPython. You can install this in Koheesio by adding `koheesio[snowflake]` to "
            "your package dependencies.",
        ) from e

    return check_result


def safe_import_snowflake_connector() -> Optional[ModuleType]:
    """Validate that the Snowflake connector is installed

    Returns
    -------
    Optional[ModuleType]
        The Snowflake connector module if it is installed, otherwise None
    """
    is_accessable_sf_conf_dir = __check_access_snowflake_config_dir()

    if not is_accessable_sf_conf_dir and on_databricks():
        snowflake_home: str = tempfile.mkdtemp(prefix="snowflake_tmp_", dir="/tmp")  # nosec B108:ignore bandit check for CWE-377
        os.environ["SNOWFLAKE_HOME"] = snowflake_home
        warn(f"Getting error for snowflake config directory. Going to use temp directory `{snowflake_home}` instead.")
    elif not is_accessable_sf_conf_dir:
        raise PermissionError("Snowflake configuration directory is not accessible. Please check the permissions.")

    try:
        # Keep the import here as it is perfroming resolution of snowflake configuration directory
        from snowflake import connector as snowflake_connector

        return snowflake_connector
    except (ImportError, ModuleNotFoundError):
        warn(
            "You need to have the `snowflake-connector-python` package installed to use the Snowflake steps that are"
            "based around SnowflakeRunQueryPython. You can install this in Koheesio by adding `koheesio[snowflake]` to "
            "your package dependencies.",
            UserWarning,
        )
        return None


SF_DEFAULT_PARAMS = {"sfCompress": "on", "continue_on_error": "off"}


class SnowflakeBaseModel(BaseModel, ExtraParamsMixin, ABC):  # type: ignore[misc]
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
    role : str
        The default security role to use for the session after connecting.
        Alias for `sfRole`.
    warehouse : str
        The default virtual warehouse to use for the session after connecting.
        Alias for `sfWarehouse`.
    authenticator : Optional[str], optional, default=None
        Authenticator for the Snowflake user. Example: "okta.com".
    database : Optional[str], optional, default=None
        The database to use for the session after connecting.
        Alias for `sfDatabase`.
    sfSchema : Optional[str], optional, default=None
        The schema to use for the session after connecting.
        Alias for `schema` ("schema" is a reserved name in Pydantic, so we use `sfSchema` as main name instead).
    options : Optional[Dict[str, Any]], optional, default={"sfCompress": "on", "continue_on_error": "off"}
        Extra options to pass to the Snowflake connector.
    """

    url: str = Field(
        default=...,
        alias="sfURL",
        description="Hostname for the Snowflake account, e.g. <account>.snowflakecomputing.com",
        examples=["example.snowflakecomputing.com"],
    )
    user: str = Field(default=..., alias="sfUser", description="Login name for the Snowflake user")
    password: SecretStr = Field(default=..., alias="sfPassword", description="Password for the Snowflake user")
    role: str = Field(
        default=..., alias="sfRole", description="The default security role to use for the session after connecting"
    )
    warehouse: str = Field(
        default=...,
        alias="sfWarehouse",
        description="The default virtual warehouse to use for the session after connecting",
    )
    authenticator: Optional[str] = Field(
        default=None,
        description="Authenticator for the Snowflake user",
        examples=["okta.com"],
    )
    database: Optional[str] = Field(
        default=None, alias="sfDatabase", description="The database to use for the session after connecting"
    )
    sfSchema: Optional[str] = Field(
        default=..., alias="schema", description="The schema to use for the session after connecting"
    )
    params: Optional[Dict[str, Any]] = Field(
        default_factory=partial(dict, **SF_DEFAULT_PARAMS),
        description="Extra options to pass to the Snowflake connector, by default it includes "
        "'sfCompress': 'on' and  'continue_on_error': 'off'",
        alias="options",
        examples=[{"sfCompress": "on", "continue_on_error": "off"}],
    )

    @property
    def options(self) -> Dict[str, Any]:
        """Shorthand for accessing self.params provided for backwards compatibility"""
        return self

    def get_options(self, by_alias: bool = True, include: Optional[Set[str]] = None) -> Dict[str, Any]:
        """Get the sfOptions as a dictionary.

        Note
        ----
        - Any parameters that are `None` are excluded from the output dictionary.
        - `sfSchema` and `password` are handled separately.
        - The values from both 'options' and 'params' (kwargs / extra params) are included as is.
        - Koheesio specific fields are excluded by default (i.e. `name`, `description`, `format`).

        Parameters
        ----------
        by_alias : bool, optional, default=True
            Whether to use the alias names or not. E.g. `sfURL` instead of `url`
        include : Optional[Set[str]], optional, default=None
            Set of keys to include in the output dictionary. When None is provided, all fields will be returned.
            Note: be sure to include all the keys you need.
        """
        exclude_set = {
            # Exclude koheesio specific fields
            "name",
            "description",
            # params are separately implemented
            "params",
            "options",
            # schema and password have to be handled separately
            "sfSchema",
            "password",
        } - (include or set())

        fields = self.model_dump(
            by_alias=by_alias,
            exclude_none=True,
            exclude=exclude_set,
        )

        # handle schema and password
        fields.update(
            {
                "sfSchema" if by_alias else "schema": self.sfSchema,
                "sfPassword" if by_alias else "password": self.password.get_secret_value(),
            }
        )

        # handle include
        if include:
            # user specified filter
            fields = {key: value for key, value in fields.items() if key in include}
        else:
            # default filter
            include = {"params"}

        # handle options
        if "options" in include:
            options = fields.pop("params", self.params)
            fields.update(**options)

        # handle params
        if "params" in include:
            params = fields.pop("params", self.params)
            fields.update(**params)

        return {key: value for key, value in fields.items() if value}


class SnowflakeStep(SnowflakeBaseModel, Step, ABC):
    """Expands the SnowflakeBaseModel so that it can be used as a Step"""


class SnowflakeTableStep(SnowflakeStep, ABC):
    """Expands the SnowflakeStep, adding a 'table' parameter"""

    table: str = Field(default=..., description="The name of the table")

    @property
    def full_name(self) -> str:
        """
        Returns the fullname of snowflake table based on schema and database parameters.

        Returns
        -------
        str
            Snowflake Complete table name (database.schema.table)
        """
        return f"{self.database}.{self.sfSchema}.{self.table}"


class SnowflakeRunQueryPython(SnowflakeStep):
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

    query: str = Field(default=..., description="The query to run", alias="sql", serialization_alias="query")
    account: Optional[str] = Field(default=None, description="Snowflake Account Name", alias="account")

    # for internal use
    _snowflake_connector: Optional[ModuleType] = PrivateAttr(default_factory=safe_import_snowflake_connector)

    class Output(SnowflakeStep.Output):
        """Output class for RunQueryPython"""

        results: List = Field(default_factory=list, description="The results of the query")

    @model_validator(mode="before")
    def _validate_account(cls, values: Dict) -> Dict:
        """Populate account from URL if not provided"""
        if not values.get("account"):
            parsed_url = urlparse(values.get("url") or values.get("sfURL"))
            base_url = parsed_url.hostname or parsed_url.path
            values["account"] = base_url.split(".")[0]

        return values

    @field_validator("query")
    def validate_query(cls, query: str) -> str:
        """Replace escape characters, strip whitespace, ensure it is not empty"""
        query = query.replace("\\n", "\n").replace("\\t", "\t").strip()
        if not query:
            raise ValueError("Query cannot be empty")
        return query

    def get_options(self, by_alias: bool = False, include: Optional[Set[str]] = None) -> Dict[str, Any]:
        if include is None:
            include = {
                "account",
                "url",
                "authenticator",
                "user",
                "role",
                "warehouse",
                "database",
                "schema",
                "password",
            }
        return super().get_options(by_alias=by_alias, include=include)

    @property
    @contextmanager
    def conn(self) -> Generator:
        if not self._snowflake_connector:
            raise RuntimeError("Snowflake connector is not installed. Please install `snowflake-connector-python`.")

        sf_options = self.get_options()
        _conn = self._snowflake_connector.connect(**sf_options)
        self.log.info(f"Connected to Snowflake account: {sf_options['account']}")

        try:
            from snowflake.connector.connection import logger as snowflake_logger

            _preserve_snowflake_logger = snowflake_logger
            snowflake_logger = self.log
            snowflake_logger.debug("Replace snowflake logger with Koheesio logger")
            yield _conn
        finally:
            if _preserve_snowflake_logger:
                if snowflake_logger:
                    snowflake_logger.debug("Restore snowflake logger")
                snowflake_logger = _preserve_snowflake_logger

            if _conn:
                _conn.close()

    def get_query(self) -> str:
        """allows to customize the query"""
        return self.query

    def execute(self) -> None:
        """Execute the query"""
        with self.conn as conn:
            cursors = conn.execute_string(
                self.get_query(),
            )
            for cursor in cursors:
                self.log.debug(f"Cursor executed: {cursor}")
                self.output.results.extend(cursor.fetchall())


class GrantPrivilegesOnObject(SnowflakeRunQueryPython):
    """
    A wrapper on Snowflake GRANT privileges

    With this Step, you can grant Snowflake privileges to a set of roles on a table, a view, or an object

    See Also
    --------
    https://docs.snowflake.com/en/sql-reference/sql/grant-privilege.html

    Parameters
    ----------
    account : str
        Snowflake Account Name.
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
        user="gid.account@abc.com",
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

    privileges: Union[conlist(str, min_length=1), str] = Field(  # type: ignore[valid-type]
        default=...,
        alias="permissions",
        description="The Privilege/Permission or list of Privileges/Permissions to grant on the given object. "
        "See https://docs.snowflake.com/en/sql-reference/sql/grant-privilege.html",
    )
    roles: Union[conlist(str, min_length=1), str] = Field(  # type: ignore[valid-type]
        default=...,
        alias="role",
        validation_alias="roles",
        description="The Role or list of Roles to grant the privileges to",
    )
    query: str = "GRANT {privileges} ON {type} {object} TO ROLE {role}"

    class Output(SnowflakeRunQueryPython.Output):
        """Output class for GrantPrivilegesOnObject"""

        query: conlist(str, min_length=1) = Field(  # type: ignore[valid-type]
            default=..., description="Query that was executed to grant privileges", validate_default=False
        )

    @model_validator(mode="before")
    def set_roles_privileges(cls, values: dict) -> dict:
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
    def validate_object_and_object_type(self) -> "GrantPrivilegesOnObject":
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

    # noinspection PyMethodOverriding
    def get_query(self, role: str) -> str:
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
        query = self.query.format(
            privileges=",".join(self.privileges),
            type=self.type,
            object=self.object,
            role=role,
        )
        return query

    def execute(self) -> None:
        self.output.query = []
        roles = self.roles

        for role in roles:
            query = self.get_query(role)
            self.output.query.append(query)

            # Create a new instance of SnowflakeRunQueryPython with the current query
            instance = SnowflakeRunQueryPython.from_step(self, query=query)
            instance.execute()
            print(f"{instance.output = }")
            self.output.results.extend(instance.output.results)


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
    def set_object_name(self) -> "GrantPrivilegesOnFullyQualifiedObject":
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
