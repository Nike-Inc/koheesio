# flake8: noqa: F811
from copy import deepcopy
from importlib.metadata import PackageNotFoundError
import os
import types
from unittest import mock

import pytest

from pydantic import ValidationError

import koheesio.integrations.snowflake as sf_module
from koheesio.integrations.snowflake import (
    GrantPrivilegesOnObject,
    GrantPrivilegesOnTable,
    GrantPrivilegesOnView,
    SnowflakeBaseModel,
    SnowflakeRunQueryPython,
    SnowflakeStep,
    SnowflakeTableStep,
    safe_import_snowflake_connector,
)
from koheesio.integrations.snowflake.test_utils import mock_query

mock_query = mock_query

COMMON_OPTIONS = {
    "url": "hostname.com",
    "user": "user",
    "password": "password",
    "database": "db",
    "schema": "schema",
    "role": "role",
    "warehouse": "warehouse",
}


class TestGrantPrivilegesOnObject:
    options = dict(
        **COMMON_OPTIONS,
        account="42",
        object="foo",
        type="TABLE",
        privileges=["DELETE", "SELECT"],
        roles=["role_1", "role_2"],
    )

    def test_execute(self, mock_query):
        """Test that the query is correctly generated"""
        # Arrange
        del self.options["role"]  # role is not required for this test as we are setting "roles"
        mock_query.expected_data = [None]
        expected_query = [
            "GRANT DELETE,SELECT ON TABLE foo TO ROLE role_1",
            "GRANT DELETE,SELECT ON TABLE foo TO ROLE role_2",
        ]

        # Act
        kls = GrantPrivilegesOnObject(**self.options)
        output = kls.execute()

        # Assert - 2 queries are expected, result should be None
        assert output.query == expected_query
        assert output.results == [None, None]


class TestGrantPrivilegesOnTable:
    options = {**COMMON_OPTIONS, **dict(account="42", table="foo", privileges=["SELECT"], roles=["role_1"])}

    def test_execute(self, mock_query):
        """Test that the query is correctly generated"""
        # Arrange
        del self.options["role"]  # role is not required for this test as we are setting "roles"
        mock_query.expected_data = [None]
        expected_query = ["GRANT SELECT ON TABLE db.schema.foo TO ROLE role_1"]

        # Act
        kls = GrantPrivilegesOnTable(**self.options)
        output = kls.execute()

        # Assert - 1 query is expected, result should be None
        assert output.query == expected_query
        assert output.results == mock_query.expected_data


class TestGrantPrivilegesOnView:
    options = {**COMMON_OPTIONS, **dict(account="42", view="foo", privileges=["SELECT"], roles=["role_1"])}

    def test_execute(self, mock_query):
        """Test that the query is correctly generated"""
        # Arrange
        del self.options["role"]  # role is not required for this test as we are setting "roles"
        mock_query.expected_data = [None]
        expected_query = ["GRANT SELECT ON VIEW db.schema.foo TO ROLE role_1"]

        # Act
        kls = GrantPrivilegesOnView(**self.options)
        output = kls.execute()

        # Assert - 1 query is expected, result should be None
        assert output.query == expected_query
        assert output.results == mock_query.expected_data


class TestSnowflakeRunQueryPython:
    def test_mandatory_fields(self):
        """Test that query and account fields are mandatory"""
        with pytest.raises(ValidationError):
            _1 = SnowflakeRunQueryPython(**COMMON_OPTIONS)

        # sql/query and account should work without raising an error
        _2 = SnowflakeRunQueryPython(**COMMON_OPTIONS, sql="SELECT foo", account="42")
        _3 = SnowflakeRunQueryPython(**COMMON_OPTIONS, query="SELECT foo", account="42")

    def test_get_options(self):
        """Test that the options are correctly generated"""
        # Arrange
        expected_query = "SELECT foo"
        kls = SnowflakeRunQueryPython(**COMMON_OPTIONS, sql=expected_query, account="42")

        # Act
        actual_options = kls.get_options()
        query_in_options = kls.get_options(include={"query"}, by_alias=True)

        # Assert
        expected_options = {
            "account": "42",
            "database": "db",
            "password": "password",
            "role": "role",
            "schema": "schema",
            "url": "hostname.com",
            "user": "user",
            "warehouse": "warehouse",
        }
        assert actual_options == expected_options
        assert query_in_options["query"] == expected_query, "query should be returned regardless of the input"

    def test_account_populated_from_url(self):
        kls = SnowflakeRunQueryPython(**COMMON_OPTIONS, sql="SELECT * FROM table")
        assert kls.account == "hostname"

    def test_account_populated_from_url2(self):
        common_options = deepcopy(COMMON_OPTIONS)
        common_options["url"] = "https://host2.host1.snowflakecomputing.com"
        kls = SnowflakeRunQueryPython(**common_options, sql="SELECT * FROM table")
        assert kls.account == "host2"

    def test_account_populated_from_sf_url(self):
        common_options = deepcopy(COMMON_OPTIONS)
        common_options["sfURL"] = common_options.pop("url")
        kls = SnowflakeRunQueryPython(**common_options, sql="SELECT * FROM table")
        assert kls.account == "hostname"

    def test_execute(self, mock_query):
        # Arrange
        query = "SELECT * FROM two_row_table"
        expected_data = [("row1",), ("row2",)]
        mock_query.expected_data = expected_data

        # Act
        instance = SnowflakeRunQueryPython(**COMMON_OPTIONS, query=query, account="42")
        instance.execute()

        # Assert
        mock_query.assert_called_with(query)
        assert instance.output.results == expected_data

    def test_with_missing_dependencies(self):
        """Missing dependency should throw a warning first, and raise an error if execution is attempted"""
        # Arrange -- remove the snowflake connector
        with mock.patch.dict("sys.modules", {"snowflake": None}):
            from koheesio.integrations.snowflake import safe_import_snowflake_connector

            # Act & Assert -- first test for the warning, then test for the error
            match_text = "You need to have the `snowflake-connector-python` package installed"
            with pytest.warns(UserWarning, match=match_text):
                safe_import_snowflake_connector()
            with pytest.warns(UserWarning, match=match_text):
                instance = SnowflakeRunQueryPython(**COMMON_OPTIONS, query="<REDACTED>", account="42")
            with pytest.raises(RuntimeError):
                instance.execute()


class TestSnowflakeBaseModel:
    def test_get_options_using_alias(self):
        """Test that the options are correctly generated using alias"""
        k = SnowflakeBaseModel(
            sfURL="hostname.com",
            sfUser="user",
            sfPassword="password",
            sfDatabase="database",
            sfRole="role",
            sfWarehouse="warehouse",
            schema="schema",
        )
        options = k.get_options()  # alias should be used by default
        assert options["sfURL"] == "hostname.com"
        assert options["sfUser"] == "user"
        assert options["sfDatabase"] == "database"
        assert options["sfRole"] == "role"
        assert options["sfWarehouse"] == "warehouse"
        assert options["sfSchema"] == "schema"

    def test_get_options(self):
        """Test that the options are correctly generated not using alias"""
        k = SnowflakeBaseModel(
            url="hostname.com",
            user="user",
            password="password",
            database="database",
            role="role",
            warehouse="warehouse",
            schema="schema",
        )
        options = k.get_options(by_alias=False)
        assert options["url"] == "hostname.com"
        assert options["user"] == "user"
        assert options["database"] == "database"
        assert options["role"] == "role"
        assert options["warehouse"] == "warehouse"
        assert options["schema"] == "schema"

        # make sure none of the koheesio options are present
        assert "description" not in options
        assert "name" not in options

    def test_get_options_include(self):
        """Test that the options are correctly generated using include"""
        k = SnowflakeBaseModel(
            url="hostname.com",
            user="user",
            password="password",
            database="database",
            role="role",
            warehouse="warehouse",
            schema="schema",
            options={"foo": "bar"},
        )
        options = k.get_options(include={"url", "user", "description", "options"}, by_alias=False)

        # should be present
        assert options["url"] == "hostname.com"
        assert options["user"] == "user"
        assert "description" in options

        # options should be expanded
        assert "options" not in options
        assert options["foo"] == "bar"

        # should not be present
        assert "database" not in options
        assert "role" not in options
        assert "warehouse" not in options
        assert "schema" not in options


class TestSnowflakeStep:
    def test_initialization(self):
        """Test that the Step fields come through correctly"""
        # Arrange
        kls = SnowflakeStep(**COMMON_OPTIONS)

        # Act
        options = kls.get_options()

        # Assert
        assert kls.name == "SnowflakeStep"
        assert kls.description == "Expands the SnowflakeBaseModel so that it can be used as a Step"
        assert (
            "name" not in options and "description" not in options
        ), "koheesio options should not be present in get_options"


class TestSnowflakeTableStep:
    def test_initialization(self):
        """Test that the table is correctly set"""
        kls = SnowflakeTableStep(**COMMON_OPTIONS, table="table")
        assert kls.table == "table"


class TestSnowflakeConfigDir:
    @mock.patch("koheesio.integrations.snowflake.__check_access_snowflake_config_dir", return_value=False)
    @mock.patch("koheesio.integrations.snowflake.on_databricks", return_value=True)
    def test_initialization_on_databricks(self, mock_on_databricks, mock_check_access):
        """Test that the config dir is correctly set"""
        safe_import_snowflake_connector()
        assert os.environ["SNOWFLAKE_HOME"].startswith("/tmp/snowflake_tmp_")

    def test_initialization(self):
        origin_snowflake_home = os.environ.get("SNOWFLAKE_HOME")
        safe_import_snowflake_connector()
        assert os.environ.get("SNOWFLAKE_HOME") == origin_snowflake_home


class TestConnectorVersionGate:
    check_access = staticmethod(getattr(sf_module, "__check_access_snowflake_config_dir"))
    uses_sf_dirs = staticmethod(getattr(sf_module, "__connector_uses_sf_dirs"))

    @pytest.mark.parametrize(
        ("connector_version", "expected"),
        [("4.0.0", True), ("3.12.1", True), ("5.0.0", False), ("5.1.2", False), ("not-a-version", False)],
    )
    def test_uses_sf_dirs_by_version(self, mocker, connector_version, expected):
        mocker.patch.object(sf_module, "version", return_value=connector_version)
        assert self.uses_sf_dirs() is expected

    def test_uses_sf_dirs_false_when_package_missing(self, mocker):
        mocker.patch.object(sf_module, "version", side_effect=PackageNotFoundError)
        assert self.uses_sf_dirs() is False

    def test_check_skipped_on_connector_5(self, mocker):
        mocker.patch.object(sf_module, "version", return_value="5.0.0")
        assert self.check_access() is True

    def test_check_runs_on_connector_4_and_reports_permission_error(self, mocker):
        mocker.patch.object(sf_module, "version", return_value="4.0.0")
        fake_dirs = mock.MagicMock()
        type(fake_dirs.return_value).user_config_path = mock.PropertyMock(side_effect=PermissionError("denied"))
        mocker.patch.dict(
            "sys.modules", {"snowflake.connector.sf_dirs": mock.MagicMock(_resolve_platform_dirs=fake_dirs)}
        )
        with pytest.warns(UserWarning, match="not accessible"):
            assert self.check_access() is False

    def test_check_runs_on_connector_4_and_passes(self, mocker):
        mocker.patch.object(sf_module, "version", return_value="4.0.0")
        mocker.patch.dict("sys.modules", {"snowflake.connector.sf_dirs": mock.MagicMock()})
        assert self.check_access() is True


class TestConnLoggerSwap:
    def _step(self, mocker):
        step = SnowflakeRunQueryPython(**COMMON_OPTIONS, query="SELECT 1")
        fake_connector = mock.MagicMock()
        mocker.patch.object(type(step), "_snowflake_connector", fake_connector, create=True)
        return step, fake_connector.connect.return_value

    def test_conn_works_when_connection_module_has_no_logger(self, mocker):
        """connector>=5: `snowflake.connector.connection` has no `logger` attribute"""
        fake_module = types.ModuleType("snowflake.connector.connection")
        mocker.patch.dict("sys.modules", {"snowflake.connector.connection": fake_module})
        step, conn = self._step(mocker)
        with step.conn as c:
            assert c is conn
        assert not hasattr(fake_module, "logger")
        conn.close.assert_called_once()

    def test_conn_swaps_and_restores_logger_when_present(self, mocker):
        """connector<5: module-level logger is replaced during the context and restored after"""
        original = object()
        fake_module = types.ModuleType("snowflake.connector.connection")
        fake_module.logger = original
        mocker.patch.dict("sys.modules", {"snowflake.connector.connection": fake_module})
        step, conn = self._step(mocker)
        with step.conn:
            assert fake_module.logger is step.log
        assert fake_module.logger is original
        conn.close.assert_called_once()
