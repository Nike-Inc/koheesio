# flake8: noqa: F811
from unittest import mock

from pydantic_core._pydantic_core import ValidationError
import pytest

from koheesio.integrations.snowflake import (
    GrantPrivilegesOnObject,
    GrantPrivilegesOnTable,
    GrantPrivilegesOnView,
    SnowflakeBaseModel,
    SnowflakeRunQueryPython,
    SnowflakeStep,
    SnowflakeTableStep,
)
from koheesio.integrations.snowflake.test_utils import mock_query

COMMON_OPTIONS = {
    "url": "url",
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
            "url": "url",
            "user": "user",
            "warehouse": "warehouse",
        }
        assert actual_options == expected_options
        assert query_in_options["query"] == expected_query, "query should be returned regardless of the input"

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
            sfURL="url",
            sfUser="user",
            sfPassword="password",
            sfDatabase="database",
            sfRole="role",
            sfWarehouse="warehouse",
            schema="schema",
        )
        options = k.get_options()  # alias should be used by default
        assert options["sfURL"] == "url"
        assert options["sfUser"] == "user"
        assert options["sfDatabase"] == "database"
        assert options["sfRole"] == "role"
        assert options["sfWarehouse"] == "warehouse"
        assert options["sfSchema"] == "schema"

    def test_get_options(self):
        """Test that the options are correctly generated not using alias"""
        k = SnowflakeBaseModel(
            url="url",
            user="user",
            password="password",
            database="database",
            role="role",
            warehouse="warehouse",
            schema="schema",
        )
        options = k.get_options(by_alias=False)
        assert options["url"] == "url"
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
            url="url",
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
        assert options["url"] == "url"
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
