from unittest.mock import Mock

import pytest

from koheesio.spark.readers.jdbc import JdbcReader

pytestmark = pytest.mark.spark


class TestJdbcReader:
    """Test the JDBC reader functionality and parameter handling

    The tests in this class define the base behavior for all JDBC-based readers,
    including derived classes like SnowflakeReader.
    """

    common_options = {
        "driver": "driver",
        "url": "url",
        "user": "user",
        "password": "password",
    }

    def test_get_options_wo_extra_options(self) -> None:
        """Test parameter handling without any extra options"""
        jr = JdbcReader(**self.common_options, dbtable="table")
        actual = jr.get_options()
        del actual["password"]  # we don't need to test for this

        expected = {**self.common_options, "dbtable": "table"}
        del expected["password"]  # we don't need to test for this

        assert actual == expected

    def test_get_options_w_extra_options(self) -> None:
        """Test parameter handling with extra options present.
        Validates proper precedence between core and extra parameters."""
        jr = JdbcReader(
            options={
                "foo": "foo",
                "bar": "bar",
            },
            query="unit test",
            dbtable="table",
            **self.common_options,
        )

        actual = jr.get_options()
        del actual["password"]  # we don't need to test for this

        expected = {
            **self.common_options,
            "query": "unit test",
            "foo": "foo",
            "bar": "bar",
        }
        del expected["password"]  # we don't need to test for this

        assert sorted(actual) == sorted(expected)
        
        # Verify dbtable is not in options
        assert "dbtable" not in actual.keys()

    def test_execute_wo_dbtable_and_query(self) -> None:
        """Test that error is raised when neither dbtable nor query is provided"""
        with pytest.raises(ValueError) as e:
            _ = JdbcReader(**self.common_options)
            assert e.type is ValueError

    def test_execute_w_dbtable_and_query(self, dummy_spark: Mock) -> None:  # pylint: disable=unused-argument
        """Test that query takes precedence over dbtable.
        
        Related to TestSnowflakeReader.test_execute_with_dbtable in test_spark_snowflake.py
        """
        jr = JdbcReader(**self.common_options, dbtable="foo", query="bar")
        jr.execute()

        assert jr.df.count() == 3
        assert jr.query == "bar"
        assert dummy_spark.options_dict["query"] == "bar"
        assert jr.dbtable is None
        assert dummy_spark.options_dict.get("dbtable") is None

    def test_execute_w_dbtable(self, dummy_spark: Mock) -> None:  # pylint: disable=unused-argument
        """Test that dbtable is passed correctly to the reader"""
        jr = JdbcReader(**self.common_options, dbtable="foo")
        jr.execute()

        assert jr.df.count() == 3
        assert dummy_spark.options_dict["dbtable"] == "foo"
