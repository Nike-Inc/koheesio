from unittest import mock

import pytest

from pyspark.sql import SparkSession

from koheesio.spark.readers.jdbc import JdbcReader

pytestmark = pytest.mark.spark


class TestJdbcReader:
    common_options = {
        "driver": "driver",
        "url": "url",
        "user": "user",
        "password": "password",
    }

    def test_get_options_wo_extra_options(self):
        jr = JdbcReader(**self.common_options)
        actual = jr.get_options()
        del actual["password"]  # we don't need to test for this

        expected = {**self.common_options}
        del expected["password"]  # we don't need to test for this

        assert actual == expected

    def test_get_options_w_extra_options(self):
        jr = JdbcReader(
            options={
                "foo": "foo",
                "bar": "bar",
            },
            **self.common_options,
        )

        actual = jr.get_options()
        del actual["password"]  # we don't need to test for this

        expected = {
            **self.common_options,
            "foo": "foo",
            "bar": "bar",
        }
        del expected["password"]  # we don't need to test for this

        assert actual == expected

    def test_execute_wo_dbtable_and_query(self):
        jr = JdbcReader(**self.common_options)
        with pytest.raises(ValueError) as e:
            jr.execute()
            assert e.type is ValueError

    def test_execute_w_dbtable_and_query(self, dummy_spark):
        with mock.patch.object(SparkSession, "getActiveSession") as mock_spark:
            mock_spark.return_value = dummy_spark

            jr = JdbcReader(**self.common_options, dbtable="foo", query="bar")
            jr.execute()

            assert jr.df.count() == 1
            assert mock_spark.return_value.options_dict["query"] == "bar"
            assert "dbtable" not in mock_spark.return_value.options_dict

    def test_execute_w_dbtable(self, dummy_spark):
        with mock.patch.object(SparkSession, "getActiveSession") as mock_spark:
            mock_spark.return_value = dummy_spark

            jr = JdbcReader(**self.common_options, dbtable="foo")
            jr.execute()

            assert jr.df.count() == 1
            assert mock_spark.return_value.options_dict["dbtable"] == "foo"
