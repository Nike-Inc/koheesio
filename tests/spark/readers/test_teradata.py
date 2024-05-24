from unittest import mock

import pytest

from pyspark.sql import SparkSession

from koheesio.steps.readers.teradata import TeradataReader

pytestmark = pytest.mark.spark


class TestTeradataReader:
    common_options = {
        "url": "url",
        "user": "user",
        "password": "password",
        "dbtable": "table",
    }

    def test_get_options(self):
        tr = TeradataReader(**self.common_options)
        o = tr.get_options()

        assert o["driver"] == "com.teradata.jdbc.TeraDriver"
        assert o["fetchsize"] == 2000
        assert o["numPartitions"] == 10

    def test_execute(self, dummy_spark):
        """Method should be callable from parent class"""
        with mock.patch.object(SparkSession, "getActiveSession") as mock_spark:
            mock_spark.return_value = dummy_spark

            tr = TeradataReader(**self.common_options)
            assert tr.execute().df.count() == 1
