from unittest import mock

import pytest

from pyspark.sql import SparkSession

from koheesio.spark.readers.hana import HanaReader

pytestmark = pytest.mark.spark


class TestHanaReader:
    common_options = {
        "url": "url",
        "user": "user",
        "password": "password",
        "dbtable": "table",
    }

    def test_get_options(self):
        hana = HanaReader(**self.common_options)
        o = hana.get_options()

        assert o["driver"] == "com.sap.db.jdbc.Driver"
        assert o["fetchsize"] == 2000
        assert o["numPartitions"] == 10

    def test_execute(self, dummy_spark):
        """Method should be callable from parent class"""
        with mock.patch.object(SparkSession, "getActiveSession") as mock_spark:
            mock_spark.return_value = dummy_spark

            hana = HanaReader(**self.common_options)
            assert hana.execute().df.count() == 1
