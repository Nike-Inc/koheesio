import pytest

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
