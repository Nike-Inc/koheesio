"""
The first test, tests that OktaAccessToken can be instantiated without pyspark
This test should not raise an ImportError

The second test, tests that a sparksession needs pyspark to be available
This test should raise an ImportError hence pytest.raises(ImportError) is expected

"""

from unittest import mock

import pytest

from pyspark.sql import SparkSession

from koheesio.models import SecretStr
from koheesio.spark import SparkStep

pytestmark = pytest.mark.spark


class TestSparkImportFailures:
    def test_import_error_no_error(self):
        secret = SecretStr("client_secret")
        with mock.patch.dict("sys.modules", {"pyspark": None}):
            from koheesio.sso.okta import OktaAccessToken

            OktaAccessToken(url="https://nike.okta.com", client_id="client_id", client_secret=secret)

    def test_import_error_with_error(self):
        with mock.patch.dict("sys.modules", {"pyspark.sql": None, "koheesio.steps.spark": None}):
            with pytest.raises(ImportError):
                from pyspark.sql import SparkSession

                SparkSession.builder.appName("tests").getOrCreate()

            pass


class TestSparkStep:
    """Test SparkStep class"""

    @pytest.fixture(scope="function")
    def spark_session(self):
        """Each test gets a fresh SparkSession, ensuring no shared state between tests."""
        spark = SparkSession.builder.appName("pytest-pyspark-local-testing").master("local[*]").getOrCreate()
        yield spark
        spark.stop()

    def test_spark_property_with_session(self, spark_session):
        step = SparkStep(spark=spark_session)
        assert step.spark is spark_session

    def test_spark_property_without_session(self, spark_session):
        step = SparkStep()
        assert step.spark is spark_session
