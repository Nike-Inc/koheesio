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
from koheesio.spark import DataFrame, SparkStep
from koheesio.spark.transformations.transform import Transform

pytestmark = pytest.mark.spark


class TestSparkImportFailures:
    def test_import_error_no_error(self):
        secret = SecretStr("client_secret")
        with mock.patch.dict("sys.modules", {"pyspark": None}):
            from koheesio.sso.okta import OktaAccessToken

            OktaAccessToken(url="https://abc.okta.com", client_id="client_id", client_secret=secret)

    def test_import_error_with_error(self):
        with mock.patch.dict("sys.modules", {"pyspark.sql": None, "koheesio.steps.spark": None}):
            with pytest.raises(ImportError):
                from pyspark.sql import SparkSession

                SparkSession.builder.appName("tests").getOrCreate()

            pass


class TestSparkStep:
    """Test SparkStep class"""

    def test_spark_property_with_session(self):
        spark = SparkSession.builder.appName("pytest-pyspark-local-testing-explicit").master("local[*]").getOrCreate()
        step = SparkStep(spark=spark)
        assert step.spark is spark

    def test_spark_property_without_session(self):
        spark = SparkSession.builder.appName("pytest-pyspark-local-testing-implicit").master("local[*]").getOrCreate()
        step = SparkStep()
        assert step.spark is spark

    def test_get_active_session(self):
        from koheesio.spark.utils.common import get_active_session

        SparkSession.builder.appName("pytest-pyspark-local-testing-active-session").master("local[*]").getOrCreate()

        session = get_active_session()
        assert session

    def test_transformation(self):
        from pyspark.sql import functions as F

        def dummy_function(df: DataFrame):
            return df.withColumn("hello", F.lit("world"))

        test_transformation = Transform(dummy_function)

        assert test_transformation
