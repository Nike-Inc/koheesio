"""
The first test, tests that OktaAccessToken can be instantiated without pyspark
This test should not raise an ImportError

The second test, tests that a sparksession needs pyspark to be available
This test should raise an ImportError hence pytest.raises(ImportError) is expected

"""

from unittest import mock

import pytest

from koheesio.models import SecretStr

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
