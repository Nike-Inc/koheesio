from unittest.mock import patch

from conftest import ScopeSecrets

from koheesio.spark.secrets.databricks import DataBricksSecret


class TestDatabricksSecret:
    def test_set_parent_to_scope(self):
        # Test when parent is not provided
        secret = DataBricksSecret(scope="secret-scope")
        assert secret.parent == "secret_scope"

        # Test when parent is provided
        secret = DataBricksSecret(scope="secret-scope", parent="custom_parent")
        assert secret.parent == "custom_parent"

    @patch("koheesio.spark.secrets.databricks.DataBricksSecret._client")
    def test_get_secrets_no_alias(self, mock_databricks_client):
        dd = {
            "key1": "value_of_key1",
            "key2": "value_of_key2",
        }
        databricks = DataBricksSecret(scope="dummy", parent="kafka")
        mock_databricks_client.secrets = ScopeSecrets(dd)
        secrets = databricks._get_secrets()

        assert secrets["key1"] == "value_of_key1"
        assert secrets["key2"] == "value_of_key2"

    @patch("koheesio.spark.secrets.databricks.DataBricksSecret._client")
    def test_get_secrets_with_alias(self, mock_databricks_client):
        dd = {
            "key1": "value_of_key1",
            "key2": "value_of_key2",
        }
        alias = {
            "key1": "new_name_key1",
            "key2": "new_name_key2",
        }
        databricks = DataBricksSecret(scope="dummy", parent="kafka", alias=alias)
        mock_databricks_client.secrets = ScopeSecrets(dd)
        secrets = databricks._get_secrets()

        assert secrets["new_name_key1"] == "value_of_key1"
        assert secrets["new_name_key2"] == "value_of_key2"
