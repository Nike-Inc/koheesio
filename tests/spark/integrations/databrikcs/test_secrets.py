from unittest.mock import patch

from conftest import ScopeSecrets

from koheesio.integrations.spark.databricks.secrets import DataBricksSecret


class TestDatabricksSecret:
    def test_set_parent_to_scope(self):
        # Test when parent is not provided
        secret = DataBricksSecret(scope="secret-scope")
        assert secret.parent == "secret_scope"

        # Test when parent is provided
        secret = DataBricksSecret(scope="secret-scope", parent="custom_parent")
        assert secret.parent == "custom_parent"

    @patch("koheesio.integrations.spark.databricks.secrets.DataBricksSecret._client")
    def test_get_secrets_no_alias(self, mock_databricks_client):
        with patch("koheesio.integrations.spark.databricks.utils.on_databricks", return_value=True):
            dd = {
                "key1": "value_of_key1",
                "key2": "value_of_key2",
            }
            databricks = DataBricksSecret(scope="dummy", parent="kafka")
            mock_databricks_client.secrets = ScopeSecrets(dd)
            secrets = databricks._get_secrets()

            assert secrets["key1"] == "value_of_key1"
            assert secrets["key2"] == "value_of_key2"

    @patch("koheesio.integrations.spark.databricks.secrets.DataBricksSecret._client")
    def test_get_secrets_alias(self, mock_databricks_client):
        with patch("koheesio.integrations.spark.databricks.utils.on_databricks", return_value=True):
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
