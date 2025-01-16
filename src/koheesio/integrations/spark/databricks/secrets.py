"""Module for retrieving secrets from DataBricks Scopes.

Secrets are stored as SecretContext and can be accessed accordingly.

See DataBricksSecret for more information.
"""

from typing import Dict, Optional
import re

from pyspark.sql import SparkSession

from koheesio.integrations.spark.databricks.utils import get_dbutils
from koheesio.models import Field, model_validator
from koheesio.secrets import Secret


class DataBricksSecret(Secret):
    """
    Retrieve secrets from DataBricks secret scope and wrap them into Context class for easy access.
    All secrets are stored under the "secret" root and "parent". "Parent" either derived from the
    secure scope by replacing "/" and "-", or manually provided by the user.
    Secrets are wrapped into the pydantic.SecretStr.

    Examples
    ---------

    ```python
    context = {
        "secrets": {
            "parent": {
                "webhook": SecretStr("**********"),
                "description": SecretStr("**********"),
            }
        }
    }
    ```

    Values can be decoded like this:
    ```python
    context.secrets.parent.webhook.get_secret_value()
    ```
    or if working with dictionary is preferable:
    ```python
    for key, value in context.get_all().items():
        value.get_secret_value()
    ```
    """

    scope: str = Field(description="Scope")
    alias: Optional[Dict[str, str]] = Field(default_factory=dict, description="Alias for secret keys")

    @model_validator(mode="before")
    def _set_parent_to_scope(cls, values):
        """
        Set default value for `parent` parameter on model initialization when it was not
        explicitly set by the user. In this scenario scope will be used:

        'secret-scope' -> secret_scope
        """
        regex = re.compile(r"[/-]")
        path = values.get("scope")

        if not values.get("parent"):
            values["parent"] = regex.sub("_", path)

        return values

    @property
    def _client(self):
        """
        Instantiated Databricks client.
        """

        return get_dbutils(SparkSession.getActiveSession())  # type: ignore

    def _get_secrets(self):
        """Dictionary of secrets."""
        all_keys = (secret_meta.key for secret_meta in self._client.secrets.list(scope=self.scope))
        secret_data = {}

        for key in all_keys:
            key_name = key if not (self.alias and self.alias.get(key)) else self.alias[key]  # pylint: disable=E1101
            secret_data[key_name] = self._client.secrets.get(scope=self.scope, key=key)

        return secret_data
