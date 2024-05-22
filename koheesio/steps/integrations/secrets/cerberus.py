"""Module for retrieving secrets from Cerberus.

Secrets are stored as SecretContext and can be accessed accordingly.

See CerberusSecret for more information.
"""

import os
import re
from typing import Optional

from boto3 import Session
from cerberus.client import CerberusClient

from koheesio.models import Field, SecretStr, model_validator
from koheesio.steps.integrations.secrets import Secret


class CerberusSecret(Secret):
    """
    Retrieve secrets from Cerberus and wrap them into Context class for easy access.
    All secrets are stored under the "secret" root and "parent". "Parent" either derived from the
    secure data path by replacing "/" and "-", or manually provided by the user.
    Secrets are wrapped into the pydantic.SecretStr.

    Example:
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

    url: str = Field(default=..., description="Cerberus URL, eg. https://cerberus.domain.com")
    path: str = Field(default=..., description="Secure data path, eg. 'app/my-sdb/my-secrets'")
    aws_session: Optional[Session] = Field(
        default=None, description="AWS Session to pass to Cerberus client, can be used for local execution."
    )
    token: Optional[SecretStr] = Field(
        default=os.environ.get("CERBERUS_TOKEN", None),
        description="Cerberus token, can be used for local development without AWS auth mechanism."
        "Note: Token has priority over AWS session.",
    )
    verbose: bool = Field(default=False, description="Enable verbose for Cerberus client")

    @model_validator(mode="before")
    def _set_parent_to_path(cls, values):
        """
        Set default value for `parent` parameter on model initialization when it was not
        explicitly set by the user. In this scenario secure data path will be used:

        'app/my-sdb/my-secrets' -> app_my_sdb_my_secrets
        """
        regex = re.compile(r"[/-]")
        path = values.get("path")
        if not values.get("parent"):
            values["parent"] = regex.sub("_", path)
        return values

    @property
    def _client(self):
        """
        Instantiated Cerberus client.
        """
        self.token: Optional[SecretStr]
        token = self.token.get_secret_value() if self.token else None

        return CerberusClient(cerberus_url=self.url, token=token, aws_session=self.aws_session, verbose=self.verbose)

    def _get_secrets(self):
        """
        Dictionary of secrets.
        """
        return self._client.get_secrets_data(self.path)
