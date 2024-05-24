"""
This module contains Okta integration steps.
"""

from __future__ import annotations

from typing import Dict, Optional, Union
from logging import Filter

from requests import HTTPError

from koheesio.logger import LoggingFactory, MaskedString
from koheesio.models import Field, SecretStr, model_validator
from koheesio.steps.http import HttpPostStep


class Okta(HttpPostStep):
    """
    Base Okta class
    """

    client_id: str = Field(default=..., alias="okta_id", description="Okta account ID")
    client_secret: SecretStr = Field(default=..., alias="okta_secret", description="Okta account secret", repr=False)
    data: Optional[Union[Dict[str, str], str]] = Field(
        default={"grant_type": "client_credentials"}, description="Data to be sent along with the token request"
    )

    @model_validator(mode="before")
    def _set_auth_param(cls, v):
        """
        Assign auth parameter with Okta client and secret to the params dictionary.
        If auth parameter already exists, it will be overwritten.
        """
        auth = (v["client_id"], MaskedString(v["client_secret"].get_secret_value()))
        v["params"] = {"auth": auth} if not v.get("params") else {**v["params"], "auth": auth}
        return v


class LoggerOktaTokenFilter(Filter):
    """Filter which hides token value from log."""

    def __init__(self, okta_object: OktaAccessToken, name: str = "OktaToken"):
        self.__okta_object = okta_object
        super().__init__(name=name)

    def filter(self, record):
        # noinspection PyUnresolvedReferences
        if token := self.__okta_object.output.token:
            token_value = token.get_secret_value()
            record.msg = record.msg.replace(token_value, "<SECRET_TOKEN>")

        return True


class OktaAccessToken(Okta):
    """
    Get Okta authorization token

    Example:
    ```python
    token = (
        OktaAccessToken(
            url="https://org.okta.com",
            client_id="client",
            client_secret=SecretStr("secret"),
            params={
                "p1": "foo",
                "p2": "bar",
            },
        )
        .execute()
        .token
    )
    ```
    """

    class Output(Okta.Output):
        """Output class for OktaAccessToken."""

        token: Optional[SecretStr] = Field(default=None, description="Okta authentication token")

    def __init__(self, **kwargs):
        _logger = LoggingFactory.get_logger(name=self.__class__.__name__, inherit_from_koheesio=True)
        logger_filter = LoggerOktaTokenFilter(okta_object=self)
        _logger.addFilter(logger_filter)
        super().__init__(**kwargs)

    def execute(self):
        """
        Execute an HTTP Post call to Okta service and retrieve the access token.
        """
        HttpPostStep.execute(self)

        # noinspection PyUnresolvedReferences
        status_code = self.output.status_code
        # noinspection PyUnresolvedReferences
        raw_payload = self.output.raw_payload

        if status_code != 200:
            raise HTTPError(f"Request failed with '{status_code}' code. Payload: {raw_payload}")

        # noinspection PyUnresolvedReferences
        json_payload = self.output.json_payload

        if token := json_payload.get("access_token"):
            self.output.token = SecretStr(token)
        else:
            raise ValueError(f"No 'access_token' found in the Okta response: {json_payload}")
