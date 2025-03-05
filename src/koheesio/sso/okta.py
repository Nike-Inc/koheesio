"""
This module contains Okta integration steps.
"""

from __future__ import annotations

from typing import Dict, Optional
from logging import Filter, LogRecord

from requests import HTTPError

from koheesio.logger import LoggingFactory, MaskedString
from koheesio.models import Field, SecretStr
from koheesio.steps.http import HttpPostStep


class Okta(HttpPostStep):
    """
    Base Okta class
    """

    client_id: str = Field(default=..., alias="okta_id", description="Okta account ID")
    client_secret: SecretStr = Field(default=..., alias="okta_secret", description="Okta account secret", repr=False)
    data: Dict[str, str] = Field(
        default={"grant_type": "client_credentials"}, description="Data to be sent along with the token request"
    )

    # headers are not used in this class
    headers: dict = {}

    def get_options(self) -> dict:
        """options to be passed to requests.request()"""
        _options = super().get_options()
        _options["auth"] = (self.client_id, MaskedString(self.client_secret.get_secret_value()))
        return _options


class LoggerOktaTokenFilter(Filter):
    """Filter which hides token value from log."""

    def __init__(self, okta_object: OktaAccessToken, name: str = "OktaToken"):
        self.__okta_object = okta_object
        super().__init__(name=name)

    def filter(self, record: LogRecord) -> bool:
        # noinspection PyUnresolvedReferences
        if token := self.__okta_object.output.token:
            token_value = token.get_secret_value()
            record.msg = record.msg.replace(token_value, "<SECRET_TOKEN>")

        return True


class OktaAccessToken(Okta):
    """
    Get Okta authorization token

    Examples
    --------
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

    def __init__(self, **kwargs):  # type: ignore[no-untyped-def]
        _logger = LoggingFactory.get_logger(name=self.__class__.__name__, inherit_from_koheesio=True)
        logger_filter = LoggerOktaTokenFilter(okta_object=self)
        _logger.addFilter(logger_filter)
        super().__init__(**kwargs)

    def execute(self) -> None:
        """
        Execute an HTTP Post call to Okta service and retrieve the access token.
        """
        super().execute()

        # noinspection PyUnresolvedReferences
        status_code = self.output.status_code
        # noinspection PyUnresolvedReferences
        raw_payload = self.output.raw_payload

        if status_code != 200:
            raise HTTPError(
                f"Request failed with '{status_code}' code. Payload: {raw_payload}",
                response=self.output.response_raw,
                request=None,
            )

        # noinspection PyUnresolvedReferences
        json_payload = self.output.json_payload

        if token := json_payload.get("access_token"):
            self.output.token = SecretStr(token)
        else:
            raise ValueError(f"No 'access_token' found in the Okta response: {json_payload}")
