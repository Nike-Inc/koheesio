from typing import Generator
from io import StringIO
import logging

import pytest
from requests_mock.mocker import Mocker

from koheesio.logger import LoggingFactory
from koheesio.models import SecretStr
from koheesio.sso import okta as o

log = LoggingFactory.get_logger(name="test_download_file", inherit_from_koheesio=True)


@pytest.fixture(scope="function")
def requests_mocker() -> Generator[Mocker, None, None]:
    """Requests mocker fixture"""
    with Mocker() as rm:
        yield rm


# pylint: disable=assignment-from-no-return, redefined-outer-name
class TestOktaToken:
    """Tests for OktaAccessToken class"""

    url = "https://host.okta.com/oauth2/auth/v1/token"
    ot = o.OktaAccessToken(
        url=url,
        client_id="client",
        client_secret=SecretStr("secret"),
    )
    default_options = {
        "headers": {},
        "data": {"grant_type": "client_credentials"},
        "url": "url",
        "timeout": 3,
    }

    def test_okta_token_w_token(self, requests_mocker: Mocker) -> None:
        """Test OktaAccessToken with token"""
        # Arrange
        requests_mocker.post(self.url, json={"access_token": "bar"}, status_code=int(200))
        # Act
        output = self.ot.execute()
        # Assert
        assert output.token.get_secret_value() == "bar"

    def test_okta_token_wo_token(self, requests_mocker: Mocker) -> None:
        """Test OktaAccessToken without token"""
        requests_mocker.post(self.url, json={"foo": "bar"}, status_code=int(200))
        with pytest.raises(ValueError):
            self.ot.execute()

    def test_okta_token_non_200(self, requests_mocker: Mocker) -> None:
        """Test OktaAccessToken with non-200 response"""
        requests_mocker.post(self.url, status_code=int(404))
        with pytest.raises(o.HTTPError):
            self.ot.execute()

    def test_wo_extra_params(self) -> None:
        """Test OktaAccessToken without extra params"""
        oat = o.OktaAccessToken(url="url", client_id="client", client_secret=SecretStr("secret"))
        actual = oat.get_options()
        assert actual == {"auth": ("client", "secret"), **self.default_options}

    def test_w_extra_params(self) -> None:
        """Test OktaAccessToken with extra params"""
        oat = o.OktaAccessToken(url="url", client_id="client", client_secret=SecretStr("secret"), params={"foo": "bar"})
        assert oat.get_options() == {"foo": "bar", "auth": ("client", "secret"), **self.default_options}

    def test_log_extra_params_secret_happy(self, caplog: pytest.FixtureRequest) -> None:
        """Test that secret values are masked in logs"""
        # Arrange
        with caplog.at_level("DEBUG"):
            secret_val = "secret_value"
            # Act
            oat = o.OktaAccessToken(
                url="url", client_id="client", client_secret=SecretStr(secret_val), params={"foo": "bar"}
            )
            log.debug(f"{oat.get_options() = }")
            log.info(f"{oat.get_options() = }")
            log.warning(f"{oat.get_options() = }")
            log.error(f"{oat.get_options() = }")
            log.critical(f"{oat.get_options() = }")
            # Assert
            assert secret_val not in caplog.text
            assert "*" * len(secret_val) + "(Masked)" in caplog.text

    def test_secrets_not_in_log_unhappy(self, requests_mocker: Mocker, caplog: pytest.FixtureRequest) -> None:
        """Test that secret values are not logged upon errors"""
        # Arrange
        requests_mocker.post(self.url, status_code=int(404))
        with caplog.at_level("DEBUG"):
            secret_val = "unique_secret_value"
            # Act
            oat = o.OktaAccessToken(url=self.url, client_id="client", client_secret=SecretStr(secret_val))
            with pytest.raises(o.HTTPError):
                oat.execute()
            # Assert
            assert secret_val not in caplog.text

    def test_ensure_header_is_blank(self) -> None:
        """Test that the header is blank"""
        assert self.ot.headers == {}
