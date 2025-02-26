from io import StringIO
import logging

import pytest
from requests_mock.mocker import Mocker

from koheesio.models import SecretStr
from koheesio.sso import okta as o


@pytest.fixture(scope="function")
def requests_mocker():
    with Mocker() as rm:
        yield rm


class TestOktaToken:
    url = "https://host.okta.com/oauth2/auth/v1/token"
    ot = o.OktaAccessToken(
        url=url,
        client_id="client",
        client_secret=SecretStr("secret"),
    )

    def test_okta_token_w_token(self, requests_mocker):
        requests_mocker.post(self.url, json={"access_token": "bar"}, status_code=int(200))
        output = self.ot.execute()
        assert output.token.get_secret_value() == "bar"

    def test_okta_token_wo_token(self, requests_mocker):
        requests_mocker.post(self.url, json={"foo": "bar"}, status_code=int(200))
        with pytest.raises(ValueError):
            self.ot.execute()

    def test_okta_token_non_200(self, requests_mocker):
        requests_mocker.post(self.url, status_code=int(404))
        with pytest.raises(o.HTTPError):
            self.ot.execute()

    def test_wo_extra_params(self):
        oat = o.OktaAccessToken(url="url", client_id="client", client_secret=SecretStr("secret"))
        assert oat.params == {"auth": ("client", "secret")}

    def test_w_extra_params(self):
        oat = o.OktaAccessToken(url="url", client_id="client", client_secret=SecretStr("secret"), params={"foo": "bar"})
        assert oat.params == {"foo": "bar", "auth": ("client", "secret")}

    def test_log_extra_params_secret(self):
        log_capture_string = StringIO()
        ch = logging.StreamHandler(log_capture_string)
        ch.setLevel(logging.DEBUG)
        logger = logging.getLogger("tests")
        logger.addHandler(ch)
        secret_val = "secret_value"
        oat = o.OktaAccessToken(
            url="url", client_id="client", client_secret=SecretStr(secret_val), params={"foo": "bar"}
        )
        logger.warning(f"{oat.params = }")
        log_contents = log_capture_string.getvalue()
        assert "secret" not in log_contents
        assert "*" * len(secret_val) + "(Masked)" in log_contents
