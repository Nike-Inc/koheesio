import pytest
import requests
from requests import HTTPError
from requests.adapters import HTTPAdapter
from requests.exceptions import RetryError
import requests_mock
from urllib3 import Retry

from koheesio.logger import LoggingFactory
from koheesio.models import SecretStr
from koheesio.steps.http import (
    HttpDeleteStep,
    HttpGetStep,
    HttpMethod,
    HttpPostStep,
    HttpPutStep,
    HttpStep,
)

BASE_URL = "https://httpbin.org"
GET_ENDPOINT = f"{BASE_URL}/get"
POST_ENDPOINT = f"{BASE_URL}/post"
PUT_ENDPOINT = f"{BASE_URL}/put"
DELETE_ENDPOINT = f"{BASE_URL}/delete"
STATUS_404_ENDPOINT = f"{BASE_URL}/status/404"
STATUS_500_ENDPOINT = f"{BASE_URL}/status/500"
STATUS_503_ENDPOINT = f"{BASE_URL}/status/503"


log = LoggingFactory.get_logger(name="test_delta", inherit_from_koheesio=True)


@pytest.mark.parametrize(
    "endpoint,step,method,return_value,expected_status_code",
    [
        pytest.param(
            GET_ENDPOINT,
            HttpGetStep,
            "get",
            {"job_id": "1234"},
            200,
            id="httpGetStep_success",
        ),
        pytest.param(GET_ENDPOINT, HttpGetStep, "get", {"message": "Error Code"}, 400, id="httpGetStep_error"),
        pytest.param(GET_ENDPOINT, HttpGetStep, "get", None, 404, id="httpGetStep_emptyresponse"),
        pytest.param(
            POST_ENDPOINT,
            HttpPostStep,
            "post",
            {"job_id": "1234"},
            200,
            id="httpPostStep_success",
        ),
        pytest.param(POST_ENDPOINT, HttpPostStep, "post", {"message": "Error Code"}, 400, id="httpPosStep_error"),
        pytest.param(
            PUT_ENDPOINT,
            HttpPutStep,
            "put",
            {"detail": "Updated"},
            202,
            id="httpPutStep_success",
        ),
        pytest.param(PUT_ENDPOINT, HttpPutStep, "put", {"message": "Error Code"}, 400, id="httpPutStep_error"),
        pytest.param(
            PUT_ENDPOINT, HttpPutStep, "put", {"message": "Validation Error"}, 422, id="httpPutStep_validationError"
        ),
        pytest.param(
            DELETE_ENDPOINT,
            HttpDeleteStep,
            "delete",
            {"detail": "Deleted"},
            204,
            id="httpDeleteStep_success",
        ),
        pytest.param(
            DELETE_ENDPOINT, HttpDeleteStep, "delete", {"message": "Error Code"}, 400, id="httpDeleteStep_error"
        ),
        pytest.param(
            DELETE_ENDPOINT,
            HttpDeleteStep,
            "delete",
            {"message": "Validation Error"},
            422,
            id="httpDeleteStep_validationError",
        ),
    ],
)
def test_http_step(endpoint: str, step: HttpStep, method: str, return_value: dict, expected_status_code: int) -> None:
    """
    Unit Testing the GET functions.
    Above parameters are for the success and failed GET API calls
    """

    with requests_mock.Mocker() as rm:
        rm.request(method=method, url=endpoint, json=return_value, status_code=int(expected_status_code))
        step = step(
            url=endpoint,
            headers={
                "Authorization": "Bearer token",
                "Content-Type": "application/json",
            },
        )
        # Unhappy path
        if expected_status_code not in (200, 202, 204):
            with pytest.raises(HTTPError) as excinfo:
                step.execute()

            assert excinfo.value.response.status_code == expected_status_code
        # Happy path
        else:
            step.execute()
            assert step.output.status_code == expected_status_code  # type: ignore
            assert step.output.response_json == return_value  # type: ignore


def test_http_step_with_valid_http_method():
    """
    Unit Testing the GET functions.
    Above parameters are for the success and failed GET API calls
    """

    with requests_mock.Mocker() as rm:
        rm.get(GET_ENDPOINT, status_code=int(200))
        response = HttpStep(method="get", url=GET_ENDPOINT).execute()
        assert response.status_code == 200


def test_http_step_with_invalid_http_method():
    with requests_mock.Mocker() as rm:
        rm.get(GET_ENDPOINT, status_code=int(200))
        # Will be raised during class instantiation
        with pytest.raises(AttributeError):
            HttpStep(method="foo", url=GET_ENDPOINT).execute()


def test_http_step_request():
    with requests_mock.Mocker() as rm:
        rm.put(PUT_ENDPOINT, status_code=int(200))
        # The default method for HttpStep class is GET, however the method specified in `request` options is PUT and
        # it will override the default
        step = HttpStep(url=PUT_ENDPOINT)
        with step._request(method=HttpMethod.PUT) as response:
            assert response.status_code == 200


EXAMPLE_DIGEST_AUTH = (
    'Digest username="Zaphod", realm="galaxy@heartofgold.com", nonce="42answer", uri="/dir/restaurant.html", '
    'response="dontpanic42", opaque="babelfish"'
)


@pytest.mark.parametrize(
    "params, expected",
    [
        pytest.param(
            dict(url=GET_ENDPOINT, headers={"Authorization": "Bearer token", "Content-Type": "application/json"}),
            "Bearer token",
            id="bearer_token_in_headers",
        ),
        pytest.param(
            dict(url=GET_ENDPOINT, headers={"Content-Type": "application/json"}, auth_header="Bearer token"),
            "Bearer token",
            id="bearer_token_through_auth_header",
        ),
        pytest.param(
            dict(url=GET_ENDPOINT, auth_header=EXAMPLE_DIGEST_AUTH),
            EXAMPLE_DIGEST_AUTH,
            id="digest_auth_with_nonce",
        ),
    ],
)
def test_get_headers(params: dict, expected: str, caplog: pytest.LogCaptureFixture) -> None:
    """
    Authorization headers are being converted into SecretStr under the hood to avoid dumping any
    sensitive content into logs. However, when calling the `get_headers` method, the SecretStr is being
    converted back to string, otherwise sensitive info would have looked like '**********'.
    """
    # Arrange and Act
    with requests_mock.Mocker() as rm:
        rm.get(params["url"], status_code=int(200))  # Mock the request to be always successful
        step = HttpStep(**params)
        caplog.set_level("DEBUG", logger=step.log.name)
        auth = step.headers.get("Authorization")
        step.execute()

    # Check that the token doesn't accidentally leak in the logs
    assert len(caplog.records) > 1, "No logs were generated"
    for record in caplog.records:
        assert expected not in record.message

    # Ensure that the Authorization header is properly parsed to a SecretStr
    assert auth is not None, "Authorization header is missing"
    assert isinstance(auth, SecretStr)
    assert str(auth) == "**********"
    assert auth.get_secret_value() == expected

    # Ensure that the Content-Type header is properly parsed while not being a SecretStr
    assert step.headers["Content-Type"] == "application/json"


@pytest.mark.parametrize(
    "max_retries, endpoint, status_code, expected_count, error_type",
    [
        pytest.param(4, STATUS_503_ENDPOINT, 503, 5, RetryError, id="max_retries_4_503"),
        pytest.param(7, STATUS_404_ENDPOINT, 404, 8, RetryError, id="max_retries_7_404"),
        pytest.param(17, STATUS_500_ENDPOINT, 500, 18, RetryError, id="max_retries_17_500"),
        pytest.param(17, STATUS_404_ENDPOINT, 503, 1, HTTPError, id="max_retries_17_404"),
    ],
)
def test_max_retries(
    max_retries: int, endpoint: str, status_code: int, expected_count: int, error_type: Exception
) -> None:
    session = requests.Session()
    retry_logic = Retry(total=max_retries, status_forcelist=[status_code])
    session.mount("https://", HTTPAdapter(max_retries=retry_logic))
    # noinspection HttpUrlsUsage
    session.mount("http://", HTTPAdapter(max_retries=retry_logic))

    step = HttpGetStep(url=endpoint, session=session)

    with pytest.raises(error_type):  # type: ignore
        step.execute()

    first_pool = [v for _, v in session.adapters["https://"].poolmanager.pools._container.items()][0]

    assert first_pool.num_requests == expected_count


@pytest.mark.parametrize(
    "backoff,expected",
    [
        pytest.param(7, [0, 14, 28], id="backoff_7"),
        pytest.param(3, [0, 6, 12], id="backoff_3"),
        pytest.param(2, [0, 4, 8], id="backoff_2"),
        pytest.param(1, [0, 2, 4], id="backoff_1"),
    ],
)
def test_initial_delay_and_backoff(monkeypatch: pytest.FixtureRequest, backoff: int, expected: list) -> None:
    session = requests.Session()
    retry_logic = Retry(total=3, backoff_factor=backoff, status_forcelist=[503])
    session.mount("https://", HTTPAdapter(max_retries=retry_logic))
    # noinspection HttpUrlsUsage
    session.mount("http://", HTTPAdapter(max_retries=retry_logic))

    step = HttpGetStep(
        url=STATUS_503_ENDPOINT,
        headers={
            "Authorization": "Bearer token",
            "Content-Type": "application/json",
        },
        session=session,
    )

    # Save the original get_backoff_time method
    original_get_backoff_time = Retry.get_backoff_time

    backoff_values = []

    # Define a new function to replace get_backoff_time
    def mock_get_backoff_time(self, *args, **kwargs):
        result = original_get_backoff_time(self, *args, **kwargs)
        backoff_values.append(result)
        return result

    # Use monkeypatch to replace get_backoff_time
    monkeypatch.setattr(Retry, "get_backoff_time", mock_get_backoff_time)

    with pytest.raises(RetryError):
        step.execute()

    # Restore the original get_backoff_time method
    monkeypatch.undo()

    assert backoff_values == expected
