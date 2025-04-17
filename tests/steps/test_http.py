from typing import Any, Dict, List, Type

import pytest
from requests import HTTPError
from requests.exceptions import RetryError
import responses
from urllib3 import Retry

from pydantic import ValidationError

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

# Mock URLs instead of real endpoints
BASE_URL = "http://mock-api.test"
GET_ENDPOINT = f"{BASE_URL}/get"
POST_ENDPOINT = f"{BASE_URL}/post"
PUT_ENDPOINT = f"{BASE_URL}/put"
DELETE_ENDPOINT = f"{BASE_URL}/delete"
STATUS_404_ENDPOINT = f"{BASE_URL}/status/404"
STATUS_500_ENDPOINT = f"{BASE_URL}/status/500"
STATUS_503_ENDPOINT = f"{BASE_URL}/status/503"

log = LoggingFactory.get_logger(name="test_http", inherit_from_koheesio=True)


@responses.activate
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
    """Test basic HTTP methods with success and error responses"""
    responses.add(
        getattr(responses, method.upper()),
        endpoint,
        json=return_value,
        status=expected_status_code
    )

    step = step(
        url=endpoint,
        headers={
            "Authorization": "Bearer token",
            "Content-Type": "application/json",
        },
    )
    
    if expected_status_code not in (200, 202, 204):
        with pytest.raises(HTTPError) as excinfo:
            step.execute()
        assert excinfo.value.response.status_code == expected_status_code
    else:
        response = step.execute()
        assert response.status_code == expected_status_code
        assert response.response_json == return_value


def test_http_step_with_valid_http_method() -> None:
    """Unit Testing the GET functions with valid method."""
    responses.add(responses.GET, GET_ENDPOINT, status=200)
    step = HttpStep(method="get", url=GET_ENDPOINT)
    step.execute()
    assert step.output.status_code == 200  # type: ignore


def test_http_step_with_invalid_http_method():
    responses.add(responses.GET, GET_ENDPOINT, status=200)
    # Will be raised during class instantiation
    with pytest.raises(ValidationError):
        HttpStep(method="foo", url=GET_ENDPOINT).execute()


def test_http_step_request():
    responses.add(responses.PUT, PUT_ENDPOINT, status=200)
    # The default method for HttpStep class is GET, however the method specified in `request` options is PUT and
    # it will override the default
    step = HttpStep(url=PUT_ENDPOINT)
    with step._request(method=HttpMethod.PUT) as response:
        assert response.status_code == 200


EXAMPLE_DIGEST_AUTH = (
    'Digest username="Zaphod", realm="galaxy@heartofgold.com", nonce="42answer", uri="/dir/restaurant.html", '
    'response="dontpanic42", opaque="babelfish"'
)


@responses.activate
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
    responses.add(responses.GET, params["url"], status=200)  # Mock the request to be always successful
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
        pytest.param(1, STATUS_503_ENDPOINT, 503, 2, RetryError, id="max_retries_2_503"),
        # Only 502, 503, 504 are included in the retry list
        pytest.param(3, STATUS_404_ENDPOINT, 404, 1, HTTPError, id="max_retries_1_404"),
    ],
)
def test_max_retries(
    max_retries: int, endpoint: str, status_code: int, expected_count: int, error_type: Type[Exception]
) -> None:
    """Test retry behavior for different status codes"""
    request_count = 0

    def request_callback(request):
        nonlocal request_count
        request_count += 1
        return (status_code, {}, 'Error')

    responses.add_callback(
        responses.GET,
        endpoint,
        callback=request_callback
    )

    step = HttpGetStep(url=endpoint, max_retries=max_retries)
    
    with pytest.raises(error_type):
        step.execute()
        
    assert request_count == expected_count


@pytest.mark.parametrize(
    "backoff_factor,expected",
    [
        pytest.param(0.5, [0, 1, 2], id="backoff_0.5"),
    ],
)
def test_backoff_factor(monkeypatch: pytest.FixtureRequest, backoff_factor: float, expected: list) -> None:
    """Test exponential backoff behavior"""
    backoff_values: List[float] = []
    
    def mock_sleep(seconds: float) -> None:
        backoff_values.append(seconds)
    
    monkeypatch.setattr('time.sleep', mock_sleep)
    
    responses.add(
        responses.GET,
        STATUS_503_ENDPOINT,
        status=503
    )
    
    step = HttpGetStep(
        url=STATUS_503_ENDPOINT,
        headers={
            "Authorization": "Bearer token",
            "Content-Type": "application/json",
        },
        backoff_factor=backoff_factor,
    )
    
    with pytest.raises(RetryError):
        step.execute()
    
    assert backoff_values == expected
