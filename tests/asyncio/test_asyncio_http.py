import warnings

import pytest
from aiohttp import ClientResponseError, ClientSession, TCPConnector
from aiohttp_retry import ExponentialRetry
from yarl import URL

from pydantic import ValidationError

from koheesio.asyncio.http import AsyncHttpStep
from koheesio.steps.http import HttpMethod

ASYNC_BASE_URL = "http://httpbin.org"
ASYNC_GET_ENDPOINT = URL(f"{ASYNC_BASE_URL}/get")
ASYNC_STATUS_503_ENDPOINT = URL(f"{ASYNC_BASE_URL}/status/503")
ASYNC_STATUS_404_ENDPOINT = URL(f"{ASYNC_BASE_URL}/status/404")


def test_async_http_get_step_positive():
    """
    Testing the GET function with a positive scenario.
    """
    step = AsyncHttpStep(
        method=HttpMethod.GET,
        url=[
            ASYNC_GET_ENDPOINT,
            ASYNC_GET_ENDPOINT,
            ASYNC_GET_ENDPOINT,
            ASYNC_GET_ENDPOINT,
            ASYNC_GET_ENDPOINT,
            ASYNC_GET_ENDPOINT,
            ASYNC_GET_ENDPOINT,
            ASYNC_GET_ENDPOINT,
            ASYNC_GET_ENDPOINT,
            ASYNC_GET_ENDPOINT,
        ],
    )
    step.execute()
    responses_urls = step.output.responses_urls

    assert len(responses_urls) == 10
    response, url = responses_urls[0]
    assert url == ASYNC_GET_ENDPOINT
    assert response["url"] == str(ASYNC_GET_ENDPOINT)
    assert response["headers"]["Host"] == "httpbin.org"


@pytest.mark.parametrize(
    "http_method, status_endpoint, expected_status",
    [
        (HttpMethod.GET, ASYNC_STATUS_503_ENDPOINT, 503),
        (HttpMethod.GET, ASYNC_STATUS_404_ENDPOINT, 404),
        (HttpMethod.POST, ASYNC_STATUS_503_ENDPOINT, 503),
        (HttpMethod.POST, ASYNC_STATUS_404_ENDPOINT, 404),
        (HttpMethod.PUT, ASYNC_STATUS_503_ENDPOINT, 503),
        (HttpMethod.PUT, ASYNC_STATUS_404_ENDPOINT, 404),
        (HttpMethod.DELETE, ASYNC_STATUS_503_ENDPOINT, 503),
        (HttpMethod.DELETE, ASYNC_STATUS_404_ENDPOINT, 404),
    ],
)
def test_async_http_step_negative(http_method, status_endpoint, expected_status):
    """
    Testing the function with a negative scenario (503 and 404 status codes).
    """
    step = AsyncHttpStep(method=http_method, url=[status_endpoint])
    with pytest.raises(ClientResponseError) as excinfo:
        step.execute()

    assert excinfo.value.status == expected_status


@pytest.mark.asyncio
async def test_async_http_step():
    """
    Testing the AsyncHttpStep class.
    """
    # Initialize the AsyncHttpStep
    session = ClientSession()
    urls = [URL(ASYNC_GET_ENDPOINT), URL(ASYNC_GET_ENDPOINT)]
    retry_options = ExponentialRetry()
    connector = TCPConnector(limit=10)
    headers = {"Content-Type": "application/json"}
    step = AsyncHttpStep(
        client_session=session, url=urls, retry_options=retry_options, connector=connector, headers=headers
    )

    # Execute the step
    responses_urls = await step.get()

    # Assert the responses_urls
    assert isinstance(responses_urls, list)
    assert len(responses_urls) == 2


@pytest.mark.asyncio
async def test_async_http_step_with_timeout():
    """
    Testing the AsyncHttpStep class with timeout.
    """
    # Initialize the AsyncHttpStep
    session = ClientSession()
    urls = [URL(ASYNC_GET_ENDPOINT), URL(ASYNC_GET_ENDPOINT)]
    retry_options = ExponentialRetry()
    connector = TCPConnector(limit=10)
    headers = {"Content-Type": "application/json"}

    # Assert the warning
    with pytest.raises(ValidationError):
        AsyncHttpStep(
            client_session=session,
            url=urls,
            retry_options=retry_options,
            connector=connector,
            headers=headers,
            timeout=10,
        )


def test_async_http_step_with_invalid_http_method():
    """
    Testing the function with an invalid HTTP method.
    """
    invalid_method = "INVALID_METHOD"

    with pytest.raises(ValueError) as exc_info:
        step = AsyncHttpStep(method=invalid_method, url=[ASYNC_GET_ENDPOINT])
        step.execute()

    assert str(exc_info.value) == f"Method {invalid_method} not implemented in AsyncHttpStep."


@pytest.mark.asyncio
async def test_async_http_step_set_outputs_warning():
    """
    Testing the AsyncHttpStep class's set_outputs method for warning.
    """
    # Initialize the AsyncHttpStep
    session = ClientSession()
    urls = [URL(ASYNC_GET_ENDPOINT), URL(ASYNC_GET_ENDPOINT)]
    retry_options = ExponentialRetry()
    connector = TCPConnector(limit=10)
    headers = {"Content-Type": "application/json"}
    step = AsyncHttpStep(
        client_session=session,
        url=urls,
        retry_options=retry_options,
        connector=connector,
        headers=headers,
    )

    # Assert the warning
    with warnings.catch_warnings(record=True) as w:
        # Cause all warnings to always be triggered.
        warnings.simplefilter("always")
        # Trigger a warning.
        step.set_outputs(None)
        # Verify some things
        assert len(w) == 1
        assert issubclass(w[-1].category, UserWarning)
        assert "set outputs is not implemented in AsyncHttpStep." == str(w[-1].message)


@pytest.mark.asyncio
async def test_async_http_step_get_options_warning():
    """
    Testing the AsyncHttpStep class's get_options method for warning.
    """
    # Initialize the AsyncHttpStep
    session = ClientSession()
    urls = [URL(ASYNC_GET_ENDPOINT), URL(ASYNC_GET_ENDPOINT)]
    retry_options = ExponentialRetry()
    connector = TCPConnector(limit=10)
    headers = {"Content-Type": "application/json"}
    step = AsyncHttpStep(
        client_session=session,
        url=urls,
        retry_options=retry_options,
        connector=connector,
        headers=headers,
    )

    # Assert the warning
    with warnings.catch_warnings(record=True) as w:
        # Cause all warnings to always be triggered.
        warnings.simplefilter("always")
        # Trigger a warning.
        step.get_options()
        # Verify some things
        assert len(w) == 1
        assert issubclass(w[-1].category, UserWarning)
        assert "get_options is not implemented in AsyncHttpStep." == str(w[-1].message)
