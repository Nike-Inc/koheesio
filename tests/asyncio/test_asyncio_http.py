import warnings

from aiohttp import ClientResponseError, ClientSession, TCPConnector
from aiohttp_retry import ExponentialRetry
import pytest
from yarl import URL

from pydantic import ValidationError

from koheesio.asyncio.http import AsyncHttpStep
from koheesio.steps.http import HttpMethod


@pytest.fixture
def get_endpoint(fake_http_server: str) -> URL:
    return URL(f"{fake_http_server}/get")


@pytest.fixture
def status_503_endpoint(fake_http_server: str) -> URL:
    return URL(f"{fake_http_server}/status/503")


@pytest.fixture
def status_404_endpoint(fake_http_server: str) -> URL:
    return URL(f"{fake_http_server}/status/404")


@pytest.mark.asyncio
def test_async_http_get_step_positive(get_endpoint: URL) -> None:
    """Testing the GET function with a positive scenario."""
    step = AsyncHttpStep(
        method=HttpMethod.GET,
        url=[get_endpoint] * 10,
    )
    step.execute()
    responses_urls = step.output.responses_urls

    assert len(responses_urls) == 10
    response, url = responses_urls[0]
    assert url == get_endpoint
    assert response["url"] == str(get_endpoint)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "http_method, endpoint_fixture, expected_status",
    [
        (HttpMethod.GET, "status_503_endpoint", 503),
        (HttpMethod.GET, "status_404_endpoint", 404),
        (HttpMethod.POST, "status_503_endpoint", 503),
        (HttpMethod.POST, "status_404_endpoint", 404),
        (HttpMethod.PUT, "status_503_endpoint", 503),
        (HttpMethod.PUT, "status_404_endpoint", 404),
        (HttpMethod.DELETE, "status_503_endpoint", 503),
        (HttpMethod.DELETE, "status_404_endpoint", 404),
    ],
)
def test_async_http_step_negative(
    request: pytest.FixtureRequest,
    http_method: HttpMethod,
    endpoint_fixture: str,
    expected_status: int,
) -> None:
    """Testing the function with a negative scenario (503 and 404 status codes)."""
    endpoint: URL = request.getfixturevalue(endpoint_fixture)
    step = AsyncHttpStep(method=http_method, url=[endpoint])
    with pytest.raises(ClientResponseError) as excinfo:
        step.execute()

    assert excinfo.value.status == expected_status


@pytest.mark.asyncio
async def test_async_http_step(get_endpoint: URL) -> None:
    """Testing the AsyncHttpStep class."""
    step = AsyncHttpStep(
        client_session=ClientSession(),
        connector=TCPConnector(limit=10),
        urls=[get_endpoint, get_endpoint],
        retry_options=ExponentialRetry(),
        headers={"Content-Type": "application/json"},
    )

    responses_urls = await step.get()

    assert isinstance(responses_urls, list)
    assert len(responses_urls) == 2


@pytest.mark.asyncio
async def test_async_http_step_with_timeout(get_endpoint: URL) -> None:
    """Testing the AsyncHttpStep class with timeout."""
    with pytest.raises(ValidationError):
        AsyncHttpStep(
            client_session=ClientSession(),
            connector=TCPConnector(limit=10),
            timeout=10,
            urls=[get_endpoint, get_endpoint],
            retry_options=ExponentialRetry(),
            headers={"Content-Type": "application/json"},
        )


def test_async_http_step_with_invalid_http_method(get_endpoint: URL) -> None:
    """Testing the function with an invalid HTTP method."""
    invalid_method = "INVALID_METHOD"

    with pytest.raises(ValueError) as exc_info:
        step = AsyncHttpStep(method=invalid_method, url=[get_endpoint])
        step.execute()

    assert str(exc_info.value) == f"Method {invalid_method} not implemented in AsyncHttpStep."


@pytest.mark.asyncio
async def test_async_http_step_set_outputs_warning(get_endpoint: URL) -> None:
    """Testing the AsyncHttpStep class's set_outputs method for warning."""
    step = AsyncHttpStep(
        client_session=ClientSession(),
        connector=TCPConnector(limit=10),
        urls=[get_endpoint, get_endpoint],
        retry_options=ExponentialRetry(),
        headers={"Content-Type": "application/json"},
    )

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        step.set_outputs(None)
        assert len(w) == 1
        assert issubclass(w[-1].category, UserWarning)
        assert "set outputs is not implemented in AsyncHttpStep." == str(w[-1].message)


@pytest.mark.asyncio
async def test_async_http_step_get_options_warning(get_endpoint: URL) -> None:
    """Testing the AsyncHttpStep class's get_options method for warning."""
    step = AsyncHttpStep(
        client_session=ClientSession(),
        connector=TCPConnector(limit=10),
        urls=[get_endpoint, get_endpoint],
        retry_options=ExponentialRetry(),
        headers={"Content-Type": "application/json"},
    )

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        step.get_options()
        assert len(w) == 1
        assert issubclass(w[-1].category, UserWarning)
        assert "get_options is not implemented in AsyncHttpStep." == str(w[-1].message)
