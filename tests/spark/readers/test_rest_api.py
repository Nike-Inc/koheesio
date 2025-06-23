from aiohttp import ClientSession, TCPConnector
from aiohttp_retry import ExponentialRetry
from aioresponses import aioresponses
import pytest
import responses
from responses.registries import OrderedRegistry
from yarl import URL

from pyspark.sql.types import MapType, StringType, StructField, StructType

from koheesio.asyncio.http import AsyncHttpStep
from koheesio.spark.readers.rest_api import AsyncHttpGetStep, RestApiReader
from koheesio.steps.http import HttpGetStep, PaginatedHttpGetStep

ASYNC_BASE_URL = "https://42.koheesio.test"
ASYNC_GET_ENDPOINT = URL(f"{ASYNC_BASE_URL}/get")

pytestmark = pytest.mark.spark


@pytest.fixture(scope="function", name="mock_aiohttp")
def mock_aiohttp():
    with aioresponses() as m:
        yield m


@responses.activate(registry=OrderedRegistry)
def test_paginated_api():
    for i in range(1, 4):  # Mock 3 pages of data
        data = [{"id": j, "page": i, "value": f"data_{i}_{j}"} for j in range(1, 11)]  # 10 records per page
        responses.get(f"https://api.example.com/data?page={i}", json=data)

    # Test that the paginated API returns all the data
    transport = PaginatedHttpGetStep(url="https://api.example.com/data?page={page}", paginate=True, pages=3)
    task = RestApiReader(transport=transport, spark_schema="id: int, page:int, value: string")

    assert isinstance(task.transport, PaginatedHttpGetStep)

    task.execute()

    # Convert the DataFrame to a list of dictionaries
    all_data = [row.asDict() for row in task.output.df.collect()]
    expected_data = [{"id": j, "page": i, "value": f"data_{i}_{j}"} for i in range(1, 4) for j in range(1, 11)]

    assert all_data == expected_data


@pytest.mark.asyncio
async def test_async_rest_api_reader(mock_aiohttp):
    """
    Testing the AsyncHttpStep class.
    """
    mock_aiohttp.get(str(ASYNC_GET_ENDPOINT), status=200, repeat=True, payload={"url": str(ASYNC_GET_ENDPOINT)})

    transport = AsyncHttpGetStep(
        client_session=ClientSession(),
        url=[URL(ASYNC_GET_ENDPOINT), URL(ASYNC_GET_ENDPOINT)],
        retry_options=ExponentialRetry(),
        connector=TCPConnector(limit=10),
        headers={"Content-Type": "application/json", "X-type": "Koheesio RestApiReader Test"},
    )

    spark_schema = StructType(
        [
            StructField("origin", StringType(), True),
            StructField("url", StringType(), True),
        ]
    )
    task = RestApiReader(transport=transport, spark_schema=spark_schema)

    assert isinstance(task.transport, AsyncHttpStep)

    task.execute()

    rows = [row.asDict() for row in task.output.df.collect()]
    all_data = [row["url"] for row in rows]

    # Assert the responses_urls
    assert len(all_data) == 2
    assert all_data == [f"{ASYNC_BASE_URL}/get"] * 2


@responses.activate
def test_rest_api_reader(mock_aiohttp):
    """
    Testing the AsyncHttpStep class.
    """

    def request_callback(request):
        import json

        body = [
            {
                "headers": dict(request.headers),
                "url": str(ASYNC_GET_ENDPOINT),
            }
        ]
        return (200, request.headers, json.dumps(body))

    responses.add_callback(
        responses.GET,
        str(ASYNC_GET_ENDPOINT),
        callback=request_callback,
        content_type="application/json",
    )

    transport = HttpGetStep(
        url=str(ASYNC_GET_ENDPOINT),
        headers={"Content-Type": "application/json", "X-Type": "Koheesio RestApiReader Test"},
    )

    spark_schema = StructType(
        [
            StructField(
                "headers",
                StructType(
                    [
                        StructField("Accept", StringType(), True),
                        StructField("Accept-Encoding", StringType(), True),
                        StructField("Connection", StringType(), True),
                        StructField("Content-Type", StringType(), True),
                        StructField("User-Agent", StringType(), True),
                        StructField("X-Type", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField("url", StringType(), True),
        ]
    )

    task = RestApiReader(transport=transport, spark_schema=spark_schema)

    assert isinstance(task.transport, HttpGetStep)

    task.execute()

    rows = [row.asDict() for row in task.output.df.collect()]
    all_data = [{row["url"]: row.get("headers", {}).asDict()["X-Type"]} for row in rows]

    # Assert the responses_urls
    assert len(all_data) == 1
    assert all_data == [{f"{ASYNC_BASE_URL}/get": "Koheesio RestApiReader Test"}]
