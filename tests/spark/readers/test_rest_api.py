import pytest
import requests_mock
from aiohttp import ClientSession, TCPConnector
from aiohttp_retry import ExponentialRetry
from yarl import URL

from pyspark.sql.types import MapType, StringType, StructField, StructType

from koheesio.asyncio.http import AsyncHttpStep
from koheesio.spark.readers.rest_api import AsyncHttpGetStep, RestApiReader
from koheesio.steps.http import PaginatedHtppGetStep

ASYNC_BASE_URL = "http://httpbin.org"
ASYNC_GET_ENDPOINT = URL(f"{ASYNC_BASE_URL}/get")

pytestmark = pytest.mark.spark


@pytest.fixture
def mock_paginated_api():
    with requests_mock.Mocker() as m:
        for i in range(1, 4):  # Mock 3 pages of data
            data = [{"id": j, "page": i, "value": f"data_{i}_{j}"} for j in range(1, 11)]  # 10 records per page
            m.get(f"https://api.example.com/data?page={i}", json=data)
        yield m


def test_paginated_api(mock_paginated_api):
    # Test that the paginated API returns all the data
    transport = PaginatedHtppGetStep(url="https://api.example.com/data?page={page}", paginate=True, pages=3)
    task = RestApiReader(transport=transport, spark_schema="id: int, page:int, value: string")

    assert isinstance(task.transport, PaginatedHtppGetStep)

    task.execute()

    # Convert the DataFrame to a list of dictionaries
    all_data = [row.asDict() for row in task.output.df.collect()]
    expected_data = [{"id": j, "page": i, "value": f"data_{i}_{j}"} for i in range(1, 4) for j in range(1, 11)]

    assert all_data == expected_data


@pytest.mark.asyncio
async def test_async_rest_api_reader():
    """
    Testing the AsyncHttpStep class.
    """
    session = ClientSession()
    urls = [URL(ASYNC_GET_ENDPOINT), URL(ASYNC_GET_ENDPOINT)]
    retry_options = ExponentialRetry()
    connector = TCPConnector(limit=10)
    headers = {"Content-Type": "application/json", "X-type": "Koheesio RestApiReader Test"}
    transport = AsyncHttpGetStep(
        client_session=session, url=urls, retry_options=retry_options, connector=connector, headers=headers
    )
    spark_schema = StructType(
        [
            StructField("args", MapType(StringType(), StringType()), True),
            StructField(
                "headers",
                StructType(
                    [
                        StructField("Accept", StringType(), True),
                        StructField("Accept-Encoding", StringType(), True),
                        StructField("Content-Type", StringType(), True),
                        StructField("Host", StringType(), True),
                        StructField("User-Agent", StringType(), True),
                        StructField("X-Amzn-Trace-Id", StringType(), True),
                        StructField("X-Type", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField("origin", StringType(), True),
            StructField("url", StringType(), True),
        ]
    )
    task = RestApiReader(transport=transport, spark_schema=spark_schema)

    assert isinstance(task.transport, AsyncHttpStep)

    task.execute()

    rows = [row.asDict() for row in task.output.df.collect()]
    all_data = [{row["url"]: row["headers"].asDict()["X-Type"]} for row in rows]

    # Assert the responses_urls
    assert len(all_data) == 2
    assert all_data == [{f"{ASYNC_BASE_URL}/get": "Koheesio RestApiReader Test"}] * 2
