from typing import Any, Dict

from aiohttp import ClientSession, TCPConnector
from aiohttp_retry import ExponentialRetry
from aioresponses import aioresponses
import pytest
from requests_mock import Mocker
from yarl import URL

from pyspark.sql.types import MapType, StringType, StructField, StructType

from koheesio.spark.readers.rest_api import AsyncHttpGetStep, RestApiReader
from koheesio.steps.http import PaginatedHttpGetStep

ASYNC_BASE_URL = "http://mock-api.test"
ASYNC_GET_ENDPOINT = URL(f"{ASYNC_BASE_URL}/get")

pytestmark = pytest.mark.spark


@pytest.fixture
def mock_paginated_api(mocker: Mocker) -> None:
    """Mock paginated API responses"""
    for i in range(1, 4):
        mocker.get(
            f"https://mock-api.test/items?page={i}",
            json=[{"id": f"item{j}", "value": f"value{j}"} for j in range((i-1)*2, i*2)]
        )


def test_paginated_api() -> None:
    """Test paginated API responses"""
    with Mocker() as mocker:
        for i in range(1, 4):
            mocker.get(
                f"https://mock-api.test/items?page={i}",
                json=[{"id": f"item{j}", "value": f"value{j}"} for j in range((i-1)*2, i*2)]
            )

        step = PaginatedHttpGetStep(
            url="https://mock-api.test/items?page={page}",
            paginate=True,
            pages=3,
            limit=2
        )
        step.execute()

        assert len(step.output.response_json) == 6
        for i, item in enumerate(step.output.response_json):
            assert item["id"] == f"item{i}"
            assert item["value"] == f"value{i}"


@pytest.mark.asyncio
async def test_async_rest_api_reader() -> None:
    """Test async REST API reader with Spark schema"""
    mock_response = {
        "args": {},
        "headers": {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Content-Type": "application/json",
            "Host": "mock-api.test",
            "User-Agent": "python-requests/2.28.2",
            "X-type": "Koheesio RestApiReader Test"
        }
    }

    session = ClientSession()
    urls = [URL(ASYNC_GET_ENDPOINT), URL(ASYNC_GET_ENDPOINT)]
    retry_options = ExponentialRetry()
    connector = TCPConnector(limit=10)
    headers = {"Content-Type": "application/json", "X-type": "Koheesio RestApiReader Test"}
    transport = AsyncHttpGetStep(
        client_session=session, url=urls, retry_options=retry_options, connector=connector, headers=headers
    )

    spark_schema = StructType([
        StructField("args", MapType(StringType(), StringType()), True),
        StructField("headers", StructType([
            StructField("Accept", StringType(), True),
            StructField("Accept-Encoding", StringType(), True),
            StructField("Content-Type", StringType(), True),
            StructField("Host", StringType(), True),
            StructField("User-Agent", StringType(), True),
            StructField("X-type", StringType(), True)
        ]), True)
    ])

    with aioresponses() as m:
        # Mock both URLs since we're making two requests
        for _ in range(2):
            m.get(str(ASYNC_GET_ENDPOINT), payload=mock_response)

        reader = RestApiReader(transport=transport, schema=spark_schema)
        await reader.read_async()
        
        # Clean up
        await session.close()
