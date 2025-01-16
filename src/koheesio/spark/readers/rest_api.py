"""
This module provides the RestApiReader class for interacting with RESTful APIs.

The RestApiReader class is designed to fetch data from RESTful APIs and store the response in a DataFrame. It supports
different transports, e.g. Paginated Http or Async HTTP. The main entry point is the `execute`
method, which performs transport.execute() call and provide data from the API calls.

For more details on how to use this class and its methods, refer to the class docstring.

"""

from typing import List, Tuple, Union

from pydantic import Field, InstanceOf

# noinspection PyProtectedMember
from pyspark.sql.types import AtomicType, StructType

from koheesio.asyncio.http import AsyncHttpGetStep
from koheesio.spark.readers import Reader
from koheesio.steps.http import HttpGetStep


# noinspection HttpUrlsUsage
class RestApiReader(Reader):
    # noinspection HttpUrlsUsage
    """
    A reader class that executes an API call and stores the response in a DataFrame.

    Parameters
    ----------
    transport : Union[InstanceOf[AsyncHttpGetStep], InstanceOf[HttpGetStep]]
        The HTTP transport step.
    spark_schema : Union[str, StructType, List[str], Tuple[str, ...], AtomicType]
        The pyspark schema of the response.

    Attributes
    ----------
    transport : Union[InstanceOf[AsyncHttpGetStep], InstanceOf[HttpGetStep]]
        The HTTP transport step.
    spark_schema : Union[str, StructType, List[str], Tuple[str, ...], AtomicType]
        The pyspark schema of the response.

    Returns
    -------
    Reader.Output
        The output of the reader, which includes the DataFrame.


    Examples
    --------
    Here are some examples of how to use this class:

    Example 1: Paginated Transport
    ```python
    import requests
    from urllib3 import Retry

    from koheesio.steps.http import HttpGetStep
    from koheesio.spark.readers.rest_api import RestApiReader

    session = requests.Session()
    retry_logic = Retry(total=max_retries, status_forcelist=[503])
    session.mount("https://", HTTPAdapter(max_retries=retry_logic))
    session.mount("http://", HTTPAdapter(max_retries=retry_logic))

    transport = PaginatedHttpGetStep(
        url="https://api.example.com/data?page={page}",
        paginate=True,
        pages=3,
        session=session,
    )
    task = RestApiReader(
        transport=transport,
        spark_schema="id: int, page:int, value: string",
    )
    task.execute()
    all_data = [row.asDict() for row in task.output.df.collect()]
    ```

    Example 2: Async Transport
    ```python
    from aiohttp import ClientSession, TCPConnector
    from aiohttp_retry import ExponentialRetry
    from yarl import URL

    from koheesio.steps.asyncio.http import AsyncHttpGetStep
    from koheesio.spark.readers.rest_api import RestApiReader

    session = ClientSession()
    urls = [URL("http://httpbin.org/get"), URL("http://httpbin.org/get")]
    retry_options = ExponentialRetry()
    connector = TCPConnector(limit=10)
    transport = AsyncHttpGetStep(
        client_session=session,
        url=urls,
        retry_options=retry_options,
        connector=connector,
    )

    task = RestApiReader(
        transport=transport,
        spark_schema="id: int, page:int, value: string",
    )
    task.execute()
    all_data = [row.asDict() for row in task.output.df.collect()]
    ```

    """

    transport: Union[InstanceOf[AsyncHttpGetStep], InstanceOf[HttpGetStep]] = Field(
        ..., description="HTTP transport step", exclude=True
    )
    spark_schema: Union[str, StructType, List[str], Tuple[str, ...], AtomicType] = Field(
        ..., description="The pyspark schema of the response"
    )

    def execute(self) -> Reader.Output:
        """
        Executes the API call and stores the response in a DataFrame.

        Returns
        -------
        Reader.Output
            The output of the reader, which includes the DataFrame.
        """
        raw_data = self.transport.execute()

        data = None
        if isinstance(raw_data, HttpGetStep.Output):
            data = raw_data.response_json
        elif isinstance(raw_data, AsyncHttpGetStep.Output):
            data = [d for d, _ in raw_data.responses_urls]  # type: ignore

        if data:
            self.output.df = self.spark.createDataFrame(data=data, schema=self.spark_schema)  # type: ignore
