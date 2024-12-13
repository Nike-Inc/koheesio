"""
This module contains async implementation of HTTP step.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Union
import asyncio
import warnings

from aiohttp import BaseConnector, ClientSession, TCPConnector
from aiohttp_retry import ExponentialRetry, RetryClient, RetryOptionsBase
import nest_asyncio  # type: ignore[import-untyped]
import yarl

from pydantic import Field, SecretStr, field_validator, model_validator

from koheesio.asyncio import AsyncStep, AsyncStepOutput
from koheesio.models import ExtraParamsMixin
from koheesio.steps.http import HttpMethod


# noinspection PyUnresolvedReferences
class AsyncHttpStep(AsyncStep, ExtraParamsMixin):
    """
    Asynchronous HTTP step for making HTTP requests using aiohttp.

    Parameters
    ----------
    client_session : Optional[ClientSession]
        Aiohttp ClientSession.
    url : List[yarl.URL]
        List of yarl.URL.
    retry_options : Optional[RetryOptionsBase]
        Retry options for the request.
    connector : Optional[BaseConnector]
        Connector for the aiohttp request.
    headers : Optional[Dict[str, Union[str, SecretStr]]]
        Request headers.

    Output
    ------
    responses_urls : Optional[List[Tuple[Dict[str, Any], yarl.URL]]]
        List of responses from the API and request URL.

    Examples
    --------
    ```python
    import asyncio
    from aiohttp import ClientSession
    from aiohttp.connector import TCPConnector
    from aiohttp_retry import ExponentialRetry
    from koheesio.asyncio.http import AsyncHttpStep
    from yarl import URL
    from typing import Dict, Any, Union, List, Tuple


    # Initialize the AsyncHttpStep
    async def main():
        session = ClientSession()
        urls = [
            URL("https://example.com/api/1"),
            URL("https://example.com/api/2"),
        ]
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

        # Execute the step
        responses_urls = await step.get()

        return responses_urls


    # Run the main function
    responses_urls = asyncio.run(main())
    ```
    """

    client_session: Optional[ClientSession] = Field(default=None, description="Aiohttp ClientSession", exclude=True)
    url: List[yarl.URL] = Field(
        default_factory=list,
        alias="urls",
        description="""Expecting list, as there is no value in executing async request for one value.
        yarl.URL is preferable, because params/data can be injected into URL instance""",
        exclude=True,
    )
    retry_options: Optional[RetryOptionsBase] = Field(
        default=None, description="Retry options for the request", exclude=True
    )
    connector: Optional[BaseConnector] = Field(
        default=None, description="Connector for the aiohttp request", exclude=True
    )
    headers: Dict[str, Union[str, SecretStr]] = Field(
        default_factory=dict,
        description="Request headers",
        alias="header",
        exclude=True,
    )

    timeout: None = Field(default=None, description="[Optional] Request timeout")

    method: Union[str, HttpMethod] = Field(
        default=HttpMethod.GET,
        description="What type of Http call to perform. One of 'get', 'post', 'put', 'delete'. Defaults to 'get'.",
    )

    class Output(AsyncStepOutput):
        """Output class for Step"""

        responses_urls: Optional[List[Tuple[Dict[str, Any], yarl.URL]]] = Field(
            default=None, description="List of responses from the API and request URL", repr=False
        )

    def __tasks_generator(self, method: HttpMethod) -> List[asyncio.Task]:
        """
        Generate a list of tasks for making HTTP requests.

        Parameters
        ----------
        method : HttpMethod
            The HTTP method to use for the requests.

        Returns
        -------
        List[Task]
            A list of asyncio tasks for making HTTP requests.
        """
        tasks = [
            asyncio.create_task(
                self.request(
                    method=method,
                    url=u,
                    headers=self.get_headers(),
                )
            )
            for u in self.url
        ]

        return tasks

    @model_validator(mode="after")
    def _move_extra_params_to_params(self) -> AsyncHttpStep:
        """
        Move extra_params to params dict.

        Returns
        -------
        AsyncHttpStep
            The updated AsyncHttpStep instance.
        """
        return self

    async def _execute(self, tasks: List[asyncio.Task]) -> List[Tuple[Dict[str, Any], yarl.URL]]:
        """
        Execute the HTTP requests asynchronously.

        Parameters
        ----------
        tasks : List[Task]
            A list of asyncio tasks for making HTTP requests.

        Returns
        -------
        List[Tuple[Dict[str, Any], yarl.URL]]
            A list of response data and corresponding request URLs.
        """
        self._init_session()
        try:
            responses_urls = await asyncio.gather(*tasks)
        finally:
            if self.client_session:
                await self.client_session.close()
            await self.__retry_client.close()

        return responses_urls

    def _init_session(self) -> None:
        """
        Initialize the aiohttp session and retry client.
        """
        self.connector = self.connector or TCPConnector(limit=10)
        self.client_session = self.client_session or ClientSession(connector=self.connector)
        self.retry_options = self.retry_options or ExponentialRetry()
        # Disable pylint warning: attribute is not initialized in __init__
        # pylint: disable=W0201
        self.__retry_client = RetryClient(
            client_session=self.client_session, logger=self.log, retry_options=self.retry_options, raise_for_status=True
        )

    @field_validator("timeout")
    def validate_timeout(cls, timeout: Any) -> None:
        """
        Validate the 'timeout' field.

        Parameters
        ----------
        timeout : Any
            The value of the 'timeout' field.

        Raises
        ------
        ValueError
            If 'data' is not allowed in AsyncHttpStep.
        """
        if timeout:
            raise ValueError("timeout is not allowed in AsyncHttpStep. Provide timeout through retry_options.")

    def get_headers(self) -> Union[None, dict]:
        """
        Get the request headers.

        Returns
        -------
        Optional[Dict[str, Union[str, SecretStr]]]
            The request headers.
        """
        _headers = None

        if self.headers:
            _headers = {k: v.get_secret_value() if isinstance(v, SecretStr) else v for k, v in self.headers.items()}

            for k, v in self.headers.items():
                if isinstance(v, SecretStr):
                    self.headers[k] = v.get_secret_value()

        return _headers or self.headers

    # noinspection PyUnusedLocal,PyMethodMayBeStatic
    def set_outputs(self, response) -> None:  # type: ignore[no-untyped-def]
        """
        Set the outputs of the step.

        Parameters
        ----------
        response : Any
            The response data.
        """
        warnings.warn("set outputs is not implemented in AsyncHttpStep.")

    # noinspection PyMethodMayBeStatic
    def get_options(self) -> None:
        """
        Get the options of the step.
        """
        warnings.warn("get_options is not implemented in AsyncHttpStep.")

    # Disable pylint warning: method was expected to be 'non-async'
    # pylint: disable=W0236
    async def request(  # type: ignore[no-untyped-def]
        self,
        method: HttpMethod,
        url: yarl.URL,
        **kwargs,
    ) -> Tuple[Dict[str, Any], yarl.URL]:
        """
        Make an HTTP request.

        Parameters
        ----------
        method : HttpMethod
            The HTTP method to use for the request.
        url : yarl.URL
            The URL to make the request to.
        kwargs : Any
            Additional keyword arguments to pass to the request.

        Returns
        -------
        Tuple[Dict[str, Any], yarl.URL]
            A tuple containing the response data and the request URL.
        """
        async with self.__retry_client.request(method=method, url=url, **kwargs) as response:
            res = await response.json()

        return res, response.request_info.url

    # Disable pylint warning: method was expected to be 'non-async'
    # pylint: disable=W0236
    # noinspection PyMethodOverriding
    async def get(self) -> List[Tuple[Dict[str, Any], yarl.URL]]:
        """
        Make GET requests.

        Returns
        -------
        List[Tuple[Dict[str, Any], yarl.URL]]
            A list of response data and corresponding request URLs.
        """
        tasks = self.__tasks_generator(method=HttpMethod.GET)
        responses_urls = await self._execute(tasks=tasks)

        return responses_urls

    # Disable pylint warning: method was expected to be 'non-async'
    # pylint: disable=W0236
    async def post(self) -> List[Tuple[Dict[str, Any], yarl.URL]]:
        """
        Make POST requests.

        Returns
        -------
        List[Tuple[Dict[str, Any], yarl.URL]]
            A list of response data and corresponding request URLs.
        """
        tasks = self.__tasks_generator(method=HttpMethod.POST)
        responses_urls = await self._execute(tasks=tasks)

        return responses_urls

    # Disable pylint warning: method was expected to be 'non-async'
    # pylint: disable=W0236
    async def put(self) -> List[Tuple[Dict[str, Any], yarl.URL]]:
        """
        Make PUT requests.

        Returns
        -------
        List[Tuple[Dict[str, Any], yarl.URL]]
            A list of response data and corresponding request URLs.
        """
        tasks = self.__tasks_generator(method=HttpMethod.PUT)
        responses_urls = await self._execute(tasks=tasks)

        return responses_urls

    # Disable pylint warning: method was expected to be 'non-async'
    # pylint: disable=W0236
    async def delete(self) -> List[Tuple[Dict[str, Any], yarl.URL]]:
        """
        Make DELETE requests.

        Returns
        -------
        List[Tuple[Dict[str, Any], yarl.URL]]
            A list of response data and corresponding request URLs.
        """
        tasks = self.__tasks_generator(method=HttpMethod.DELETE)
        responses_urls = await self._execute(tasks=tasks)

        return responses_urls

    def execute(self) -> None:
        """
        Execute the step.

        Raises
        ------
        ValueError
            If the specified HTTP method is not implemented in AsyncHttpStep.
        """
        # By design asyncio does not allow its event loop to be nested. This presents a practical problem:
        #   When in an environment where the event loop is already running
        #   it’s impossible to run tasks and wait for the result.
        #   Trying to do so will give the error “RuntimeError: This event loop is already running”.
        #   The issue pops up in various environments, such as web servers, GUI applications and in
        #   Jupyter/DataBricks notebooks.
        nest_asyncio.apply()

        map_method_func = {
            HttpMethod.GET: self.get,
            HttpMethod.POST: self.post,
            HttpMethod.PUT: self.put,
            HttpMethod.DELETE: self.delete,
        }

        if self.method not in map_method_func:
            raise ValueError(f"Method {self.method} not implemented in AsyncHttpStep.")

        self.output.responses_urls = asyncio.run(map_method_func[self.method]())  # type: ignore[index]


class AsyncHttpGetStep(AsyncHttpStep):
    """
    Represents an asynchronous HTTP GET step.

    This class inherits from the AsyncHttpStep class and specifies the HTTP method as GET.

    Attributes:
        method (HttpMethod): The HTTP method for the step, set to HttpMethod.GET.

    """

    method: HttpMethod = HttpMethod.GET
