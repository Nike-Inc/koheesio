"""
This module contains a few simple HTTP Steps that can be used to perform API Calls to HTTP endpoints

Example
-------
```python
from koheesio.steps.http import HttpGetStep

response = HttpGetStep(url="https://google.com").execute().json_payload
```

In the above example, the `response` variable will contain the JSON response from the HTTP request.
"""

import json
from typing import Any, Dict, List, Optional, Union
from enum import Enum

import requests

from koheesio import Step
from koheesio.models import (
    ExtraParamsMixin,
    Field,
    SecretStr,
    field_serializer,
    field_validator,
)

__all__ = [
    "HttpMethod",
    "HttpStep",
    "HttpGetStep",
    "HttpPostStep",
    "HttpPutStep",
    "HttpDeleteStep",
    "PaginatedHtppGetStep",
]


class HttpMethod(str, Enum):
    """
    Enumeration of allowed http methods
    """

    GET = "get"
    POST = "post"
    PUT = "put"
    DELETE = "delete"

    @classmethod
    def from_string(cls, value: str):
        """Allows for getting the right Method Enum by simply passing a string value
        This method is not case-sensitive
        """
        return getattr(cls, value.upper())


class HttpStep(Step, ExtraParamsMixin):
    """
    Can be used to perform API Calls to HTTP endpoints

    Understanding Retries
    ----------------------

    This class includes a built-in retry mechanism for handling temporary issues, such as network errors or server
    downtime, that might cause the HTTP request to fail. The retry mechanism is controlled by three parameters:
    `max_retries`, `initial_delay`, and `backoff`.

    - `max_retries` determines the number of retries after the initial request. For example, if `max_retries` is set to
        4, the request will be attempted a total of 5 times (1 initial attempt + 4 retries). If `max_retries` is set to
        0, no retries will be attempted, and the request will be tried only once.

    - `initial_delay` sets the waiting period before the first retry. If `initial_delay` is set to 3, the delay before
        the first retry will be 3 seconds. Changing the `initial_delay` value directly affects the amount of delay
        before each retry.

    - `backoff` controls the rate at which the delay increases for each subsequent retry. If `backoff` is set to 2 (the
        default), the delay will double with each retry. If `backoff` is set to 1, the delay between retries will
        remain constant. Changing the `backoff` value affects how quickly the delay increases.

    Given the default values of `max_retries=3`, `initial_delay=2`, and `backoff=2`, the delays between retries would
    be 2 seconds, 4 seconds, and 8 seconds, respectively. This results in a total delay of 14 seconds before all
    retries are exhausted.

    For example, if you set `initial_delay=3` and `backoff=2`, the delays before the retries would be `3 seconds`,
    `6 seconds`, and `12 seconds`. If you set `initial_delay=2` and `backoff=3`, the delays before the retries would be
    `2 seconds`, `6 seconds`, and `18 seconds`. If you set `initial_delay=2` and `backoff=1`, the delays before the
    retries would be `2 seconds`, `2 seconds`, and `2 seconds`.
    """

    url: str = Field(
        default=...,
        description="API endpoint URL",
        alias="uri",
    )
    headers: Optional[Dict[str, Union[str, SecretStr]]] = Field(
        default_factory=dict,
        description="Request headers",
        alias="header",
    )
    data: Optional[Union[Dict[str, str], str]] = Field(
        default_factory=dict, description="[Optional] Data to be sent along with the request", alias="body"
    )
    params: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="[Optional] Set of extra parameters that should be passed to HTTP request",
    )
    timeout: Optional[int] = Field(default=3, description="[Optional] Request timeout")
    method: Union[str, HttpMethod] = Field(
        default=HttpMethod.GET,
        description="What type of Http call to perform. One of 'get', 'post', 'put', 'delete'. Defaults to 'get'.",
    )
    session: requests.Session = Field(
        default_factory=requests.Session,
        description="Requests session object to be used for making HTTP requests",
        exclude=True,
        repr=False,
    )

    class Output(Step.Output):
        """Output class for HttpStep"""

        response_raw: Optional[requests.Response] = Field(
            default=None,
            alias="response",
            description="The raw requests.Response object returned by the appropriate requests.request() call",
        )
        response_json: Optional[Union[Dict, List]] = Field(
            default=None, alias="json_payload", description="The JSON response for the request"
        )
        raw_payload: Optional[str] = Field(
            default=None, alias="response_text", description="The raw response for the request"
        )
        status_code: Optional[int] = Field(default=None, description="The status return code of the request")

        @property
        def json_payload(self):
            """Alias for response_json"""
            return self.response_json

    @field_validator("method")
    def get_proper_http_method_from_str_value(cls, method_value):
        """Converts string value to HttpMethod enum value"""
        if isinstance(method_value, str):
            try:
                method_value = HttpMethod.from_string(method_value)
            except AttributeError as e:
                raise AttributeError(
                    "Only values from HttpMethod class are allowed! "
                    f"Provided value: '{method_value}', allowed values: {', '.join(HttpMethod.__members__.keys())}"
                ) from e

        return method_value

    @field_validator("headers", mode="before")
    def encode_sensitive_headers(cls, headers):
        """
        Encode potentially sensitive data into pydantic.SecretStr class to prevent them
        being displayed as plain text in logs.
        """
        if auth := headers.get("Authorization"):
            headers["Authorization"] = auth if isinstance(auth, SecretStr) else SecretStr(auth)
        return headers

    @field_serializer("headers", when_used="json")
    def decode_sensitive_headers(self, headers):
        """
        Authorization headers are being converted into SecretStr under the hood to avoid dumping any
        sensitive content into logs by the `encode_sensitive_headers` method.

        However, when calling the `get_headers` method, the SecretStr should be converted back to
        string, otherwise sensitive info would have looked like '**********'.

        This method decodes values of the `headers` dictionary that are of type SecretStr into plain text.
        """
        for k, v in headers.items():
            headers[k] = v.get_secret_value() if isinstance(v, SecretStr) else v
        return headers

    def get_headers(self):
        """
        Dump headers into JSON without SecretStr masking.
        """
        return json.loads(self.model_dump_json()).get("headers")

    def set_outputs(self, response):
        """
        Types of response output
        """
        self.output.response_raw = response
        self.output.raw_payload = response.text
        self.output.status_code = response.status_code

        # Only decode non empty payloads to avoid triggering decoding error unnecessarily.
        if self.output.raw_payload:
            try:
                self.output.response_json = response.json()

            except json.decoder.JSONDecodeError as e:
                self.log.info(f"An error occurred while processing the JSON payload. Error message:\n{e.msg}")

    def get_options(self):
        """options to be passed to requests.request()"""
        return {
            "url": self.url,
            "headers": self.get_headers(),
            "data": self.data,
            "timeout": self.timeout,
            **self.params,  # type: ignore
        }

    def request(self, method: Optional[HttpMethod] = None) -> requests.Response:
        """
        Executes the HTTP request with retry logic.

        Actual http_method execution is abstracted into this method.
        This is to avoid unnecessary code duplication. Allows to centrally log, set outputs, and validated.

        This method will try to execute `requests.request` up to `self.max_retries` times. If `self.request()` raises
        an exception, it logs a warning message and the error message, then waits for
        `self.initial_delay * (self.backoff ** i)` seconds before retrying. The delay increases exponentially
        after each failed attempt due to the `self.backoff ** i` term.

        If `self.request()` still fails after `self.max_retries` attempts, it logs an error message and re-raises the
        last exception that was caught.

        This is a good way to handle temporary issues that might cause `self.request()` to fail, such as network errors
        or server downtime. The exponential backoff ensures that you're not constantly bombarding a server with
        requests if it's struggling to respond.

        Parameters
        ----------
        method : HttpMethod
            Optional parameter that allows calls to different HTTP methods and bypassing class level `method`
            parameter.

        Raises
        ------
        requests.RequestException, requests.HTTPError
            The last exception that was caught if `requests.request()` fails after `self.max_retries` attempts.
        """
        _method = (method or self.method).value.upper()
        options = self.get_options()

        self.log.debug(f"Making {_method} request to {options['url']} with headers {options['headers']}")

        response = self.session.request(method=_method, **options)
        response.raise_for_status()

        self.log.debug(f"Received response with status code {response.status_code} and body {response.text}")
        self.set_outputs(response)

        return response

    def get(self) -> requests.Response:
        """Execute an HTTP GET call"""
        self.method = HttpMethod.GET
        return self.request()

    def post(self) -> requests.Response:
        """Execute an HTTP POST call"""
        self.method = HttpMethod.POST
        return self.request()

    def put(self) -> requests.Response:
        """Execute an HTTP PUT call"""
        self.method = HttpMethod.PUT
        return self.request()

    def delete(self) -> requests.Response:
        """Execute an HTTP DELETE call"""
        self.method = HttpMethod.DELETE
        return self.request()

    def execute(self) -> Output:
        """
        Executes the HTTP request.

        This method simply calls `self.request()`, which includes the retry logic. If `self.request()` raises an
        exception, it will be propagated to the caller of this method.

        Raises
        ------
        requests.RequestException, requests.HTTPError
            The last exception that was caught if `self.request()` fails after `self.max_retries` attempts.
        """
        self.request()


class HttpGetStep(HttpStep):
    """send GET requests

    Example
    -------
    ```python
    response = HttpGetStep(url="https://google.com").execute().json_payload
    ```
    In the above example, the `response` variable will contain the JSON response from the HTTP request.
    """

    method: HttpMethod = HttpMethod.GET


class HttpPostStep(HttpStep):
    """send POST requests"""

    method: HttpMethod = HttpMethod.POST


class HttpPutStep(HttpStep):
    """send PUT requests"""

    method: HttpMethod = HttpMethod.PUT


class HttpDeleteStep(HttpStep):
    """send DELETE requests"""

    method: HttpMethod = HttpMethod.DELETE


class PaginatedHtppGetStep(HttpGetStep):
    """
    Represents a paginated HTTP GET step.

    Parameters
    ----------
    paginate : bool, optional
        Whether to paginate the API response. Defaults to False.
    pages : int, optional
        Number of pages to paginate. Defaults to 1.
    offset : int, optional
        Offset for paginated API calls. Offset determines the starting page. Defaults to 1.
    limit : int, optional
        Limit for paginated API calls. Defaults to 100.
    """

    paginate: Optional[bool] = Field(
        default=False,
        description="Whether to paginate the API response. Defaults to False. When set to True, the API response will "
        "be paginated. The url should contain a named 'page' parameter for example: "
        "api.example.com/data?page={page}",
    )
    pages: Optional[int] = Field(default=1, description="Number of pages to paginate. Defaults to 1")
    offset: Optional[int] = Field(
        default=1,
        description="Offset for paginated API calls. Offset determines the starting page. Defaults to 1. "
        "The url can (optionally) contain a named 'offset' parameter, for example: "
        "api.example.com/data?offset={offset}",
    )
    limit: Optional[int] = Field(
        default=100,
        description="Limit for paginated API calls. The url should (optionally) contain a named limit parameter, "
        "for example: api.example.com/data?limit={limit}",
    )

    def _adjust_params(self) -> Dict[str, Any]:
        """
        Adjusts the parameters by removing the 'paginate' key.

        Returns
        -------
        dict
            The adjusted parameters.
        """
        return {k: v for k, v in self.params.items() if k not in ["paginate"]}  # type: ignore

    def get_options(self):
        """
        Returns the options to be passed to the requests.request() function.

        Returns
        -------
        dict
            The options.
        """
        options = {
            "url": self.url,
            "headers": self.get_headers(),
            "data": self.data,
            "timeout": self.timeout,
            **self._adjust_params(),  # type: ignore
        }

        return options

    def _url(self, basic_url: str, page: Optional[int] = None) -> str:
        """
        Adds additional parameters to the URL.

        Parameters
        ----------
        basic_url : str
            The basic URL without any parameters.
        page : int, optional
            The page number for pagination. Defaults to None.

        Returns
        -------
        str
            The URL with additional parameters.
        """
        url_params = self._adjust_params()

        if self.paginate:
            url_params.update(
                {
                    "offset": self.offset,
                    "limit": self.limit,
                    "page": page,
                }
            )

        return basic_url.format(**url_params)

    def execute(self) -> HttpGetStep.Output:
        """
        Executes the HTTP GET request and handles pagination.

        Returns
        -------
        HttpGetStep.Output
            The output of the HTTP GET request.
        """
        # Set up pagination parameters
        offset, pages = (self.offset, self.pages + 1) if self.paginate else (1, 1)  # type: ignore
        data = []
        _basic_url = self.url

        for page in range(offset, pages):
            if self.paginate:
                self.log.info(f"Fetching page {page} of {pages - 1}")

            self.url = self._url(basic_url=_basic_url, page=page)
            self.request()

            if isinstance(self.output.response_json, list):
                data += self.output.response_json
            else:
                data.append(self.output.response_json)

        self.url = _basic_url
        self.output.response_json = data
        self.output.response_raw = None
        self.output.raw_payload = None
        self.output.status_code = None
