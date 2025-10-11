"""
This module contains several HTTP Steps that can be used to perform API Calls to HTTP endpoints

Example
-------
```python
from koheesio.steps.http import HttpGetStep

response = (
    HttpGetStep(url="https://google.com").execute().json_payload
)
```

In the above example, the `response` variable will contain the JSON response from the HTTP request.
"""

from typing import Any, Dict, Generator, List, Optional, Union
import contextlib
from enum import Enum
import json

import requests  # type: ignore[import-untyped]
from urllib3.util import Retry

from koheesio import Step
from koheesio.models import (
    ExtraParamsMixin,
    Field,
    SecretStr,
    field_serializer,
    field_validator,
    model_validator,
)

__all__ = [
    "HttpMethod",
    "HttpStep",
    "HttpGetStep",
    "HttpPostStep",
    "HttpPutStep",
    "HttpDeleteStep",
    "PaginatedHttpGetStep",
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
    def from_string(cls, value: str) -> str:
        """Allows for getting the right Method Enum by simply passing a string value
        This method is not case-sensitive
        """
        return getattr(cls, value.upper())


class HttpStep(Step, ExtraParamsMixin):
    """
    Can be used to perform API Calls to HTTP endpoints

    Authorization
    -------------
    The optional `auth_header` parameter in HttpStep allows you to pass an authorization header, such as a bearer token.
    For example: `auth_header = "Bearer <token>"`.

    The `auth_header` value is stored as a `SecretStr` object to prevent sensitive information from being displayed in logs.

    Of course, authorization can also just be passed as part of the regular `headers` parameter.

    For example, either one of these parameters would semantically be the same:
    ```python
    headers = {
        "Authorization": "Bearer <token>",
        "Content-Type": "application/json",
    }
    ```
    # or
    auth_header = "Bearer <token>"
    ```

    The `auth_header` parameter is useful when you want to keep the authorization separate from the other headers, for
    example when your implementation requires you to pass some custom headers in addition to the authorization header.

    > Note: The `auth_header` parameter can accept any authorization header value, including basic authentication
        tokens, digest authentication strings, NTLM, etc.

    Understanding Retries
    ----------------------
    This class includes a built-in retry mechanism for handling temporary issues, such as network errors or server
    downtime, that might cause the HTTP request to fail. The retry mechanism is controlled by two parameters:
    `max_retries` and `backoff_factor`, and will only be triggered for error codes 502,503 and 504.

    - `max_retries` determines the number of retries after the initial request. For example, if `max_retries` is set to
        4, the request will be attempted a total of 5 times (1 initial attempt + 4 retries). If `max_retries` is set to
        0, no retries will be attempted, and the request will be tried only once.

    - `backoff_factor` controls the rate at which the delay increases for each subsequent retry. If `backoff_factor` is
        set to 2 (the default), the delay will double with each retry. If `backoff` is set to 1, the delay between
        retries will remain constant. Changing the `backoff_factor` value affects how quickly the delay increases.

    Given the default values of `max_retries=3` and `backoff=2`, the delays between retries would be 2 seconds,
    4 seconds, and 8 seconds, respectively. This results in a total delay of 14 seconds before all retries are
    exhausted.

    Parameters
    ----------
    url : str, required
        API endpoint URL.
    headers : Dict[str, Union[str, SecretStr]], optional, default={"Content-Type": "application/json"}
        Request headers.
    auth_header : Optional[SecretStr], optional, default=None
        Authorization header. An optional parameter that can be used to pass an authorization, such as a bearer token.
    data : Union[Dict[str, str], str], optional, default={}
        Data to be sent along with the request.
    timeout : int, optional, default=3
        Request timeout. Defaults to 3 seconds.
    method : Union[str, HttpMethod], required, default='get'
        What type of Http call to perform. One of 'get', 'post', 'put', 'delete'. Defaults to 'get'.
    session : requests.Session, optional, default=requests.Session()
        Existing requests session object to be used for making HTTP requests. If not provided, a new session object
        will be created.
    params : Optional[Dict[str, Any]]
        Set of extra parameters that should be passed to the HTTP request. Note: any kwargs passed to the class will be
        added to this dictionary.
    max_retries : int, optional, default=3
        Maximum number of retries before giving up. Defaults to 3.
    backoff_factor : float, optional, default=2
        Backoff factor for retries. Defaults to 2.

    Output
    ------
    response_raw : Optional[requests.Response]
        The raw requests.Response object returned by the appropriate requests.request() call.
    response_json : Optional[Union[Dict, List]]
        The JSON response for the request.
    raw_payload : Optional[str]
        The raw response for the request.
    status_code : Optional[int]
        The status return code of the request.
    """

    url: str = Field(
        default=...,
        description="API endpoint URL",
        alias="uri",
    )
    headers: Dict[str, Union[str, SecretStr]] = Field(
        default={"Content-Type": "application/json"},
        description="Request headers",
        alias="header",
    )
    auth_header: Optional[SecretStr] = Field(
        default=None,
        description="[Optional] Authorization header",
        alias="authorization_header",
        examples=["Bearer <token>"],
    )
    data: Union[Dict[str, str], str] = Field(
        default_factory=dict, description="[Optional] Data to be sent along with the request", alias="body"
    )
    params: Optional[Dict[str, Any]] = Field(  # type: ignore[assignment]
        default_factory=dict,
        description="[Optional] Set of extra parameters that should be passed to HTTP request",
    )
    timeout: int = Field(default=3, description="[Optional] Request timeout")
    method: Union[str, HttpMethod] = Field(
        default=HttpMethod.GET,
        description="What type of Http call to perform. One of 'get', 'post', 'put', 'delete'. Defaults to 'get'.",
    )
    session: requests.Session = Field(
        default_factory=requests.Session,
        description=(
            "Existing requests session object to be used for making HTTP requests. If not provided, a new session "
            "object will be created."
        ),
        exclude=True,
        repr=False,
    )
    max_retries: int = Field(
        default=3,
        description="Maximum number of retries before giving up. Defaults to 3.",
    )
    backoff_factor: float = Field(
        default=2,
        description="Backoff factor for retries. Defaults to 2.",
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
        def json_payload(self) -> Union[dict, list, None]:
            """Alias for response_json"""
            return self.response_json

    @field_validator("method")
    def get_proper_http_method_from_str_value(cls, method_value: str) -> str:
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

    @model_validator(mode="after")
    def encode_sensitive_headers(self) -> "HttpStep":
        """
        Encode potentially sensitive data into pydantic.SecretStr class to prevent them
        being displayed as plain text in logs.
        """
        if auth_header := self.auth_header:
            # ensure the token is preceded with the word 'Bearer'
            self.headers["Authorization"] = auth_header
            del self.auth_header
        if auth := self.headers.get("Authorization"):
            self.headers["Authorization"] = auth if isinstance(auth, SecretStr) else SecretStr(auth)
        return self

    @field_serializer("headers", when_used="json")
    def decode_sensitive_headers(self, headers: dict) -> dict:
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

    def get_headers(self) -> dict:
        """
        Dump headers into JSON without SecretStr masking.
        """
        return json.loads(self.model_dump_json()).get("headers")

    def set_outputs(self, response: requests.Response) -> None:
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
                self.log.error(f"An error occurred while processing the JSON payload. Error message:\n{e.msg}")

    def get_options(self) -> dict:
        """options to be passed to requests.request()"""
        return {
            "url": self.url,
            "headers": self.get_headers(),
            "data": self.data,
            "timeout": self.timeout,
            **self.params,  # type: ignore
        }

    def _configure_session(self) -> None:
        """Configure session with current retry settings"""
        retries = Retry(
            total=self.max_retries,
            connect=None,  # Only retry on status codes
            read=None,  # Only retry on status codes
            redirect=0,  # No redirect retries
            status=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=[502, 503, 504],
            allowed_methods=frozenset(["GET", "POST", "PUT", "DELETE"]),
            respect_retry_after_header=True,
        )
        adapter = requests.adapters.HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    @contextlib.contextmanager
    def _request(
        self, method: Optional[HttpMethod] = None, stream: bool = False
    ) -> Generator[requests.Response, None, None]:
        """
        Executes the HTTP request with retry logic.

        Actual http_method execution is abstracted into this method.
        This is to avoid unnecessary code duplication. Allows to centrally log, set outputs, and validated.

        This method will try to execute `requests.request` up to `self.max_retries` times. If `self.request()` raises
        an exception with error code 502,503 or 504, it logs a warning message and the error message, then waits for
        `(self.backoff_factor ** i)` seconds before retrying, where the delay increases exponentially after each failed
        attempt.

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
        stream : bool
            Whether to stream the response content. Defaults to False.

        Raises
        ------
        requests.RequestException, requests.HTTPError
            The last exception that was caught if `requests.request()` fails after `self.max_retries` attempts.
        """
        _method = (method or self.method).value.upper()
        self.log.debug(f"Making {_method} request to {self.url} with headers {self.headers}")
        options = self.get_options()

        self._configure_session()

        with self.session.request(method=_method, **options, stream=stream) as response:
            response.raise_for_status()
            self.log.debug(f"Received response with status code {response.status_code} and body {response.text}")

            try:
                yield response
            finally:
                self.log.debug("Request context manager exiting")

    # noinspection PyMethodOverriding
    def get(self) -> requests.Response:
        """Execute an HTTP GET call"""
        self.method = HttpMethod.GET
        with self.request() as response:
            return response

    def post(self) -> requests.Response:
        """Execute an HTTP POST call"""
        self.method = HttpMethod.POST
        with self.request() as response:
            return response

    def put(self) -> requests.Response:
        """Execute an HTTP PUT call"""
        self.method = HttpMethod.PUT
        with self.request() as response:
            return response

    def delete(self) -> requests.Response:
        """Execute an HTTP DELETE call"""
        self.method = HttpMethod.DELETE
        with self.request() as response:
            return response

    def execute(self) -> None:
        """
        Executes the HTTP request.

        This method simply calls `self.request()`, which includes the retry logic. If `self.request()` raises an
        exception, it will be propagated to the caller of this method.

        Raises
        ------
        requests.RequestException, requests.HTTPError
            The last exception that was caught if `self.request()` fails after `self.max_retries` attempts.
        """
        with self._request() as response:
            self.log.info(f"HTTP request to {self.url}, status code {response.status_code}")
            self.set_outputs(response)


class HttpGetStep(HttpStep):
    """send GET requests

    Example
    -------
    ```python
    response = (
        HttpGetStep(url="https://google.com").execute().json_payload
    )
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


class PaginatedHttpGetStep(HttpGetStep):
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

    def get_options(self) -> dict:
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

    def execute(self) -> None:
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

        for page in range(offset, pages):  # type: ignore[arg-type]
            if self.paginate:
                self.log.info(f"Fetching page {page} of {pages - 1}")

            self.url = self._url(basic_url=_basic_url, page=page)

            with self._request() as response:
                if isinstance(response_json := response.json(), list):
                    data += response_json
                else:
                    data.append(response_json)

        self.url = _basic_url
        self.output.response_json = data
        self.output.response_raw = None
        self.output.raw_payload = None
        self.output.status_code = None
