"""

Classes
-------
HttpRequestTransformation
    A transformation class that downloads content from URLs in the specified column 
    and stores the downloaded file paths in a new column.

"""
from typing import Union

from pyspark.sql.types import Row

from koheesio.models import DirectoryPath, Field
from koheesio.spark import Column
from koheesio.spark.transformations import Transformation
from koheesio.spark.utils.common import get_column_name
from koheesio.steps.download_file import DownloadFileStep, FileWriteMode
from pyspark.sql.functions import udf, col, lit
from koheesio.models import SecretStr, Optional
import re

import socket
import ssl

url_parts = re.compile(
    # protocol is a sequence of letters, digits, and characters followed by ://
    r'^(?P<protocol>[^:]+)://'
    # base_url is a sequence of characters until encountering a `/` or a `:` followed by 1 to 5 digits
    r'(?P<base_url>\S+?(?=:\d{1,5}/|/|$))'
    # port is an optional sequence of digits between 0 and 65535 preceded by a colon
    r'(?::(?P<port>\d{1,5}))?'
    # path is the path of the url
    r'(?P<path>\/.*)?$'
)

@udf
def execute_http_request(url: str, method: str, content_type: str, authorization_token: str = None, body: str = None, chunk_size: int = 8196) -> str:

    match = url_parts.match(url)
    protocol = match.group("protocol")
    base_url = match.group("base_url") 
    port = int(match.group("port")) if not match.group("port") is None else 80
    path = match.group("path") if not match.group("path") is None else "/"

    path = "/" + path

    result = None

    with socket.socket() as s:

        s.connect((base_url, port))

        # Wrap the socket with SSL if the protocol is https
        if protocol == "https":
            context = ssl.create_default_context()
            s = context.wrap_socket(s, server_hostname=base_url)

        # Send the HTTP request to the server
        # Use HTTP/1.0 to close the connection as soon as it is done 
        http_request_string = f"{method} {path} HTTP/1.0\r\nHost: {base_url}\r\n"
        http_request_string += "Accept: */*\r\n"
        
        # if authorization is required, add the Authorization header to the request
        if authorization_token:
            http_request_string += f"Authorization: Bearer {authorization_token}\r\n"
        
        if method != "GET" and body and content_type:
            # content_type is required if a body is sent with it. "application/json" for JSON data, "application/x-www-form-urlencoded" for form data, ...
            http_request_string += f"Content-Type: {content_type}\r\n"
            http_request_string += f"Content-Length: {str(len(body))}\r\n" + \
                                "\r\n" + str(body)
            
        else :
            http_request_string += "\r\n"
        
        request = str.encode(http_request_string)
        sent = s.send(request)

        # Receive the response from the server. This can be in chunks. Hence the use of a while loop.
        response = b""
        while True:
            chunk = s.recv(chunk_size)
            if len(chunk) == 0:     # No more data received, quitting
                break
            response += chunk

        result = response.decode('iso-8859-1')

    # Extract the status code and content from the HTTP response
    status_code = result.split("\r\n")[0].split(" ")[1]
    content = "\r\n".join(result.split("\r\n\r\n")[1:])

    if status_code != "200":
        content = "ERROR - " + status_code

    return content

class HttpRequestTransformation(Transformation):

    column: Union[Column, str] = Field(
        default="",
        description="The column that holds the URLs to request data from.",
    )
    target_column: str = Field(
        default=None,
        alias="target_suffix",
        description="The column that the http result body is written to.",
    )
    chunk_size: int = Field(
        8192,
        ge=16,
        description="The size (in bytes) of the chunks to download the file in, must be greater than or equal to 16.",
    )
    method: str = Field(
        default="GET",
        description="The HTTP method to use for the request.",
    )
    content_type: str = Field(
        default="application/json",
        description="The content type of the request.",
    )
    authorization_token: Optional[SecretStr] = Field(
        default=None,
        description="The authorization token for the request.",
    )
    body_column: Optional[str] = Field(
        default=None,
        description="The body_column name to be used in the request. If None, no body is sent with the request. GET requests do not have a body for instance.",
    )

    def execute(self):
        """
        Download files from URLs in the specified column.
        """
        # Collect the URLs from the DataFrame and process them
        source_column_name = self.column
        if not isinstance(source_column_name, str):
            source_column_name = get_column_name(source_column_name)

        self.df = self.df.withColumn(
            self.target_column,
            execute_http_request(
                col(source_column_name), 
                lit(self.method),
                lit(self.content_type),
                lit(self.authorization_token),
                col(self.body_column) if self.body_column else lit(None), 
                lit(self.chunk_size)
            )
        )

        self.output.df = self.df
