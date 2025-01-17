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

import socket
import ssl

_HttpRequestTransformation_chunk_size_column = "HttpRequestTransformation_chunk_size_column"
_HttpRequestTransformation_method_column = "HttpRequestTransformation_method_column"
_HttpRequestTransformation_content_type_column = "HttpRequestTransformation_content_type_column"
_HttpRequestTransformation_authorization_token_column = "HttpRequestTransformation_authorization_token_column"
_HttpRequestTransformation_body_column = "HttpRequestTransformation_authorization_body_column"

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
    authorization_token: Union[str, None] = Field(
        default=None,
        description="The authorization token for the request.",
    )
    body: Union[str, None] = Field(
        default=None,
        description="The body of the request.",
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
            _HttpRequestTransformation_chunk_size_column,
            lit(self.chunk_size)
        )
        self.df = self.df.withColumn(
            _HttpRequestTransformation_method_column,
            lit(self.method)
        )
        self.df = self.df.withColumn(
            _HttpRequestTransformation_content_type_column,
            lit(self.content_type)
        )
        self.df = self.df.withColumn(
            _HttpRequestTransformation_authorization_token_column,
            lit(self.authorization_token)
        )
        self.df = self.df.withColumn(
            _HttpRequestTransformation_body_column,
            lit(self.body)
        )

        self.df = self.df.withColumn(
            self.target_column,
            execute_http_request(
                source_column_name, 
                _HttpRequestTransformation_method_column, 
                _HttpRequestTransformation_content_type_column, 
                _HttpRequestTransformation_authorization_token_column,
                _HttpRequestTransformation_body_column, 
                _HttpRequestTransformation_chunk_size_column
            )
        )

        self.df = self.df.drop(
            _HttpRequestTransformation_chunk_size_column,
            _HttpRequestTransformation_method_column,
            _HttpRequestTransformation_content_type_column,
            _HttpRequestTransformation_authorization_token_column,
            _HttpRequestTransformation_body_column
        )

        self.output.df = self.df

@udf
def execute_http_request(url: str, method: str, content_type: str, authorization_token: str = None, body: str = None, chunk_size: int = 8196) -> str:
    
    protocol, rest = url.split("://")
    base_url = ""
    port = 0
    path = ""
    # Check if there is a port number in the URL. If not, use the default port number for the protocol. 80 for http and 443 for https
    if ":" in rest:
        base_url, rest = rest.split(":", 1)
        if "/" in rest:
            port, path = rest.split("/", 1)
        else:
            port = rest
        port = int(port)
    else:
        base_url, path = rest.split("/", 1)
        port = 80 if protocol == "http" else 443

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
            response = response + chunk

        result = response.decode('iso-8859-1')

    # Extract the status code and content from the HTTP response
    status_code = result.split("\r\n")[0].split(" ")[1]
    content = "\r\n".join(result.split("\r\n\r\n")[1:])

    if status_code != "200":
        content = "ERROR - " + status_code

    return content