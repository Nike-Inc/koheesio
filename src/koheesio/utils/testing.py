"""Test helpers shipped with koheesio (used by the test suite; not part of the public API).

`serve_fake_http` starts a plain stdlib loopback HTTP server suitable for
exercising the async HTTP stack end-to-end without third-party mocking
libraries (originally introduced to replace the unmaintained `aioresponses`
after the aiohttp 3.14 `stream_writer` break — see aioresponses#289). It
speaks real HTTP on `127.0.0.1:<random-port>`, so it works equally well for
sync entry points (`step.execute()` -> `asyncio.run(...)`) and for async tests
running under `pytest-asyncio`.
"""

from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
from typing import Iterator

__all__ = ["FakeHttpRequestHandler", "serve_fake_http"]


class FakeHttpRequestHandler(BaseHTTPRequestHandler):
    """Request handler for the fake HTTP server.

    Routes:
      - `ANY  /get`             -> 200 JSON `{"url": "<full request URL>"}`
      - `ANY  /status/{code}`   -> `{code}` with an empty JSON body
      - otherwise               -> 404
    """

    def _route(self) -> None:
        path = self.path.split("?", 1)[0]

        if path == "/get":
            host = self.headers.get("Host", f"127.0.0.1:{self.server.server_address[1]}")
            body = json.dumps({"url": f"http://{host}{path}"}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)
            return

        if path.startswith("/status/"):
            try:
                code = int(path.rsplit("/", 1)[-1])
            except ValueError:
                code = 400
            body = b"{}"
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)
            return

        self.send_response(404)
        self.end_headers()

    do_GET = _route
    do_POST = _route
    do_PUT = _route
    do_DELETE = _route
    do_HEAD = _route

    def log_message(self, *_args: object, **_kwargs: object) -> None:
        # Silence per-request stderr noise during the test suite.
        return


def serve_fake_http() -> Iterator[str]:
    """Start the fake HTTP server on an OS-assigned port and yield its base URL.

    Meant to be used from a pytest fixture as a generator body:

        @pytest.fixture(scope="session")
        def fake_http_server():
            yield from serve_fake_http()
    """
    server = ThreadingHTTPServer(("127.0.0.1", 0), FakeHttpRequestHandler)
    thread = threading.Thread(target=server.serve_forever, name="koheesio-fake-http", daemon=True)
    thread.start()
    host, port = server.server_address[0], server.server_address[1]
    base_url = f"http://{host}:{port}"
    try:
        yield base_url
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
