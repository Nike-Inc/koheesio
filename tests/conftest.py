import os
from pathlib import Path
import time
import uuid

import pytest

from koheesio.logger import LoggingFactory
from koheesio.utils import get_project_root
from koheesio.utils.testing import serve_fake_http

if os.name != "nt":  # 'nt' is the name for Windows
    # force time zone to be UTC
    os.environ["TZ"] = "UTC"
    time.tzset()


PROJECT_ROOT = get_project_root()

TEST_DATA_PATH = Path(PROJECT_ROOT / "tests" / "_data")
DELTA_FILE = Path(TEST_DATA_PATH / "readers" / "delta_file")


@pytest.fixture(scope="session")
def random_uuid():
    return str(uuid.uuid4()).replace("-", "_")


@pytest.fixture(scope="session")
def logger(random_uuid):
    return LoggingFactory.get_logger(name="conf_test" + random_uuid)


@pytest.fixture(scope="session")
def data_path():
    return TEST_DATA_PATH.as_posix()


@pytest.fixture(scope="session")
def delta_file():
    return DELTA_FILE.as_posix()


@pytest.fixture(scope="session")
def fake_http_server():
    """Session-scoped fake HTTP server. Yields the base URL (e.g. `http://127.0.0.1:54321`)."""
    yield from serve_fake_http()
