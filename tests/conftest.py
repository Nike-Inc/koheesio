import os
import time
import uuid

import pytest

from koheesio.logger import LoggingFactory

if os.name != 'nt':  # 'nt' is the name for Windows
    # force time zone to be UTC
    os.environ["TZ"] = "UTC"
    time.tzset()


@pytest.fixture(scope="session")
def random_uuid():
    return str(uuid.uuid4()).replace("-", "_")


@pytest.fixture(scope="session")
def logger(random_uuid):
    return LoggingFactory.get_logger(name="conf_test" + random_uuid)
