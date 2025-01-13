import os
from pathlib import Path
import time

from koheesio.testing import pytest, register_fixtures
from koheesio.testing.fixtures import logger, random_uuid
from koheesio.utils import get_project_root

# TODO:
#  - create an 'on_windows' util
#  - move to utils, and make it a fixture
#  - add documentation
#  - investigate if we can do this for windows as well
#  - somehow test it on windows
if os.name != "nt":  # 'nt' is the name for Windows
    # force time zone to be UTC
    os.environ["TZ"] = "UTC"
    # `tzset()` is specific to Unix-based systems. Hence, the `time.tzset()` function is not available on Windows.
    # If you try to run it on a Windows system, you will get an AttributeError indicating that the module 'time' has
    # no attribute 'tzset'.
    time.tzset()


PROJECT_ROOT = get_project_root()
TEST_DATA_PATH = Path(PROJECT_ROOT / "tests" / "_data")
DELTA_FILE = Path(TEST_DATA_PATH / "readers" / "delta_file")

random_uuid = register_fixtures(random_uuid, scope="function")
logger = register_fixtures(logger, scope="session")


@pytest.fixture(scope="session")
def data_path() -> str:
    return TEST_DATA_PATH.as_posix()


@pytest.fixture(scope="session")
def delta_file() -> str:
    return DELTA_FILE.as_posix()
