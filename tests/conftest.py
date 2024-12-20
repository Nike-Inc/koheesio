import os
from pathlib import Path
import time

from koheesio.utils import get_project_root
from koheesio.utils.testing import logger, pytest, random_uuid, register_fixtures

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

register_fixtures(random_uuid, logger, scope="session")


@pytest.fixture(scope="session")
def data_path():
    return TEST_DATA_PATH.as_posix()


@pytest.fixture(scope="session")
def delta_file():
    return DELTA_FILE.as_posix()
