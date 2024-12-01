from io import StringIO
import logging
from logging import Logger
from unittest.mock import MagicMock, patch

import pytest

from koheesio.logger import (
    LoggingFactory,
    MaskedDict,
    MaskedFloat,
    MaskedInt,
    MaskedString,
)


class TestLoggingFactory:
    def test_create_logger(self):
        logger = LoggingFactory.get_logger(name="TestLogger")
        assert isinstance(logger, Logger)

    def test_get_logger(self):
        child_name = "child_logger"
        ind_name = "independent_logger"
        logger = LoggingFactory.get_logger(name=child_name, inherit_from_koheesio=True)
        logger.debug("Test Child DEBUG")
        logger_independent = LoggingFactory.get_logger(name=ind_name)
        logger_independent.setLevel("INFO")

        assert isinstance(logger, Logger)
        assert logger.name == f"{LoggingFactory.LOGGER_NAME}.{child_name}"
        assert logger.parent.level == logging._nameToLevel[LoggingFactory.LOGGER_LEVEL]
        assert isinstance(logger_independent, Logger)
        assert logger_independent.name == ind_name
        assert logger_independent.level == logging.INFO


class TestAddHandlers:
    @pytest.fixture
    def mock_import_class(self):
        with patch("koheesio.logger.import_class") as mock:
            yield mock

    @pytest.fixture
    def mock_handler_class(self, mock_import_class):
        mock_handler_class = MagicMock()
        mock_import_class.return_value = mock_handler_class
        return mock_handler_class

    @pytest.fixture
    def mock_handler_instance(self, mock_handler_class):
        mock_handler_instance = MagicMock()
        mock_handler_class.return_value = mock_handler_instance
        return mock_handler_instance

    @pytest.fixture
    def handlers_config(self):
        return [("handler_module.Class1", {"level": "DEBUG"})]

    def test_add_handlers(self, mock_import_class, mock_handler_instance, handlers_config):
        # Configure the LoggingFactory attributes
        LoggingFactory.LOGGER = MagicMock()
        LoggingFactory.LOGGER_FILTER = MagicMock()
        LoggingFactory.LOGGER_FORMATTER = MagicMock()

        # Call the add_handlers method
        LoggingFactory.add_handlers(handlers_config)

        # Assertions
        mock_import_class.assert_called_with("handler_module.Class1")
        mock_handler_instance.setLevel.assert_called_with("DEBUG")
        mock_handler_instance.addFilter.assert_called_with(LoggingFactory.LOGGER_FILTER)
        mock_handler_instance.setFormatter.assert_called_with(LoggingFactory.LOGGER_FORMATTER)
        LoggingFactory.LOGGER.addHandler.assert_called_with(mock_handler_instance)


@pytest.mark.parametrize(
    "foo, bar, baz, params",
    [
        pytest.param(
            MaskedString("some value"),
            MaskedInt(123),
            MaskedFloat(1.23),
            MaskedDict({"key": "value"}),
            id="test_log_masked",
        ),
    ],
)
def test_log_masked(foo, bar, baz, params):
    log_capture_string = StringIO()
    ch = logging.StreamHandler(log_capture_string)
    ch.setLevel(logging.DEBUG)
    logger = logging.getLogger("tests")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(ch)

    logger.warning(f"{foo = }")
    logger.warning(foo)

    log_contents = log_capture_string.getvalue().split("\n")
    assert log_contents[-3] == "foo = '**********(Masked)'"
    assert log_contents[-2] == "'**********(Masked)'"

    logger.warning(f"{bar = }")
    logger.warning("this is supposed to be a masked field: %s", baz)
    log_contents = log_capture_string.getvalue().split("\n")
    assert log_contents[-3] == "bar = '***(Masked)'"
    assert log_contents[-2] == "this is supposed to be a masked field: '****(Masked)'"

    logger.warning(MaskedFloat(bar + baz))
    log_contents = log_capture_string.getvalue().split("\n")
    assert log_contents[-2] == "'******(Masked)'"

    logger.info("this is supposed to be a masked field2: %s", params)
    log_contents = log_capture_string.getvalue().split("\n")
    print(log_contents)
    assert log_contents[-2] == "this is supposed to be a masked field2: '****************(Masked)'"
