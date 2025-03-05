"""Loggers are used to log messages from your application.

For a comprehensive guide on the usage, examples, and additional features of the logging classes, please refer to the
[reference/concepts/logger](../reference/concepts/logger.md) section of the Koheesio documentation.

Classes
-------
LoggingFactory
    Logging factory to be used to generate logger instances.
Masked
    Represents a masked value.
MaskedString
    Represents a masked string value.
MaskedInt
    Represents a masked integer value.
MaskedFloat
    Represents a masked float value.
MaskedDict
    Represents a masked dictionary value.
LoggerIDFilter
    Filter which injects run_id information into the log.

Functions
---------
warn
    Issue a warning.

"""

from __future__ import annotations

from typing import Any, Dict, Generator, Generic, List, Optional, Tuple, TypeVar
import inspect
import logging
from logging import Formatter, Logger, LogRecord, getLogger
import os
import sys
from uuid import uuid4
from warnings import warn

from koheesio.utils import import_class

T = TypeVar("T")

__all__ = [
    "Masked",
    "MaskedString",
    "MaskedInt",
    "MaskedFloat",
    "MaskedDict",
    "Logger",
    "LoggerIDFilter",
    "LoggingFactory",
    "warn",
]


class Masked(Generic[T]):
    """
    Represents a masked value.

    Parameters
    ----------
    value : T
        The value to be masked.

    Attributes
    ----------
    _value : T
        The original value.

    Methods
    -------
    __repr__() -> str
        Returns a string representation of the masked value.
    __str__() -> str
        Returns a string representation of the masked value.
    __get_validators__() -> Generator
        Returns a generator of validators for the masked value.
    validate(v: Any, values)
        Validates the masked value.

    """

    def __init__(self, value: T):
        self._value = value

    def __repr__(self) -> str:
        used_as_output = [
            line
            for f in inspect.stack()
            for line in (f.code_context or [])
            if any(
                e in line.strip()
                for e in [".debug(", ".info(", ".log(", ".warning(", ".error(", ".critical(", "print("]
            )
        ]
        return repr(self._value if not used_as_output else "*" * len(str(self._value)) + "(Masked)")

    def __str__(self) -> str:
        return self.__repr__()

    @classmethod
    def __get_validators__(cls) -> Generator:
        """
        Returns a generator that yields the validator function for the class.

        Yields:
            Generator: The validator function for the class.
        """
        yield cls.validate

    @classmethod
    def validate(cls, v: Any, _values: Any) -> Masked:
        """
        Validate the input value and return an instance of the class.

        Parameters
        ----------
        v : Any
            The input value to validate.
        _values : Any
            Additional values used for validation.

        Returns
        -------
        instance : cls
            An instance of the class.

        """
        return cls(v)


class MaskedString(Masked, str):
    """
    Represents a masked string value.
    """

    ...


class MaskedInt(Masked, int):
    """
    Represents a masked integer value.
    """

    ...


class MaskedFloat(Masked, float):
    """
    Represents a masked float value.
    """

    ...


class MaskedDict(Masked, dict):
    """
    Represents a masked dictionary value.
    """

    ...


class LoggerIDFilter(logging.Filter):
    """Filter which injects run_id information into the log."""

    LOGGER_ID: str = str(uuid4())

    def filter(self, record: LogRecord) -> bool:
        record.logger_id = LoggerIDFilter.LOGGER_ID

        return True


class LoggingFactory:
    """Logging factory to be used to generate logger instances."""

    LOGGER: Optional[logging.Logger] = None
    LOGGER_FILTER: Optional[logging.Filter] = None
    LOGGER_NAME: str = "koheesio"
    LOGGER_LEVEL: str = os.environ.get("KOHEESIO_LOGGING_LEVEL", "WARNING")
    LOGGER_ENV: str = "local"
    LOGGER_FORMAT: str = (
        "[%(logger_id)s] [%(asctime)s] [%(levelname)s] [%(name)s] {%(module)s.py:%(funcName)s:%(lineno)d} - %(message)s"
    )
    LOGGER_FORMATTER: Formatter = Formatter(LOGGER_FORMAT)
    CONSOLE_HANDLER: Optional[logging.Handler] = None
    ENV: Optional[str] = None

    def __init__(
        self,
        name: Optional[str] = None,
        env: Optional[str] = None,
        level: Optional[str] = None,
        logger_id: Optional[str] = None,
    ):
        """Logging factory to be used in pipeline.Prepare logger instance.

        Parameters
        ----------
        name logger name.
        env environment ("local", "qa", "prod).
        logger_id unique identifier for the logger.
        """

        LoggingFactory.LOGGER_NAME = name or LoggingFactory.LOGGER_NAME
        LoggerIDFilter.LOGGER_ID = logger_id or LoggerIDFilter.LOGGER_ID
        LoggingFactory.LOGGER_FILTER = LoggingFactory.LOGGER_FILTER or LoggerIDFilter()
        LoggingFactory.ENV = env or LoggingFactory.ENV

        console_handler = logging.StreamHandler(sys.stdout if LoggingFactory.ENV == "local" else sys.stderr)
        console_handler.setFormatter(LoggingFactory.LOGGER_FORMATTER)
        console_handler.addFilter(LoggingFactory.LOGGER_FILTER)
        # WARNING is default level for root logger in python
        logging.basicConfig(level=logging.WARNING, handlers=[console_handler], force=True)

        LoggingFactory.CONSOLE_HANDLER = console_handler

        logger = getLogger(LoggingFactory.LOGGER_NAME)
        logger.setLevel(level or LoggingFactory.LOGGER_LEVEL)
        LoggingFactory.LOGGER = logger

    @classmethod
    def __check_koheesio_logger_initialized(cls) -> None:
        """Check that koheesio logger has been initialized"""
        if not cls.LOGGER:
            cls(name=cls.LOGGER_NAME, env=cls.LOGGER_ENV, level=cls.LOGGER_LEVEL, logger_id=LoggerIDFilter.LOGGER_ID)

    @staticmethod
    def add_handlers(handlers: List[Tuple[str, Dict]]) -> None:
        """Add handlers to existing root logger.

        Parameters
        ----------
        handler_class handler module and class for importing.
        handlers_config configuration for handler.

        """
        for handler_module_class, handler_conf in handlers:
            handler_class: logging.Handler = import_class(handler_module_class)
            handler_level = handler_conf.pop("level") if "level" in handler_conf else "WARNING"
            # noinspection PyCallingNonCallable
            handler = handler_class(**handler_conf)  # type: ignore[operator]
            handler.setLevel(handler_level)
            handler.addFilter(LoggingFactory.LOGGER_FILTER)
            handler.setFormatter(LoggingFactory.LOGGER_FORMATTER)

            if LoggingFactory.LOGGER:
                LoggingFactory.LOGGER.addHandler(handler)

    @staticmethod
    def get_logger(name: str, inherit_from_koheesio: bool = False) -> Logger:
        """Provide logger. If inherit_from_koheesio then inherit from LoggingFactory.PIPELINE_LOGGER_NAME.

        Parameters
        ----------
        name: Name of logger.
        inherit_from_koheesio: Inherit logger from koheesio

        Returns
        -------
        logger: Logger

        """
        if inherit_from_koheesio:
            LoggingFactory.__check_koheesio_logger_initialized()
            name = f"{LoggingFactory.LOGGER_NAME}.{name}"

        return getLogger(name)
