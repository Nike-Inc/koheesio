# pragma: no cover
from os import environ, getenv

from koheesio.__about__ import __version__, _about
from koheesio.context import Context
from koheesio.logger import LoggingFactory
from koheesio.models import BaseModel, ExtraParamsMixin
from koheesio.steps import Step, StepOutput

ABOUT = _about()
VERSION = __version__

__all__ = [
    "ABOUT",
    "BaseModel",
    "Context",
    "ExtraParamsMixin",
    "LoggingFactory",
    "Step",
    "StepOutput",
    "VERSION",
]


def print_logo():
    if not getenv("KOHEESIO_LOGO_PRINTED", False):
        print(ABOUT)
        environ["KOHEESIO_LOGO_PRINTED"] = "True"


print_logo()