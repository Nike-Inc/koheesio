# pragma: no cover
from os import getenv, environ

from koheesio.__about__ import __version__, _about
from koheesio.models import BaseModel, ExtraParamsMixin
from koheesio.steps import Step, StepOutput
from koheesio.context import Context
from koheesio.logger import LoggingFactory

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
