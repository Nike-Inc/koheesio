# pragma: no cover
from koheesio.__about__ import __version__, _about
from koheesio.models import BaseModel, ExtraParamsMixin
from koheesio.steps import Step, StepOutput

_logo_printed = False
ABOUT = _about()
VERSION = __version__

__all__ = ["ABOUT", "VERSION", "BaseModel", "ExtraParamsMixin", "Step", "StepOutput"]


def print_logo():
    global _logo_printed
    if not _logo_printed:
        print(ABOUT)
        _logo_printed = True


print_logo()
