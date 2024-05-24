# pragma: no cover

import os

from koheesio.__about__ import __version__, _about
from koheesio.models import BaseModel, ExtraParamsMixin
from koheesio.steps import Step, StepOutput
from koheesio.utils import convert_str_to_bool

_koheesio_print_logo = convert_str_to_bool(os.environ.get("KOHEESIO__PRINT_LOGO", "True"))
_logo_printed = False
ABOUT = _about()
VERSION = __version__

__all__ = ["ABOUT", "VERSION", "BaseModel", "ExtraParamsMixin", "Step", "StepOutput"]


def print_logo():
    global _logo_printed
    global _koheesio_print_logo

    if not _logo_printed and _koheesio_print_logo:
        print(ABOUT)
        _logo_printed = True


print_logo()
