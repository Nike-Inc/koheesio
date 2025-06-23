"""A type-safe Python framework for building efficient data pipelines

Koheesio is a Python framework for building type-safe and efficient data pipelines, particularly useful for
large-scale data processing. It promotes modularity, composability, and collaboration, allowing for the creation of
complex pipelines from simple, reusable components.

Leveraging Pydantic, it ensures predictable pipeline execution and structured configurations within pipeline components.

With its simple and straightforward API, Koheesio makes it easy for developers to build and manage data pipelines,
enhancing productivity and code maintainability.
"""

LICENSE_INFO = "Licensed as Apache 2.0"
SOURCE = "https://github.com/Nike-Inc/koheesio"
__version__ = "0.10.4"
__logo__ = (
    75,
    (
        b"\x1f\x8b\x08\x00TiGf\x02\xff}\x91\xbb\r\xc30\x0cD{Nq\x1bh\n\x01\x16R\xa4pK@\x8bh\xf8\xe8\xf8\x89\xe9\x04\xf0\x15"
        b"\xc4\x91\x10\x9f(J`z\xbd4B\xea8J\xf2\xa01T\x02\x01,\x0b\x85Q\x92\x07\xe9\x9cK\x92\xd1,\xe0mRBL\x9c\xa6\x9b\xee"
        b"\xeet)\x07Av\xc9/\x0b\x98\x93\xb4=\xd1v\xa4\xf5NG\xc6\xe5\xce\x93nk\x8d\x81\xf5\xed\x92\x80AmC\xbb\xde,.\x7f\x1fc"
        b"\x0fU\xa79\x19\x82\x16]\x1248\x8f\xa5\x7f\x1c|\x92\xe2\xb8\xa59\xfd\xa5\x86\x8b.I\x9a\xf3\xd4W\x80\x8a\xd3\x9e"
        b"\xfb\xba\\\xecm\x9f#\xee\xea\x92}M+\xffb\xb7\xb2\xc4\xc4K\x88Zui\xda\xedD\xfb\x00\xcfU6\xd3_\x02\x00\x00"
    ),
)
__short_description__ = __doc__.split("\n", maxsplit=1)[0]
__about__ = f"""Koheesio - v{__version__}
{__short_description__}
{LICENSE_INFO}
Source: {SOURCE}
"""


# fmt: off
def _about() -> str:  # pragma: no cover
    """Return the Koheesio logo and version/about information as a string
    Note: this code is not meant to be readable, instead it is written to be as compact as possible
    """
    import gzip
    _w, _logo = __logo__
    _h, _v, _n = "\u2500", "\u2502", "\n"
    return gzip.decompress(_logo).decode() + (
        _n + '\u256d' + _h * _w + "\u256e" + _n +
        f"{_n.join([f'{_v}{_l:^{_w}}{_v}' for _l in __about__.split(_n)[:-1]])}" +
        _n + "\u2570" + _h * _w + "\u256f"
    )
# fmt: on


if __name__ == "__main__":
    print(_about())
