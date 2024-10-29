"""Transform module

Transform aims to provide an easy interface for calling transformations on a Spark DataFrame, where the transformation
is a function that accepts a DataFrame (df) and any number of keyword args.
"""

from __future__ import annotations

from typing import Callable, Dict, Optional
from functools import partial

from koheesio.models import ExtraParamsMixin, Field
from koheesio.spark import DataFrame
from koheesio.spark.transformations import Transformation
from koheesio.utils import get_args_for_func


class Transform(Transformation, ExtraParamsMixin):
    """
    Transform aims to provide an easy interface for calling transformations on a Spark DataFrame,
    where the transformation is a function that accepts a DataFrame (df) and any number of keyword args.

    The implementation is inspired by and based upon:
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.transform.html

    Parameters
    ----------
    func : Callable
        The function to be called on the DataFrame.
    params : Dict, optional, default=None
        The keyword arguments to be passed to the function. Defaults to None. Alternatively, keyword arguments can be
        passed directly as keyword arguments - they will be merged with the `params` dictionary.

    Example
    -------
    ### a function compatible with Transform:
    ```python
    def some_func(df, a: str, b: str):
        return df.withColumn(a, f.lit(b))
    ```

    ### verbose style input in Transform
    ```python
    Transform(func=some_func, params={"a": "foo", "b": "bar"})
    ```

    ### shortened style notation (easier to read)
    ```python
    Transform(some_func, a="foo", b="bar")
    ```

    ### when too much input is given, Transform will ignore extra input
    ```python
    Transform(
        some_func,
        a="foo",
        # ignored input
        c="baz",
        title=42,
        author="Adams",
        # order of params input should not matter
        b="bar",
    )
    ```

    ### using the from_func classmethod
    ```python
    SomeFunc = Transform.from_func(some_func, a="foo")
    some_func = SomeFunc(b="bar")
    ```
    """

    func: Callable = Field(default=..., description="The function to be called on the DataFrame.")

    def __init__(self, func: Callable, params: Dict = None, df: Optional[DataFrame] = None, **kwargs: dict):
        params = {**(params or {}), **kwargs}
        super().__init__(func=func, params=params, df=df)

    def execute(self) -> Transformation.Output:
        """Call the function on the DataFrame with the given keyword arguments."""
        func, kwargs = get_args_for_func(self.func, self.params)
        self.output.df = self.df.transform(func=func, **kwargs)

    @classmethod
    def from_func(cls, func: Callable, **kwargs: dict) -> Callable[..., Transform]:
        """Create a Transform class from a function. Useful for creating a new class with a different name.

        This method uses the `functools.partial` function to create a new class with the given function and keyword
        arguments. This way you can pre-define some of the keyword arguments for the function that might be needed for
        the specific use case.

        Example
        -------
        ```python
        CustomTransform = Transform.from_func(some_func, a="foo")
        some_func = CustomTransform(b="bar")
        ```

        In this example, `CustomTransform` is a Transform class with the function `some_func` and the keyword argument
        `a` set to "foo". When calling `some_func(b="bar")`, the function `some_func` will be called with the keyword
        arguments `a="foo"` and `b="bar"`.
        """
        return partial(cls, func=func, **kwargs)
