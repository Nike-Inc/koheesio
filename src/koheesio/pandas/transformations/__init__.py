from typing import Optional
from abc import ABC, abstractmethod

from koheesio.models import Field
from koheesio.models.dataframe import BaseTransformation
from koheesio.pandas import PandasStep
from koheesio.pandas import pandas as pd


class Transformation(BaseTransformation, PandasStep, ABC):
    """Base class for all transformations

    Concept
    -------
    A Transformation is a Step that takes a DataFrame as input and returns a DataFrame as output. The DataFrame is
    transformed based on the logic implemented in the `execute` method. Any additional parameters that are needed for
    the transformation can be passed to the constructor.

    Parameters
    ----------
    df : Optional[pandas.DataFrame]
        The DataFrame to apply the transformation to. If not provided, the DataFrame has to be passed to the
        transform-method.

    Example
    -------
    ### Implementing a transformation using the Transformation class:
    ```python
    from koheesio.pandas.transformations import Transformation
    from koheesio.pandas import pandas as pd


    class AddOne(Transformation):
        target_column: str = "new_column"

        def execute(self):
            self.output.df = self.df.copy()
            self.output.df[self.target_column] = self.df["old_column"] + 1
    ```

    In the example above, the `execute` method is implemented to add 1 to the values of the `old_column` and store the
    result in a new column called `new_column`.

    ### Using the transformation:
    In order to use this transformation, we can call the `transform` method:

    ```python
    from koheesio.pandas import pandas as pd

    # create a DataFrame with 3 rows
    df = pd.DataFrame({"old_column": [0, 1, 2]})

    output_df = AddOne().transform(df)
    ```

    The `output_df` will now contain the original DataFrame with an additional column called `new_column` with the
    values of `old_column` + 1.

    __output_df:__

    | old_column | new_column |
    |------------|------------|
    |          0 |         1 |
    |          1 |         2 |
    |          2 |         3 |
    ...

    ### Alternative ways to use the transformation:
    Alternatively, we can pass the DataFrame to the constructor and call the `execute` or `transform` method without
    any arguments:

    ```python
    output_df = AddOne(df).transform()
    # or
    output_df = AddOne(df).execute().output.df
    ```

    > Note: that the transform method was not implemented explicitly in the AddOne class. This is because the `transform`
    method is already implemented in the `Transformation` class. This means that all classes that inherit from the
    Transformation class will have the `transform` method available. Only the execute method needs to be implemented.

    ### Using the transformation as a function:
    The transformation can also be used as a function as part of a DataFrame's `pipe` method:

    ```python
    input_df = pd.DataFrame({"old_column": [0, 1, 2]})
    add_baz = AddOne(target_column="baz")

    output_df = (
        input_df
        # using the transform method
        .pipe(AddOne(target_column="foo").transform)
        # using the transformation as a function
        .pipe(AddOne(target_column="bar"))
        # or, using a variable that holds the transformation
        .pipe(add_baz)
    )
    ```

    In the above example, the `AddOne` transformation is applied to the `input_df` DataFrame using the `transform`
    method. The `output_df` will now contain the original DataFrame with an additional columns called `foo` and
    `bar', each with the values of `id` + 1.

    Note: Because `Transformation` classes are callable, they can be used as functions in the `pipe` method of a DataFrame.
    """

    df: Optional[pd.DataFrame] = Field(default=None, description="The Spark DataFrame")

    @abstractmethod
    def execute(self) -> PandasStep.Output:
        # self.df  # input DataFrame
        # self.output.df # output DataFrame
        self.output.df = ...  # implement the transformation logic
        raise NotImplementedError

    execute.__doc__ = BaseTransformation.execute.__doc__
