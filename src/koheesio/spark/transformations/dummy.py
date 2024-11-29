"""Dummy transformation for testing purposes."""

from pyspark.sql.functions import lit

from koheesio.spark.transformations import Transformation


class DummyTransformation(Transformation):
    """Dummy transformation for testing purposes.

    This transformation adds a new column `hello` to the DataFrame with the value `world`.

    It is intended for testing purposes or for use in examples or reference documentation.

    Example
    -------
    __input_df:__

    | id |
    |----|
    | 1  |

    ```python
    output_df = DummyTransformation().transform(input_df)
    ```

    __output_df:__

    | id | hello |
    |----|-------|
    | 1  | world |

    In this example, the `hello` column is added to the DataFrame `input_df`.

    """

    def execute(self) -> Transformation.Output:
        self.output.df = self.df.withColumn("hello", lit("world"))
