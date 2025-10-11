"""SQL Transform module

SQL Transform module provides an easy interface to transform a dataframe using SQL. This SQL can originate from a
string or a file and may contain placeholders for templating.
"""

from koheesio.models.sql import SqlBaseStep
from koheesio.spark.transformations import Transformation
from koheesio.spark.utils import SPARK_MINOR_VERSION, on_databricks
from koheesio.utils import get_random_string


class SqlTransform(SqlBaseStep, Transformation):
    """
    SQL Transform module provides an easy interface to transform a dataframe using SQL.

    This SQL can originate from a string or a file and may contain placeholder (parameters) for templating.

    - Placeholders are identified with `${placeholder}`.
    - Placeholders can be passed as explicit params (params) or as implicit params (kwargs).

    Example sql script:

    ```sql
    SELECT id, id + 1 AS incremented_id, ${dynamic_column} AS extra_column
    FROM ${table_name}
    ```
    """

    def execute(self) -> Transformation.Output:
        table_name = get_random_string(prefix="sql_transform")
        self.params = {**self.params, "table_name": table_name}

        from koheesio.spark.utils.connect import is_remote_session

        if 3.4 < SPARK_MINOR_VERSION < 4.0 and is_remote_session() and self.df.isStreaming and not on_databricks():
            raise RuntimeError(
                "SQL Transform is not supported in remote sessions with streaming dataframes."
                "See https://issues.apache.org/jira/browse/SPARK-45957"
                "It is fixed in PySpark 4.0.0"
            )

        self.df.createOrReplaceTempView(table_name)
        query = self.query

        self.output.df = self.spark.sql(query)
