"""SQL Transform module

SQL Transform module provides an easy interface to transform a dataframe using SQL. This SQL can originate from a
string or a file and may contain placeholders for templating.
"""

from koheesio.models.sql import SqlBaseStep
from koheesio.spark.transformations import Transformation
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

    def execute(self):
        # table_name = get_random_string(prefix="sql_transform")
        # self.df.createTempView(table_name)

        # query = self.query.format(table_name=table_name, **{k: v for k, v in self.params.items() if k != "table_name"})
        self.output.df = self.spark.sql(sqlQuery=self.query, args=self.params)
