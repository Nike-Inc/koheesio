"""
This module contains the SparkSqlReader class which reads the SparkSQL compliant query and returns the dataframe.
"""

from koheesio.models.sql import SqlBaseStep
from koheesio.spark.readers import Reader


class SparkSqlReader(SqlBaseStep, Reader):
    """
    SparkSqlReader reads the SparkSQL compliant query and returns the dataframe.

    This SQL can originate from a string or a file and may contain placeholder (parameters) for templating.
    - Placeholders are identified with ${placeholder}.
    - Placeholders can be passed as explicit params (params) or as implicit params (kwargs).

    Example
    -------
    SQL script (example.sql):
    ```sql
    SELECT id, id + 1 AS incremented_id, ${dynamic_column} AS extra_column
    FROM ${table_name}
    ```

    Python code:
    ```python
    from koheesio.spark.readers import SparkSqlReader

    reader = SparkSqlReader(
        sql_path="example.sql",
        # params can also be passed as kwargs
        dynamic_column"="name",
        "table_name"="my_table"
    )
    reader.execute()
    ```

    In this example, the SQL script is read from a file and the placeholders are replaced with the given params.
    The resulting SQL query is:
    ```sql
    SELECT id, id + 1 AS incremented_id, name AS extra_column
    FROM my_table
    ```

    The query is then executed and the resulting DataFrame is stored in the `output.df` attribute.

    Parameters
    ----------
    sql_path : str or Path
        Path to a SQL file
    sql : str
        SQL query to execute
    params : dict
        Placeholders (parameters) for templating. These are identified with ${placeholder} in the SQL script.

    Notes
    -----
    Any arbitrary kwargs passed to the class will be added to params.
    """

    def execute(self) -> Reader.Output:
        self.output.df = self.spark.sql(self.query)
