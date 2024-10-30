"""
This module provides utility functions while working with delta framework.
"""

from typing import Optional

from delta import DeltaTable
from py4j.java_gateway import JavaObject

from koheesio.spark import SparkSession
from koheesio.spark.utils import SPARK_MINOR_VERSION


class SparkConnectDeltaTableException(AttributeError):
    EXCEPTION_CONNECT_TEXT: str = """`DeltaTable.forName` is not supported due to delta calling _sc, 
                    which is not available in Spark Connect and PySpark>=3.5,<4.0. Required version of PySpark >=4.0.
                    Possible workaround to use spark.read and Spark SQL for any Delta operation (e.g. merge)"""

    def __init__(self, original_exception: AttributeError):
        custom_message = f"{self.EXCEPTION_CONNECT_TEXT}\nOriginal exception: {str(original_exception)}"
        super().__init__(custom_message)


def log_clauses(clauses: JavaObject, source_alias: str, target_alias: str) -> Optional[str]:
    """
    Prepare log message for clauses of DeltaMergePlan statement.

    Parameters
    ----------
    clauses : JavaObject
        The clauses of the DeltaMergePlan statement.
    source_alias : str
        The source alias.
    target_alias : str
        The target alias.

    Returns
    -------
    Optional[str]
        The log message if there are clauses, otherwise None.

    Notes
    -----
    This function prepares a log message for the clauses of a DeltaMergePlan statement. It iterates over the clauses,
    processes the conditions, and constructs the log message based on the clause type and columns.

    If the condition is a value, it replaces the source and target aliases in the condition string. If the condition is
    None, it sets the condition_clause to "No conditions required".

    The log message includes the clauses type, the clause type, the columns, and the condition.
    """
    log_message = None

    if not clauses.isEmpty():
        clauses_type = clauses.last().nodeName().replace("DeltaMergeInto", "")
        _processed_clauses: dict = {}

        for i in range(0, clauses.length()):
            clause = clauses.apply(i)
            condition = clause.condition()

            if "value" in dir(condition):
                condition_clause = (
                    condition.value()
                    .toString()
                    .replace(f"'{source_alias}", source_alias)
                    .replace(f"'{target_alias}", target_alias)
                )
            elif condition.toString() == "None":
                condition_clause = "No conditions required"
            else:
                raise ValueError(f"Condition {condition} is not supported")

            clause_type: str = clause.clauseType().capitalize()
            columns = "ALL" if clause_type == "Delete" else clause.actions().toList().apply(0).toString()

            if clause_type.lower() not in _processed_clauses:
                _processed_clauses[clause_type.lower()] = []

            log_message = (
                f"{clauses_type} will perform action:{clause_type} columns ({columns}) if `{condition_clause}`"
            )

    return log_message


def get_delta_table_for_name(spark_session: SparkSession, table_name: str) -> DeltaTable:
    """
    Retrieves the DeltaTable instance for the specified table name.

    This method attempts to get the DeltaTable using the provided Spark session and table name.
    If an AttributeError occurs and the Spark version is between 3.4 and 4.0, and the session is remote,
    it raises a SparkConnectDeltaTableException.

    Parameters
    ----------
    spark_session : SparkSession
        The Spark Session to use.
    table_name : str
        The table name.

    Returns
    -------
    DeltaTable
        The DeltaTable instance for the specified table name.

    Raises
    ------
    SparkConnectDeltaTableException
        If the Spark version is between 3.4 and 4.0, the session is remote, and an AttributeError occurs.
    """
    try:
        delta_table = DeltaTable.forName(sparkSession=spark_session, tableOrViewName=table_name)
    except AttributeError as e:
        from koheesio.spark.utils.connect import is_remote_session

        if 3.4 < SPARK_MINOR_VERSION < 4.0 and is_remote_session():
            raise SparkConnectDeltaTableException(e) from e
        else:
            raise e

    return delta_table
