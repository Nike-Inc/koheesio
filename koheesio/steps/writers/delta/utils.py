"""
This module provides utility functions while working with delta framework.
"""

from typing import Optional

from py4j.java_gateway import JavaObject


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
        _processed_clauses = {}

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

            clause_type: str = clause.clauseType().capitalize()
            columns = "ALL" if clause_type == "Delete" else clause.actions().toList().apply(0).toString()

            if clause_type.lower() not in _processed_clauses:
                _processed_clauses[clause_type.lower()] = []

            log_message = (
                f"{clauses_type} will perform action:{clause_type} columns ({columns}) if `{condition_clause}`"
            )

    return log_message
