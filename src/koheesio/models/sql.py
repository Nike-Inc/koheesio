"""This module contains the base class for SQL steps."""

from typing import Any, Dict, Optional, Union
from abc import ABC
from pathlib import Path

from koheesio import Step
from koheesio.models import ExtraParamsMixin, Field, model_validator


class SqlBaseStep(Step, ExtraParamsMixin, ABC):
    """Base class for SQL steps

    `params` are used as placeholders for templating. The substitutions are identified by braces ('{' and '}') and can
    optionally contain a $-sign - e.g. `${placeholder}` or `{placeholder}`.

    Parameters
    ----------
    sql_path : Optional[Union[Path, str]], optional, default=None
        Path to a SQL file
    sql : Optional[str], optional, default=None
        SQL script to apply
    params : Dict[str, Any], optional, default_factory=dict
        Placeholders (parameters) for templating. These are identified with `${placeholder}` in the SQL script.\n
        __Note__: any arbitrary kwargs passed to the class will be added to params.
    """

    sql_path: Optional[Union[Path, str]] = Field(default=None, description="Path to a SQL file")
    sql: Optional[str] = Field(default=None, description="SQL script to apply")
    params: Dict[str, Any] = Field(
        default_factory=dict,
        description="Placeholders (parameters) for templating. The substitutions are identified by braces ('{' and '}')"
        "and can optionally contain a $-sign. Note: any arbitrary kwargs passed to the class will be added to params.",
    )

    @model_validator(mode="after")
    def _validate_sql_and_sql_path(self) -> "SqlBaseStep":
        """Validate the SQL and SQL path"""
        sql = self.sql
        sql_path = self.sql_path

        if sql_path is None and sql is None:
            raise ValueError("Please specify either `sql_path` or `sql`")

        if sql_path is not None and sql is not None:
            raise ValueError("You cannot specify both `sql_path` and `sql`")

        if sql_path is not None:
            sql_path = Path(sql_path)
            if not sql_path.exists():
                raise FileNotFoundError(f"Unable to locate specified {sql_path.as_posix()}")
            self.sql = sql_path.read_text(encoding="utf-8")
            return self

        if sql is None or len(sql.strip()) == 0:
            raise ValueError("SQL is empty")

        return self

    @property
    def query(self) -> str:
        """Returns the query while performing params replacement"""

        if self.sql:
            query = self.sql

            for key, value in self.params.items():
                query = query.replace(f"${{{key}}}", value)

            self.log.debug(f"Generated query: {query}")
        else:
            query = ""

        return query
