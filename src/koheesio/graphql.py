from abc import ABC
from logging import Logger
from typing import Any, Dict, List, Optional, Union

from gql.client import AsyncClientSession, Client, SyncClientSession
from gql.dsl import DSLSchema
from graphql import DocumentNode, print_ast
from pydantic import Field

from koheesio.steps import Step


class GrapqhQLClient(Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._dsl_schema: Optional[DSLSchema] = None
        self._logger: Optional[Logger] = kwargs.get("logger")

    @property
    def dsl_schema(self) -> Optional[DSLSchema]:
        if not self._dsl_schema:
            with self:
                self._dsl_schema = DSLSchema(self.schema)  # type: ignore

        return self._dsl_schema

    @dsl_schema.setter
    def dsl_schema(self, value: DSLSchema):
        self._dsl_schema = value

    @staticmethod
    def _extract_data_items(result: dict, query: DocumentNode) -> Union[list, dict]:
        return result

    @staticmethod
    def _processing(
        query: DocumentNode,
        session: Union[SyncClientSession, AsyncClientSession],
        variable_values: Optional[dict] = None,
        operation_name: Optional[str] = None,
        logger: Optional[Logger] = None,
        **kwargs,
    ) -> list:
        if logger:
            logger.debug(f"Executing query:\n{print_ast(query)}")

        result = session.execute(
            document=query,
            operation_name=operation_name,
            variable_values=variable_values,
        )

        return [result]

    def execute_query(
        self,
        query: DocumentNode,
        session: Union[SyncClientSession, AsyncClientSession],
        operation_name: Optional[str] = None,
        variable_values: Optional[Dict[str, Any]] = None,
        logger: Optional[Logger] = None,
        **kwargs,
    ) -> list:
        result = self._processing(
            session=session,
            query=query,
            variable_values=variable_values,
            logger=logger,
            operation_name=operation_name,
            **kwargs,
        )

        return result


class GraphQLStep(Step, ABC):
    client: GrapqhQLClient = Field(..., description="The transport to use for the GraphQL client")
    operation_name: Optional[str] = Field(None, description="The name of the operation to execute")
    variable_values: Optional[Dict[str, Any]] = Field(None, description="The variable values to use in the query")

    __query__: DocumentNode

    class Output(Step.Output):
        graphql_data: Optional[List[dict]] = Field(None, description="The data extracted from the query", repr=False)

    @property
    def query(self) -> DocumentNode:
        return self.__query__

    @query.setter
    def query(self, value: DocumentNode):
        self.__query__ = value
