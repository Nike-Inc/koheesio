from logging import Logger
from typing import Any, Dict, Optional, Union

from gql.client import AsyncClientSession, Client, SyncClientSession
from gql.dsl import DSLSchema
from gql.transport import AsyncTransport
from graphql import DocumentNode, print_ast

from koheesio.graphql import GrapqhQLClient


class AsyncGrapqhQLClient(GrapqhQLClient):
    @property
    async def dsl_schema(self) -> Optional[DSLSchema]:
        if not self._dsl_schema:
            async with self:
                self._dsl_schema = DSLSchema(self.schema)  # type: ignore

        return self._dsl_schema
    
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
