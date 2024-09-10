from typing import Any, Dict, List, Optional, Union

import pyarrow as pa
from graphql import DocumentNode, ExecutionResult, print_ast
from pydantic import Field

from koheesio.asyncio.graphql import AsyncGrapqhQLClient
from koheesio.graphql import GrapqhQLClient
from koheesio.models.reader import BaseReader


class GraphQLReader(BaseReader):
    client: Union[GrapqhQLClient, AsyncGrapqhQLClient] = Field(..., description="GraphQL client. Can be async")
    document: DocumentNode = Field(..., description="GraphQL query as AST Node object.")
    variable_values: Optional[Dict[str, Any]] = Field(None, description="Dictionary of input parameters")
    operation_name: Optional[str] = Field(None, description="Name of the operation that shall be executed")
    get_execution_result: bool = Field(
        default=True,
        description=(
            "Return the full ExecutionResult instance instead of only the 'data' field."
            "Necessary if you want to get the 'extensions' field."
        ),
    )

    @staticmethod
    def extract_data(result: Union[Dict[str, Any], ExecutionResult]) -> List[Dict[str, Any]]:
        if isinstance(result, ExecutionResult):
            _data = result.data
        else:
            _data = result

        return _data  # type: ignore
    
    @staticmethod
    def _execute(client: Union[GrapqhQLClient, AsyncGrapqhQLClient], document: DocumentNode, operation_name: Optional[str], variable_values: Optional[Dict[str, Any]], get_execution_result: bool) -> ExecutionResult:
        with client as session:
            result = session.execute(
                document=document,
                operation_name=operation_name,
                variable_values=variable_values,
                get_execution_result=get_execution_result,
            )
        return result

    def execute(self):
        self.log.debug(f"Executing query:\n{print_ast(self.document)}")
        

        with self.client as session:
            result = session.execute(
                document=self.document,
                operation_name=self.operation_name,
                variable_values=self.variable_values,
                get_execution_result=self.get_execution_result,
            )
            
        data = self.extract_data(result=result)  # type: ignore
        data_dict = {key: [d[key] for d in data] for key in data[0]}
        self.output.df = pa.Table.from_pydict(data_dict)  # type: ignore
