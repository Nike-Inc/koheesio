from typing import Any

import pyarrow
import pytest
from gql.dsl import DSLQuery, DSLSchema, dsl_gql
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.requests import RequestsHTTPTransport
from graphql import ExecutionResult
from koheesio.asyncio.graphql import AsyncGrapqhQLClient
from koheesio.graphql import GrapqhQLClient
from koheesio.steps.readers.graphql import GraphQLReader

GRAPH_QL_API_IRL = "https://countries.trevorblades.com"


class TestGraphQLClient:
    transport = RequestsHTTPTransport(url=GRAPH_QL_API_IRL)

    def test_dsl_schema(self):
        graphql_client = GrapqhQLClient(transport=self.transport, fetch_schema_from_transport=True)

        assert isinstance(graphql_client.dsl_schema, DSLSchema)


class TestAsyncGraphQLClient:
    transport = AIOHTTPTransport(url=GRAPH_QL_API_IRL)

    @pytest.mark.asyncio
    async def test_dsl_schema(self):
        graphql_client = AsyncGrapqhQLClient(transport=self.transport, fetch_schema_from_transport=True)

        assert isinstance(await graphql_client.dsl_schema, DSLSchema)


class TestGraphQLReader:
    transport = RequestsHTTPTransport(url=GRAPH_QL_API_IRL)

    def test_execute_query(self):
        graphql_client = GrapqhQLClient(transport=self.transport, fetch_schema_from_transport=True)
        dsl_schema: DSLSchema = graphql_client.dsl_schema  # type: ignore
        operation_name = "Continents"
        query = DSLQuery(dsl_schema.Query.continents.select(dsl_schema.Continent.code, dsl_schema.Continent.name))
        document = dsl_gql(**{operation_name: query})
        reader = GraphQLReader(client=graphql_client, document=document, operation_name=operation_name)

        def extract_continent(result: ExecutionResult) -> Any:
            return result.data["continents"]

        reader.extract_data = staticmethod(extract_continent)  # type: ignore
        reader.execute()  # type: ignore
        table = reader.output.df

        assert isinstance(table, pyarrow.Table)
        assert len(table.column("code").to_pylist()) == 7
        assert "Africa" in table.column("name").to_pylist()
        assert ["AF", "AN", "AS", "EU", "NA", "OC", "SA"] == table.column("code").to_pylist()
        
class TestAsyncGraphQLReader:
    transport = AIOHTTPTransport(url=GRAPH_QL_API_IRL)

    @pytest.mark.asyncio
    async def test_execute_query(self):
        graphql_client = AsyncGrapqhQLClient(transport=self.transport, fetch_schema_from_transport=True)
        dsl_schema: DSLSchema = await graphql_client.dsl_schema  # type: ignore
        operation_name = "Continents"
        query = DSLQuery(dsl_schema.Query.continents.select(dsl_schema.Continent.code, dsl_schema.Continent.name))
        document = dsl_gql(**{operation_name: query})
        reader = GraphQLReader(client=graphql_client, document=document, operation_name=operation_name)

        def extract_continent(result: ExecutionResult) -> Any:
            return result.data["continents"]

        reader.extract_data = staticmethod(extract_continent)  # type: ignore
        await reader.execute()  # type: ignore
        table = reader.output.df

        assert isinstance(table, pyarrow.Table)
        assert len(table.column("code").to_pylist()) == 7
        assert "Africa" in table.column("name").to_pylist()
        assert ["AF", "AN", "AS", "EU", "NA", "OC", "SA"] == table.column("code").to_pylist()
