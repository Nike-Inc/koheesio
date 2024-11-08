"""Module holding re-usable test utilities for Snowflake modules"""

from typing import Generator
from unittest.mock import MagicMock, patch

# safe import pytest fixture
try:
    import pytest
except (ImportError, ModuleNotFoundError):
    pytest = MagicMock()


@pytest.fixture(scope="function")
def mock_query() -> Generator:
    """Mock the query execution for SnowflakeRunQueryPython

    This can be used to test the query execution without actually connecting to Snowflake.

    Example
    -------
    ```python
    def test_execute(self, mock_query):
        # Arrange
        query = "SELECT * FROM two_row_table"
        mock_query.expected_data = [("row1",), ("row2",)]

        # Act
        instance = SnowflakeRunQueryPython(
            **COMMON_OPTIONS, query=query, account="42"
        )
        instance.execute()

        # Assert
        mock_query.assert_called_with(query)
        assert instance.output.results == mock_query.expected_data
    ```

    In this example, we are using the mock_query fixture to test the execution of a query.
    - We set the expected data to a known value by setting `mock_query.expected_data`,
    - Then, we execute the query.
    - We then assert that the query was called with the expected query by using `mock_query.assert_called_with` and
        that the results are as expected.
    """
    with patch("koheesio.integrations.snowflake.SnowflakeRunQueryPython.conn", new_callable=MagicMock) as mock_conn:
        mock_cursor = MagicMock()
        mock_conn.__enter__.return_value.execute_string.return_value = [mock_cursor]

        class MockQuery:
            def __init__(self) -> None:
                self.mock_conn = mock_conn
                self.mock_cursor = mock_cursor
                self._expected_data: list = []

            def assert_called_with(self, query: str) -> None:
                self.mock_conn.__enter__.return_value.execute_string.assert_called_once_with(query)
                self.mock_cursor.fetchall.return_value = self.expected_data

            @property
            def expected_data(self) -> list:
                return self._expected_data

            @expected_data.setter
            def expected_data(self, data: list) -> None:
                self._expected_data = data
                self.set_expected_data()

            def set_expected_data(self) -> None:
                self.mock_cursor.fetchall.return_value = self.expected_data

        mock_query_instance = MockQuery()
        yield mock_query_instance
