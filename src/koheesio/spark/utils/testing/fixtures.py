from typing import Any, Generator, Optional
from dataclasses import dataclass, field

from koheesio.spark import DataFrame, SparkSession
from koheesio.utils.testing import pytest, register_fixtures, FixtureValue


__all__ = [
    "mock_spark_reader",
    "SparkContextData",
    "register_fixtures",
]


@dataclass
class SparkContextData:
    """Helper class to mock the behavior of a Spark session's `read` object.

    This class is used to mock the behavior of a Spark session's `read` object. It allows the user to set the output
    DataFrame that will be returned by the `load` method. The class also provides methods to get the options that were
    passed to the `load` method.

    See `mock_spark_reader` for more details
    """

    spark: SparkSession
    _options_dict: dict[str, Any] = field(default_factory=dict)
    _output_df: Optional[DataFrame] = None

    def mock_options(self, *args, **kwargs) -> SparkSession.read:  # type: ignore
        """Mock method for the `options` and `option` methods of a Spark session's `read` object."""
        self._options_dict.update(kwargs)
        return self.spark.read

    def set_output_df(self, df: DataFrame) -> None:
        """Allows the user to set the output DataFrame that will be returned by the `load` method."""
        self._output_df = df  # type: ignore

    set_df = set_output_df
    """Alias for `set_output_df`"""

    @property
    def get_options(self) -> dict[str, Any]:
        """Returns the options that were passed"""
        return self._options_dict

    def load(self) -> DataFrame:
        """Mock method for the `load` method of a Spark session's `read` object."""
        return self._output_df

    def assert_option_called_with(self, key: str, value: Any) -> None:
        """Asserts that a specific option was called with the expected value."""
        assert (
            self._options_dict.get(key) is not None
        ), f"No option with name {key} was called. Actual options: {self._options_dict}"
        assert (
            self._options_dict.get(key) == value
        ), f"Expected {key} to be {value}, but got {self._options_dict.get(key)}"


@pytest.fixture
def mock_spark_reader(
    spark: SparkSession, sample_df_with_strings: DataFrame, mocker: FixtureValue
) -> Generator[SparkContextData]:
    """SparkSession fixture that makes any call to `SparkSession.read.load()` return a custom DataFrame.

    Note: This fixture is meant to be used with read operations of a Spark session.

    Because of the use of `type(spark.read)`, this fixture automatically alters its behavior for either a remote or
    regular Spark session.

    By default, `mock_spark_reader` will set the output data to `sample_df_with_strings`. You can use the
    `set_output_df` or `set_df` methods to overwrite this with any DataFrame of your choosing.

    Example
    -------
    ### Example of setting up `mock_spark_reader` with a custom DataFrame
    ```python
    def test_with_mock_spark_reader(mock_spark_reader):
        my_custom_df = ...  # dataframe the user creates
        mock_spark_reader.set_output_df(my_custom_df)
        df = mock_spark_reader.spark.read.load()
        assert df.count() == my_custom_df.count()
    ```

    ### Checking that specific options were called
    ```python
    def test_spark_called_with_right_options(mock_spark_reader):
        spark.read.format("csv").option("foo", "bar")
        mock_spark_reader.assert_option_called_with("foo", "bar")
    ```

    Parameters
    ----------
    spark : SparkSession
        The Spark session to use.
    sample_df_with_strings : DataFrame
        Default DataFrame if not specifically being overwritten
    mocker : MockerFixture

    Returns
    -------
    SparkContextData
        An instance of SparkContextData containing the SparkReader and methods to set the output DataFrame and get
        options.
    """
    context_data = SparkContextData(spark)
    context_data.set_output_df(sample_df_with_strings)

    spark_reader = type(spark.read)

    mocker.patch.object(spark_reader, "options", side_effect=context_data.mock_options)
    mocker.patch.object(spark_reader, "load", side_effect=context_data.load)

    yield context_data
