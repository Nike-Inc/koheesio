from koheesio.spark.transformations.debug import Peek
import pytest


@pytest.mark.parametrize(
    "input_values,expected_calls",
    [
        (
            # description: Peek is active, print schema, truncate output, vertical display
            dict(is_active=True, print_schema=True, truncate_output=True, vertical=True, n=10),
            dict(printSchema=1, show=1),
        ),
        (
            # description: Peek is active, do not print schema, do not truncate output, horizontal display
            dict(is_active=True, print_schema=False, truncate_output=False, vertical=False, n=5),
            dict(printSchema=0, show=1),
        ),
        (
            # description: Peek is inactive
            dict(is_active=False, print_schema=True, truncate_output=True, vertical=True, n=10),
            dict(printSchema=0, show=0),
        ),
    ],
)
def test_peek(spark, mocker, input_values, expected_calls):
    input_df = spark.createDataFrame(
        [("Banana lemon orange", 1000, "USA"), ("Carrots Blueberries", 1500, "USA"), ("Beans", 1600, "USA")],
        schema="product string, amount int, country string",
    )

    peek = Peek(**input_values)
    peek.df = input_df

    # Mock the methods
    peek.df.printSchema = mocker.MagicMock()
    peek.df.show = mocker.MagicMock()

    peek.execute()

    # Check if the methods were called the expected number of times
    assert peek.df.printSchema.call_count == expected_calls["printSchema"]
    assert peek.df.show.call_count == expected_calls["show"]

    if expected_calls["show"] > 0:
        peek.df.show.assert_called_with(truncate=input_values["truncate_output"], n=input_values["n"], vertical=input_values["vertical"])