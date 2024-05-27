import pytest

from koheesio.spark.transformations.drop_column import DropColumn

pytestmark = pytest.mark.spark


@pytest.mark.parametrize(
    "input_values,expected",
    [
        (
            # description: base test
            # input values,
            dict(column="product"),
            # expected data
            [
                dict(amount=1000, country="USA"),
                dict(amount=1500, country="USA"),
                dict(amount=1600, country="USA"),
            ],
        ),
        (
            # description: test with non-existent columns
            # input values,
            dict(column="does_not_exist"),
            # expected data
            [
                dict(product="Banana lemon orange", amount=1000, country="USA"),
                dict(product="Carrots Blueberries", amount=1500, country="USA"),
                dict(product="Beans", amount=1600, country="USA"),
            ],
        ),
        (
            # description: drop multiple columns
            # input values,
            dict(column=["product", "country"]),
            # expected data
            [
                dict(amount=1000),
                dict(amount=1500),
                dict(amount=1600),
            ],
        ),
        (
            # description: multiple columns input where only one exists
            # input values,
            dict(column=["product", "does_not_exist_1", "does_not_exist_2"]),
            # expected data
            [
                dict(amount=1000, country="USA"),
                dict(amount=1500, country="USA"),
                dict(amount=1600, country="USA"),
            ],
        ),
    ],
)
def test_drop_column(input_values, expected, spark):
    input_df = spark.createDataFrame(
        [("Banana lemon orange", 1000, "USA"), ("Carrots Blueberries", 1500, "USA"), ("Beans", 1600, "USA")],
        schema="product string, amount int, country string",
    )
    df = DropColumn(**input_values).transform(input_df)

    actual = [k.asDict() for k in df.collect()]

    assert actual == expected
