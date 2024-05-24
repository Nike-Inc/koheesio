from textwrap import dedent

import pytest
from conftest import TEST_DATA_PATH

from koheesio.logger import LoggingFactory
from koheesio.steps.transformations.sql_transform import SqlTransform

pytestmark = pytest.mark.spark

log = LoggingFactory.get_logger(name="test_sql_transform")


@pytest.mark.parametrize(
    "input_values,expected",
    [
        (
            # Description: simple sql script
            # input values
            dict(table_name="dummy", sql="SELECT id AS new_id FROM ${table_name}"),
            # expected output
            {"new_id": 0},
        ),
        (
            # Description: Using explicit params (new_column)
            # input values
            dict(
                table_name="dummy_table",
                params=dict(new_column="incr_id"),  # explicit param
                sql="SELECT id, id + 1 as ${new_column} FROM ${table_name}",
            ),
            # expected output
            {"id": 0, "incr_id": 1},
        ),
        (
            # Description: Using implicit params (new_column)
            # input values
            dict(
                table_name="dummy_table",
                new_column="incr_id",  # implicit param
                sql="SELECT id, id + 1 as ${new_column} FROM ${table_name}",
            ),
            # expected output
            {"id": 0, "incr_id": 1},
        ),
        (
            # Description: with sql_path as Path like object
            # input values
            dict(
                table_name="dummy_table",
                sql_path=TEST_DATA_PATH / "transformations" / "dummy.sql",
            ),
            # expected output
            {"id": 0, "incremented_id": 1},
        ),
        (
            # Description: with sql_path as str
            # input values
            dict(
                table_name="dummy_table",
                sql_path=str((TEST_DATA_PATH / "transformations" / "dummy.sql").as_posix()),
            ),
            # expected output
            {"id": 0, "incremented_id": 1},
        ),
    ],
)
def test_sql_transform(input_values, expected, dummy_df):
    result = SqlTransform(**input_values).transform(dummy_df)
    actual = result.head().asDict()

    log.info(
        dedent(
            f"""
            input values: {input_values}
            actual: {actual}
            expected: {expected}
            """
        )
    )

    assert expected == actual


@pytest.mark.parametrize(
    "input_values,error",
    [
        (
            # non-existent path
            dict(table_name="...", sql_path="../none_existent_path"),
            FileNotFoundError,
        ),
        (
            # both sql and sql_path specified
            dict(table_name="...", sql_path="foo", sql="bar"),
            ValueError,
        ),
        (
            # no sql or sql_path specified
            dict(table_name="..."),
            ValueError,
        ),
        (
            # missing required keyword-only argument
            dict(),
            ValueError,
        ),
    ],
)
def test_errors(input_values, error):
    log.info(
        dedent(
            f"""
            input values: {input_values}
            expected error: {error}
            """
        )
    )

    with pytest.raises(error):
        SqlTransform(**input_values)
