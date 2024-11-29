from datetime import datetime

import pytest

from pyspark.sql import functions as F

from koheesio.spark.transformations.row_number_dedup import RowNumberDedup

pytestmark = pytest.mark.spark


@pytest.mark.parametrize("target_column", ["col_row_number"])
def test_row_number_dedup(spark, target_column: str) -> None:
    df = spark.createDataFrame(
        [
            (
                "1",
                "a",
                "f1",
                datetime.strptime("2023-10-30", "%Y-%m-%d").date(),
            ),
            (
                "1",
                "a",
                "f2",
                datetime.strptime("2023-10-29", "%Y-%m-%d").date(),
            ),
            (
                "1",
                "a",
                "f3",
                datetime.strptime("2023-10-28", "%Y-%m-%d").date(),
            ),
        ],
        schema="key string, second_key string, field string, dt date",
    )
    assert (
        RowNumberDedup(df=df, columns=["key", "second_key"], sort_columns="dt", target_column=target_column)
        .transform()
        .head()
        .asDict()
    ) == {
        "key": "1",
        "second_key": "a",
        "field": "f1",
        "dt": datetime.strptime("2023-10-30", "%Y-%m-%d").date(),
    }


@pytest.mark.parametrize("target_column", ["col_row_number"])
def test_row_number_dedup_not_list_column(spark, target_column: str) -> None:
    df = spark.createDataFrame(
        [
            (
                "1",
                "a",
                "f1",
                datetime.strptime("2023-10-30", "%Y-%m-%d").date(),
            ),
            (
                "1",
                "a",
                "f2",
                datetime.strptime("2023-10-29", "%Y-%m-%d").date(),
            ),
            (
                "1",
                "a",
                "f3",
                datetime.strptime("2023-10-28", "%Y-%m-%d").date(),
            ),
        ],
        schema="key string, second_key string, field string, dt date",
    )
    assert (
        RowNumberDedup(
            df=df, columns=["key", "second_key"], sort_columns=F.col("dt").desc(), target_column=target_column
        )
        .transform()
        .head()
        .asDict()
    ) == {
        "key": "1",
        "second_key": "a",
        "field": "f1",
        "dt": datetime.strptime("2023-10-30", "%Y-%m-%d").date(),
    }


@pytest.mark.parametrize("target_column", ["col_row_number"])
def test_row_number_dedup_with_columns(spark, target_column: str) -> None:
    df = spark.createDataFrame(
        [
            (
                "1",
                "a",
                "f1",
                datetime.strptime("2023-10-30", "%Y-%m-%d").date(),
            ),
            (
                "1",
                "a",
                "f2",
                datetime.strptime("2023-10-29", "%Y-%m-%d").date(),
            ),
            (
                "1",
                "a",
                "f3",
                datetime.strptime("2023-10-28", "%Y-%m-%d").date(),
            ),
        ],
        schema="key string, second_key string, field string, dt date",
    )
    assert (
        RowNumberDedup(
            df=df, columns=["key", "second_key"], sort_columns=[F.col("dt").desc()], target_column=target_column
        )
        .transform()
        .head()
        .asDict()
    ) == {
        "key": "1",
        "second_key": "a",
        "field": "f1",
        "dt": datetime.strptime("2023-10-30", "%Y-%m-%d").date(),
    }


@pytest.mark.parametrize("target_column", ["col_row_number"])
def test_row_number_dedup_with_duplicated_columns(spark, target_column: str) -> None:
    df = spark.createDataFrame(
        [
            (
                "1",
                "a",
                "f1",
                datetime.strptime("2023-10-30", "%Y-%m-%d").date(),
            ),
            (
                "1",
                "a",
                "f2",
                datetime.strptime("2023-10-29", "%Y-%m-%d").date(),
            ),
            (
                "1",
                "a",
                "f3",
                datetime.strptime("2023-10-28", "%Y-%m-%d").date(),
            ),
        ],
        schema="key string, second_key string, field string, dt date",
    )
    transformation = RowNumberDedup(
        df=df,
        columns=["key", "second_key"],
        sort_columns=[F.col("dt").desc(), F.col("dt").desc(), F.col("dt").asc()],
        target_column=target_column,
    )

    assert [str(c) for c in transformation.sort_columns] == [str(c) for c in [F.col("dt").desc(), F.col("dt").asc()]]
    assert (transformation.transform().head().asDict()) == {
        "key": "1",
        "second_key": "a",
        "field": "f1",
        "dt": datetime.strptime("2023-10-30", "%Y-%m-%d").date(),
    }
