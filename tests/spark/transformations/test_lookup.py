import pytest

from koheesio.spark.transformations.lookup import (
    DataframeLookup,
    JoinHint,
    JoinMapping,
    JoinType,
    TargetColumn,
)

pytestmark = pytest.mark.spark


def test_join_mapping_column_test() -> None:
    assert str(JoinMapping(source_column="source", other_column="joined").column) == "Column<'joined AS source'>"


def test_target_column_column_test() -> None:
    assert str(TargetColumn(target_column="target", target_column_alias="alias").column) == "Column<'target AS alias'>"


def test_join_type_values() -> None:
    assert JoinType.INNER == "inner"
    assert JoinType.FULL == "full"
    assert JoinType.LEFT == "left"
    assert JoinType.RIGHT == "right"
    assert JoinType.CROSS == "cross"
    assert JoinType.SEMI == "semi"
    assert JoinType.ANTI == "anti"


def test_join_hint_values() -> None:
    assert JoinHint.BROADCAST == "broadcast"
    assert JoinHint.MERGE == "merge"


@pytest.mark.parametrize("join_hint", [None, JoinHint.BROADCAST])
def test_dataframe_lookup(spark, join_hint: JoinHint) -> None:
    df = spark.createDataFrame(
        [("1", "a", "a"), ("2", "b", "b")],
        schema="key string, second_key string, field string",
    )
    right_df = spark.createDataFrame(
        [("1", "a", "aa", "aaa", ""), ("3", "c", "cc", "ccc", "")],
        schema="other_key string, other_second_key string, bad_field string, worse_field string, useless_field string",
    )
    assert (
        DataframeLookup(
            df=df,
            other=right_df,
            on=[
                JoinMapping(source_column="key", other_column="other_key"),
                JoinMapping(source_column="second_key", other_column="other_second_key"),
            ],
            targets=[
                TargetColumn(target_column="bad_field", target_column_alias="good_field"),
                TargetColumn(target_column="worse_field", target_column_alias="better_field"),
            ],
            how=JoinType.INNER,
            hint=join_hint,
        )
        .execute()
        .df.head()
        .asDict()
    ) == {
        "key": "1",
        "second_key": "a",
        "field": "a",
        "good_field": "aa",
        "better_field": "aaa",
    }
