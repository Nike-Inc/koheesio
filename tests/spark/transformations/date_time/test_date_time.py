from datetime import datetime

import pytest

from koheesio.logger import LoggingFactory
from koheesio.spark.transformations.date_time import (
    ChangeTimeZone,
    DateFormat,
    ToTimestamp,
)

pytestmark = pytest.mark.spark

log = LoggingFactory.get_logger(name="test_date_time")


@pytest.mark.parametrize(
    "input_values,expected",
    [
        (
            # description: base test
            # input values,
            dict(target_column="a_date", column="a_date", format="yyyyMMdd HH:mm"),
            # expected data
            [
                dict(id=1, a_date="19700420 12:33", a_timestamp=datetime(2000, 7, 1, 1, 1)),
                dict(id=2, a_date="19800521 13:34", a_timestamp=datetime(2010, 8, 2, 2, 2)),
                dict(id=3, a_date="19900622 14:35", a_timestamp=datetime(2020, 9, 3, 3, 3)),
            ],
        ),
        (
            # description: giving a list of the same columns names should not cause an error
            # input values,
            dict(target_column="a_date", columns=["a_date", "a_date", "a_date"], format="yyyyMMdd HH:mm"),
            # expected data
            [
                dict(id=1, a_date="19700420 12:33", a_timestamp=datetime(2000, 7, 1, 1, 1)),
                dict(id=2, a_date="19800521 13:34", a_timestamp=datetime(2010, 8, 2, 2, 2)),
                dict(id=3, a_date="19900622 14:35", a_timestamp=datetime(2020, 9, 3, 3, 3)),
            ],
        ),
        (
            # description: multiple columns at once, no target_columns -> should output to original column name
            # input values,
            dict(columns=["a_date", "a_timestamp"], format="yyyyMMdd HH:mm"),
            # expected data
            [
                dict(id=1, a_date="19700420 12:33", a_timestamp="20000701 01:01"),
                dict(id=2, a_date="19800521 13:34", a_timestamp="20100802 02:02"),
                dict(id=3, a_date="19900622 14:35", a_timestamp="20200903 03:03"),
            ],
        ),
        (
            # description: multiple columns at once, with target_columns -> should add suffix to the column name
            # input values,
            dict(target_column="add", columns=["a_date", "a_timestamp"], format="yyyyMMdd HH:mm"),
            # expected data
            [
                dict(
                    id=1,
                    a_date=datetime(1970, 4, 20, 12, 33, 9),
                    a_date_add="19700420 12:33",
                    a_timestamp=datetime(2000, 7, 1, 1, 1),
                    a_timestamp_add="20000701 01:01",
                ),
                dict(
                    id=2,
                    a_date=datetime(1980, 5, 21, 13, 34, 8),
                    a_date_add="19800521 13:34",
                    a_timestamp=datetime(2010, 8, 2, 2, 2),
                    a_timestamp_add="20100802 02:02",
                ),
                dict(
                    id=3,
                    a_date=datetime(1990, 6, 22, 14, 35, 7),
                    a_date_add="19900622 14:35",
                    a_timestamp=datetime(2020, 9, 3, 3, 3),
                    a_timestamp_add="20200903 03:03",
                ),
            ],
        ),
        (
            # description: multiple columns at once, overlapping target_columns
            #               -> should add 'a_date' as suffix to the 'a_timestamp' column, while overwriting 'a_date'
            # input values,
            dict(target_column="a_date", columns=["a_date", "a_timestamp"], format="yyyyMMdd HH:mm"),
            # expected data
            [
                dict(
                    id=1,
                    a_date="19700420 12:33",
                    a_timestamp=datetime(2000, 7, 1, 1, 1),
                    a_timestamp_a_date="20000701 01:01",
                ),
                dict(
                    id=2,
                    a_date="19800521 13:34",
                    a_timestamp=datetime(2010, 8, 2, 2, 2),
                    a_timestamp_a_date="20100802 02:02",
                ),
                dict(
                    id=3,
                    a_date="19900622 14:35",
                    a_timestamp=datetime(2020, 9, 3, 3, 3),
                    a_timestamp_a_date="20200903 03:03",
                ),
            ],
        ),
    ],
)
def test_date_format(input_values, expected, sample_df_with_timestamp):
    df = DateFormat(**input_values).transform(sample_df_with_timestamp)
    actual = [k.asDict() for k in df.collect()]
    assert actual == expected
    log.info("DatetimeToFormattedString unit test completed")


@pytest.mark.parametrize(
    "input_values,expected",
    [
        (
            # description: base test
            # input values
            dict(
                target_column="a_date",
                column="a_timestamp",
                from_timezone="CET",
                to_timezone="UTC",
                add_target_timezone=False,
            ),
            # expected data
            [
                dict(id=1, a_date=datetime(2000, 6, 30, 23, 1, 0), a_timestamp=datetime(2000, 7, 1, 1, 1)),
                dict(id=2, a_date=datetime(2010, 8, 2, 0, 2, 0), a_timestamp=datetime(2010, 8, 2, 2, 2)),
                dict(id=3, a_date=datetime(2020, 9, 3, 1, 3, 0), a_timestamp=datetime(2020, 9, 3, 3, 3)),
            ],
        ),
        (
            # description: base test
            # input values
            dict(
                target_column="a_date",
                column="a_timestamp",
                from_timezone="CET",
                to_timezone="UTC",
                add_target_timezone=True,
            ),
            # expected data
            [
                dict(
                    id=1,
                    a_date=datetime(2000, 6, 30, 23, 1, 0),
                    a_timestamp=datetime(2000, 7, 1, 1, 1),
                    a_date_timezone="UTC",
                ),
                dict(
                    id=2,
                    a_date=datetime(2010, 8, 2, 0, 2, 0),
                    a_timestamp=datetime(2010, 8, 2, 2, 2),
                    a_date_timezone="UTC",
                ),
                dict(
                    id=3,
                    a_date=datetime(2020, 9, 3, 1, 3, 0),
                    a_timestamp=datetime(2020, 9, 3, 3, 3),
                    a_date_timezone="UTC",
                ),
            ],
        ),
    ],
)
def test_change_time_zone(input_values, expected, sample_df_with_timestamp):
    df = ChangeTimeZone(**input_values).transform(df=sample_df_with_timestamp)
    actual = [k.asDict() for k in df.collect()]
    assert actual == expected
    log.info("ChangeTimeZone unit test completed")


@pytest.mark.parametrize(
    "input_values,expected",
    [
        (
            # description: base test
            # input values
            dict(
                target_column="a_timestamp",
                column="a_string_timestamp",
                format="yyyyMMddHHmm",
            ),
            # expected data
            [
                dict(id=1, a_string_timestamp="202301010420", a_timestamp=datetime(2023, 1, 1, 4, 20, 0)),
                dict(id=2, a_string_timestamp="202302020314", a_timestamp=datetime(2023, 2, 2, 3, 14, 0)),
            ],
        )
    ],
)
def test_to_timestamp(input_values, expected, sample_df_with_string_timestamp):
    df = ToTimestamp(**input_values).transform(sample_df_with_string_timestamp)
    actual = [k.asDict() for k in df.collect()]
    assert actual == expected
    log.info("ToTimestamp unit test completed")
