from chispa import assert_df_equality
import pytest

from pyspark.sql.types import StructType

from koheesio.spark.readers.memory import DataFormat, InMemoryDataReader

pytestmark = pytest.mark.spark


class TestInMemoryDataReader:
    # fmt: off
    @pytest.mark.parametrize(
        "data,format,params,expect_filter",
        [
            pytest.param(
                "id,string\n1,hello\n2,world", DataFormat.CSV, {"header":True}, "id < 3"
            ),
            pytest.param(
                b"id,string\n1,hello\n2,world", DataFormat.CSV, {"header":0}, "id < 3"
            ),
            pytest.param(
                '{"id": 1, "string": "hello"}', DataFormat.JSON, {}, "id < 2"
            ),
            pytest.param(
                {"id": 1, "string": "hello"}, DataFormat.JSON, {}, "id < 2"
            ),
            pytest.param(
                ['{"id": 1, "string": "hello"}', '{"id": 2, "string": "world"}'], DataFormat.JSON, {}, "id < 3"
            ),
            pytest.param(
                [{"id": 1, "string": "hello"}, {"id": 2, "string": "world"}], DataFormat.JSON, {}, "id < 3"
            ),
        ],
    )
    # fmt: on
    def test_execute(self, spark, sample_df_with_strings, data, format, params, expect_filter):
        df = (
            InMemoryDataReader(
                data=data,
                format=format,
                schema_=StructType.fromJson(
                    {
                        "fields": [
                            {"metadata": {}, "name": "id", "nullable": True, "type": "long"},
                            {"metadata": {}, "name": "string", "nullable": True, "type": "string"},
                        ],
                        "type": "struct",
                    }
                ),
                params=params,
            )
            .execute()
            .df
        )
        assert_df_equality(df1=df, df2=sample_df_with_strings.where(expect_filter))
