from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual

from koheesio.spark.readers import Reader
from koheesio.spark.transformations.dataframe.cast_to_target import CastToTarget


def test_get_schema_with_reader(spark, mocker):
    reader = mocker.MagicMock(spec=Reader)
    reader.read.return_value = spark.createDataFrame([], "col1:string, col2:int")

    transformation = CastToTarget(target=reader)
    schema = transformation._get_schema()

    assert schema == [("col1", StringType()), ("col2", IntegerType())]


def test_get_schema_with_dataframe(spark):
    df = spark.createDataFrame([], "col1:string, col2:int")

    transformation = CastToTarget(target=df)
    schema = transformation._get_schema()

    assert schema == [("col1", StringType()), ("col2", IntegerType())]


def test_get_schema_with_structtype():
    struct_type = StructType(
        [
            StructField("col1", StringType(), True),
            StructField("col2", IntegerType(), True),
        ]
    )

    transformation = CastToTarget(target=struct_type)
    schema = transformation._get_schema()

    assert schema == [("col1", StringType()), ("col2", IntegerType())]


def test_cast_to_target(spark):
    # col6 should not appear in the output
    input_df = spark.createDataFrame(
        [
            ("val1", 123, 45.67, "extra1"),
        ],
        "col1:string, col2:int, col3:float, col6:string",
    )

    target_schema = StructType(
        [
            StructField("col1", StringType(), True),
            StructField("col2", StringType(), True),  # This will be casted from int
            StructField("col3", FloatType(), True),
            StructField("col4", IntegerType(), True),  # This is not present in the input, should be lit(None)
            StructField("col5", IntegerType(), True),  # This is not present in the input, should be lit(None)
        ]
    )

    expected = spark.createDataFrame(
        [
            ("val1", "123", 45.67, None, None),
        ],
        target_schema,
    )

    transformation = CastToTarget(target=target_schema, df=input_df)
    transformation.execute()
    actual = transformation.output.df

    assertDataFrameEqual(actual, expected)
