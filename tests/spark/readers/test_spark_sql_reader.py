import pytest

from koheesio.spark.readers.spark_sql_reader import SparkSqlReader

pytestmark = pytest.mark.spark


def test_spark_sql_reader(spark, data_path):
    spark.sql("CREATE TABLE IF NOT EXISTS table_A (id INT, a_name STRING) USING DELTA")
    spark.sql("CREATE TABLE IF NOT EXISTS table_B (id INT, b_name STRING) USING DELTA")
    spark.sql("INSERT INTO table_A VALUES (1, 'Table_A'), (2, 'B'), (3, 'C')")
    spark.sql("INSERT INTO table_B VALUES (1, 'Table_B'), (2, 'D'), (3, 'E')")

    reader = SparkSqlReader(
        sql="""SELECT * FROM table_A JOIN table_B ON table_A.${join_col} = table_B.${join_col}
               WHERE table_A.${join_col} = 1""",
        join_col="id",
    )
    df = reader.read()
    actual = df.head().asDict()
    expected = {"id": 1, "a_name": "Table_A", "b_name": "Table_B"}
    assert actual == expected

    reader = SparkSqlReader(sql_path=f"{data_path}/sql/spark_sql_reader.sql")
    df = reader.read()
    actual = df.head().asDict()
    assert actual == expected


def test_spark_sql_reader_failed():
    with pytest.raises(ValueError):
        SparkSqlReader(sql="SELECT 1", sql_path="tests/resources/sql/none_existent_path.sql")

    with pytest.raises(FileNotFoundError):
        SparkSqlReader(sql_path="tests/resources/sql/none_existent_path.sql")

    with pytest.raises(ValueError):
        SparkSqlReader(sql=None)

    with pytest.raises(ValueError):
        SparkSqlReader(sql="")
