import os
from typing import Any, List, Optional, Union
from abc import ABC, abstractmethod
from pathlib import PurePath
from tempfile import TemporaryDirectory

from tableauhyperapi import (
    NOT_NULLABLE,
    NULLABLE,
    Connection,
    CreateMode,
    HyperProcess,
    Inserter,
    SqlType,
    TableDefinition,
    TableName,
    Telemetry,
)

from pydantic import Field, conlist

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from koheesio.spark.readers import SparkStep
from koheesio.spark.transformations.cast_to_datatype import CastToDatatype
from koheesio.spark.utils import spark_minor_version
from koheesio.steps import Step, StepOutput


class HyperFile(Step, ABC):
    """
    Base class for all HyperFile classes
    """

    schema_: str = Field(default="Extract", alias="schema", description="Internal schema name within the Hyper file")
    table: str = Field(default="Extract", description="Table name within the Hyper file")

    @property
    def table_name(self) -> TableName:
        """
        Return TableName object for the Hyper file TableDefinition.
        """
        return TableName(self.schema_, self.table)


class HyperFileReader(HyperFile, SparkStep):
    """
    Read a Hyper file and return a Spark DataFrame.

    Examples
    --------
    ```python
    df = HyperFileReader(
        path=PurePath(hw.hyper_path),
    ).execute().df
    ```
    """

    path: PurePath = Field(
        default=..., description="Path to the Hyper file", examples=["PurePath(~/data/my-file.hyper)"]
    )

    def execute(self):
        type_mapping = {
            "date": StringType,
            "text": StringType,
            "double": FloatType,
            "bool": BooleanType,
            "small_int": ShortType,
            "big_int": LongType,
            "timestamp": StringType,
            "timestamp_tz": StringType,
            "int": IntegerType,
            "numeric": DecimalType,
        }
        df_cols = []
        timestamp_cols = []
        date_cols = []

        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hp:
            with Connection(endpoint=hp.endpoint, database=self.path) as connection:
                table_definition = connection.catalog.get_table_definition(name=self.table_name)

                select_cols = []
                self.log.debug(f"Schema for {self.table_name} in {self.path}:")
                for column in table_definition.columns:
                    self.log.debug(f"|-- {column.name}: {column.type} (nullable = {column.nullability})")

                    column_name = column.name.unescaped.__str__()
                    tableau_type = column.type.__str__().lower()

                    if tableau_type.startswith("numeric"):
                        spark_type = DecimalType(precision=18, scale=5)
                    else:
                        spark_type = type_mapping.get(tableau_type, StringType)()

                    if tableau_type == "timestamp" or tableau_type == "timestamp_tz":
                        timestamp_cols.append(column_name)
                        _col = f'cast("{column_name}" as text)'
                    elif tableau_type == "date":
                        date_cols.append(column_name)
                        _col = f'cast("{column_name}" as text)'
                    elif tableau_type.startswith("numeric"):
                        _col = f'cast("{column_name}" as decimal(18,5))'
                    else:
                        _col = f'"{column_name}"'

                    df_cols.append(StructField(column_name, spark_type))
                    select_cols.append(_col)

                data = connection.execute_list_query(f"select {','.join(select_cols)} from {self.table_name}")

        df_schema = StructType(df_cols)
        df = self.spark.createDataFrame(data, schema=df_schema)
        if timestamp_cols:
            df = CastToDatatype(column=timestamp_cols, datatype="timestamp").transform(df)
        if date_cols:
            df = CastToDatatype(column=date_cols, datatype="date").transform(df)

        self.output.df = df


class HyperFileWriter(HyperFile):
    """
    Base class for all HyperFileWriter classes
    """

    path: PurePath = Field(
        default=TemporaryDirectory().name, description="Path to the Hyper file", examples=["PurePath(/tmp/hyper/)"]
    )
    name: str = Field(default="extract", description="Name of the Hyper file")
    table_definition: TableDefinition = Field(
        default=None,
        description="Table definition to write to the Hyper file as described in "
        "https://tableau.github.io/hyper-db/lang_docs/py/tableauhyperapi.html#tableauhyperapi.TableDefinition",
    )

    class Output(StepOutput):
        """
        Output class for HyperFileListWriter
        """

        hyper_path: PurePath = Field(default=..., description="Path to created Hyper file")

    @property
    def hyper_path(self) -> Connection:
        """
        Return full path to the Hyper file.
        """
        if not os.path.exists(self.path):
            os.makedirs(self.path)

        hyper_path = PurePath(self.path, f"{self.name}.hyper" if ".hyper" not in self.name else self.name)
        self.log.info(f"Destination file: {hyper_path}")
        return hyper_path

    def write(self):
        self.execute()

    @abstractmethod
    def execute(self):
        pass


class HyperFileListWriter(HyperFileWriter):
    """
    Write list of rows to a Hyper file.

    Reference
    ---------
    Datatypes in https://tableau.github.io/hyper-db/docs/sql/datatype/ for supported data types.

    Examples
    --------
    ```python
    hw = HyperFileListWriter(
        name="test",
        table_definition=TableDefinition(
            table_name=TableName("Extract", "Extract"),
            columns=[
                TableDefinition.Column(name="string", type=SqlType.text(), nullability=NOT_NULLABLE),
                TableDefinition.Column(name="int", type=SqlType.int(), nullability=NULLABLE),
                TableDefinition.Column(name="timestamp", type=SqlType.timestamp(), nullability=NULLABLE),
            ]
        ),
        data=[
            ["text_1", 1, datetime(2024, 1, 1, 0, 0, 0, 0)],
            ["text_2", 2, datetime(2024, 1, 2, 0, 0, 0, 0)],
            ["text_3", None, None],
        ],
    ).execute()

    # do somthing with returned file path
    hw.hyper_path
    ```
    """

    data: conlist(List[Any], min_length=1) = Field(default=..., description="List of rows to write to the Hyper file")

    def execute(self):
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hp:
            with Connection(
                endpoint=hp.endpoint, database=self.hyper_path, create_mode=CreateMode.CREATE_AND_REPLACE
            ) as connection:
                connection.catalog.create_schema(schema=self.table_definition.table_name.schema_name)
                connection.catalog.create_table(table_definition=self.table_definition)
                with Inserter(connection, self.table_definition) as inserter:
                    inserter.add_rows(rows=self.data)
                    inserter.execute()

        self.output.hyper_path = self.hyper_path


class HyperFileParquetWriter(HyperFileWriter):
    """
    Read one or multiple parquet files and write them to a Hyper file.

    Notes
    -----
    This method is much faster than HyperFileListWriter for large files.

    References
    ----------
    Copy from external format: https://tableau.github.io/hyper-db/docs/sql/command/copy_from
    Datatypes in https://tableau.github.io/hyper-db/docs/sql/datatype/ for supported data types.
    Parquet format limitations:
        https://tableau.github.io/hyper-db/docs/sql/external/formats/#external-format-parquet

    Examples
    --------
    ```python
    hw = HyperFileParquetWriter(
        name="test",
        table_definition=TableDefinition(
            table_name=TableName("Extract", "Extract"),
            columns=[
                TableDefinition.Column(name="string", type=SqlType.text(), nullability=NOT_NULLABLE),
                TableDefinition.Column(name="int", type=SqlType.int(), nullability=NULLABLE),
                TableDefinition.Column(name="timestamp", type=SqlType.timestamp(), nullability=NULLABLE),
            ]
        ),
        files=["/my-path/parquet-1.snappy.parquet","/my-path/parquet-2.snappy.parquet"]
    ).execute()

    # do somthing with returned file path
    hw.hyper_path
    ```
    """

    file: conlist(Union[str, PurePath], min_length=1) = Field(
        default=..., alias="files", description="One or multiple parquet files to write to the Hyper file"
    )

    def execute(self):
        _file = [str(f) for f in self.file]
        array_files = "'" + "','".join(_file) + "'"

        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hp:
            with Connection(
                endpoint=hp.endpoint, database=self.hyper_path, create_mode=CreateMode.CREATE_AND_REPLACE
            ) as connection:
                connection.catalog.create_schema(schema=self.table_definition.table_name.schema_name)
                connection.catalog.create_table(table_definition=self.table_definition)
                sql = f'copy "{self.schema_}"."{self.table}" ' f"from array [{array_files}] " f"with (format parquet)"
                self.log.debug(f"Executing SQL: {sql}")
                connection.execute_command(sql)

        self.output.hyper_path = self.hyper_path


class HyperFileDataFrameWriter(HyperFileWriter):
    """
    Write a Spark DataFrame to a Hyper file.
    The process will write the DataFrame to a parquet file and then use the HyperFileParquetWriter to write to the
    Hyper file.

    Examples
    --------
    ```python
    hw = HyperFileDataFrameWriter(
        df=spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "name"]),
        name="test",
    ).execute()

    # do somthing with returned file path
    hw.hyper_path
    ```
    """
    df: DataFrame = Field(default=..., description="Spark DataFrame to write to the Hyper file")
    table_definition: Optional[TableDefinition] = None  # table_definition is not required for this class

    @staticmethod
    def table_definition_column(column: StructField) -> TableDefinition.Column:
        """
        Convert a Spark StructField to a Tableau Hyper SqlType
        """
        type_mapping = {
            IntegerType(): SqlType.int,
            LongType(): SqlType.big_int,
            ShortType(): SqlType.small_int,
            DoubleType(): SqlType.double,
            FloatType(): SqlType.double,
            BooleanType(): SqlType.bool,
            DateType(): SqlType.date,
            StringType(): SqlType.text,
        }

        # Handling the TimestampNTZType for Spark 3.4+
        # Mapping both TimestampType and TimestampNTZType to NTZ type of Hyper
        if spark_minor_version >= 3.4:
            from pyspark.sql.types import TimestampNTZType

            type_mapping[TimestampNTZType()] = SqlType.timestamp
            type_mapping[TimestampType()] = SqlType.timestamp
        # In older versions of Spark, only TimestampType is available and is mapped to TZ type of Hyper
        else:
            type_mapping[TimestampType()] = SqlType.timestamp_tz

        if column.dataType in type_mapping:
            sql_type = type_mapping[column.dataType]()
        elif str(column.dataType).startswith("DecimalType"):
            # Tableau Hyper API limits the precision to 18 decimal places
            # noinspection PyUnresolvedReferences
            sql_type = SqlType.numeric(
                precision=column.dataType.precision if column.dataType.precision <= 18 else 18,
                scale=column.dataType.scale,
            )
        else:
            raise ValueError(f"Unsupported datatype '{column.dataType}' for column '{column.name}'.")

        return TableDefinition.Column(
            name=column.name, type=sql_type, nullability=NULLABLE if column.nullable else NOT_NULLABLE
        )

    @property
    def _table_definition(self) -> TableDefinition:
        schema = self.df.schema
        columns = list(map(self.table_definition_column, schema))

        td = TableDefinition(table_name=self.table_name, columns=columns)
        self.log.debug(f"Table definition for {self.table_name}:")
        for column in td.columns:
            self.log.debug(f"|-- {column.name}: {column.type} (nullable = {column.nullability})")

        return td

    def clean_dataframe(self) -> DataFrame:
        """
        - Replace NULLs for string and numeric columns
        - Convert data types to ensure compatibility with Tableau Hyper API
        """
        _df = self.df
        _schema = self.df.schema

        integer_cols = [field.name for field in _schema if field.dataType == IntegerType()]
        long_cols = [field.name for field in _schema if field.dataType == LongType()]
        double_cols = [field.name for field in _schema if field.dataType == DoubleType()]
        float_cols = [field.name for field in _schema if field.dataType == FloatType()]
        string_cols = [field.name for field in _schema if field.dataType == StringType()]
        decimal_cols = [field for field in _schema if str(field.dataType).startswith("DecimalType")]
        timestamp_cols = [field.name for field in _schema if field.dataType == TimestampType()]

        # Cast decimal fields to DecimalType(18,3)
        for d_col in decimal_cols:
            # noinspection PyUnresolvedReferences
            if d_col.dataType.precision > 18:
                _df = self.df.withColumn(d_col.name, col(d_col.name).cast(DecimalType(precision=18, scale=5)))

        # Handling the TimestampNTZType for Spark 3.4+
        # Any TimestampType column will be cast to TimestampNTZType for compatibility with Tableau Hyper API
        if spark_minor_version >= 3.4:
            from pyspark.sql.types import TimestampNTZType

            for t_col in timestamp_cols:
                _df = _df.withColumn(t_col, col(t_col).cast(TimestampNTZType()))

        # Replace null and NaN values with 0
        if len(integer_cols) > 0:
            _df = _df.na.fill(0, integer_cols)
        elif len(long_cols) > 0:
            _df = _df.na.fill(0, long_cols)
        elif len(double_cols) > 0:
            _df = _df.na.fill(0.0, double_cols)
        elif len(float_cols) > 0:
            _df = _df.na.fill(0.0, float_cols)
        elif len(decimal_cols) > 0:
            _df = _df.na.fill(0.0, decimal_cols)
        elif len(string_cols) > 0:
            _df = _df.na.fill("", string_cols)

        return _df

    def write_parquet(self):
        _path = self.path.joinpath("parquet")
        (
            self.clean_dataframe()
            .coalesce(1)
            .write.option("delimiter", ",")
            .option("header", "true")
            .mode("overwrite")
            .parquet(_path.as_posix())
        )

        for _, _, files in os.walk(_path):
            for file in files:
                if file.endswith(".parquet"):
                    fp = PurePath(_path, file)
                    self.log.info("Parquet file created: %s", fp)
                    return [fp]

    def execute(self):
        w = HyperFileParquetWriter(
            path=self.path, name=self.name, table_definition=self._table_definition, files=self.write_parquet()
        )
        w.execute()
        self.output.hyper_path = w.output.hyper_path
