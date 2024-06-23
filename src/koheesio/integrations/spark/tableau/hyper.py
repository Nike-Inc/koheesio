from koheesio.steps import Step, StepOutput
from koheesio.spark.readers import SparkStep
from koheesio.models import conlist

from koheesio.spark.transformations.cast_to_datatype import CastToDatatype

import os
from pydantic import Field
from abc import ABC, abstractmethod

from typing import Any, List
from tempfile import TemporaryDirectory

from pyspark.sql.types import StringType, FloatType, BooleanType, LongType, StructField, StructType

from pathlib import PurePath
from tableauhyperapi import (
    Connection,
    CreateMode,
    HyperProcess,
    Inserter,
    NOT_NULLABLE,
    NULLABLE,
    SqlType,
    TableDefinition,
    TableName,
    Telemetry,
)


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

    Example:
        df = HyperFileReader(
            path=PurePath(hw.hyper_path),
        ).execute().df
    """
    path: PurePath = Field(
        default=...,
        description="Path to the Hyper file",
        examples=["PurePath(~/data/my-file.hyper)"]
    )

    def execute(self):
        type_mapping = {
            "date": StringType,
            "text": StringType,
            "double": FloatType,
            "bool": BooleanType,
            "big_int": LongType,
            "timestamp": StringType,
        }

        df_cols = []
        timestamp_cols = []

        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hp:
            with Connection(endpoint=hp.endpoint, database=self.path) as connection:
                table_definition = connection.catalog.get_table_definition(name=self.table_name)

                select_cols = []
                self.log.debug(f"Schema for {self.table_name} in {self.path}:")
                for column in table_definition.columns:
                    self.log.debug(f"|-- {column.name}: {column.type} (nullable = {column.nullability})")

                    column_name = column.name.unescaped.__str__()
                    tableau_type = column.type.__str__().lower()
                    spark_type = type_mapping.get(tableau_type, StringType)

                    if tableau_type == "timestamp":
                        timestamp_cols.append(column_name)
                        col = f'cast("{column_name}" as text)'
                    elif tableau_type == "date":
                        col = f'cast("{column_name}" as text)'
                    else:
                        col = f'"{column_name}"'

                    df_cols.append(StructField(column_name, spark_type()))
                    select_cols.append(col)

                data = connection.execute_list_query(f"select {','.join(select_cols)} from {self.table_name}")

        df_schema = StructType(df_cols)
        df = self.spark.createDataFrame(data, schema=df_schema)
        df = CastToDatatype(column=timestamp_cols, datatype="timestamp").transform(df)

        self.output.df = df


class HyperFileWriter(HyperFile):
    """
    Base class for all HyperFileWriter classes
    """
    path: PurePath = Field(
        default=TemporaryDirectory().name,
        description="Path to the Hyper file",
        examples=["PurePath(/tmp/hyper/)"]
    )
    name: str = Field(default="extract", description="Name of the Hyper file")
    table_definition: TableDefinition = Field(
        default=...,
        description="Table definition to write to the Hyper file as described in "
        "https://tableau.github.io/hyper-db/lang_docs/py/tableauhyperapi.html#tableauhyperapi.TableDefinition"
    )

    class Output(StepOutput):
        """Output class for HyperFileListWriter"""
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

    @abstractmethod
    def execute(self):
        pass


class HyperFileListWriter(HyperFileWriter):
    """
    Write list of rows to a Hyper file.

    Example:
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
    """
    table_definition: TableDefinition = Field(
        default=...,
        description="Table definition to write to the Hyper file as described in "
        "https://tableau.github.io/hyper-db/lang_docs/py/tableauhyperapi.html#tableauhyperapi.TableDefinition"
    )
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
    Refer to: https://tableau.github.io/hyper-db/docs/sql/command/copy_from

    Note:
        This method is much faster than HyperFileListWriter for large files.

    Example:
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
    """
    file: conlist(str, min_length=1) = Field(
        default=...,
        alias="files",
        description="One or multiple parquet files to write to the Hyper file"
    )

    def execute(self):
        array_files = "'" + "','".join(self.file) + "'"

        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hp:
            with Connection(
                endpoint=hp.endpoint, database=self.hyper_path, create_mode=CreateMode.CREATE_AND_REPLACE
            ) as connection:
                connection.catalog.create_schema(schema=self.table_definition.table_name.schema_name)
                connection.catalog.create_table(table_definition=self.table_definition)
                sql = f'copy "{self.schema_}"."{self.table}" ' \
                    f'from array [{array_files}] ' \
                    f'with (format parquet)'
                self.log.debug(f"Executing SQL: {sql}")
                connection.execute_command(sql)

        self.output.hyper_path = self.hyper_path

