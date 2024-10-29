"""Kafka writer to write batch or streaming data into kafka topics"""

from typing import Dict, Optional, Union

from pyspark.sql import DataFrameWriter
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery
from pyspark.sql.types import BinaryType, StringType

from koheesio.models import ExtraParamsMixin, Field, field_validator
from koheesio.spark.writers import Writer
from koheesio.spark.writers.stream import Trigger


class KafkaWriter(Writer, ExtraParamsMixin):
    """Kafka writer to write batch or streaming data into kafka topics

    All kafka specific options can be provided as additional init params

    Parameters
    ----------
    broker : str
        broker url of the kafka cluster
    topic : str
        full topic name to write the data to
    trigger : Optional[Union[Trigger, str, Dict]]
        Indicates optionally how to stream the data into kafka, continuous or batch
    checkpoint_location : str
        In case of a failure or intentional shutdown, you can recover the previous progress and state of a previous
        query, and continue where it left off. This is done using checkpointing and write-ahead logs.

    Example
    -------
    ```python
    KafkaWriter(
        write_broker="broker.com:9500",
        topic="test-topic",
        trigger=Trigger(continuous=True)
        includeHeaders: "true",
        key.serializer: "org.apache.kafka.common.serialization.StringSerializer",
        value.serializer: "org.apache.kafka.common.serialization.StringSerializer",
        kafka.group.id: "test-group",
        checkpoint_location: "s3://bucket/test-topic"
    )
    ```
    """

    format: str = "kafka"
    broker: str = Field(default=..., description="Kafka brokers to write to")
    topic: str = Field(default=..., description="Kafka topic to write to")
    trigger: Optional[Union[Trigger, str, Dict]] = Field(
        Trigger(available_now=True),
        description="Set the trigger for the stream query. If not set data is processed in batch",
    )
    checkpoint_location: str = Field(
        default=...,
        alias="checkpointLocation",
        description="In case of a failure or intentional shutdown, you can recover the previous progress and state of "
        "a previous query, and continue where it left off. This is done using checkpointing and write-ahead logs. You "
        "can configure a query with a checkpoint location, and the query will save all the progress information and "
        "the running aggregates to the checkpoint location. This checkpoint location has to be a path in an HDFS "
        "compatible file system that is accessible by Spark (e.g. Databricks Unity Catalog External Location)",
    )

    class Output(Writer.Output):
        """Output of the KafkaWriter"""

        streaming_query: Optional[Union[str, StreamingQuery]] = Field(
            default=None, description="Query ID of the stream query"
        )

    @property
    def streaming_query(self) -> Optional[Union[str, StreamingQuery]]:
        """return the streaming query"""
        return self.output.streaming_query

    @property
    def _trigger(self) -> dict[str, str]:
        """return the value of the Trigger object"""
        return self.trigger.value

    @field_validator("trigger")
    def _validate_trigger(cls, trigger: Optional[Union[Trigger, str, Dict]]) -> Trigger:
        """Validate the trigger value and convert it to a Trigger object if it is not already one."""
        return Trigger.from_any(trigger)

    def _validate_dataframe(self) -> None:
        """Validate the dataframe to be written to kafka"""
        if "value" not in self.df.columns:
            raise ValueError('Dataframe should at least have a "value" column.')
        if not isinstance(self.df.schema["value"].dataType, (StringType, BinaryType)):
            raise ValueError('The "value" column should be of type string or binary.')
        if "key" in self.df.columns:
            if not isinstance(self.df.schema["key"].dataType, (StringType, BinaryType)):
                raise ValueError('The "key" column should be of type string or binary.')

    @property
    def stream_writer(self) -> DataStreamWriter:
        """returns a stream writer

        Returns
        -------
        DataStreamWriter
        """
        write_stream = self.df.writeStream

        if self._trigger:
            write_stream = write_stream.trigger(**self._trigger)

        return write_stream

    @property
    def batch_writer(self) -> DataFrameWriter:
        """returns a batch writer

        Returns
        -------
        DataFrameWriter
        """
        return self.df.write

    @property
    def writer(self) -> Union[DataStreamWriter, DataFrameWriter]:
        """function to get the writer of proper type according to whether the data to written is a stream or not
        This function will also set the trigger property in case of a datastream.

        Returns
        -------
        Union[DataStreamWriter, DataFrameWriter]
            In case of streaming data -> DataStreamWriter, else -> DataFrameWriter
        """
        return self.stream_writer if self.streaming else self.batch_writer

    @property
    def options(self) -> Dict[str, str]:
        """retrieve the kafka options incl topic and broker.

        Returns
        -------
        dict
            Dict being the combination of kafka options + topic + broker
        """
        options = {
            **self.extra_params,
            "topic": self.topic,
            "kafka.bootstrap.servers": self.broker,
        }

        if self.checkpoint_location:
            options["checkpointLocation"] = self.checkpoint_location

        return options

    @property
    def logged_option_keys(self) -> set:
        """keys to be logged"""
        return {
            "kafka.bootstrap.servers",
            "topic",
            "includeHeaders",
            "key.serializer",
            "value.serializer",
            # "trigger",
            "checkpointLocation",
        }

    def execute(self) -> Writer.Output:
        """Effectively write the data from the dataframe (streaming of batch) to kafka topic.

        Returns
        -------
        KafkaWriter.Output
            streaming_query function can be used to gain insights on running write.
        """
        applied_options = {k: v for k, v in self.options.items() if k in self.logged_option_keys}
        self.log.debug(f"Applying options {applied_options}")

        self._validate_dataframe()

        _writer = self.writer.format(self.format).options(**self.options)
        self.output.streaming_query = _writer.start() if self.streaming else _writer.save()
