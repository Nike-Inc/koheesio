"""
Module for KafkaReader and KafkaStreamReader.
"""

from typing import Dict, Optional

from koheesio.models import ExtraParamsMixin, Field
from koheesio.spark import DataFrameReader, DataStreamReader
from koheesio.spark.readers import Reader


class KafkaReader(Reader, ExtraParamsMixin):
    """
    Reader for Kafka topics.

    Wrapper around Spark's kafka read format. Supports both batch and streaming reads.

    Parameters
    ----------
    read_broker : str
        Kafka brokers to read from. Should be passed as a single string with multiple brokers passed in a comma
        separated list
    topic : str
        Kafka topic to consume.
    streaming : Optional[bool]
        Whether to read the kafka topic as a stream or not.
    params : Optional[Dict[str, str]]
        Arbitrary options to be applied when creating NSP Reader. If a user provides values for `subscribe` or
        `kafka.bootstrap.servers`, they will be ignored in favor of configuration passed through `topic` and
        `read_broker` respectively. Defaults to an empty dictionary.

    Notes
    -----
    * The `read_broker` and `topic` parameters are required.
    * The `streaming` parameter defaults to `False`.
    * The `params` parameter defaults to an empty dictionary. This parameter is also aliased as `kafka_options`.
    * Any extra kafka options can also be passed as key-word arguments; these will be merged with the `params` parameter

    Example
    -------
    ```python
    from koheesio.spark.readers.kafka import KafkaReader

    kafka_reader = KafkaReader(
        read_broker="kafka-broker-1:9092,kafka-broker-2:9092",
        topic="my-topic",
        streaming=True,
        # extra kafka options can be passed as key-word arguments
        startingOffsets="earliest",
    )
    ```

    In the example above, the `KafkaReader` will read from the `my-topic` Kafka topic, using the brokers
    `kafka-broker-1:9092` and `kafka-broker-2:9092`. The reader will read the topic as a stream and will start reading
    from the earliest available offset.

    The stream can be started by calling the `read` or `execute` method on the `kafka_reader` object.

    > Note: The `KafkaStreamReader` could be used in the example above to achieve the same result. `streaming` would
        default to `True` in that case and could be omitted from the parameters.

    See Also
    --------
    - Official Spark Documentation: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    """

    read_broker: str = Field(
        ...,
        description="Kafka brokers to read from, should be passed as a single string with multiple brokers passed in a "
        "comma separated list",
    )

    topic: str = Field(default=..., description="Kafka topic to consume.")
    streaming: Optional[bool] = Field(
        default=False, description="Whether to read the kafka topic as a stream or not. Defaults to False."
    )
    params: Dict[str, str] = Field(
        default_factory=dict,
        alias="kafka_options",
        description="Arbitrary options to be applied when creating NSP Reader. If a user provides values for "
        "'subscribe' or 'kafka.bootstrap.servers', they will be ignored in favor of configuration passed through "
        "'topic' and 'read_broker' respectively.",
    )

    @property
    def stream_reader(self) -> DataStreamReader:
        """Returns the Spark readStream object."""
        return self.spark.readStream

    @property
    def batch_reader(self) -> DataFrameReader:
        """Returns the Spark read object for batch processing."""
        return self.spark.read

    @property
    def reader(self) -> Reader:
        """Returns the appropriate reader based on the streaming flag."""
        if self.streaming:
            return self.stream_reader
        return self.batch_reader

    @property
    def options(self) -> Dict[str, str]:
        """Merge fixed parameters with arbitrary options provided by user."""
        return {
            **self.params,
            "subscribe": self.topic,
            "kafka.bootstrap.servers": self.read_broker,
        }

    @property
    def logged_option_keys(self) -> set:
        """Keys that are allowed to be logged for the options."""
        return {
            "kafka.bootstrap.servers",
            "subscribe",
            "subscribePattern",
            "includeHeaders",
            "startingOffsets",
            "endingOffsets",
            "key.deserializer",
            "value.deserializer",
            "kafka.group.id",
        }

    def execute(self) -> Reader.Output:
        applied_options = {k: v for k, v in self.options.items() if k in self.logged_option_keys}
        self.log.debug(f"Applying options {applied_options}")

        self.output.df = self.reader.format("kafka").options(**self.options).load()  # type: ignore


class KafkaStreamReader(KafkaReader):
    """KafkaStreamReader is a KafkaReader that reads data as a stream

    This class is identical to KafkaReader, with the `streaming` parameter defaulting to `True`.
    """

    streaming: bool = True
