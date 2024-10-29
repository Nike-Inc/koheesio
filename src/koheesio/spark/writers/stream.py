"""Module that holds some classes and functions to be able to write to a stream

Classes
-------
Trigger
    class to set the trigger for a stream query
StreamWriter
    abstract class for stream writers
ForEachBatchStreamWriter
    class to run a writer for each batch

Functions
--------
writer_to_foreachbatch
    function to be used as batch_function for StreamWriter (sub)classes
"""

from typing import Callable, Dict, Optional, Union
from abc import ABC, abstractmethod

from koheesio import Step
from koheesio.models import ConfigDict, Field, field_validator, model_validator
from koheesio.spark import DataFrame, DataStreamWriter, StreamingQuery
from koheesio.spark.writers import StreamingOutputMode, Writer
from koheesio.utils import convert_str_to_bool


class Trigger(Step):
    """Trigger types for a stream query.

    Only one trigger can be set!

    Example
    -------

    - processingTime='5 seconds'
    - continuous='5 seconds'
    - availableNow=True
    - once=True

    See Also
    --------
    - https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers
    """

    processing_time: Optional[str] = Field(
        default=None,
        alias="processingTime",
        description="a processing time interval as a string, e.g. '5 seconds', '1 minute'."
        "Set a trigger that runs a microbatch query periodically based on the processing time.",
    )
    continuous: Optional[str] = Field(
        default=None,
        description="a time interval as a string, e.g. '5 seconds', '1 minute'."
        "Set a trigger that runs a continuous query with a given checkpoint interval.",
    )
    once: Optional[bool] = Field(
        default=None,
        deprecated=True,
        description="if set to True, set a trigger that processes only one batch of data in a streaming query then "
        "terminates the query. use `available_now` instead of `once`.",
    )
    available_now: Optional[bool] = Field(
        default=None,
        alias="availableNow",
        description="if set to True, set a trigger that processes all available data in multiple batches then "
        "terminates the query.",
    )

    model_config = ConfigDict(validate_default=False, extra="forbid")

    @classmethod
    def _all_triggers_with_alias(cls) -> list:
        """Internal method to return all trigger types with their alias. Used for logging purposes"""
        fields = cls.model_fields
        triggers = [
            f"{field_info.alias}/{field}" if field_info.alias else field
            for field, field_info in fields.items()
            if field not in ["name", "description"]
        ]
        return triggers

    @property
    def triggers(self) -> Dict:
        """Returns a list of tuples with the value for each trigger"""
        return self.model_dump(exclude={"name", "description"}, by_alias=True)

    @model_validator(mode="before")
    def validate_triggers(cls, triggers: Dict) -> Dict:
        """Validate the trigger value"""
        params = [*triggers.values()]

        # adapted from `pyspark.sql.streaming.readwriter.DataStreamWriter.trigger`; modified to work with pydantic v2
        if not triggers:
            raise ValueError("No trigger provided")
        if len(params) > 1:
            raise ValueError("Multiple triggers not allowed.")

        return triggers

    @field_validator("processing_time", mode="before")
    def validate_processing_time(cls, processing_time: str) -> str:
        """Validate the processing time trigger value"""
        # adapted from `pyspark.sql.streaming.readwriter.DataStreamWriter.trigger`
        if not isinstance(processing_time, str):
            raise ValueError(f"Value for processing_time must be a string. Got: {processing_time}")

        if len(processing_time.strip()) == 0:
            raise ValueError(f"Value for processingTime must be a non empty string. Got: {processing_time}")
        return processing_time

    @field_validator("continuous", mode="before")
    def validate_continuous(cls, continuous: str) -> str:
        """Validate the continuous trigger value"""
        # adapted from `pyspark.sql.streaming.readwriter.DataStreamWriter.trigger` except that the if statement is not
        # split in two parts
        if not isinstance(continuous, str):
            raise ValueError(f"Value for continuous must be a string. Got: {continuous}")

        if len(continuous.strip()) == 0:
            raise ValueError(f"Value for continuous must be a non empty string. Got: {continuous}")
        return continuous

    @field_validator("once", mode="before")
    def validate_once(cls, once: str) -> bool:
        """Validate the once trigger value"""
        # making value a boolean when given
        once = convert_str_to_bool(once)

        # adapted from `pyspark.sql.streaming.readwriter.DataStreamWriter.trigger`
        if once is not True:
            raise ValueError(f"Value for once must be True. Got: {once}")
        return once

    @field_validator("available_now", mode="before")
    def validate_available_now(cls, available_now: str) -> bool:
        """Validate the available_now trigger value"""
        # making value a boolean when given
        available_now = convert_str_to_bool(available_now)

        # adapted from `pyspark.sql.streaming.readwriter.DataStreamWriter.trigger`
        if available_now is not True:
            raise ValueError(f"Value for availableNow must be True. Got:{available_now}")
        return available_now

    @property
    def value(self) -> Dict[str, str]:
        """Returns the trigger value as a dictionary"""
        trigger = {k: v for k, v in self.triggers.items() if v is not None}
        return trigger

    @classmethod
    def from_dict(cls, _dict: dict) -> "Trigger":
        """Creates a Trigger class based on a dictionary"""
        return cls(**_dict)

    @classmethod
    def from_string(cls, trigger: str) -> "Trigger":
        """Creates a Trigger class based on a string

        Example
        -------
        ### happy flow

        * processingTime='5 seconds'
        * processing_time="5 hours"
        * processingTime=4 minutes
        * once=True
        * once=true
        * available_now=true
        * continuous='3 hours'
        * once=TrUe
        * once=TRUE

        ### unhappy flow
        valid values, but should fail the validation check of the class

        * availableNow=False
        * continuous=True
        * once=false
        """
        import re

        trigger_from_string = re.compile(r"(?P<triggerType>\w+)=[\'\"]?(?P<value>.+)[\'\"]?")
        _match = trigger_from_string.match(trigger)

        if _match is None:
            raise ValueError(
                f"Cannot parse value for Trigger: '{trigger}'. \n"
                f"Valid types are {', '.join(cls._all_triggers_with_alias())}"
            )

        trigger_type, value = _match.groups()

        # strip the value of any quotes
        value = value.strip("'").strip('"')

        # making value a boolean when given
        value = convert_str_to_bool(value)

        return cls.from_dict({trigger_type: value})

    @classmethod
    def from_any(cls, value: Union["Trigger", str, dict]) -> "Trigger":
        """Dynamically creates a Trigger class based on either another Trigger class, a passed string value, or a
        dictionary

        This way Trigger.from_any can be used as part of a validator, without needing to worry about supported types
        """
        if isinstance(value, Trigger):
            return value

        if isinstance(value, str):
            return cls.from_string(value)

        if isinstance(value, dict):
            return cls.from_dict(value)

        raise RuntimeError(f"Unable to create Trigger based on the given value: {value}")

    def execute(self) -> None:
        """Returns the trigger value as a dictionary
        This method can be skipped, as the value can be accessed directly from the `value` property
        """
        self.log.warning("Trigger.execute is deprecated. Use Trigger.value directly instead")
        self.output.value = self.value


class StreamWriter(Writer, ABC):
    """ABC Stream Writer"""

    checkpoint_location: str = Field(
        default=...,
        alias="checkpointLocation",
        description="In case of a failure or intentional shutdown, you can recover the previous progress and state of "
        "a previous query, and continue where it left off. This is done using checkpointing and write-ahead logs. You "
        "can configure a query with a checkpoint location, and the query will save all the progress information and "
        "the running aggregates to the checkpoint location. This checkpoint location has to be a path in an HDFS "
        "compatible file system that is accessible by Spark (e.g. Databricks Unity Catalog External Location)",
    )

    output_mode: StreamingOutputMode = Field(
        default=StreamingOutputMode.APPEND, alias="outputMode", description=StreamingOutputMode.__doc__
    )

    batch_function: Optional[Callable] = Field(
        default=None,
        description="allows you to run custom batch functions for each micro batch",
        alias="batch_function_for_each_df",
    )

    trigger: Optional[Union[Trigger, str, Dict]] = Field(
        default=Trigger(available_now=True),  # type: ignore[call-arg]
        description="Set the trigger for the stream query. If this is not set it process data as batch",
    )

    streaming_query: Optional[Union[str, StreamingQuery]] = Field(
        default=None, description="Query ID of the stream query"
    )

    @property
    def _trigger(self) -> dict:
        """Returns the trigger value as a dictionary"""
        return self.trigger.value

    @field_validator("output_mode")
    def _validate_output_mode(cls, mode: Union[str, StreamingOutputMode]) -> str:
        """Ensure that the given mode is a valid StreamingOutputMode"""
        if isinstance(mode, str):
            return mode
        return str(mode.value)

    @field_validator("trigger")
    def _validate_trigger(cls, trigger: Union[Trigger, str, Dict]) -> Trigger:
        """Ensure that the given trigger is a valid Trigger class"""
        return Trigger.from_any(trigger)

    def await_termination(self, timeout: Optional[int] = None) -> None:
        """Await termination of the stream query"""
        self.streaming_query.awaitTermination(timeout=timeout)

    @property
    def stream_writer(self) -> DataStreamWriter:  # type: ignore
        """Returns the stream writer for the given DataFrame and settings"""
        write_stream = self.df.writeStream.format(self.format).outputMode(self.output_mode)

        if self.checkpoint_location:
            write_stream = write_stream.option("checkpointLocation", self.checkpoint_location)

        if self.batch_function:
            write_stream = write_stream.foreachBatch(self.batch_function)

        # set trigger
        write_stream = write_stream.trigger(**self._trigger)

        return write_stream

    @property
    def writer(self) -> DataStreamWriter:  # type: ignore
        """Returns the stream writer since we don't have a batch mode for streams"""
        return self.stream_writer

    @abstractmethod
    def execute(self) -> None:
        raise NotImplementedError


class ForEachBatchStreamWriter(StreamWriter):
    """Runnable ForEachBatchWriter"""

    @field_validator("batch_function")
    def _validate_batch_function_exists(cls, batch_function: Callable) -> Callable:
        """Ensure that a batch_function is defined"""
        if not batch_function or not isinstance(batch_function, Callable):  # type: ignore[truthy-function, arg-type]
            raise ValueError(f"{cls.__name__} requires a defined for `batch_function`")
        return batch_function

    def execute(self) -> None:
        self.streaming_query = self.writer.start()


def writer_to_foreachbatch(writer: Writer) -> Callable:
    """Call `writer.execute` on each batch

    To be passed as batch_function for StreamWriter (sub)classes.

    Example
    -------
    ### Writing to a Delta table and a Snowflake table
    ```python
    DeltaTableStreamWriter(
        table="my_table",
        checkpointLocation="my_checkpointlocation",
        batch_function=writer_to_foreachbatch(
            SnowflakeWriter(
                **sfOptions,
                table="snowflake_table",
                insert_type=SnowflakeWriter.InsertType.APPEND,
            )
        ),
    )
    ```
    """

    def inner(df: DataFrame, batch_id: int) -> None:
        """Inner method

        As per the Spark documentation:
        In every micro-batch, the provided function will be called in every micro-batch with (i) the output rows as a
        DataFrame and (ii) the batch identifier. The batchId can be used deduplicate and transactionally write the
        output (that is, the provided Dataset) to external systems. The output DataFrame is guaranteed to exactly
        same for the same batchId (assuming all operations are deterministic in the query).
        """
        writer.log.debug(f"Running batch function for batch {batch_id}")
        writer.write(df)

    return inner
