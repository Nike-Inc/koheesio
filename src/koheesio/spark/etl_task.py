"""
ETL Task

Extract -> Transform -> Load
"""

import datetime

from koheesio import Step
from koheesio.models import Field, InstanceOf, conlist
from koheesio.spark import DataFrame
from koheesio.spark.readers import Reader
from koheesio.spark.transformations import Transformation
from koheesio.spark.writers import Writer
from koheesio.utils import utc_now


class EtlTask(Step):
    """ETL Task

    Etl stands for: Extract -> Transform -> Load

    This task is a composition of a Reader (extract), a series of Transformations (transform) and a Writer (load).
    In other words, it _reads_ data from a _source_, applies a series of _transformations_, and _writes_ the result to
    a _target_.

    Parameters
    ----------
    name : str
        Name of the task
    description : str
        Description of the task
    source : koheesio.steps.readers.Reader
        Source to read from [extract]
    transformations : list[koheesio.steps.transformations.Transformation]
        Series of transformations [transform]. The order of the transformations is important!
    target : koheesio.steps.writers.Writer
        Target to write to [load]


    Example
    -------
    ```python
    from koheesio.tasks import EtlTask

    from koheesio.steps.readers import CsvReader
    from koheesio.steps.transformations.repartition import Repartition
    from koheesio.steps.writers import CsvWriter

    etl_task = EtlTask(
        name="My ETL Task",
        description="This is an example ETL task",
        source=CsvReader(path="path/to/source.csv"),
        transformations=[Repartition(num_partitions=2)],
        target=DummyWriter(),
    )

    etl_task.execute()
    ```

    This code will read from a CSV file, repartition the DataFrame to 2 partitions, and write the result to the console.

    Extending the EtlTask
    ---------------------
    The EtlTask is designed to be a simple and flexible way to define ETL processes. It is not designed to be a
    one-size-fits-all solution, but rather a starting point for building more complex ETL processes. If you need more
    complex functionality, you can extend the EtlTask class and override the `extract`, `transform` and `load` methods.
    You can also implement your own `execute` method to define the entire ETL process from scratch should you need more
    flexibility.

    Advantages of using the EtlTask
    -------------------------------
    - It is a simple way to define ETL processes
    - It is easy to understand and extend
    - It is easy to test and debug
    - It is easy to maintain and refactor
    - It is easy to integrate with other tools and libraries
    - It is easy to use in a production environment
    """

    source: InstanceOf[Reader] = Field(default=..., description="Source to read from [extract]")
    transformations: conlist(min_length=0, item_type=InstanceOf[Transformation]) = Field(
        default_factory=list, description="Series of transformations", alias="transforms"
    )
    target: InstanceOf[Writer] = Field(default=..., description="Target to write to [load]")

    # private attrs
    etl_date: datetime = Field(
        default_factory=utc_now,
        description="Date time when this object was created as iso format. Example: '2023-01-24T09:39:23.632374'",
    )

    class Output(Step.Output):
        """Output class for EtlTask"""

        source_df: DataFrame = Field(default=..., description="The Spark DataFrame produced by .extract() method")
        transform_df: DataFrame = Field(default=..., description="The Spark DataFrame produced by .transform() method")
        target_df: DataFrame = Field(default=..., description="The Spark DataFrame used by .load() method")

    def extract(self) -> DataFrame:
        """Read from Source

        logging is handled by the Reader.execute()-method's @do_execute decorator
        """
        reader: Reader = self.source
        return reader.read()

    def transform(self, df: DataFrame) -> DataFrame:
        """Transform recursively

        logging is handled by the Transformation.execute()-method's @do_execute decorator
        """
        for t in self.transformations:
            df = t.transform(df)
        return df

    def load(self, df: DataFrame) -> DataFrame:
        """Write to Target

        logging is handled by the Writer.execute()-method's @do_execute decorator
        """
        writer: Writer = self.target
        writer.write(df)
        return df

    def execute(self) -> Step.Output:
        """Run the ETL process"""
        self.log.info(f"Task started at {self.etl_date}")

        # extract from source
        self.output.source_df = self.extract()

        # transform
        self.output.transform_df = self.transform(self.output.source_df)

        # load to target
        self.output.target_df = self.load(self.output.transform_df)
