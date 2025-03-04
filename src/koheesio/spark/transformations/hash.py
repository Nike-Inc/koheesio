"""
Module for hashing data using SHA-2 family of hash functions

See the docstring of the Sha2Hash class for more information.
"""

from typing import List, Literal, Optional

from pyspark.sql import Column
from pyspark.sql.functions import col, concat_ws, sha2

from koheesio.models import Field
from koheesio.spark.transformations import ColumnsTransformation
from koheesio.spark.utils import SparkDatatype

HASH_ALGORITHM = Literal[224, 256, 384, 512]
STRING = SparkDatatype.STRING


def sha2_hash(columns: List[str], delimiter: Optional[str] = "|", num_bits: Optional[HASH_ALGORITHM] = 256) -> Column:
    """
    hash the value of 1 or more columns using SHA-2 family of hash functions

    Mild wrapper around pyspark.sql.functions.sha2

    - https://spark.apache.org/docs/3.3.2/api/python/reference/api/pyspark.sql.functions.sha2.html

    Returns the hex string result of SHA-2 family of hash functions (SHA-224, SHA-256, SHA-384, and SHA-512).
    This function allows concatenating the values of multiple columns together prior to hashing.

    If a null is passed, the result will also be null.

    Parameters
    ----------
    columns : List[str]
        The columns to hash
    delimiter : Optional[str], optional, default=|
        Optional separator for the string that will eventually be hashed. Defaults to '|'
    num_bits : Optional[HASH_ALGORITHM], optional, default=256
        Algorithm to use for sha2 hash. Defaults to 256. Should be one of 224, 256, 384, 512
    """
    # make sure all columns are of type pyspark.sql.Column and cast to string
    _columns = []
    for c in columns:
        if isinstance(c, str):
            c: Column = col(c)  # type: ignore
        _columns.append(c.cast(STRING.spark_type()))

    # concatenate columns if more than 1 column is provided
    if len(_columns) > 1:
        column = concat_ws(delimiter, *_columns)  # type: ignore
    else:
        column = _columns[0]

    return sha2(col=column, numBits=num_bits)  # type: ignore


# TODO: convert this class to a ColumnsTransformationWithTarget
class Sha2Hash(ColumnsTransformation):
    """
    hash the value of 1 or more columns using SHA-2 family of hash functions

    Mild wrapper around pyspark.sql.functions.sha2

    - https://spark.apache.org/docs/3.3.2/api/python/reference/api/pyspark.sql.functions.sha2.html

    Returns the hex string result of SHA-2 family of hash functions (SHA-224, SHA-256, SHA-384, and SHA-512).

    Note
    ----
    This function allows concatenating the values of multiple columns together prior to hashing.

    Parameters
    ----------
    columns : Union[str, List[str]]
        The column (or list of columns) to hash. Alias: column
    delimiter : Optional[str], optional, default=|
        Optional separator for the string that will eventually be hashed. Defaults to '|'
    num_bits : Optional[HASH_ALGORITHM], optional, default=256
        Algorithm to use for sha2 hash. Defaults to 256. Should be one of 224, 256, 384, 512
    target_column : str
        The generated hash will be written to the column name specified here
    """

    delimiter: Optional[str] = Field(
        default="|",
        description="Optional separator for the string that will eventually be hashed. Defaults to '|'",
    )
    num_bits: Optional[HASH_ALGORITHM] = Field(
        default=256, description="Algorithm to use for sha2 hash. Defaults to 256. Should be one of 224, 256, 384, 512"
    )
    target_column: str = Field(
        default=..., description="The generated hash will be written to the column name specified here"
    )

    def execute(self) -> "Sha2Hash.Output":
        if not (columns := list(self.get_columns())):
            self.output.df = self.df
            return self.output

        # check if columns exist in the dataframe
        missing_columns = set(columns) - set(self.df.columns)
        if missing_columns:
            raise ValueError(f"Columns {missing_columns} not found in dataframe")

        self.output.df = self.df.withColumn(
            self.target_column, sha2_hash(columns=columns, delimiter=self.delimiter, num_bits=self.num_bits)
        )
