"""Ability to generate UUID5 using native pyspark (no udf)"""

from typing import Optional, Union
import uuid

from pyspark.sql import functions as f

from koheesio.models import Field, ListOfColumns, field_validator
from koheesio.spark.transformations import Transformation


def uuid5_namespace(ns: Optional[Union[str, uuid.UUID]]) -> uuid.UUID:
    """Helper function used to provide a UUID5 hashed namespace based on the passed str

    Parameters
    ----------
    ns : Optional[Union[str, uuid.UUID]]
        A str, an empty string (or None), or an existing UUID can be passed

    Returns
    -------
    uuid.UUID
        UUID5 hashed namespace
    """
    # if we already have a UUID, we just return it
    if isinstance(ns, uuid.UUID):
        return ns

    # if ns is empty or none, we simply return the default NAMESPACE_DNS
    if not ns:
        ns = uuid.NAMESPACE_DNS
        return ns

    # else we hash the string against the NAMESPACE_DNS
    ns = uuid.uuid5(uuid.NAMESPACE_DNS, ns)
    return ns


def hash_uuid5(
    input_value: str,
    namespace: Union[str, uuid.UUID] = "",
    extra_string: str = "",
) -> str:
    """pure python implementation of HashUUID5

    See: https://docs.python.org/3/library/uuid.html#uuid.uuid5

    Parameters
    ----------
    input_value : str
        value that will be hashed
    namespace : str | uuid.UUID, optional, default=""
        namespace DNS
    extra_string : str, optional, default=""
        optional extra string that will be prepended to the input_value

    Returns
    -------
    str
        uuid.UUID (uuid5) cast to string
    """
    if not isinstance(namespace, uuid.UUID):
        hashed_namespace = uuid5_namespace(namespace)
    else:
        hashed_namespace = namespace
    return str(uuid.uuid5(hashed_namespace, (extra_string + input_value)))


class HashUUID5(Transformation):
    """Generate a UUID with the UUID5 algorithm

    Spark does not provide inbuilt API to generate version 5 UUID, hence we have to use a custom implementation
    to provide this capability.

    Prerequisites: this function has no side effects. But be aware that in most cases, the expectation is that your
    data is clean (e.g. trimmed of leading and trailing spaces)

    Concept
    -------
    UUID5 is based on the SHA-1 hash of a namespace identifier (which is a UUID) and a name (which is a string).
    https://docs.python.org/3/library/uuid.html#uuid.uuid5

    Based on https://github.com/MrPowers/quinn/pull/96 with the difference that since Spark 3.0.0 an OVERLAY function
    from ANSI SQL 2016 is available which saves coding space and string allocation(s) in place of CONCAT + SUBSTRING.

    For more info on OVERLAY, see: https://docs.databricks.com/sql/language-manual/functions/overlay.html

    Example
    -------
    Input is a DataFrame with two columns:

    | id | string |
    |----|--------|
    | 1  | hello  |
    | 2  | world  |
    | 3  |        |

    Input parameters:

    - source_columns = ["id", "string"]
    - target_column = "uuid5"

    Result:

    | id | string | uuid5                                |
    |----|--------| ------------------------------------ |
    | 1  | hello  | f3e99bbd-85ae-5dc3-bf6e-cd0022a0ebe6 |
    | 2  | world  | b48e880f-c289-5c94-b51f-b9d21f9616c0 |
    | 3  |        | 2193a99d-222e-5a0c-a7d6-48fbe78d2708 |

    In code:
    ```python
    HashUUID5(
        source_columns=["id", "string"], target_column="uuid5"
    ).transform(input_df)
    ```

    In this example, the `id` and `string` columns are concatenated and hashed using the UUID5 algorithm. The result is
    stored in the `uuid5` column.
    """

    target_column: str = Field(
        default=..., description="The generated UUID will be written to the column name specified here"
    )
    source_columns: ListOfColumns = Field(
        default=...,
        description="List of columns that should be hashed. Should contain the name of at least 1 column. A list of "
        "columns or a single column can be specified. For example: `['column1', 'column2']` or `'column1'`",
    )
    delimiter: str = Field(default="|", description="Separator for the string that will eventually be hashed")
    namespace: Optional[Union[str, uuid.UUID]] = Field(default="", description="Namespace DNS")
    extra_string: Optional[str] = Field(
        default="",
        description="In case of collisions, one can pass an extra string to hash on.",
    )

    # setting a shorter description to avoid excessive logging output
    description: str = "Generate a UUID with the UUID5 algorithm"

    @field_validator("source_columns")
    def _set_columns(cls, columns: ListOfColumns) -> ListOfColumns:
        """Ensures every column is wrapped in backticks"""
        columns = [f"`{column}`" for column in columns]
        return columns

    def execute(self) -> None:
        ns = f.lit(uuid5_namespace(self.namespace).bytes)
        self.log.info(f"UUID5 namespace '{ns}' derived from '{self.namespace}'")
        cols_to_hash = f.concat_ws(self.delimiter, *self.source_columns)
        cols_to_hash = f.concat(f.lit(self.extra_string), cols_to_hash)
        cols_to_hash = f.encode(cols_to_hash, "utf-8")
        cols_to_hash = f.concat(ns, cols_to_hash)
        source_columns_sha1 = f.sha1(cols_to_hash)
        variant_part = f.substring(source_columns_sha1, 17, 4)
        variant_part = f.conv(variant_part, 16, 2)
        variant_part = f.lpad(variant_part, 16, "0")
        variant_part = f.overlay(variant_part, f.lit("10"), 1, 2)  # RFC 4122 variant.
        variant_part = f.lower(f.conv(variant_part, 2, 16))
        target_col_uuid = f.concat_ws(
            "-",
            f.substring(source_columns_sha1, 1, 8),
            f.substring(source_columns_sha1, 9, 4),
            f.concat(f.lit("5"), f.substring(source_columns_sha1, 14, 3)),  # Set version.
            variant_part,
            f.substring(source_columns_sha1, 21, 12),
        )
        # Applying the transformation to the input df, storing the result in the column specified in `target_column`.
        self.output.df = self.df.withColumn(self.target_column, target_col_uuid)
