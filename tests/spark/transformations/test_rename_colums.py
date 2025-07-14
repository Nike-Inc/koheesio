import json

import pytest

from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType


from koheesio.spark.transformations.rename_columns import RenameColumns
from koheesio.spark.transformations.camel_to_snake import camel_to_snake


pytestmark = pytest.mark.spark




class TestRenameColumns:
    def __process_keys(self, data):
        if isinstance(data, dict):
            return {camel_to_snake(k): self.__process_keys(v) for k, v in data.items()}
        if isinstance(data, list):
            return [self.__process_keys(v) for v in data]
        return data

    def __compare_df_json(self, old_df: DataFrame, new_df: DataFrame) -> bool:
        old_json = old_df.toJSON().collect()
        old_json_snake_case = [self.__process_keys(json.loads(record)) for record in old_json]
        new_json = new_df.toJSON().collect()
        new_json_snake_case = [json.loads(record) for record in new_json]

        return old_json_snake_case == new_json_snake_case

    def test_rename_columns_camel_to_snake(self):
        assert camel_to_snake("camelCase") == "camel_case"
        assert camel_to_snake("CamelCase") == "camel_case"
        assert camel_to_snake("camelCaseExample") == "camel_case_example"
        assert camel_to_snake("camel2Case") == "camel2_case"

    def test_rename_schema(self):
        schema = StructType(
            [
                StructField("camelCaseField", StringType(), True),
                StructField("nestedField", StructType([StructField("innerCamelCaseField", IntegerType(), True)]), True),
                StructField(
                    "arrayField", ArrayType(StructType([StructField("arrayCamelCaseField", StringType(), True)])), True
                ),
            ]
        )

        rename_columns = RenameColumns(rename_func=camel_to_snake)
        new_schema = rename_columns.rename_schema(schema)

        expected_schema = StructType(
            [
                StructField("camel_case_field", StringType(), True),
                StructField(
                    "nested_field", StructType([StructField("inner_camel_case_field", IntegerType(), True)]), True
                ),
                StructField(
                    "array_field",
                    ArrayType(StructType([StructField("array_camel_case_field", StringType(), True)])),
                    True,
                ),
            ]
        )

        assert new_schema == expected_schema

    def test_execute(self, spark):
        data = [("value1", 1), ("value2", 2)]
        schema = StructType(
            [
                StructField("camelCaseField", StringType(), True),
                StructField("anotherCamelCaseField", IntegerType(), True),
            ]
        )
        df = spark.createDataFrame(data, schema)
        rename_columns = RenameColumns(rename_func=camel_to_snake)

        rename_columns.df = df
        rename_columns.execute()

        expected_schema = StructType(
            [
                StructField("camel_case_field", StringType(), True),
                StructField("another_camel_case_field", IntegerType(), True),
            ]
        )
        assert rename_columns.output.df.schema == expected_schema
        assert self.__compare_df_json(old_df=df, new_df=rename_columns.output.df)  # type: ignore

    def test_execute_with_complex_schema(self, spark):
        data = [
            ("value1", {"innerCamelCaseField": 1}, [{"arrayCamelCaseField": "arrayValue1"}]),
            ("value2", {"innerCamelCaseField": 2}, [{"arrayCamelCaseField": "arrayValue2"}]),
        ]
        schema = StructType(
            [
                StructField("camelCaseField", StringType(), True),
                StructField("nestedField", StructType([StructField("innerCamelCaseField", IntegerType(), True)]), True),
                StructField(
                    "arrayField", ArrayType(StructType([StructField("arrayCamelCaseField", StringType(), True)])), True
                ),
            ]
        )
        df = spark.createDataFrame(data, schema)
        rename_columns = RenameColumns(rename_func=camel_to_snake)

        rename_columns.df = df
        rename_columns.execute()

        expected_schema = StructType(
            [
                StructField("camel_case_field", StringType(), True),
                StructField(
                    "nested_field", StructType([StructField("inner_camel_case_field", IntegerType(), True)]), True
                ),
                StructField(
                    "array_field",
                    ArrayType(StructType([StructField("array_camel_case_field", StringType(), True)])),
                    True,
                ),
            ]
        )
        assert rename_columns.output.df.schema == expected_schema
        assert self.__compare_df_json(old_df=df, new_df=rename_columns.output.df)  # type: ignore

    def test_execute_with_even_more_nested_schema(self, spark):
        data = [
            (
                "value1",
                {"innerCamelCaseField": 1, "innerArrayField": [{"innerArrayCamelCaseField": "arrayValue1"}]},
                [
                    {
                        "arrayStructField": {
                            "deepInnerCamelCaseField": "deepValue1",
                            "deepInnerArrayField": [{"deepArrayCamelCaseField": "deepArrayValue1"}],
                        }
                    }
                ],
            ),
            (
                "value2",
                {"innerCamelCaseField": 2, "innerArrayField": [{"innerArrayCamelCaseField": "arrayValue2"}]},
                [
                    {
                        "arrayStructField": {
                            "deepInnerCamelCaseField": "deepValue2",
                            "deepInnerArrayField": [{"deepArrayCamelCaseField": "deepArrayValue2"}],
                        }
                    }
                ],
            ),
        ]
        schema = StructType(
            [
                StructField("camelCaseField", StringType(), True),
                StructField(
                    "nestedField",
                    StructType(
                        [
                            StructField("innerCamelCaseField", IntegerType(), True),
                            StructField(
                                "innerArrayField",
                                ArrayType(StructType([StructField("innerArrayCamelCaseField", StringType(), True)])),
                                True,
                            ),
                        ]
                    ),
                    True,
                ),
                StructField(
                    "arrayField",
                    ArrayType(
                        StructType(
                            [
                                StructField(
                                    "arrayStructField",
                                    StructType(
                                        [
                                            StructField("deepInnerCamelCaseField", StringType(), True),
                                            StructField(
                                                "deepInnerArrayField",
                                                ArrayType(
                                                    StructType(
                                                        [StructField("deepArrayCamelCaseField", StringType(), True)]
                                                    )
                                                ),
                                                True,
                                            ),
                                        ]
                                    ),
                                    True,
                                ),
                            ]
                        )
                    ),
                    True,
                ),
            ]
        )
        df = spark.createDataFrame(data, schema)
        rename_columns = RenameColumns(rename_func=camel_to_snake)

        rename_columns.df = df
        rename_columns.execute()

        expected_schema = StructType(
            [
                StructField("camel_case_field", StringType(), True),
                StructField(
                    "nested_field",
                    StructType(
                        [
                            StructField("inner_camel_case_field", IntegerType(), True),
                            StructField(
                                "inner_array_field",
                                ArrayType(
                                    StructType([StructField("inner_array_camel_case_field", StringType(), True)])
                                ),
                                True,
                            ),
                        ]
                    ),
                    True,
                ),
                StructField(
                    "array_field",
                    ArrayType(
                        StructType(
                            [
                                StructField(
                                    "array_struct_field",
                                    StructType(
                                        [
                                            StructField("deep_inner_camel_case_field", StringType(), True),
                                            StructField(
                                                "deep_inner_array_field",
                                                ArrayType(
                                                    StructType(
                                                        [StructField("deep_array_camel_case_field", StringType(), True)]
                                                    )
                                                ),
                                                True,
                                            ),
                                        ]
                                    ),
                                    True,
                                ),
                            ]
                        )
                    ),
                    True,
                ),
            ]
        )
        assert rename_columns.output.df.schema == expected_schema
        assert self.__compare_df_json(old_df=df, new_df=rename_columns.output.df)  # type: ignore

    def test_execute_with_deeply_nested_schema(self, spark):
        data = [
            (
                "value1",
                {"innerCamelCaseField": 1, "innerArrayField": [{"innerArrayCamelCaseField": "arrayValue1"}]},
                [
                    {
                        "arrayStructField": {
                            "deepInnerCamelCaseField": "deepValue1",
                            "deepInnerArrayField": [
                                {
                                    "deeperArrayStructField": {
                                        "deeperInnerCamelCaseField": "deeperValue1",
                                        "deeperInnerArrayField": [
                                            {
                                                "deepestArrayStructField": {
                                                    "deepestInnerCamelCaseField": "deepestValue1",
                                                    "deepestInnerArrayField": [
                                                        {"deepestArrayCamelCaseField": "deepestArrayValue1"}
                                                    ],
                                                }
                                            }
                                        ],
                                    }
                                }
                            ],
                        }
                    }
                ],
            ),
            (
                "value2",
                {"innerCamelCaseField": 2, "innerArrayField": [{"innerArrayCamelCaseField": "arrayValue2"}]},
                [
                    {
                        "arrayStructField": {
                            "deepInnerCamelCaseField": "deepValue2",
                            "deepInnerArrayField": [
                                {
                                    "deeperArrayStructField": {
                                        "deeperInnerCamelCaseField": "deeperValue2",
                                        "deeperInnerArrayField": [
                                            {
                                                "deepestArrayStructField": {
                                                    "deepestInnerCamelCaseField": "deepestValue2",
                                                    "deepestInnerArrayField": [
                                                        {"deepestArrayCamelCaseField": "deepestArrayValue2"}
                                                    ],
                                                }
                                            }
                                        ],
                                    }
                                }
                            ],
                        }
                    }
                ],
            ),
        ]
        schema = StructType(
            [
                StructField("camelCaseField", StringType(), True),
                StructField(
                    "nestedField",
                    StructType(
                        [
                            StructField("innerCamelCaseField", IntegerType(), True),
                            StructField(
                                "innerArrayField",
                                ArrayType(StructType([StructField("innerArrayCamelCaseField", StringType(), True)])),
                                True,
                            ),
                        ]
                    ),
                    True,
                ),
                StructField(
                    "arrayField",
                    ArrayType(
                        StructType(
                            [
                                StructField(
                                    "arrayStructField",
                                    StructType(
                                        [
                                            StructField("deepInnerCamelCaseField", StringType(), True),
                                            StructField(
                                                "deepInnerArrayField",
                                                ArrayType(
                                                    StructType(
                                                        [
                                                            StructField(
                                                                "deeperArrayStructField",
                                                                StructType(
                                                                    [
                                                                        StructField(
                                                                            "deeperInnerCamelCaseField",
                                                                            StringType(),
                                                                            True,
                                                                        ),
                                                                        StructField(
                                                                            "deeperInnerArrayField",
                                                                            ArrayType(
                                                                                StructType(
                                                                                    [
                                                                                        StructField(
                                                                                            "deepestArrayStructField",
                                                                                            StructType(
                                                                                                [
                                                                                                    StructField(
                                                                                                        "deepestInnerCamelCaseField",
                                                                                                        StringType(),
                                                                                                        True,
                                                                                                    ),
                                                                                                    StructField(
                                                                                                        "deepestInnerArrayField",
                                                                                                        ArrayType(
                                                                                                            StructType(
                                                                                                                [
                                                                                                                    StructField(
                                                                                                                        "deepestArrayCamelCaseField",
                                                                                                                        StringType(),
                                                                                                                        True,
                                                                                                                    )
                                                                                                                ]
                                                                                                            )
                                                                                                        ),
                                                                                                        True,
                                                                                                    ),
                                                                                                ]
                                                                                            ),
                                                                                            True,
                                                                                        )
                                                                                    ]
                                                                                )
                                                                            ),
                                                                            True,
                                                                        ),
                                                                    ]
                                                                ),
                                                                True,
                                                            )
                                                        ]
                                                    )
                                                ),
                                                True,
                                            ),
                                        ]
                                    ),
                                    True,
                                ),
                            ]
                        )
                    ),
                    True,
                ),
            ]
        )
        df = spark.createDataFrame(data, schema)
        rename_columns = RenameColumns(rename_func=camel_to_snake)

        rename_columns.df = df
        rename_columns.execute()

        expected_schema = StructType(
            [
                StructField("camel_case_field", StringType(), True),
                StructField(
                    "nested_field",
                    StructType(
                        [
                            StructField("inner_camel_case_field", IntegerType(), True),
                            StructField(
                                "inner_array_field",
                                ArrayType(
                                    StructType([StructField("inner_array_camel_case_field", StringType(), True)])
                                ),
                                True,
                            ),
                        ]
                    ),
                    True,
                ),
                StructField(
                    "array_field",
                    ArrayType(
                        StructType(
                            [
                                StructField(
                                    "array_struct_field",
                                    StructType(
                                        [
                                            StructField("deep_inner_camel_case_field", StringType(), True),
                                            StructField(
                                                "deep_inner_array_field",
                                                ArrayType(
                                                    StructType(
                                                        [
                                                            StructField(
                                                                "deeper_array_struct_field",
                                                                StructType(
                                                                    [
                                                                        StructField(
                                                                            "deeper_inner_camel_case_field",
                                                                            StringType(),
                                                                            True,
                                                                        ),
                                                                        StructField(
                                                                            "deeper_inner_array_field",
                                                                            ArrayType(
                                                                                StructType(
                                                                                    [
                                                                                        StructField(
                                                                                            "deepest_array_struct_field",
                                                                                            StructType(
                                                                                                [
                                                                                                    StructField(
                                                                                                        "deepest_inner_camel_case_field",
                                                                                                        StringType(),
                                                                                                        True,
                                                                                                    ),
                                                                                                    StructField(
                                                                                                        "deepest_inner_array_field",
                                                                                                        ArrayType(
                                                                                                            StructType(
                                                                                                                [
                                                                                                                    StructField(
                                                                                                                        "deepest_array_camel_case_field",
                                                                                                                        StringType(),
                                                                                                                        True,
                                                                                                                    )
                                                                                                                ]
                                                                                                            )
                                                                                                        ),
                                                                                                        True,
                                                                                                    ),
                                                                                                ]
                                                                                            ),
                                                                                            True,
                                                                                        )
                                                                                    ]
                                                                                )
                                                                            ),
                                                                            True,
                                                                        ),
                                                                    ]
                                                                ),
                                                                True,
                                                            )
                                                        ]
                                                    )
                                                ),
                                                True,
                                            ),
                                        ]
                                    ),
                                    True,
                                ),
                            ]
                        )
                    ),
                    True,
                ),
            ]
        )
        assert rename_columns.output.df.schema == expected_schema
        assert self.__compare_df_json(old_df=df, new_df=rename_columns.output.df)  # type: ignore
