from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType

from tests.spark._testing import assertSchemaEqual

from koheesio.spark.transformations.renames import RenameColumns


class TestRenameColumns:
    def test_rename_schema(self, spark):
        schema = StructType(
            [
                StructField("camelCaseField", StringType(), True),
                StructField("nestedField", StructType([StructField("innerCamelCaseField", IntegerType(), True)]), True),
                StructField(
                    "arrayField", ArrayType(StructType([StructField("arrayCamelCaseField", StringType(), True)])), True
                ),
            ]
        )
        rename_columns = RenameColumns()
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

        assertSchemaEqual(new_schema, expected_schema)

    def test_execute(self, spark):
        data = [("value1", 1), ("value2", 2)]
        schema = StructType(
            [
                StructField("camelCaseField", StringType(), True),
                StructField("anotherCamelCaseField", IntegerType(), True),
            ]
        )
        df = spark.createDataFrame(data, schema)
        rename_columns = RenameColumns()

        rename_columns.df = df
        rename_columns.execute()

        expected_schema = StructType(
            [
                StructField("camel_case_field", StringType(), True),
                StructField("another_camel_case_field", IntegerType(), True),
            ]
        )
        assertSchemaEqual(rename_columns.output.df.schema, expected_schema)

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
        rename_columns = RenameColumns()

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
        assertSchemaEqual(rename_columns.output.df.schema, expected_schema)

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
        rename_columns = RenameColumns()

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
        assertSchemaEqual(rename_columns.output.df.schema, expected_schema)

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
        rename_columns = RenameColumns()

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
        assertSchemaEqual(rename_columns.output.df.schema, expected_schema)
