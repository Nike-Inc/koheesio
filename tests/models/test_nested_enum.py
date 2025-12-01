"""Tests for the NestedEnum utility."""

import pytest
from enum import Enum

from koheesio.models import Field, NestedEnumMeta, nested_enum
from koheesio.models import BaseModel


# Test fixtures using different approaches


@nested_enum
class SampleMode:
    """Test enum container using decorator syntax."""

    class BATCH(str, Enum):
        APPEND = "append"
        OVERWRITE = "overwrite"

    class STREAMING(str, Enum):
        APPEND = "append"
        COMPLETE = "complete"


class SampleModeWithMeta(metaclass=NestedEnumMeta):
    """Test enum container using metaclass syntax."""

    class BATCH(str, Enum):
        APPEND = "append"
        OVERWRITE = "overwrite"

    class STREAMING(str, Enum):
        APPEND = "append"
        COMPLETE = "complete"


class TestNestedEnum:
    """Test suite for NestedEnum functionality."""

    def test_decorator_syntax(self):
        """Test that the @nested_enum decorator works correctly."""
        assert SampleMode.BATCH.APPEND == "append"
        assert SampleMode.STREAMING.COMPLETE == "complete"

    def test_metaclass_syntax(self):
        """Test that the metaclass syntax works correctly."""
        assert SampleModeWithMeta.BATCH.APPEND == "append"
        assert SampleModeWithMeta.STREAMING.COMPLETE == "complete"

    def test_case_insensitive_uppercase(self):
        """Test case-insensitive access with uppercase."""
        assert SampleMode.BATCH.APPEND == "append"
        assert SampleMode.STREAMING.COMPLETE == "complete"

    def test_case_insensitive_lowercase(self):
        """Test case-insensitive access with lowercase."""
        assert SampleMode.batch.APPEND == "append"
        assert SampleMode.streaming.COMPLETE == "complete"

    def test_case_insensitive_mixed_case(self):
        """Test case-insensitive access with mixed case."""
        assert SampleMode.Batch.APPEND == "append"
        assert SampleMode.Streaming.COMPLETE == "complete"
        assert SampleMode.BaTcH.OVERWRITE == "overwrite"
        assert SampleMode.sTrEaMiNg.COMPLETE == "complete"

    def test_enum_value_equality(self):
        """Test that enum values are equal regardless of access case."""
        assert SampleMode.BATCH.APPEND == SampleMode.batch.APPEND
        assert SampleMode.BATCH.APPEND == SampleMode.Batch.APPEND
        assert SampleMode.STREAMING.COMPLETE == SampleMode.streaming.COMPLETE
        assert SampleMode.STREAMING.COMPLETE == SampleMode.Streaming.COMPLETE

    def test_enum_identity(self):
        """Test that enum classes are identical regardless of access case."""
        assert SampleMode.BATCH is SampleMode.batch
        assert SampleMode.BATCH is SampleMode.Batch
        assert SampleMode.STREAMING is SampleMode.streaming
        assert SampleMode.STREAMING is SampleMode.Streaming

    def test_invalid_attribute_error(self):
        """Test that accessing non-existent attributes raises helpful AttributeError."""
        with pytest.raises(AttributeError) as exc_info:
            _ = SampleMode.INVALID

        error_message = str(exc_info.value)
        assert "SampleMode" in error_message
        assert "INVALID" in error_message

    def test_enum_membership(self):
        """Test that enum values work with membership checks."""
        assert "append" in [mode.value for mode in SampleMode.BATCH]
        assert "overwrite" in [mode.value for mode in SampleMode.BATCH]
        assert "complete" in [mode.value for mode in SampleMode.STREAMING]

    def test_enum_iteration(self):
        """Test that enums can be iterated."""
        batch_values = [mode.value for mode in SampleMode.BATCH]
        assert "append" in batch_values
        assert "overwrite" in batch_values

        streaming_values = [mode.value for mode in SampleMode.STREAMING]
        assert "append" in streaming_values
        assert "complete" in streaming_values

    def test_string_comparison(self):
        """Test that enum values work with string comparison."""
        assert SampleMode.BATCH.APPEND == "append"
        assert SampleMode.batch.OVERWRITE == "overwrite"
        assert SampleMode.STREAMING.COMPLETE == "complete"
        assert SampleMode.streaming.APPEND == "append"

    def test_type_checking(self):
        """Test that type checking works correctly."""
        assert isinstance(SampleMode.BATCH.APPEND, str)
        assert isinstance(SampleMode.batch.OVERWRITE, str)
        assert isinstance(SampleMode.STREAMING.COMPLETE, str)

    def test_enum_name_access(self):
        """Test accessing enum names."""
        assert SampleMode.BATCH.APPEND.name == "APPEND"
        assert SampleMode.batch.OVERWRITE.name == "OVERWRITE"
        assert SampleMode.STREAMING.COMPLETE.name == "COMPLETE"


class TestNestedEnumWithPydantic:
    """Test NestedEnum integration with Pydantic models."""

    def test_pydantic_field_with_nested_enum(self):
        """Test using nested enum in Pydantic Field."""

        class MyModel(BaseModel):
            mode: SampleMode.BATCH = Field(
                default=SampleMode.BATCH.APPEND, description="The batch mode"
            )

        model = MyModel()
        assert model.mode == SampleMode.BATCH.APPEND
        assert model.mode == "append"

    def test_pydantic_field_assignment_case_insensitive(self):
        """Test that Pydantic fields work with case-insensitive enum access."""

        class MyModel(BaseModel):
            mode: SampleMode.BATCH = Field(
                default=SampleMode.batch.APPEND,  # lowercase access
                description="The batch mode",
            )

        model = MyModel()
        assert model.mode == SampleMode.BATCH.APPEND
        assert model.mode == SampleMode.batch.APPEND

    def test_pydantic_validation_with_enum_values(self):
        """Test Pydantic validation with enum values."""

        class MyModel(BaseModel):
            mode: str = Field(default=SampleMode.BATCH.APPEND, description="The mode")

        # Should accept enum value
        model = MyModel(mode=SampleMode.batch.OVERWRITE)
        assert model.mode == "overwrite"

        # Should also accept string directly
        model = MyModel(mode="append")
        assert model.mode == "append"


class TestOutputModeRefactoring:
    """Test the refactored OutputMode from spark.writers."""

    def test_output_mode_import(self):
        """Test that OutputMode can be imported."""
        from koheesio.spark.writers import OutputMode

        assert OutputMode is not None

    def test_output_mode_batch_access(self):
        """Test accessing batch modes."""
        from koheesio.spark.writers import OutputMode

        assert OutputMode.BATCH.APPEND == "append"
        assert OutputMode.batch.APPEND == "append"
        assert OutputMode.Batch.OVERWRITE == "overwrite"

    def test_output_mode_streaming_access(self):
        """Test accessing streaming modes."""
        from koheesio.spark.writers import OutputMode

        assert OutputMode.STREAMING.APPEND == "append"
        assert OutputMode.streaming.COMPLETE == "complete"
        assert OutputMode.Streaming.UPDATE == "update"

    def test_backward_compatibility_batch_output_mode(self):
        """Test that BatchOutputMode alias still works."""
        from koheesio.spark.writers import BatchOutputMode, OutputMode

        # Backward compatibility alias should work
        assert BatchOutputMode.APPEND == OutputMode.BATCH.APPEND
        assert BatchOutputMode.OVERWRITE == OutputMode.BATCH.OVERWRITE
        assert BatchOutputMode is OutputMode.BATCH

    def test_backward_compatibility_streaming_output_mode(self):
        """Test that StreamingOutputMode alias still works."""
        from koheesio.spark.writers import StreamingOutputMode, OutputMode

        # Backward compatibility alias should work
        assert StreamingOutputMode.APPEND == OutputMode.STREAMING.APPEND
        assert StreamingOutputMode.COMPLETE == OutputMode.STREAMING.COMPLETE
        assert StreamingOutputMode is OutputMode.STREAMING

    def test_all_batch_modes_preserved(self):
        """Test that all batch mode values are preserved."""
        from koheesio.spark.writers import OutputMode

        # Check all modes exist
        assert hasattr(OutputMode.BATCH, "APPEND")
        assert hasattr(OutputMode.BATCH, "OVERWRITE")
        assert hasattr(OutputMode.BATCH, "IGNORE")
        assert hasattr(OutputMode.BATCH, "ERROR")
        assert hasattr(OutputMode.BATCH, "ERRORIFEXISTS")
        assert hasattr(OutputMode.BATCH, "MERGE")
        assert hasattr(OutputMode.BATCH, "MERGE_ALL")
        assert hasattr(OutputMode.BATCH, "MERGEALL")

        # Check values
        assert OutputMode.BATCH.APPEND.value == "append"
        assert OutputMode.BATCH.OVERWRITE.value == "overwrite"
        assert OutputMode.BATCH.IGNORE.value == "ignore"
        assert OutputMode.BATCH.ERROR.value == "error"
        assert OutputMode.BATCH.ERRORIFEXISTS.value == "error"
        assert OutputMode.BATCH.MERGE.value == "merge"
        assert OutputMode.BATCH.MERGE_ALL.value == "merge_all"
        assert OutputMode.BATCH.MERGEALL.value == "merge_all"

    def test_all_streaming_modes_preserved(self):
        """Test that all streaming mode values are preserved."""
        from koheesio.spark.writers import OutputMode

        # Check all modes exist
        assert hasattr(OutputMode.STREAMING, "APPEND")
        assert hasattr(OutputMode.STREAMING, "COMPLETE")
        assert hasattr(OutputMode.STREAMING, "UPDATE")

        # Check values
        assert OutputMode.STREAMING.APPEND.value == "append"
        assert OutputMode.STREAMING.COMPLETE.value == "complete"
        assert OutputMode.STREAMING.UPDATE.value == "update"

    def test_aliases_work_correctly(self):
        """Test that enum aliases work correctly."""
        from koheesio.spark.writers import OutputMode

        # ERRORIFEXISTS should equal ERROR
        assert OutputMode.BATCH.ERRORIFEXISTS == OutputMode.BATCH.ERROR
        assert OutputMode.BATCH.ERRORIFEXISTS.value == "error"

        # MERGEALL should equal MERGE_ALL
        assert OutputMode.BATCH.MERGEALL == OutputMode.BATCH.MERGE_ALL
        assert OutputMode.BATCH.MERGEALL.value == "merge_all"

    def test_type_alias(self):
        """Test that the OutputModeType type alias exists."""
        from koheesio.spark.writers import OutputModeType

        # Just verify it's importable and is a type
        assert OutputModeType is not None
