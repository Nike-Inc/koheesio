import pytest


def test_imports():
    try:
        from koheesio import BaseModel, ExtraParamsMixin, Step, StepOutput
    except ImportError as e:
        pytest.fail(f"Import failed: {e}")
