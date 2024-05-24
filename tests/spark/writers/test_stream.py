"""unit tests for Trigger"""

import pytest

from koheesio.spark.writers.stream import Trigger

pytestmark = pytest.mark.spark


@pytest.mark.parametrize(
    "args, expected",
    [
        # key can be written either in lowerCamelCase or snake_case
        ({"processingTime": "5 seconds"}, {"processingTime": "5 seconds"}),
        ({"processingTime": "5 seconds"}, {"processingTime": "5 seconds"}),
        ({"processing_time": "5 hours"}, {"processingTime": "5 hours"}),
        ({"processingTime": "4 minutes"}, {"processingTime": "4 minutes"}),
        # boolean values can be passed as a string or as a boolean (case-insensitive)
        ({"once": True}, {"once": True}),
        ({"once": "true"}, {"once": True}),
        ({"once": "TrUe"}, {"once": True}),
        ({"once": "TRUE"}, {"once": True}),
        # all keys should work
        ({"available_now": "true"}, {"availableNow": True}),
        ({"availableNow": True}, {"availableNow": True}),
        ({"continuous": "3 hours"}, {"continuous": "3 hours"}),
        # from a string
        ("continuous='5 seconds'", {"continuous": "5 seconds"}),
        ("processingTime=4 minutes", {"processingTime": "4 minutes"}),
    ],
)
def test_from_any(args, expected):
    """Test that Trigger.from_any can handle different input types
    Inherently also tests that Trigger.from_string works as expected
    """
    trigger = Trigger.from_any(args)
    actual = trigger.value
    assert actual == expected


@pytest.mark.parametrize(
    "args, expected",
    [
        # valid values, but should fail the validation check of the class
        ({"availableNow": False}, ValueError),
        ({"continuous": True}, ValueError),
        ({"once": False}, ValueError),
        ({"once": "string should fail"}, ValueError),
        # invalid values
        ({"something_random": "false"}, ValueError),
        # multiple keys should trigger a ValueError
        ({"availableNow": True, "continuous": "5 seconds"}, ValueError),
    ],
)
def test_from_any_unhappy(args, expected):
    """Test that Trigger.from_any raises a ValueError when an invalid type is passed"""
    with pytest.raises(expected):
        Trigger.from_any(args)


def test_execute():
    """Test that Trigger.execute returns a valid Spark Trigger"""
    trigger = Trigger(processing_time="5 seconds")
    actual_value = trigger.value
    actual_execute = trigger.execute()
    assert actual_value == {"processingTime": "5 seconds"} == actual_execute.value
