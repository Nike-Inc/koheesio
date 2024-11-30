import logging
from pydantic import BaseModel, ValidationError

# Configure logging
logging.basicConfig(level=logging.INFO)

class DataSchema(BaseModel):
    """
    Represents the schema for validating input data.

    Attributes:
        id (int): A unique identifier for the individual.
        name (str): The name of the individual.
        age (int): The age of the individual.
        is_active (bool): Whether the individual is active.
    """
    id: int
    name: str
    age: int
    is_active: bool


def validate_data(data):
    """
    Validates the input data against the defined schema.

    Args:
        data (dict): A dictionary containing the input data. 
            Expected keys:
                - id (int): A unique identifier.
                - name (str): The name of the individual.
                - age (int): The age of the individual.
                - is_active (bool): Active status.

    Returns:
        DataSchema: An object representing the validated data.

    Raises:
        ValidationError: If the input data fails validation.

    Example:
        Input:
        {
            "id": 1,
            "name": "John Doe",
            "age": 30,
            "is_active": True
        }

        Output:
        DataSchema(id=1, name="John Doe", age=30, is_active=True)
    """
    try:
        validated_data = DataSchema(**data)
        logging.info("Validation Successful!")
        return validated_data
    except ValidationError as e:
        logging.error("Validation Failed:")
        logging.error(e.json())
        raise
