import logging
from pydantic import BaseModel, ValidationError

# Configure logging
logging.basicConfig(level=logging.INFO)

class DataSchema(BaseModel):
    id: int
    name: str
    age: int
    is_active: bool

def validate_data(data):
    """
    Validates the input data against the defined schema.

    Args:
        data (dict): Input data to validate. Expected keys: id (int), name (str), age (int), is_active (bool).

    Returns:
        DataSchema: Validated data as a DataSchema object.

    Raises:
        ValidationError: If the input data does not match the schema.

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

