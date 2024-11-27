import logging
from validate import validate_data

logging.basicConfig(level=logging.INFO)

def main():
    # Test case 1: Valid data
    logging.info("Testing with valid data...")
    valid_data = {"id": 1, "name": "John Doe", "age": 30, "is_active": True}
    try:
        result = validate_data(valid_data)
        logging.info(f"Result: {result}")
    except Exception as e:
        logging.error(f"Error with valid data test: {e}")

    # Test case 2: Invalid data - wrong types
    logging.info("\nTesting with invalid data (wrong types)...")
    invalid_data_types = {"id": "wrong_type", "name": "John Doe", "age": 30, "is_active": "not_boolean"}
    try:
        result = validate_data(invalid_data_types)
        logging.info(f"Result: {result}")
    except Exception as e:
        logging.error(f"Error with invalid data test: {e}")

    # Test case 3: Missing required fields
    logging.info("\nTesting with invalid data (missing fields)...")
    missing_fields = {"name": "John Doe", "age": 30}
    try:
        result = validate_data(missing_fields)
        logging.info(f"Result: {result}")
    except Exception as e:
        logging.error(f"Error with missing fields test: {e}")

    # Test case 4: Extra fields
    logging.info("\nTesting with extra fields...")
    extra_fields = {"id": 2, "name": "Jane Doe", "age": 25, "is_active": True, "extra_field": "unexpected"}
    try:
        result = validate_data(extra_fields)
        logging.info(f"Result: {result}")
    except Exception as e:
        logging.error(f"Error with extra fields test: {e}")

    # Test case 5: Boundary values (e.g., age = 0)
    logging.info("\nTesting with boundary values...")
    boundary_values = {"id": 3, "name": "Alice", "age": 0, "is_active": False}
    try:
        result = validate_data(boundary_values)
        logging.info(f"Result: {result}")
    except Exception as e:
        logging.error(f"Error with boundary values test: {e}")

if __name__ == "__main__":
    main()
