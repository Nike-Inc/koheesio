from validate import validate_data

# Additional test cases
def main():
    # Test case 1: Valid data
    valid_data = {
        "id": 1,
        "name": "John Doe",
        "age": 30,
        "is_active": True
    }
    try:
        print("\nTesting with valid data...")
        result = validate_data(valid_data)
        print("Result:", result)
    except Exception as e:
        print("Error with valid data test:", e)

    # Test case 2: Invalid data (wrong types)
    invalid_data = {
        "id": "wrong_type",
        "name": "John Doe",
        "age": 30,
        "is_active": "not_boolean"
    }
    try:
        print("\nTesting with invalid data (wrong types)...")
        result = validate_data(invalid_data)
    except Exception as e:
        print("Error with invalid data test:", e)

    # Test case 3: Missing required fields
    missing_fields = {
        "name": "Jane Doe",
        "age": 25
    }
    try:
        print("\nTesting with missing required fields...")
        result = validate_data(missing_fields)
    except Exception as e:
        print("Error with missing fields test:", e)

    # Test case 4: Extra unexpected fields
    extra_fields = {
        "id": 2,
        "name": "Jane Doe",
        "age": 28,
        "is_active": True,
        "extra_field": "unexpected"
    }
    try:
        print("\nTesting with extra unexpected fields...")
        result = validate_data(extra_fields)
        print("Result:", result)
    except Exception as e:
        print("Error with extra fields test:", e)

    # Test case 5: Boundary values
    boundary_values = {
        "id": 3,
        "name": "Alice",
        "age": 0,
        "is_active": False
    }
    try:
        print("\nTesting with boundary values...")
        result = validate_data(boundary_values)
        print("Result:", result)
    except Exception as e:
        print("Error with boundary values test:", e)

    # Test case 6: Nested invalid data
    invalid_nested_data = {
        "id": "invalid_id",  # should be an integer
        "name": 12345,      # should be a string
        "age": -5,          # might later add validation for non-negative
        "is_active": "yes"  # should be a boolean
    }
    try:
        print("\nTesting with entirely invalid data...")
        result = validate_data(invalid_nested_data)
    except Exception as e:
        print("Error with entirely invalid data test:", e)


if __name__ == "__main__":
    main()
