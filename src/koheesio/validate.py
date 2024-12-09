from pydantic import BaseModel, ValidationError

class ValidationConfig(BaseModel):
    required_columns: list
    dtype: dict

def validate_data(data, config: ValidationConfig):
    errors = []

    # Check required columns
    missing_columns = [col for col in config.required_columns if col not in data.columns]
    if missing_columns:
        errors.append(f"Missing columns: {missing_columns}")

    # Check data types
    for column, dtype in config.dtype.items():
        if column in data.columns and data[column].dtype != dtype:
            errors.append(f"Column '{column}' has incorrect dtype: {data[column].dtype}")

    if errors:
        raise ValueError("Data Validation Failed: " + "; ".join(errors))

    return True
