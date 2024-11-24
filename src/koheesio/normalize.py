from pydantic import BaseModel

class NormalizeConfig(BaseModel):
    min_value: float
    max_value: float

def normalize(data, config: NormalizeConfig):

    # Calculate the minimum and maximum values of the input data
    min_data = min(data)
    max_data = max(data)

    # Calculate the range of the input data
    range_data = max_data - min_data

    # Calculate the range of the target normalization
    range_config = config.max_value - config.min_value

    # Normalize each value in the data
    normalized_data = [
        ((value - min_data) / range_data) * range_config + config.min_value
        for value in data
    ]

    return normalized_data