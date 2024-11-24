from pydantic import BaseModel

class NormalizeConfig(BaseModel):
    min_value: float
    max_value: float

def normalize(data, config: NormalizeConfig):

    min_data = min(data)
    max_data = max(data)

    range_data = max_data - min_data

    range_config = config.max_value - config.min_value

    normalized_data = [
        ((value - min_data) / range_data) * range_config + config.min_value
        for value in data
    ]

    return normalized_data