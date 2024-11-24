from typing import List
from pydantic import BaseModel

class ClipConfig(BaseModel):
    min_value: float
    max_value: float

def clip_data(data: List[float], config: ClipConfig) -> List[float]:
    return [max(config.min_value, min(x, config.max_value)) for x in data]
