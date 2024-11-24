from typing import List
from pydantic import BaseModel

class ExpTransformConfig(BaseModel):
    base: float  

def exp_transform(data: List[float], config: ExpTransformConfig) -> List[float]:
    import math
    if config.base <= 0:
        raise ValueError("Base must be a positive number.")
    return [config.base ** x for x in data]
