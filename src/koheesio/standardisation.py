from typing import List
from pydantic import BaseModel
import statistics

class StandardizeConfig(BaseModel):
    mean: float = None  
    std_dev: float = None  

def standardize(data: List[float], config: StandardizeConfig) -> List[float]:
    mean = config.mean if config.mean is not None else statistics.mean(data)
    std_dev = config.std_dev if config.std_dev is not None else statistics.stdev(data)
    if std_dev == 0:
        raise ValueError("Standard deviation cannot be zero.")
    return [(x - mean) / std_dev for x in data]
