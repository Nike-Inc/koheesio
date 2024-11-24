from typing import List
import statistics

def z_score_normalize(data: List[float]) -> List[float]:
    if not data:
        raise ValueError("Data list cannot be empty.")
    mean = statistics.mean(data)
    std_dev = statistics.stdev(data)
    if std_dev == 0:
        return [0.0 for _ in data]  
    return [(x - mean) / std_dev for x in data]
