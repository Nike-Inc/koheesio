from typing import List

def scale_data(data: List[float], multiplier: float) -> List[float]:
    
    if not isinstance(multiplier, (int, float)):
        raise ValueError("Multiplier must be a numeric value.")
    if not all(isinstance(num, (int, float)) for num in data):
        raise ValueError("All elements in data must be numeric.")

    return [num * multiplier for num in data]
