from typing import List

def min_max_normalize(data: List[float], new_min: float, new_max: float) -> List[float]:
    """Normalize data to a specific range [new_min, new_max] using Min-Max scaling."""
    if not data:
        raise ValueError("Data list cannot be empty.")
    min_data = min(data)
    max_data = max(data)
    if min_data == max_data:
        return [new_min for _ in data]  # All values are the same; map to new_min
    return [(new_min + (x - min_data) * (new_max - new_min) / (max_data - min_data)) for x in data]
