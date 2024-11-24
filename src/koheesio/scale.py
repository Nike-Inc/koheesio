from typing import List

def scale_data(data: List[float], multiplier: float) -> List[float]:
    """
    Scales a list of numbers by the given multiplier.

    Args:
        data (List[float]): A list of numerical data.
        multiplier (float): The multiplier to scale each number.

    Returns:
        List[float]: A list of scaled numbers.
    """
    if not isinstance(multiplier, (int, float)):
        raise ValueError("Multiplier must be a numeric value.")
    if not all(isinstance(num, (int, float)) for num in data):
        raise ValueError("All elements in data must be numeric.")

    return [num * multiplier for num in data]
