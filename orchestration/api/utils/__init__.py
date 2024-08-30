import random
from typing import Any, List

def select_random_values(array: List[Any], random_size: int) -> List[Any]:
    """
    Selects a random subset of unique values from the given array.

    If the length of the array is less than the specified random size, 
    the original array is returned. Otherwise, a list of unique random 
    values of the specified size is returned.

    Parameters:
        array (list): The list from which to select random values.
        random_size (int): The number of unique random values to select.

    Returns:
        list: A list of unique random values if the array length 
            is greater than or equal to random_size; otherwise, 
            the original array.

    Example:
        array = [1, 2, 3, 4, 5]
        random_size = 3
        result = select_random_values(array, random_size)
        print(result)  # Output could be [2, 4, 5] (random)
    """
    if len(array) < random_size:
        return array
    else:
        return random.sample(array, random_size)
