from typing import List

import numpy as np


def normalize(an_array: List[float]) -> List[float]:
    return np.array(an_array) / sum(an_array)
