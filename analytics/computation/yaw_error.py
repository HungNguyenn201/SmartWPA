import numpy as np
import pandas as pd
from math import ceil
from typing import Literal

def yaw_errors(data: pd.DataFrame, bin_width: Literal[5, 10]=10) -> dict:
    data = data.dropna(subset=['DIRECTION_NACELLE', 'DIRECTION_WIND']).copy()
    data['delta'] = data['DIRECTION_NACELLE'] - data['DIRECTION_WIND']

    def normalize(angle):
        return angle - (ceil((angle + 180)/360)-1)*360

    normalize_np = np.vectorize(normalize)
    delta = normalize_np(data['delta'])

    obj = {}
    hist = np.histogram(delta, bins=np.arange(-180, 180 + bin_width, bin_width))

    vals = {}
    for i, v in enumerate(hist[0]):
        vals[str(hist[1][i])] = float(v)

    obj['data'] = vals

    obj['statistics'] = {
        'mean_error': np.mean(delta),
        'median_error': np.median(delta),
        'std_error': np.std(delta),
    }

    return obj