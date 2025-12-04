import pandas as pd
from scipy.stats import weibull_min

def weibull(data: pd.DataFrame) -> dict:
    wind = data['WIND_SPEED']
    shape, _, scale = weibull_min.fit(wind)

    return {
        'scale': float(scale),
        'shape': float(shape)
    }