import pandas as pd
from func_est import power_est

def fill(data: pd.DataFrame) -> pd.DataFrame:
    data[['WIND_SPEED', 'ACTIVE_POWER']] = data[['WIND_SPEED', 'ACTIVE_POWER']].interpolate(method='time')
    return data


def estimate(data: pd.DataFrame, fill_flag = False) -> pd.DataFrame:
    data = data.copy()
    if (fill_flag == True):
        data = fill(data)
    
    data['ESTIMATED_POWER'] = power_est(data)

    return data