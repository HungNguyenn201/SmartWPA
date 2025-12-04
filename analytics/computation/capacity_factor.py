import pandas as pd

def capacity_factor(binned_normalized: pd.DataFrame, constants: dict) -> dict:
    obj = {}

    A = constants['Swept_area']

    groups = binned_normalized.groupby('bin', observed=True)
    
    for _, grps in groups:
        obj[grps.iloc[0]['bin']] = grps['ACTIVE_POWER'].mean() / (0.6125 * A * grps['WIND_SPEED'].mean())

    return obj