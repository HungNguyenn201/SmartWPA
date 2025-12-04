import pandas as pd
from ._header import AIR_PRESSURE, HUMIDITY, TEMPERATURE, AIR_DENSITY

def calculate_air_density(temp: pd.Series | float, pressure: pd.Series | float, humidity: pd.Series | float) -> pd.Series | float:
    R_air = 287.05
    R_vapor = 461.5

    return 1 / temp * (pressure / R_air - humidity * 0.0631846 * temp * (1 / R_air - 1 / R_vapor))

def air_density(data: pd.DataFrame) -> pd.Series[float]:
    if 'HUMIDITY' in data.columns and 'TEMPERATURE' in data.columns and 'PRESSURE' in data.columns:
        return calculate_air_density(data['TEMPERATURE'], data['PRESSURE'], data['HUMIDITY'])
    else:
        if AIR_PRESSURE != None and HUMIDITY != None and TEMPERATURE != None:
            return pd.Series(calculate_air_density(TEMPERATURE, AIR_PRESSURE, HUMIDITY), index=data.index)
        else:
            return pd.Series(AIR_DENSITY, index=data.index)