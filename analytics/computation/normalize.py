import numpy as np
import pandas as pd
from timestamp import timestamp_prepare
from sklearn.impute import KNNImputer

allowed_column_names = [
    'TIMESTAMP',
    'WIND_SPEED',
    'ACTIVE_POWER',
    'DIRECTION_NACELLE',
    'DIRECTION_WIND',
    'PITCH_ANGLE',
    'HUMIDITY',
    'PRESSURE',
    'TEMPERATURE'
]

required_column_names = [
    'TIMESTAMP',
    'WIND_SPEED',
    'ACTIVE_POWER'
]

def check_column_names(columns: list[str]):
    for column in columns:
        if column not in allowed_column_names:
            raise ValueError(f"Column {column} not allowed.")
    for required in required_column_names:
        if required not in columns:
            raise ValueError(f"Required column {column} not found.")

def list_len(lists: list[list[any]]):
    first = len(lists[0])
    return all(len(l) == first for l in lists[1:])    

def convert_temp(data: pd.DataFrame) -> pd.DataFrame:
    if (data['TEMPERATURE'].mean() < 223):
        data['TEMPERATURE'] = data['TEMPERATURE'] + 273.15

    return data

def remove_temp_outliers(data: pd.DataFrame) -> pd.DataFrame:
    mask = (data['TEMPERATURE'] < 223) | (data['TEMPERATURE'] > 323)
    if mask.any():
        data.loc[mask, 'TEMPERATURE'] = np.nan
        imputer = KNNImputer(n_neighbors=15)
        data['TEMPERATURE'] = imputer.fit_transform(data[['TEMPERATURE']]).ravel()

    return data

def normalize_humidity(data: pd.DataFrame) -> pd.DataFrame:
    if (data['HUMIDITY'].mean() > 1):
        data['HUMIDITY'] = data['HUMIDITY'] / 100

    return data

def remove_humidity_outliers(data: pd.DataFrame) -> pd.DataFrame:
    mask = (data['HUMIDITY'] < 0) | (data['HUMIDITY'] > 1)
    if mask.any():
        data[mask]['HUMIDITY'] = np.nan
        imputer = KNNImputer(n_neighbors=15)
        data['HUMIDITY'] = imputer.fit_transform(data['HUMIDITY'])

    return data

def remove_pressure_outliers(data: pd.DataFrame) -> pd.DataFrame:
    mask = (data['PRESSURE'] < 50000) | (data['PRESSURE'] > 108500)
    if mask.any():
        data[mask]['PRESSURE'] = np.nan
        imputer = KNNImputer(n_neighbors=15)
        data['PRESSURE'] = imputer.fit_transform(data['PRESSURE'])

    return data

def preprocess(data: pd.DataFrame) -> pd.DataFrame:
    #Timestamps
    data = timestamp_prepare(data)

    if 'TEMPERATURE' in data.columns:
    #Temperatures
        data = convert_temp(data)
        data = remove_temp_outliers(data)

    #Humidity
    if 'HUMIDITY' in data.columns:
        data = normalize_humidity(data)
        data = remove_humidity_outliers(data)

    #Pressure
    if 'PRESSURE' in data.columns:
        data = remove_pressure_outliers(data)

    return data

def verify_min_hours(normals: pd.DataFrame):
    if normals.index.max() - normals.index.min() < pd.Timedelta(hours=180):
        raise ValueError("Fewer than 180 hours of normal data points is not enough to calculate KPIs.")
    
def verify_bin_data_amount(normals: pd.Dataframe, constants: dict):
    insides = normals[(normals['WIND_SPEED'] >= constants['V_cutin']) & (normals['WIND_SPEED'] <= constants['V_cutout'])]['WIND_SPEED']
    if pd.cut(insides, round((constants['V_cutout'] - constants['V_cutin']) / 0.5)).value_counts(ascending=True).iloc[0] < 3:
        raise ValueError("At least 3 normal data points per bin is required to calculate KPIs.")
    
def verify_wind_coverage(normals: pd.Dataframe, constants: dict):
    if normals['ACTIVE_POWER'].max() < constants['P_rated'] * 0.85:
        raise ValueError("Normal data points have to cover at least to 85% of rated power.")
    if normals['WIND_SPEED'].min() > constants['V_cutin'] - 1:
        raise ValueError("Normal data points have to cover at least from the cut-in speed minus 1.")
    
def verify_normal(normals: pd.DataFrame, constants: dict):
    verify_min_hours(normals)
    verify_bin_data_amount(normals, constants)
    verify_wind_coverage(normals, constants)

def normalize_wind_speed(wind: pd.Series[float], air_density: pd.Series[float]) -> pd.Series[float]:
    return wind * np.cbrt(air_density / 1.225)

def normalize_power(power: pd.Series[float], air_density: pd.Series[float]) -> pd.Series[float]:
    return power * (1.225 / air_density)

def normalize_data(data, air_density):
    data['WIND_SPEED'] = normalize_wind_speed(data['WIND_SPEED'], air_density)
    data['ACTIVE_POWER'] = normalize_power(data['ACTIVE_POWER'], air_density)

    return data