import pandas as pd

def get_resolution(data: pd.DataFrame) -> pd.Timedelta:
    return data.index.to_series().diff().mode().iloc[0]

def rescale_resolution(data: pd.DataFrame, resolution: pd.Timedelta) -> pd.DataFrame:
    if (resolution == pd.Timedelta(minutes=10)):
        return data
    elif resolution < pd.Timedelta(minutes=10):
        return data.resample('10min').mean()
    elif resolution < pd.Timedelta(minutes=60):
        raise ValueError("Time resolution is too low")
    else:
        raise ValueError("Time resolution is too low")
    
def fill_missing_timestamps(data: pd.DataFrame) -> pd.DataFrame:
    start = data.index.min()
    end = data.index.max()
    time_range = pd.date_range(start=start, end=end, freq='10min')
    return data.reindex(time_range)

def timestamp_prepare(data: pd.DataFrame) -> pd.DataFrame:
    data = data.drop_duplicates(subset=['TIMESTAMP'], keep='first')
    data = data.set_index('TIMESTAMP')
    resolution = get_resolution(data)
    data = rescale_resolution(data, resolution)
    data = fill_missing_timestamps(data)
    return data