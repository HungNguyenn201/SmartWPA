import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple

from facilities.models import Turbines
from api_gateway.turbines_analysis.helpers.computation_helper import load_turbine_data

logger = logging.getLogger('api_gateway.turbines_analysis')

SOURCE_TO_FIELD_MAPPING = {
    'power': 'ACTIVE_POWER',
    'wind_speed': 'WIND_SPEED',
    'wind_direction': 'DIRECTION_WIND',
    'nacelle_direction': 'DIRECTION_NACELLE',
    'temperature': 'TEMPERATURE',
    'pressure': 'PRESSURE',
    'humidity': 'HUMIDITY'
}


def load_timeseries_data(
    turbine: Turbines,
    sources: List[str],
    start_time: int,
    end_time: int
) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[str]]:
    field_names = [SOURCE_TO_FIELD_MAPPING.get(source) for source in sources]
    field_names = [f for f in field_names if f]
    
    if not field_names:
        return None, None, "No valid field names found for sources"
    
    df, data_source_used, error_info = load_turbine_data(turbine, start_time, end_time, 'db')
    
    if df is None or df.empty:
        error_msg = f"No data found for turbine {turbine.id} for the specified time range"
        if error_info:
            sources_tried = '; '.join(f"{k.capitalize()}: {v}" for k, v in error_info.items())
            error_msg += f". Tried: {sources_tried}"
        return None, None, error_msg
    
    available_columns = ['TIMESTAMP'] + [col for col in field_names if col in df.columns]
    if len(available_columns) == 1:
        return None, None, "None of the requested sources are available in the data"
    
    df_selected = df[available_columns].copy()
    column_mapping = {'TIMESTAMP': 'timestamp'}
    column_mapping.update({field: source for source, field in SOURCE_TO_FIELD_MAPPING.items() 
                          if field in df_selected.columns})
    
    df_selected.rename(columns=column_mapping, inplace=True)
    
    if pd.api.types.is_datetime64_any_dtype(df_selected['timestamp']):
        df_selected['timestamp'] = df_selected['timestamp'].astype(np.int64) // 10**9
    elif df_selected['timestamp'].dtype == 'int64':
        df_selected['timestamp'] = df_selected['timestamp'] // 1000
    
    return df_selected, data_source_used, None


def resample_dataframe(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    if mode == 'raw':
        return df
    
    if not pd.api.types.is_datetime64_any_dtype(df.index):
        df.index = pd.to_datetime(df.index, unit='s')
    
    resample_map = {
        'hourly': 'H',
        'daily': 'D',
        'monthly': 'M',
        'yearly': 'Y'
    }
    
    if mode in resample_map:
        return df.resample(resample_map[mode]).mean()
    elif mode == 'seasonally':
        return _resample_seasonally(df)
    
    logger.warning(f"Unknown mode: {mode}, returning raw data")
    return df


def _resample_seasonally(df: pd.DataFrame) -> pd.DataFrame:
    seasons = {1: 'Winter', 2: 'Winter', 3: 'Spring', 4: 'Spring', 5: 'Spring', 6: 'Summer',
               7: 'Summer', 8: 'Summer', 9: 'Fall', 10: 'Fall', 11: 'Fall', 12: 'Winter'}
    season_month_map = {'Spring': 4, 'Summer': 7, 'Fall': 10, 'Winter': 1}
    
    df_copy = df.copy()
    df_copy['season'] = df_copy.index.month.map(seasons)
    grouped = df_copy.groupby('season').mean()
    
    min_time = df_copy.index.min()
    max_time = df_copy.index.max()
    rows = []
    
    for year in range(min_time.year, max_time.year + 1):
        for season, month in season_month_map.items():
            current_year = year + 1 if season == 'Winter' and month == 1 else year
            ts = pd.Timestamp(current_year, month, 15)
            
            if min_time <= ts <= max_time and season in grouped.index:
                row_data = {'timestamp': ts}
                row_data.update({col: grouped.loc[season, col] for col in grouped.columns if col != 'season'})
                rows.append(row_data)
    
    if not rows:
        return pd.DataFrame()
    
    result_df = pd.DataFrame(rows)
    result_df.set_index('timestamp', inplace=True)
    return result_df.sort_index()


def format_timeseries_response(
    df: pd.DataFrame,
    turbine: Turbines,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    mode: str = 'raw',
    data_source_used: Optional[str] = None
) -> Dict:
    if df.index.name == 'timestamp' or 'timestamp' not in df.columns:
        df = df.reset_index()
        if 'index' in df.columns:
            df.rename(columns={'index': 'timestamp'}, inplace=True)
    
    if pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp'] = df['timestamp'].astype(np.int64) // 10**9
    elif df['timestamp'].dtype in ['int64', 'float64'] and df['timestamp'].max() > 1e12:
        df['timestamp'] = df['timestamp'] // 1000
    
    df.sort_values('timestamp', inplace=True)
    df = df.dropna(how='all')
    
    df = df.replace({np.nan: None})
    
    result = {
        "turbine_id": turbine.id,
        "turbine_name": turbine.name,
        "farm_name": turbine.farm.name if turbine.farm else None,
        "farm_id": turbine.farm.id if turbine.farm else None,
        "start_time": start_time,
        "end_time": end_time,
        "mode": mode,
        "data": df.to_dict('records')
    }
    
    if data_source_used:
        result["data_source_used"] = data_source_used
    
    return result


def get_cache_key(
    turbine_id: int,
    sources: List[str],
    start_time: Optional[int],
    end_time: Optional[int],
    mode: str
) -> str:
    sources_str = '-'.join(sorted(sources))
    time_str = f"{start_time}_{end_time}" if start_time and end_time else "all"
    return f"timeseries_turbine_{turbine_id}_{sources_str}_{time_str}_{mode}"
