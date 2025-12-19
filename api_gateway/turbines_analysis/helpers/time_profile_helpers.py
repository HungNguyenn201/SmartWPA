import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from ._header import (
    MONTH_NAMES,
    SEASON_INDEX_MAP,
    SEASON_NAMES_BY_INDEX,
    CLASSIFICATION_SOURCE_FIELD_MAP,
    HISTORICAL_SOURCE_FIELD_MAP
)


def prepare_combined_dataframe_from_sources(
    classification_points_dict: Dict[str, any],
    historical_data_dict: Dict[str, any],
    sources: List[str]
) -> Optional[pd.DataFrame]:
    try:
        combined_df = None
        
        for source, classification_points in classification_points_dict.items():
            if classification_points is None:
                continue
            
            data = []
            field_name = CLASSIFICATION_SOURCE_FIELD_MAP.get(source)
            if not field_name:
                continue
            
            for point in classification_points.iterator(chunk_size=1000):
                value = getattr(point, field_name, None)
                if value is None or np.isnan(value) or np.isinf(value):
                    continue
                
                timestamp_dt = pd.to_datetime(point.timestamp, unit='ms')
                data.append({
                    'timestamp': timestamp_dt,
                    source: float(value)
                })
            
            if data:
                temp_df = pd.DataFrame(data)
                temp_df = temp_df.set_index('timestamp').sort_index()
                if combined_df is None:
                    combined_df = temp_df
                else:
                    combined_df = combined_df.join(temp_df, how='outer')
        
        for source, historical_data in historical_data_dict.items():
            if historical_data is None:
                continue
            
            data = []
            field_name = HISTORICAL_SOURCE_FIELD_MAP.get(source)
            if not field_name:
                continue
            
            for hist in historical_data.iterator(chunk_size=1000):
                value = getattr(hist, field_name, None)
                if value is None or np.isnan(value) or np.isinf(value):
                    continue
                
                timestamp_dt = pd.to_datetime(hist.time_stamp)
                data.append({
                    'timestamp': timestamp_dt,
                    source: float(value)
                })
            
            if data:
                temp_df = pd.DataFrame(data)
                temp_df = temp_df.set_index('timestamp').sort_index()
                if combined_df is None:
                    combined_df = temp_df
                else:
                    combined_df = combined_df.join(temp_df, how='outer')
        
        if combined_df is None or combined_df.empty:
            return None
        
        combined_df = combined_df.reset_index()
        
        combined_df = combined_df.sort_values('timestamp')
        
        source_cols = [col for col in combined_df.columns if col != 'timestamp']
        if source_cols:
            combined_df = combined_df.dropna(subset=source_cols, how='all')
        
        return combined_df if not combined_df.empty else None
    except Exception:
        return None


def calculate_hourly_profile(df: pd.DataFrame, sources: List[str]) -> List[Dict]:
    """Calculate hourly profile - average values by hour of day (0-23)"""
    try:
        if df.empty or 'timestamp' not in df.columns:
            return []
        
        # Convert timestamp to datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        df['hour'] = df['timestamp'].dt.hour
        result_data = []
        
        for hour in range(24):
            hour_data = {'hour': hour, 'label': f"{hour}:00"}
            hour_df = df[df['hour'] == hour]
            
            if hour_df.empty:
                for source in sources:
                    hour_data[source] = None
            else:
                for source in sources:
                    if source in hour_df.columns:
                        hour_data[source] = float(hour_df[source].mean()) if not hour_df[source].isna().all() else None
                    else:
                        hour_data[source] = None
            
            result_data.append(hour_data)
        
        return result_data
    except Exception:
        return []


def calculate_daily_profile(df: pd.DataFrame, sources: List[str]) -> List[Dict]:
    """Calculate daily profile - average values by day of year (1-365/366)"""
    try:
        if df.empty or 'timestamp' not in df.columns:
            return []
        
        # Convert timestamp to datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        df['dayofyear'] = df['timestamp'].dt.dayofyear
        result_data = []
        max_day = 366  # Include leap year day
        
        for day in range(1, max_day + 1):
            try:
                date_label = pd.Timestamp(2000, 1, 1) + pd.Timedelta(days=day-1)
                label = date_label.strftime("%b %d")
            except:
                label = f"Day {day}"
            
            day_data = {'day': day, 'label': label}
            day_df = df[df['dayofyear'] == day]
            
            if day_df.empty:
                for source in sources:
                    day_data[source] = None
            else:
                for source in sources:
                    if source in day_df.columns:
                        day_data[source] = float(day_df[source].mean()) if not day_df[source].isna().all() else None
                    else:
                        day_data[source] = None
            
            result_data.append(day_data)
        
        return result_data
    except Exception:
        return []


def calculate_monthly_profile(df: pd.DataFrame, sources: List[str]) -> List[Dict]:
    """Calculate monthly profile - average values by month (1-12)"""
    try:
        if df.empty or 'timestamp' not in df.columns:
            return []
        
        # Convert timestamp to datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        df['month'] = df['timestamp'].dt.month
        result_data = []
        
        for month in range(1, 13):
            month_data = {'month': month, 'label': MONTH_NAMES[month]}
            month_df = df[df['month'] == month]
            
            if month_df.empty:
                for source in sources:
                    month_data[source] = None
            else:
                for source in sources:
                    if source in month_df.columns:
                        month_data[source] = float(month_df[source].mean()) if not month_df[source].isna().all() else None
                    else:
                        month_data[source] = None
            
            result_data.append(month_data)
        
        return result_data
    except Exception:
        return []


def calculate_seasonal_profile(df: pd.DataFrame, sources: List[str]) -> List[Dict]:
    """Calculate seasonal profile - average values by season (0-3)"""
    try:
        if df.empty or 'timestamp' not in df.columns:
            return []
        
        # Convert timestamp to datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        df['month'] = df['timestamp'].dt.month
        df['season'] = df['month'].map(SEASON_INDEX_MAP)
        
        result_data = []
        
        for season in range(4):
            season_data = {'season': season, 'label': SEASON_NAMES_BY_INDEX[season]}
            season_df = df[df['season'] == season]
            
            if season_df.empty:
                for source in sources:
                    season_data[source] = None
            else:
                for source in sources:
                    if source in season_df.columns:
                        season_data[source] = float(season_df[source].mean()) if not season_df[source].isna().all() else None
                    else:
                        season_data[source] = None
            
            result_data.append(season_data)
        
        return result_data
    except Exception:
        return []


def calculate_profile(df: pd.DataFrame, sources: List[str], profile: str) -> List[Dict]:
    """Calculate time profile based on profile type"""
    try:
        if df.empty:
            return []
        
        if profile == 'hourly':
            return calculate_hourly_profile(df, sources)
        elif profile == 'daily':
            return calculate_daily_profile(df, sources)
        elif profile == 'monthly':
            return calculate_monthly_profile(df, sources)
        elif profile == 'seasonally':
            return calculate_seasonal_profile(df, sources)
        else:
            return calculate_hourly_profile(df, sources)
    except Exception:
        return []

