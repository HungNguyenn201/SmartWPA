import pandas as pd
import numpy as np
from typing import Dict, Optional


def calculate_statistics_from_dataframe(
    df: pd.DataFrame,
    value_column: str,
    source_type: str
) -> Optional[Dict]:
    try:
        if df.empty or value_column not in df.columns:
            return None
        
        clean_df = df.dropna(subset=[value_column])
        clean_df = clean_df[~clean_df[value_column].isin([np.inf, -np.inf])]
        
        if clean_df.empty:
            return None
        
        total_records = len(df)
        valid_records = len(clean_df)
        
        values = clean_df[value_column].values
        
        if 'timestamp' in clean_df.columns:
            timestamps = clean_df['timestamp']
            min_timestamp = timestamps.min()
            max_timestamp = timestamps.max()
            
            time_diffs = timestamps.diff().dropna()
            if not time_diffs.empty:
                most_common_diff = time_diffs.mode()[0] if len(time_diffs.mode()) > 0 else pd.Timedelta(seconds=600)
                time_step_seconds = most_common_diff.total_seconds()
            else:
                time_step_seconds = 600.0  # Default: 10 min
            
            if isinstance(min_timestamp, pd.Timestamp):
                start_date = int(min_timestamp.timestamp() * 1000)
            else:
                start_date = int(min_timestamp) if not pd.isna(min_timestamp) else None
            
            if isinstance(max_timestamp, pd.Timestamp):
                end_date = int(max_timestamp.timestamp() * 1000)
            else:
                end_date = int(max_timestamp) if not pd.isna(max_timestamp) else None
        else:
            start_date = None
            end_date = None
            time_step_seconds = 600.0
        
        result_data = {
            "source": source_type,
            "type": source_type,
            "statistics": {
                "average": float(np.mean(values)),
                "min": float(np.min(values)),
                "max": float(np.max(values)),
                "standard_deviation": float(np.std(values)),
                "start_date": start_date,
                "end_date": end_date,
                "possibale_records": total_records,
                "effective_records": valid_records,
                "time_step": float(time_step_seconds)
            }
        }
        
        return result_data
    except Exception:
        return None


def prepare_dataframe_from_classification_points(
    classification_points,
    source_type: str
) -> Optional[pd.DataFrame]:
    try:
        if not classification_points.exists():
            return None
        
        source_field_map = {
            'wind_speed': 'wind_speed',
            'power': 'active_power',
        }
        
        field_name = source_field_map.get(source_type)
        if not field_name:
            return None
        
        data = []
        for point in classification_points.iterator(chunk_size=1000):
            value = getattr(point, field_name, None)
            if value is None or np.isnan(value) or np.isinf(value):
                continue
            
            timestamp_dt = pd.to_datetime(point.timestamp, unit='ms')
            data.append({
                'timestamp': timestamp_dt,
                'value': float(value)
            })
        
        if not data:
            return None
        
        df = pd.DataFrame(data)
        df = df.sort_values('timestamp')
        
        return df if not df.empty else None
    except Exception:
        return None


def prepare_dataframe_from_historical(
    historical_data,
    source_type: str
) -> Optional[pd.DataFrame]:
    try:
        if not historical_data.exists():
            return None
        
        source_field_map = {
            'wind_speed': 'wind_speed',
            'power': 'active_power',
            'wind_direction': 'wind_dir',
        }
        
        field_name = source_field_map.get(source_type)
        if not field_name:
            return None
        
        data = []
        for hist in historical_data.iterator(chunk_size=1000):
            value = getattr(hist, field_name, None)
            if value is None or np.isnan(value) or np.isinf(value):
                continue
            
            timestamp_dt = pd.to_datetime(hist.time_stamp)
            data.append({
                'timestamp': timestamp_dt,
                'value': float(value)
            })
        
        if not data:
            return None
        
        df = pd.DataFrame(data)
        df = df.sort_values('timestamp')
        
        return df if not df.empty else None
    except Exception:
        return None

