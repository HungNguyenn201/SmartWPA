import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Optional
from ._header import (
    CLASSIFICATION_SOURCE_FIELD_MAP,
    HISTORICAL_SOURCE_FIELD_MAP,
    DEFAULT_TIME_STEP_SECONDS,
    DEFAULT_DATA_DIR,
    CSV_SEPARATOR,
    CSV_ENCODING,
    CSV_DATETIME_FORMAT,
    CSV_DATETIME_DAYFIRST,
    FIELD_MAPPING,
    convert_timestamp_to_datetime
)


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
                time_step_seconds = DEFAULT_TIME_STEP_SECONDS
            
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
            time_step_seconds = DEFAULT_TIME_STEP_SECONDS
        
        return {
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
    except Exception:
        return None


def prepare_dataframe_from_classification_points(
    classification_points,
    source_type: str
) -> Optional[pd.DataFrame]:
    try:
        if not classification_points.exists():
            return None
        
        field_name = CLASSIFICATION_SOURCE_FIELD_MAP.get(source_type)
        if not field_name:
            return None
        
        data = []
        for point in classification_points.iterator(chunk_size=1000):
            value = getattr(point, field_name, None)
            if value is None or np.isnan(value) or np.isinf(value):
                continue
            
            timestamp_dt = convert_timestamp_to_datetime(point.timestamp)
            if timestamp_dt is None:
                continue
            
            data.append({
                'timestamp': timestamp_dt,
                'value': float(value)
            })
        
        if not data:
            return None
        
        df = pd.DataFrame(data)
        return df.sort_values('timestamp') if not df.empty else None
    except Exception:
        return None


def prepare_dataframe_from_historical(
    historical_data,
    source_type: str
) -> Optional[pd.DataFrame]:
    try:
        if not historical_data.exists():
            return None
        
        field_name = HISTORICAL_SOURCE_FIELD_MAP.get(source_type)
        if not field_name:
            return None
        
        data = []
        for hist in historical_data.iterator(chunk_size=1000):
            value = getattr(hist, field_name, None)
            if value is None or np.isnan(value) or np.isinf(value):
                continue
            
            data.append({
                'timestamp': pd.to_datetime(hist.time_stamp),
                'value': float(value)
            })
        
        if not data:
            return None
        
        df = pd.DataFrame(data)
        return df.sort_values('timestamp') if not df.empty else None
    except Exception:
        return None


def prepare_dataframe_from_direction_file(
    turbine,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None
) -> Optional[pd.DataFrame]:
    try:
        farm_id = turbine.farm.id
        turbine_id = turbine.id
        data_path = Path(DEFAULT_DATA_DIR) / f"Farm{farm_id}" / f"WT{turbine_id}"
        file_path = data_path / "DIRECTION_WIND.csv"
        
        if not file_path.exists():
            return None
        
        df = pd.read_csv(file_path, sep=CSV_SEPARATOR, encoding=CSV_ENCODING)
        
        if df.empty:
            return None
        
        df['DATE_TIME'] = pd.to_datetime(df['DATE_TIME'], format=CSV_DATETIME_FORMAT, dayfirst=CSV_DATETIME_DAYFIRST)
        
        direction_column = FIELD_MAPPING.get('DIRECTION_WIND.csv', 'DIRECTION_WIND')
        if direction_column not in df.columns:
            direction_column = df.columns[1]
        
        df = df.rename(columns={'DATE_TIME': 'timestamp', direction_column: 'value'})
        
        if start_time and end_time:
            start_dt = pd.to_datetime(start_time, unit='ms')
            end_dt = pd.to_datetime(end_time, unit='ms')
            df = df[(df['timestamp'] >= start_dt) & (df['timestamp'] <= end_dt)]
        
        if df.empty:
            return None
        
        df = df[['timestamp', 'value']]
        df = df.dropna(subset=['value'])
        df = df[~df['value'].isin([np.inf, -np.inf])]
        df = df.sort_values('timestamp')
        
        return df if not df.empty else None
        
    except Exception as e:
        import logging
        logger = logging.getLogger('api_gateway.turbines_analysis')
        logger.error(f"Error in prepare_dataframe_from_direction_file for turbine {turbine.id}: {str(e)}", exc_info=True)
        return None
