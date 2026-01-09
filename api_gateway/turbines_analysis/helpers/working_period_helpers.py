import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple

from facilities.models import Turbines
from api_gateway.turbines_analysis.helpers.timeseries_helpers import load_timeseries_data

logger = logging.getLogger('api_gateway.turbines_analysis')

REQUIRED_SOURCES = ['power', 'wind_speed']


def load_working_period_data(
    turbine: Turbines,
    start_time: Optional[int],
    end_time: Optional[int]
) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[str]]:
    df, data_source_used, error_msg = load_timeseries_data(
        turbine, REQUIRED_SOURCES, start_time, end_time
    )
    
    if df is None or df.empty:
        return None, None, error_msg or "No data available for the specified time range"
    
    if 'power' not in df.columns or 'wind_speed' not in df.columns:
        return None, None, "Missing required data: power and wind_speed are required"
    
    return df, data_source_used, None


def validate_working_period_params(
    variation: Optional[str],
    start_time: Optional[str],
    end_time: Optional[str]
) -> Tuple[bool, Optional[str], Optional[Dict]]:
    try:
        variation_int = int(variation) if variation else 50
        variation_int = max(1, min(100, variation_int))
    except (ValueError, TypeError):
        variation_int = 50
    
    parsed_start_time = None
    parsed_end_time = None
    
    if start_time:
        try:
            parsed_start_time = int(start_time)
        except ValueError:
            return False, "start_time must be an integer (Unix timestamp in milliseconds)", None
    
    if end_time:
        try:
            parsed_end_time = int(end_time)
        except ValueError:
            return False, "end_time must be an integer (Unix timestamp in milliseconds)", None
    
    return True, None, {
        'variation': variation_int,
        'start_time': parsed_start_time,
        'end_time': parsed_end_time
    }


def calculate_performance(
    df: pd.DataFrame,
    variation: int = 50
) -> List[Dict]:
    if 'power' not in df.columns or 'wind_speed' not in df.columns:
        logger.error("Missing required columns for performance calculation")
        return []
    
    if df.empty:
        return []
    
    result_df = df[['timestamp', 'power', 'wind_speed']].copy()
    result_df = result_df.dropna(subset=['power', 'wind_speed'])
    
    if result_df.empty:
        return []
    
    timestamps = result_df['timestamp'].values
    
    # Xử lý đúng đơn vị timestamp (milliseconds từ timeseries_helpers)
    if len(timestamps) < 2:
        sampling_time_ms = 600000.0  # 10 phút mặc định (milliseconds)
    else:
        time_diffs = np.diff(timestamps)
        time_diffs = time_diffs[time_diffs > 0]
        if len(time_diffs) == 0:
            sampling_time_ms = 600000.0
        else:
            # Timestamps đang ở milliseconds, nên time_diffs cũng là milliseconds
            sampling_time_ms = float(np.mean(time_diffs))
    
    if sampling_time_ms <= 0 or not np.isfinite(sampling_time_ms):
        sampling_time_ms = 600000.0
    
    # Convert milliseconds → hours
    sampling_time_hours = sampling_time_ms / (1000 * 3600.0)
    result_df['energy'] = result_df['power'] * sampling_time_hours
    
    if variation <= 50:
        variation_factor = 0.1 + (variation - 1) * (0.9 / 49.0)
    else:
        variation_factor = 1.0 + (variation - 50) * (1.0 / 50.0)
    
    result_df['wind_factor'] = np.minimum(result_df['wind_speed']**3 / 1000.0, 1.0)
    wind_factors = result_df['wind_factor'].values
    mean_wind_factor = float(np.mean(wind_factors))
    
    if not np.isfinite(mean_wind_factor):
        mean_wind_factor = 1.0
    
    adjusted_factors = mean_wind_factor + (wind_factors - mean_wind_factor) * variation_factor
    result_df['adjusted_wind_factor'] = np.clip(adjusted_factors, 0.1, 1.5)
    
    # Parse timestamp: nếu > 1e12 thì là milliseconds, ngược lại là seconds
    if result_df['timestamp'].max() > 1e12:
        result_df['datetime'] = pd.to_datetime(result_df['timestamp'], unit='ms')
    else:
        result_df['datetime'] = pd.to_datetime(result_df['timestamp'], unit='s')
    result_df.set_index('datetime', inplace=True)
    
    monthly_groups = result_df.groupby(pd.Grouper(freq='MS'))
    monthly_results = []
    
    for month_start, month_data in monthly_groups:
        if month_data.empty:
            continue
        
        month_energy_kwh = float(month_data['energy'].sum())
        if month_energy_kwh <= 0 or not np.isfinite(month_energy_kwh):
            continue
        
        # Tính số ngày thực tế có data dựa trên số samples và sampling interval
        num_samples = len(month_data)
        actual_hours_in_data = num_samples * sampling_time_hours
        actual_days_in_data = actual_hours_in_data / 24.0
        
        if actual_days_in_data <= 0:
            actual_days_in_data = 1.0
        
        # Giới hạn actual_days_in_data tối đa bằng số ngày trong tháng
        days_in_month = month_start.days_in_month
        actual_days_in_data = min(actual_days_in_data, days_in_month)
        
        days_in_year = 365.0
        
        # Scale từ monthly energy lên annual energy
        # Chỉ scale khi có đủ data (ít nhất 7 ngày)
        if actual_days_in_data >= 7:
            # Scale hợp lý: (month_energy / actual_days) * 365
            energy_per_year_kwh = (month_energy_kwh / actual_days_in_data) * days_in_year
        else:
            # Nếu ít data (< 7 ngày), giới hạn scale factor
            # Tối đa scale từ 7 ngày lên 365 ngày (factor = 365/7 ≈ 52)
            max_scale_factor = days_in_year / 7.0
            scale_factor = min(days_in_year / actual_days_in_data, max_scale_factor)
            energy_per_year_kwh = month_energy_kwh * scale_factor
        
        if not np.isfinite(energy_per_year_kwh) or energy_per_year_kwh <= 0:
            continue
        
        mean_adjusted_wind_factor = float(month_data['adjusted_wind_factor'].mean())
        if not np.isfinite(mean_adjusted_wind_factor):
            mean_adjusted_wind_factor = 1.0
        
        performance = energy_per_year_kwh * mean_adjusted_wind_factor
        
        if np.isfinite(performance) and performance > 0:
            monthly_results.append({
                'timestamp': int(month_start.timestamp() * 1000),  # Convert seconds → milliseconds
                'performance': float(performance)
            })
    
    if not monthly_results:
        return []
    
    return monthly_results


def format_working_period_response(
    performance_data: List[Dict],
    turbine: Turbines,
    start_time: Optional[int],
    end_time: Optional[int],
    variation: int
) -> Dict:
    return {
        "turbine_id": turbine.id,
        "turbine_name": turbine.name,
        "farm_name": turbine.farm.name if turbine.farm else None,
        "farm_id": turbine.farm.id if turbine.farm else None,
        "start_time": start_time,
        "end_time": end_time,
        "variation": variation,
        "data": performance_data
    }


def get_cache_key(
    turbine_id: int,
    start_time: Optional[int],
    end_time: Optional[int],
    variation: int
) -> str:
    time_str = f"{start_time}_{end_time}" if start_time and end_time else "all"
    return f"working_period_turbine_{turbine_id}_{time_str}_{variation}"

