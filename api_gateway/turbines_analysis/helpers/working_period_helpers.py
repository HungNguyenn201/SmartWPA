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
    
    if len(timestamps) < 2:
        sampling_time_sec = 600.0
    else:
        time_diffs = np.diff(timestamps)
        time_diffs = time_diffs[time_diffs > 0]
        if len(time_diffs) == 0:
            sampling_time_sec = 600.0
        else:
            sampling_time_sec = float(np.mean(time_diffs))
    
    if sampling_time_sec <= 0 or not np.isfinite(sampling_time_sec):
        sampling_time_sec = 600.0
    
    sampling_time_hours = sampling_time_sec / 3600.0
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
        
        first_timestamp = month_data.index.min()
        last_timestamp = month_data.index.max()
        actual_days_in_data = (last_timestamp - first_timestamp).total_seconds() / (24 * 3600)
        
        if actual_days_in_data <= 0:
            actual_days_in_data = 1.0
        
        days_in_year = 365.0
        
        energy_per_year_kwh = (month_energy_kwh / actual_days_in_data) * days_in_year
        
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

