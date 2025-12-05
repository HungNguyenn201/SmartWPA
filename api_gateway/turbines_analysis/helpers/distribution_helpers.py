import pandas as pd
import numpy as np
from typing import Dict, Optional, List, Tuple


def get_bin_name(source_type: str) -> str:
    bin_names = {
        'wind_speed': 'wind_speed_bin',
        'power': 'power_bin',
        'wind_direction': 'wind_direction_bin'
    }
    return bin_names.get(source_type, 'bin')


def calculate_global_distribution(
    df: pd.DataFrame, 
    bin_width: float, 
    source_type: str,
    bin_count: int = 50
) -> Optional[Dict]:
    try:
        if df.empty:
            return None
        
        vmean = float(df['value'].mean())
        vmax = float(df['value'].max())
        vmin = float(df['value'].min())
        
        bin_min = max(0, vmin - bin_width)
        bin_max = vmax + bin_width
        
        calculated_bin_count = int((bin_max - bin_min) / bin_width)
        if calculated_bin_count > bin_count:
            bin_width = (bin_max - bin_min) / bin_count
            bins = np.arange(bin_min, bin_max + bin_width, bin_width)
        else:
            bins = np.arange(bin_min, bin_max + bin_width, bin_width)
        
        hist, bin_edges = np.histogram(df['value'], bins=bins, density=True)
        hist = hist * 100
        
        bin_name = get_bin_name(source_type)
        bin_values = [float(bin_edges[i]) for i in range(len(bin_edges) - 1)]
        distribution_values = [float(hist[i]) for i in range(len(hist))]
        
        return {
            "global_distribution": {
                bin_name: bin_values,
                "distribution": distribution_values
            },
            "statistics": {
                "vmean": vmean,
                "vmax": vmax,
                "vmin": vmin
            }
        }
    except Exception:
        return None


def calculate_monthly_distribution(
    df: pd.DataFrame, 
    bin_width: float, 
    source_type: str,
    bin_count: int = 50
) -> Optional[Dict]:
    try:
        if df.empty:
            return None
        
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            if np.issubdtype(df['timestamp'].dtype, np.integer) or np.issubdtype(df['timestamp'].dtype, np.floating):
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            else:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        df['month'] = df['timestamp'].dt.month
        
        vmean = float(df['value'].mean())
        vmax = float(df['value'].max())
        vmin = float(df['value'].min())
        
        bin_min = max(0, vmin - bin_width)
        bin_max = vmax + bin_width
        bins = np.linspace(bin_min, bin_max, bin_count + 1)
        
        month_names = {
            1: 'January', 2: 'February', 3: 'March', 4: 'April',
            5: 'May', 6: 'June', 7: 'July', 8: 'August',
            9: 'September', 10: 'October', 11: 'November', 12: 'December'
        }
        
        monthly_distribution = []
        bin_name = get_bin_name(source_type)
        bin_values = [float(bin_edges) for bin_edges in bins[:-1]]
        
        for month in range(1, 13):
            month_df = df[df['month'] == month]
            
            if len(month_df) == 0:
                continue
            
            month_values = month_df['value'].values
            month_mean = float(np.mean(month_values))
            month_max = float(np.max(month_values))
            
            hist, _ = np.histogram(month_values, bins=bins, density=True)
            hist = hist * 100
            
            distribution_values = [float(hist[i]) for i in range(len(hist))]
            
            monthly_distribution.append({
                'month': month,
                'month_name': month_names[month],
                'mean': month_mean,
                'max': month_max,
                'data': {
                    bin_name: bin_values,
                    'distribution': distribution_values
                }
            })
        
        return {
            "time_mode": "monthly",
            "monthly_distribution": monthly_distribution,
            "statistics": {
                "vmean": vmean,
                "vmax": vmax,
                "vmin": vmin
            }
        }
    except Exception:
        return None


def calculate_day_night_distribution(
    df: pd.DataFrame, 
    bin_width: float, 
    source_type: str,
    bin_count: int = 50
) -> Optional[Dict]:
    try:
        if df.empty:
            return None
        
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            if np.issubdtype(df['timestamp'].dtype, np.integer) or np.issubdtype(df['timestamp'].dtype, np.floating):
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            else:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        df['hour'] = df['timestamp'].dt.hour
        df['period'] = 'Night'
        df.loc[(df['hour'] >= 5) & (df['hour'] <= 18), 'period'] = 'Day'
        
        vmean = float(df['value'].mean())
        vmax = float(df['value'].max())
        vmin = float(df['value'].min())
        
        bin_min = max(0, vmin - bin_width)
        bin_max = vmax + bin_width
        
        calculated_bin_count = int((bin_max - bin_min) / bin_width)
        if calculated_bin_count > bin_count:
            bin_width = (bin_max - bin_min) / bin_count
            bins = np.arange(bin_min, bin_max + bin_width, bin_width)
        else:
            bins = np.arange(bin_min, bin_max + bin_width, bin_width)
        
        day_night_distribution = []
        bin_name = get_bin_name(source_type)
        bin_values = [float(bin_edges) for bin_edges in bins[:-1]]
        
        for period in ['Day', 'Night']:
            period_df = df[df['period'] == period]
            
            if len(period_df) == 0:
                continue
            
            period_values = period_df['value'].values
            period_mean = float(np.mean(period_values))
            period_max = float(np.max(period_values))
            
            hist, _ = np.histogram(period_values, bins=bins, density=True)
            hist = hist * 100
            
            distribution_values = [float(hist[i]) for i in range(len(hist))]
            
            day_night_distribution.append({
                'period': period,
                'mean': period_mean,
                'max': period_max,
                'data': {
                    bin_name: bin_values,
                    'distribution': distribution_values
                }
            })
        
        return {
            "time_mode": "day_night",
            "day_night_distribution": day_night_distribution,
            "statistics": {
                "vmean": vmean,
                "vmax": vmax,
                "vmin": vmin
            }
        }
    except Exception:
        return None


def calculate_seasonal_distribution(
    df: pd.DataFrame, 
    bin_width: float, 
    source_type: str,
    bin_count: int = 50
) -> Optional[Dict]:
    try:
        if df.empty:
            return None
        
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            if np.issubdtype(df['timestamp'].dtype, np.integer) or np.issubdtype(df['timestamp'].dtype, np.floating):
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            else:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        df['month'] = df['timestamp'].dt.month
        season_map = {
            1: 'Winter', 2: 'Winter', 3: 'Spring', 4: 'Spring',
            5: 'Spring', 6: 'Summer', 7: 'Summer', 8: 'Summer',
            9: 'Fall', 10: 'Fall', 11: 'Fall', 12: 'Winter'
        }
        df['season'] = df['month'].map(season_map)
        
        vmean = float(df['value'].mean())
        vmax = float(df['value'].max())
        vmin = float(df['value'].min())
        
        bin_min = max(0, vmin - bin_width)
        bin_max = vmax + bin_width
        calculated_bin_count = int((bin_max - bin_min) / bin_width)
        if calculated_bin_count > bin_count:
            bin_width = (bin_max - bin_min) / bin_count
            bins = np.arange(bin_min, bin_max + bin_width, bin_width)
        else:
            bins = np.arange(bin_min, bin_max + bin_width, bin_width)
        
        seasonal_distribution = []
        bin_name = get_bin_name(source_type)
        bin_values = [float(bin_edges) for bin_edges in bins[:-1]]
        
        for season in ['Winter', 'Spring', 'Summer', 'Fall']:
            season_df = df[df['season'] == season]
            
            if len(season_df) == 0:
                continue
            
            season_values = season_df['value'].values
            season_mean = float(np.mean(season_values))
            season_max = float(np.max(season_values))
            
            hist, _ = np.histogram(season_values, bins=bins, density=True)
            hist = hist * 100
            
            distribution_values = [float(hist[i]) for i in range(len(hist))]
            
            seasonal_distribution.append({
                'season': season,
                'mean': season_mean,
                'max': season_max,
                'data': {
                    bin_name: bin_values,
                    'distribution': distribution_values
                }
            })
        
        return {
            "time_mode": "seasonally",
            "seasonal_distribution": seasonal_distribution,
            "statistics": {
                "vmean": vmean,
                "vmax": vmax,
                "vmin": vmin
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
        
        data = []
        source_field_map = {
            'wind_speed': 'wind_speed',
            'power': 'active_power',
        }
        
        field_name = source_field_map.get(source_type)
        if not field_name:
            return None
        
        for point in classification_points.iterator(chunk_size=1000):
            data.append({
                'timestamp': pd.to_datetime(point.timestamp, unit='ms'),
                'value': getattr(point, field_name)
            })
        
        if not data:
            return None
        
        df = pd.DataFrame(data)
        df = df.dropna()
        df = df[~df['value'].isin([np.inf, -np.inf])]
        
        return df if not df.empty else None
    except Exception:
        return None

