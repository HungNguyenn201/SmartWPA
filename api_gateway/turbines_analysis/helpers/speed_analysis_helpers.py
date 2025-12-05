import pandas as pd
import numpy as np
from scipy.stats import weibull_min
from typing import Dict, Optional, Tuple


def prepare_bins(values: np.ndarray, bin_width: float) -> np.ndarray:
    if len(values) == 0:
        return np.array([0, bin_width])
    
    # Filter out NaN and Inf values
    valid_values = values[~np.isnan(values) & ~np.isinf(values)]
    if len(valid_values) == 0:
        return np.array([0, bin_width])
    
    vmax = float(np.max(valid_values))
    vmin = float(np.max([0, np.min(valid_values)]))
    
    if vmax <= 0:
        vmax = bin_width * 10
    
    min_bins = 30
    max_bins = 100
    adjusted_bin_width = bin_width
    
    bins_count = int((vmax - vmin) / bin_width) + 2
    
    if bins_count < min_bins:
        adjusted_bin_width = (vmax - vmin) / (min_bins - 2) if min_bins > 2 else bin_width
    elif bins_count > max_bins:
        adjusted_bin_width = (vmax - vmin) / (max_bins - 2) if max_bins > 2 else bin_width
    
    if adjusted_bin_width <= 0:
        adjusted_bin_width = bin_width
    
    num_bins = max(2, int((vmax - vmin) / adjusted_bin_width) + 2)
    bins = np.linspace(vmin, vmax + adjusted_bin_width, num_bins)
    return bins


def compute_histogram(values: np.ndarray, bins: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    hist, bin_edges = np.histogram(values, bins=bins, density=True)
    hist = hist * 100
    return hist, bin_edges


def compute_statistics(values: np.ndarray) -> Tuple[float, float, float]:
    valid_values = values[~np.isnan(values) & ~np.isinf(values)]
    if len(valid_values) == 0:
        return 0.0, 0.0, 0.0
    return float(np.mean(valid_values)), float(np.max(valid_values)), float(np.min(valid_values))


def format_array_values(values) -> list:
    return [float(val) for val in values]


def calculate_weibull_curve(wind_speeds: np.ndarray, bin_centers: np.ndarray) -> Tuple[np.ndarray, float, float]:
    valid_speeds = wind_speeds[~np.isnan(wind_speeds) & ~np.isinf(wind_speeds) & (wind_speeds >= 0)]
    
    if len(valid_speeds) < 3:
        k = 2.0
        A = float(np.mean(valid_speeds)) if len(valid_speeds) > 0 else 5.0
        if A <= 0:
            A = 5.0
        weibull_curve = (k / A) * (bin_centers / A) ** (k - 1) * np.exp(-(bin_centers / A) ** k) * 100
        weibull_curve = np.nan_to_num(weibull_curve, nan=0.0, posinf=0.0, neginf=0.0)
        return weibull_curve, k, A
    
    try:
        shape, loc, scale = weibull_min.fit(valid_speeds, floc=0)
        k = float(shape)
        A = float(scale)
        
        if k <= 0 or A <= 0:
            k = 2.0
            A = float(np.mean(valid_speeds))
            if A <= 0:
                A = 5.0
        
        with np.errstate(divide='ignore', invalid='ignore'):
            weibull_curve = (k / A) * (bin_centers / A) ** (k - 1) * np.exp(-(bin_centers / A) ** k) * 100
        
        weibull_curve = np.nan_to_num(weibull_curve, nan=0.0, posinf=0.0, neginf=0.0)
        return weibull_curve, k, A
    except Exception:
        k = 2.0
        A = float(np.mean(valid_speeds)) if len(valid_speeds) > 0 else 5.0
        if A <= 0:
            A = 5.0
        weibull_curve = (k / A) * (bin_centers / A) ** (k - 1) * np.exp(-(bin_centers / A) ** k) * 100
        weibull_curve = np.nan_to_num(weibull_curve, nan=0.0, posinf=0.0, neginf=0.0)
        return weibull_curve, k, A


def calculate_speed_rose(
    wind_speeds: np.ndarray,
    directions: Optional[np.ndarray],
    threshold1: float,
    threshold2: float,
    sectors_number: int
) -> Optional[Dict]:
    """Calculate speed rose data"""
    if directions is None or len(directions) == 0:
        return None
    
    if len(wind_speeds) != len(directions):
        return None
    
    try:
        sector_angle = 360.0 / sectors_number
        sectors = np.zeros((sectors_number, 3), dtype=np.float64)
        
        valid_count = 0
        for speed, direction in zip(wind_speeds, directions):
            # Skip invalid values
            if np.isnan(direction) or np.isinf(direction) or np.isnan(speed) or np.isinf(speed):
                continue
            
            # Normalize direction to [0, 360)
            direction = direction % 360
            if direction < 0:
                direction += 360
            
            # Calculate sector index (0 to sectors_number-1)
            sector_idx = int(direction / sector_angle)
            # Handle edge case: direction = 360 should map to sector 0, not sectors_number
            if sector_idx >= sectors_number:
                sector_idx = 0
            
            # Categorize speed
            if speed < threshold1:
                speed_category = 0
            elif speed < threshold2:
                speed_category = 1
            else:
                speed_category = 2
            
            sectors[sector_idx, speed_category] += 1
            valid_count += 1
        
        if valid_count == 0:
            return None
        
        # Normalize to percentages
        if valid_count > 0:
            sectors = (sectors / valid_count) * 100
        
        angles = [i * sector_angle for i in range(sectors_number)]
        
        return {
            "angle": angles,
            "low_speed": format_array_values(sectors[:, 0]),
            "medium_speed": format_array_values(sectors[:, 1]),
            "high_speed": format_array_values(sectors[:, 2])
        }
    except Exception:
        return None


def calculate_global_distribution(
    df: pd.DataFrame,
    bin_width: float,
    threshold1: float,
    threshold2: float,
    sectors_number: int
) -> Optional[Dict]:
    """Calculate global wind distribution"""
    try:
        if df.empty or 'wind_speed' not in df.columns:
            return None
        
        # Filter out invalid wind speeds
        valid_df = df[~df['wind_speed'].isin([np.nan, np.inf, -np.inf])].copy()
        if valid_df.empty:
            return None
        
        wind_speeds = valid_df['wind_speed'].values
        directions = valid_df['direction'].values if 'direction' in valid_df.columns else None
        
        # Filter out NaN/Inf from directions if present
        if directions is not None:
            valid_mask = ~(np.isnan(directions) | np.isinf(directions))
            valid_wind_speeds = wind_speeds[valid_mask]
            valid_directions = directions[valid_mask]
        else:
            valid_wind_speeds = wind_speeds
            valid_directions = None
        
        if len(valid_wind_speeds) == 0:
            return None
        
        vmean, vmax, vmin = compute_statistics(valid_wind_speeds)
        bins = prepare_bins(valid_wind_speeds, bin_width)
        hist, bin_edges = compute_histogram(valid_wind_speeds, bins)
        
        # Calculate energy distribution (wind speed^3)
        wind_energy = valid_wind_speeds ** 3
        energy_hist, _ = np.histogram(valid_wind_speeds, bins=bins, weights=wind_energy, density=True)
        energy_hist = energy_hist * 100
        
        bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
        weibull_curve, k, A = calculate_weibull_curve(valid_wind_speeds, bin_centers)
        
        speed_rose_data = calculate_speed_rose(
            valid_wind_speeds, valid_directions, threshold1, threshold2, sectors_number
        )
        
        return {
            "statistics": {
                "vmean": vmean,
                "vmax": vmax,
                "vmin": vmin,
                "weibull_k": k,
                "weibull_A": A
            },
            "distribution_data": {
                "bin": format_array_values(bin_centers),
                "wind_distribution": format_array_values(hist),
                "energy_distribution": format_array_values(energy_hist),
                "weibull_curve": format_array_values(weibull_curve)
            },
            "speed_rose": speed_rose_data,
            "speed_rose_params": {
                "threshold1": threshold1,
                "threshold2": threshold2,
                "sectors_number": sectors_number
            }
        }
    except Exception:
        return None


def calculate_monthly_distribution(
    df: pd.DataFrame,
    bin_width: float,
    threshold1: float,
    threshold2: float,
    sectors_number: int
) -> Optional[Dict]:
    """Calculate monthly wind distribution"""
    try:
        if df.empty or 'wind_speed' not in df.columns:
            return None
        
        # Filter out invalid wind speeds
        valid_df = df[~df['wind_speed'].isin([np.nan, np.inf, -np.inf])].copy()
        if valid_df.empty:
            return None
        
        if not pd.api.types.is_datetime64_any_dtype(valid_df['timestamp']):
            if np.issubdtype(valid_df['timestamp'].dtype, np.integer) or np.issubdtype(valid_df['timestamp'].dtype, np.floating):
                valid_df['timestamp'] = pd.to_datetime(valid_df['timestamp'], unit='ms')
            else:
                valid_df['timestamp'] = pd.to_datetime(valid_df['timestamp'])
        
        valid_df['month'] = valid_df['timestamp'].dt.month
        
        # Prepare bins once using all data
        wind_speeds = valid_df['wind_speed'].values
        bins = prepare_bins(wind_speeds, bin_width)
        bin_centers = (bins[:-1] + bins[1:]) / 2
        bin_values = format_array_values(bin_centers)
        
        month_names = {
            1: 'January', 2: 'February', 3: 'March', 4: 'April',
            5: 'May', 6: 'June', 7: 'July', 8: 'August',
            9: 'September', 10: 'October', 11: 'November', 12: 'December'
        }
        
        months = []
        month_names_array = []
        monthly_data = {}
        monthly_speed_roses = {}
        
        for month in range(1, 13):
            month_df = valid_df[valid_df['month'] == month]
            if len(month_df) == 0:
                continue
            
            month_wind_speeds = month_df['wind_speed'].values
            if len(month_wind_speeds) == 0:
                continue
            
            months.append(month)
            month_names_array.append(month_names[month])
            
            month_directions = month_df['direction'].values if 'direction' in month_df.columns else None
            if month_directions is not None:
                # Filter to match valid wind speeds
                valid_mask = ~(np.isnan(month_directions) | np.isinf(month_directions))
                valid_month_speeds = month_wind_speeds[valid_mask]
                valid_month_directions = month_directions[valid_mask]
            else:
                valid_month_speeds = month_wind_speeds
                valid_month_directions = None
            
            hist, _ = compute_histogram(valid_month_speeds, bins)
            wind_energy = valid_month_speeds ** 3
            energy_hist, _ = np.histogram(valid_month_speeds, bins=bins, weights=wind_energy, density=True)
            energy_hist = energy_hist * 100
            
            weibull_curve, k, A = calculate_weibull_curve(valid_month_speeds, bin_centers)
            speed_rose_data = calculate_speed_rose(
                valid_month_speeds, valid_month_directions, threshold1, threshold2, sectors_number
            )
            
            month_key = str(month)
            monthly_data[month_key] = {
                "wind_distribution": format_array_values(hist),
                "energy_distribution": format_array_values(energy_hist),
                "weibull_curve": format_array_values(weibull_curve),
                "weibull_params": {"k": k, "A": A}
            }
            monthly_speed_roses[month_key] = speed_rose_data
        
        if not months:
            return None
        
        filtered_monthly_data = {k: v for k, v in monthly_data.items() if k in [str(m) for m in months]}
        filtered_monthly_speed_roses = {k: v for k, v in monthly_speed_roses.items() if k in [str(m) for m in months]}
        
        # Get overall statistics
        _, overall_k, overall_A = calculate_weibull_curve(wind_speeds, bin_centers)
        
        return {
            "time_mode": "monthly",
            "monthly_distribution": {
                "months": months,
                "month_names": month_names_array,
                "data": {
                    "bin": bin_values,
                    **filtered_monthly_data
                }
            },
            "monthly_speed_roses": filtered_monthly_speed_roses,
            "speed_rose_params": {
                "threshold1": threshold1,
                "threshold2": threshold2,
                "sectors_number": sectors_number
            },
            "statistics": {
                "weibull_k": overall_k,
                "weibull_A": overall_A
            }
        }
    except Exception:
        return None


def calculate_day_night_distribution(
    df: pd.DataFrame,
    bin_width: float,
    threshold1: float,
    threshold2: float,
    sectors_number: int
) -> Optional[Dict]:
    """Calculate day/night wind distribution"""
    try:
        if df.empty or 'wind_speed' not in df.columns:
            return None
        
        # Filter out invalid wind speeds
        valid_df = df[~df['wind_speed'].isin([np.nan, np.inf, -np.inf])].copy()
        if valid_df.empty:
            return None
        
        if not pd.api.types.is_datetime64_any_dtype(valid_df['timestamp']):
            if np.issubdtype(valid_df['timestamp'].dtype, np.integer) or np.issubdtype(valid_df['timestamp'].dtype, np.floating):
                valid_df['timestamp'] = pd.to_datetime(valid_df['timestamp'], unit='ms')
            else:
                valid_df['timestamp'] = pd.to_datetime(valid_df['timestamp'])
        
        valid_df['hour'] = valid_df['timestamp'].dt.hour
        valid_df['period'] = 'Night'
        valid_df.loc[(valid_df['hour'] >= 6) & (valid_df['hour'] < 18), 'period'] = 'Day'
        
        # Prepare bins once using all data
        wind_speeds = valid_df['wind_speed'].values
        bins = prepare_bins(wind_speeds, bin_width)
        bin_centers = (bins[:-1] + bins[1:]) / 2
        bin_values = format_array_values(bin_centers)
        
        periods = []
        day_night_data = {}
        day_night_speed_roses = {}
        
        for period in ['Day', 'Night']:
            period_df = valid_df[valid_df['period'] == period]
            if len(period_df) == 0:
                continue
            
            period_wind_speeds = period_df['wind_speed'].values
            if len(period_wind_speeds) == 0:
                continue
            
            periods.append(period)
            period_directions = period_df['direction'].values if 'direction' in period_df.columns else None
            if period_directions is not None:
                # Filter to match valid wind speeds
                valid_mask = ~(np.isnan(period_directions) | np.isinf(period_directions))
                valid_period_speeds = period_wind_speeds[valid_mask]
                valid_period_directions = period_directions[valid_mask]
            else:
                valid_period_speeds = period_wind_speeds
                valid_period_directions = None
            
            hist, _ = compute_histogram(valid_period_speeds, bins)
            wind_energy = valid_period_speeds ** 3
            energy_hist, _ = np.histogram(valid_period_speeds, bins=bins, weights=wind_energy, density=True)
            energy_hist = energy_hist * 100
            
            weibull_curve, k, A = calculate_weibull_curve(valid_period_speeds, bin_centers)
            speed_rose_data = calculate_speed_rose(
                valid_period_speeds, valid_period_directions, threshold1, threshold2, sectors_number
            )
            
            day_night_data[period] = {
                "wind_distribution": format_array_values(hist),
                "energy_distribution": format_array_values(energy_hist),
                "weibull_curve": format_array_values(weibull_curve),
                "weibull_params": {"k": k, "A": A}
            }
            day_night_speed_roses[period] = speed_rose_data
        
        if not periods:
            return None
        
        filtered_day_night_data = {k: v for k, v in day_night_data.items() if k in periods}
        filtered_day_night_speed_roses = {k: v for k, v in day_night_speed_roses.items() if k in periods}
        
        _, overall_k, overall_A = calculate_weibull_curve(wind_speeds, bin_centers)
        
        return {
            "time_mode": "day_night",
            "day_night_distribution": {
                "periods": periods,
                "data": {
                    "bin": bin_values,
                    **filtered_day_night_data
                }
            },
            "day_night_speed_roses": filtered_day_night_speed_roses,
            "speed_rose_params": {
                "threshold1": threshold1,
                "threshold2": threshold2,
                "sectors_number": sectors_number
            },
            "statistics": {
                "weibull_k": overall_k,
                "weibull_A": overall_A
            }
        }
    except Exception:
        return None


def calculate_seasonal_distribution(
    df: pd.DataFrame,
    bin_width: float,
    threshold1: float,
    threshold2: float,
    sectors_number: int
) -> Optional[Dict]:
    """Calculate seasonal wind distribution"""
    try:
        if df.empty or 'wind_speed' not in df.columns:
            return None
        
        # Filter out invalid wind speeds
        valid_df = df[~df['wind_speed'].isin([np.nan, np.inf, -np.inf])].copy()
        if valid_df.empty:
            return None
        
        if not pd.api.types.is_datetime64_any_dtype(valid_df['timestamp']):
            if np.issubdtype(valid_df['timestamp'].dtype, np.integer) or np.issubdtype(valid_df['timestamp'].dtype, np.floating):
                valid_df['timestamp'] = pd.to_datetime(valid_df['timestamp'], unit='ms')
            else:
                valid_df['timestamp'] = pd.to_datetime(valid_df['timestamp'])
        
        valid_df['month'] = valid_df['timestamp'].dt.month
        season_map = {
            1: 'Winter', 2: 'Winter', 3: 'Spring', 4: 'Spring',
            5: 'Spring', 6: 'Summer', 7: 'Summer', 8: 'Summer',
            9: 'Fall', 10: 'Fall', 11: 'Fall', 12: 'Winter'
        }
        valid_df['season'] = valid_df['month'].map(season_map)
        
        # Prepare bins once using all data
        wind_speeds = valid_df['wind_speed'].values
        bins = prepare_bins(wind_speeds, bin_width)
        bin_centers = (bins[:-1] + bins[1:]) / 2
        bin_values = format_array_values(bin_centers)
        
        seasons = []
        seasonal_data = {}
        seasonal_speed_roses = {}
        
        for season in ['Winter', 'Spring', 'Summer', 'Fall']:
            season_df = valid_df[valid_df['season'] == season]
            if len(season_df) == 0:
                continue
            
            season_wind_speeds = season_df['wind_speed'].values
            if len(season_wind_speeds) == 0:
                continue
            
            seasons.append(season)
            season_directions = season_df['direction'].values if 'direction' in season_df.columns else None
            if season_directions is not None:
                # Filter to match valid wind speeds
                valid_mask = ~(np.isnan(season_directions) | np.isinf(season_directions))
                valid_season_speeds = season_wind_speeds[valid_mask]
                valid_season_directions = season_directions[valid_mask]
            else:
                valid_season_speeds = season_wind_speeds
                valid_season_directions = None
            
            hist, _ = compute_histogram(valid_season_speeds, bins)
            wind_energy = valid_season_speeds ** 3
            energy_hist, _ = np.histogram(valid_season_speeds, bins=bins, weights=wind_energy, density=True)
            energy_hist = energy_hist * 100
            
            weibull_curve, k, A = calculate_weibull_curve(valid_season_speeds, bin_centers)
            speed_rose_data = calculate_speed_rose(
                valid_season_speeds, valid_season_directions, threshold1, threshold2, sectors_number
            )
            
            seasonal_data[season] = {
                "wind_distribution": format_array_values(hist),
                "energy_distribution": format_array_values(energy_hist),
                "weibull_curve": format_array_values(weibull_curve),
                "weibull_params": {"k": k, "A": A}
            }
            seasonal_speed_roses[season] = speed_rose_data
        
        if not seasons:
            return None
        
        filtered_seasonal_data = {k: v for k, v in seasonal_data.items() if k in seasons}
        filtered_seasonal_speed_roses = {k: v for k, v in seasonal_speed_roses.items() if k in seasons}
        
        _, overall_k, overall_A = calculate_weibull_curve(wind_speeds, bin_centers)
        
        return {
            "time_mode": "seasonally",
            "seasonal_distribution": {
                "data": {
                    "bin": bin_values,
                    **filtered_seasonal_data
                }
            },
            "seasonal_speed_roses": filtered_seasonal_speed_roses,
            "speed_rose_params": {
                "threshold1": threshold1,
                "threshold2": threshold2,
                "sectors_number": sectors_number
            },
            "statistics": {
                "weibull_k": overall_k,
                "weibull_A": overall_A
            }
        }
    except Exception:
        return None


def prepare_dataframe_from_classification_and_historical(
    classification_points,
    historical_data_list: Optional[list] = None
) -> Optional[pd.DataFrame]:
    """Prepare DataFrame from ClassificationPoint and optionally merge with FactoryHistorical wind_dir"""
    try:
        if not classification_points.exists():
            return None
        
        # Prepare classification points data
        cp_data = []
        for point in classification_points.iterator(chunk_size=1000):
            if point.wind_speed is None or np.isnan(point.wind_speed) or np.isinf(point.wind_speed):
                continue
            
            timestamp_dt = pd.to_datetime(point.timestamp, unit='ms')
            cp_data.append({
                'timestamp': timestamp_dt,
                'wind_speed': float(point.wind_speed)
            })
        
        if not cp_data:
            return None
        
        df_cp = pd.DataFrame(cp_data)
        df_cp = df_cp.set_index('timestamp').sort_index()
        
        # Merge with historical wind direction data if available
        if historical_data_list and len(historical_data_list) > 0:
            hist_data = []
            for hist in historical_data_list:
                if hist.get('wind_dir') is None:
                    continue
                wind_dir = float(hist['wind_dir'])
                if np.isnan(wind_dir) or np.isinf(wind_dir):
                    continue
                
                hist_ts = pd.to_datetime(hist['time_stamp'])
                hist_data.append({
                    'timestamp': hist_ts,
                    'wind_dir': wind_dir
                })
            
            if hist_data:
                df_hist = pd.DataFrame(hist_data)
                df_hist = df_hist.set_index('timestamp').sort_index()
                
                # Use merge_asof for efficient time-series merging (nearest match within 5 minutes)
                df = pd.merge_asof(
                    df_cp,
                    df_hist,
                    left_index=True,
                    right_index=True,
                    direction='nearest',
                    tolerance=pd.Timedelta('5min')
                )
            else:
                df = df_cp.copy()
                df['wind_dir'] = None
        else:
            df = df_cp.copy()
            df['wind_dir'] = None
        
        # Rename wind_dir to direction for consistency
        if 'wind_dir' in df.columns:
            df = df.rename(columns={'wind_dir': 'direction'})
        
        # Reset index to make timestamp a column
        df = df.reset_index()
        
        # Final cleanup
        df = df.dropna(subset=['wind_speed'])
        df = df[~df['wind_speed'].isin([np.inf, -np.inf])]
        
        return df if not df.empty else None
    except Exception:
        return None

