import numpy as np
import pandas as pd
from math import ceil
from typing import Literal, Optional, List, Dict

# Optional column name for pre-computed yaw error (degrees). If present, use it instead of nacelle - wind.
YAW_ERROR_COLUMN_ALIASES = ('YAW_ERROR', 'YawError', 'yaw_error')


def _apply_advanced_filters(
    data: pd.DataFrame,
    months: Optional[List[int]] = None,
    day_night: Optional[str] = None,
    direction_sector_deg: Optional[tuple] = None,
    direction_sectors: Optional[tuple] = None,
    source_filters: Optional[Dict[str, Dict[str, float]]] = None,
) -> pd.DataFrame:
    """Apply optional month, day/night, direction sector, and source min/max filters. data must have DatetimeIndex for month/day_night."""
    if months is not None and len(months) > 0 and hasattr(data.index, 'month'):
        data = data[data.index.month.isin(months)]
    if day_night is not None and hasattr(data.index, 'hour'):
        h = data.index.hour
        if day_night == 'day':
            data = data[(h >= 6) & (h < 18)]
        elif day_night == 'night':
            data = data[(h < 6) | (h >= 18)]
    if direction_sector_deg is not None and len(direction_sector_deg) >= 2:
        min_deg, max_deg = direction_sector_deg[0], direction_sector_deg[1]
        dir_col = 'DIRECTION_WIND' if 'DIRECTION_WIND' in data.columns else ('DIRECTION_NACELLE' if 'DIRECTION_NACELLE' in data.columns else None)
        if dir_col:
            d = data[dir_col].astype(float)
            data = data[(d >= min_deg) & (d < max_deg)]
    if direction_sectors is not None and len(direction_sectors) >= 2:
        n_sectors, sector_indices = direction_sectors[0], direction_sectors[1]
        dir_col = 'DIRECTION_WIND' if 'DIRECTION_WIND' in data.columns else ('DIRECTION_NACELLE' if 'DIRECTION_NACELLE' in data.columns else None)
        if dir_col and n_sectors > 0:
            d = data[dir_col].astype(float) % 360
            sector_deg = 360.0 / n_sectors
            mask = pd.Series(False, index=data.index)
            for i in sector_indices:
                lo, hi = i * sector_deg, (i + 1) * sector_deg
                mask = mask | ((d >= lo) & (d < hi))
            data = data[mask]
    if source_filters:
        for col, bounds in source_filters.items():
            if col not in data.columns or not isinstance(bounds, dict):
                continue
            s = data[col].astype(float)
            if 'min' in bounds:
                data = data[s >= float(bounds['min'])]
                s = data[col].astype(float)
            if 'max' in bounds:
                data = data[s <= float(bounds['max'])]
    return data


def yaw_errors(
    data: pd.DataFrame,
    bin_width: Literal[5, 10, 15] = 10,
    v_cutin: Optional[float] = None,
    v_cutout: Optional[float] = None,
    only_computed_states: Optional[list] = None,
    use_precomputed_yaw_column: Optional[str] = None,
    months: Optional[List[int]] = None,
    day_night: Optional[str] = None,
    direction_sector_deg: Optional[tuple] = None,
    direction_sectors: Optional[tuple] = None,
    source_filters: Optional[Dict[str, Dict[str, float]]] = None,
) -> dict:
    """
    Compute yaw error histogram and statistics.
    Delta = nacelle direction - wind direction, normalized to [-180, 180).
    Optionally filter by wind speed (cut-in/cut-out) and by classification state.
    """
    if bin_width not in (5, 10, 15):
        bin_width = 10
    # Resolve pre-computed yaw column if requested
    precomputed_col = None
    if use_precomputed_yaw_column and use_precomputed_yaw_column in data.columns:
        precomputed_col = use_precomputed_yaw_column
    else:
        for alias in YAW_ERROR_COLUMN_ALIASES:
            if alias in data.columns:
                precomputed_col = alias
                break

    if precomputed_col is not None:
        # Use pre-computed yaw error column (degrees)
        data = data.dropna(subset=[precomputed_col]).copy()
        data['delta'] = data[precomputed_col].astype(float)
    else:
        # Compute from nacelle and wind direction
        required = ['DIRECTION_NACELLE', 'DIRECTION_WIND']
        if not all(c in data.columns for c in required):
            return {'data': {}, 'statistics': {'mean_error': None, 'median_error': None, 'std_error': None}}
        data = data.dropna(subset=required).copy()
        data['delta'] = data['DIRECTION_NACELLE'] - data['DIRECTION_WIND']

    def normalize(angle):
        return angle - (ceil((angle + 180) / 360) - 1) * 360

    normalize_np = np.vectorize(normalize)
    data['delta'] = normalize_np(data['delta'].astype(float))

    # Advanced filters (manual: month, day/night, direction sector, source min/max)
    data = _apply_advanced_filters(
        data,
        months=months,
        day_night=day_night,
        direction_sector_deg=direction_sector_deg,
        direction_sectors=direction_sectors,
        source_filters=source_filters,
    )

    # Filter by wind speed (cut-in / cut-out) when provided
    if 'WIND_SPEED' in data.columns:
        if v_cutin is not None:
            data = data[data['WIND_SPEED'] >= v_cutin]
        if v_cutout is not None:
            data = data[data['WIND_SPEED'] <= v_cutout]

    # Filter by classification state (only computed data) when requested
    if only_computed_states is not None and 'status' in data.columns:
        data = data[data['status'].isin(only_computed_states)]

    delta = data['delta'].values
    if len(delta) == 0:
        return {
            'data': {},
            'statistics': {'mean_error': None, 'median_error': None, 'std_error': None},
        }

    bins = np.arange(-180, 180 + bin_width, bin_width)
    if bins[-1] > 180:
        bins = bins[bins <= 180]
    hist = np.histogram(delta, bins=bins)

    vals = {}
    for i, v in enumerate(hist[0]):
        vals[str(float(hist[1][i]))] = float(v)

    obj = {'data': vals}
    obj['statistics'] = {
        'mean_error': float(np.mean(delta)),
        'median_error': float(np.median(delta)),
        'std_error': float(np.std(delta)),
    }
    return obj