import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple

from facilities.models import Turbines
from api_gateway.turbines_analysis.helpers._header import to_epoch_ms
from api_gateway.turbines_analysis.helpers.computation_helper import load_turbine_data
from analytics.models import Computation, ClassificationPoint

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
    start_time: Optional[int],
    end_time: Optional[int]
) -> Tuple[Optional[pd.DataFrame], Optional[str], Optional[Dict], Optional[str]]:
    field_names = [SOURCE_TO_FIELD_MAPPING.get(source) for source in sources]
    field_names = [f for f in field_names if f]
    
    if not field_names:
        return None, None, "No valid field names found for sources"
    
    # Fast path (query-only): if requesting only wind_speed/power and we have persisted classification points.
    req_set = set(sources)
    if req_set.issubset({"power", "wind_speed"}):
        try:
            comp_q = Computation.objects.filter(
                turbine=turbine, computation_type="classification", is_latest=True
            )
            if start_time is not None and end_time is not None:
                comp = comp_q.filter(start_time=start_time, end_time=end_time).first()
                # If there's no exact match, try a covering computation (latest) as a fallback.
                if comp is None:
                    comp = comp_q.order_by("-end_time").first()
                    if comp and not (int(comp.start_time) <= int(start_time) and int(comp.end_time) >= int(end_time)):
                        comp = None
            else:
                comp = comp_q.order_by("-end_time").first()
            if comp is not None:
                pts = ClassificationPoint.objects.filter(computation=comp)
                if start_time is not None:
                    pts = pts.filter(timestamp__gte=int(start_time))
                if end_time is not None:
                    pts = pts.filter(timestamp__lte=int(end_time))
                cols = ["timestamp"]
                if "wind_speed" in req_set:
                    cols.append("wind_speed")
                if "power" in req_set:
                    cols.append("active_power")
                rows = list(pts.values_list(*cols))
                if rows:
                    df_selected = pd.DataFrame(rows, columns=["timestamp"] + [c for c in cols if c != "timestamp"])
                    # Normalize timestamp to ms (ClassificationPoint is stored in ms).
                    df_selected["timestamp"] = pd.to_numeric(df_selected["timestamp"], errors="coerce").astype("int64")
                    # Rename to requested source names
                    if "active_power" in df_selected.columns:
                        df_selected.rename(columns={"active_power": "power"}, inplace=True)
                    units_meta = {
                        "canonical": {
                            "TIMESTAMP": "ms",
                            "WIND_SPEED": "m/s",
                            "ACTIVE_POWER": "kW",
                        },
                        "raw_config": {
                            "config_scope": "classification_points",
                            "config_id": None,
                            "data_source": "db",
                        },
                    }
                    return df_selected, "classification_points", units_meta, None
        except Exception as e:
            logger.warning("ClassificationPoint fast-path failed: %s", str(e), exc_info=True)

    df, data_source_used, error_info, units_meta = load_turbine_data(turbine, start_time, end_time, 'db')
    
    if df is None or df.empty:
        if start_time is None or end_time is None:
            error_msg = f"No data found for turbine {turbine.id}"
        else:
            error_msg = f"No data found for turbine {turbine.id} for the specified time range"
        if error_info:
            sources_tried = '; '.join(f"{k.capitalize()}: {v}" for k, v in error_info.items())
            error_msg += f". Tried: {sources_tried}"
        return None, None, None, error_msg
    
    available_columns = ['TIMESTAMP'] + [col for col in field_names if col in df.columns]
    if len(available_columns) == 1:
        return None, None, None, "None of the requested sources are available in the data"
    
    df_selected = df[available_columns].copy()
    column_mapping = {'TIMESTAMP': 'timestamp'}
    column_mapping.update({field: source for source, field in SOURCE_TO_FIELD_MAPPING.items() 
                          if field in df_selected.columns})
    
    df_selected.rename(columns=column_mapping, inplace=True)
    
    # Giữ nguyên timestamp ở dạng milliseconds để đồng nhất với input
    if pd.api.types.is_datetime64_any_dtype(df_selected['timestamp']):
        # Convert từ nanoseconds (pandas datetime) sang milliseconds
        df_selected['timestamp'] = df_selected['timestamp'].astype(np.int64) // 10**6
    elif df_selected['timestamp'].dtype == 'int64':
        df_selected['timestamp'] = df_selected['timestamp'].apply(
            lambda x: to_epoch_ms(x) if pd.notna(x) else None
        )
    
    return df_selected, data_source_used, units_meta, None


def resample_dataframe(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    if mode == 'raw':
        return df
    
    if not pd.api.types.is_datetime64_any_dtype(df.index):
        ms = df.index.to_series().apply(lambda x: to_epoch_ms(x) if pd.notna(x) else None)
        valid = ms.notna()
        if valid.any():
            df = df.loc[valid].copy()
            df.index = pd.to_datetime(ms[valid].astype("int64"), unit="ms")
    
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
    data_source_used: Optional[str] = None,
    units_meta: Optional[Dict] = None
) -> Dict:
    if df.index.name == 'timestamp' or 'timestamp' not in df.columns:
        df = df.reset_index()
        if 'index' in df.columns:
            df.rename(columns={'index': 'timestamp'}, inplace=True)
    
    # Giữ nguyên timestamp ở dạng milliseconds để đồng nhất với input
    if pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        # Convert từ nanoseconds (pandas datetime) sang milliseconds
        df['timestamp'] = df['timestamp'].astype(np.int64) // 10**6
    elif df['timestamp'].dtype in ['int64', 'float64']:
        df['timestamp'] = df['timestamp'].apply(lambda x: to_epoch_ms(x) if pd.notna(x) else None)
    
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
        "units": units_meta,
        "data": df.to_dict('records')
    }
    
    return result


def get_cache_key(
    turbine_id: int,
    sources: List[str],
    start_time: Optional[int],
    end_time: Optional[int],
    mode: str
) -> str:
    sources_str = '-'.join(sorted(sources))
    # Avoid cache collisions when only one bound is provided.
    # Example bug (before): start_time=... & end_time=None shared the same key as "no bounds".
    st = "none" if start_time is None else str(start_time)
    et = "none" if end_time is None else str(end_time)
    time_str = f"{st}_{et}"
    return f"timeseries_turbine_{turbine_id}_{sources_str}_{time_str}_{mode}"
