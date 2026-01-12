from typing import Dict, Optional, Tuple
import pandas as pd
import numpy as np
import os
import logging
from pathlib import Path
from django.db import transaction
from django.utils import timezone

from acquisition.models import FactoryHistorical
from facilities.models import Turbines, Farm
from analytics.models import (
    Computation, PowerCurveAnalysis, PowerCurveData,
    ClassificationSummary, ClassificationPoint,
    IndicatorData, YawErrorData, YawErrorStatistics,
    DailyProduction, CapacityFactorData, WeibullData
)
from ._header import (
    CSV_SEPARATOR,
    CSV_ENCODING,
    FIELD_MAPPING,
    REQUIRED_FILES,
    OPTIONAL_FILES,
    REQUIRED_TURBINE_CONSTANTS,
    DEFAULT_SWEPT_AREA,
    DEFAULT_DATA_DIR
)

logger = logging.getLogger('api_gateway.turbines_analysis')


def _detect_csv_separator(file_path: Path) -> str:
    """Tự động detect CSV separator từ file."""
    if not file_path.exists():
        return CSV_SEPARATOR
    
    try:
        with open(file_path, 'r', encoding=CSV_ENCODING) as f:
            first_line = f.readline().strip()
        
        if not first_line:
            return CSV_SEPARATOR
        semicolon_count = first_line.count(';')
        comma_count = first_line.count(',')
        
        if semicolon_count > 0 and comma_count > 0:
            return ';' if semicolon_count >= comma_count else ','
        elif semicolon_count > 0:
            return ';'
        elif comma_count > 0:
            return ','
        else:
            return CSV_SEPARATOR
    except Exception as e:
        logger.warning(f"Error detecting separator for {file_path}: {str(e)}, using default '{CSV_SEPARATOR}'")
        return CSV_SEPARATOR


def _parse_date_time(date_str: str) -> Optional[pd.Timestamp]:
    """
    Parse date string với nhiều format khác nhau.
    Hỗ trợ: dd/mm/yyyy, mm/dd/yyyy, M/d/yyyy, d/M/yyyy (có và không có leading zero)
    """
    if pd.isna(date_str) or not date_str:
        return None
    
    date_str = str(date_str).strip()
    
    # Thử các format cụ thể trước (với leading zero)
    date_formats = [
        '%d/%m/%Y %H:%M',      # 01/01/2012 00:00 (dd/mm/yyyy với leading zero)
        '%m/%d/%Y %H:%M',      # 07/30/2023 15:55 (mm/dd/yyyy với leading zero)
        '%d/%m/%Y %H:%M:%S',   # Với seconds (dd/mm/yyyy)
        '%m/%d/%Y %H:%M:%S',   # Với seconds (mm/dd/yyyy)
    ]
    
    for fmt in date_formats:
        try:
            return pd.to_datetime(date_str, format=fmt)
        except (ValueError, TypeError):
            continue
    try:
        result = pd.to_datetime(date_str, dayfirst=False, errors='raise')
        if pd.notna(result):
            return result
    except (ValueError, TypeError):
        pass
    
    try:
        result = pd.to_datetime(date_str, dayfirst=True, errors='raise')
        if pd.notna(result):
            return result
    except (ValueError, TypeError):
        pass
    
    try:
        result = pd.to_datetime(date_str, errors='coerce')
        if pd.notna(result):
            return result
    except (ValueError, TypeError):
        pass
    
    return None


def _parse_date_column_vectorized(date_series: pd.Series) -> pd.Series:
    """
    Parse date column sử dụng vectorized operations (nhanh hơn nhiều so với apply).
    Tự động detect format từ sample đầu tiên.
    """
    if date_series.empty:
        return pd.Series(dtype='datetime64[ns]')
    sample_size = min(10, len(date_series))
    sample = date_series.head(sample_size).dropna()
    
    if sample.empty:
        return pd.to_datetime(date_series, dayfirst=False, errors='coerce')
    sample_str = str(sample.iloc[0]).strip() if len(sample) > 0 else None
    
    if sample_str and '/' in sample_str:
        date_part = sample_str.split()[0] if ' ' in sample_str else sample_str
        parts = date_part.split('/')
        
        if len(parts) == 3:
            first_part = parts[0]
            second_part = parts[1]
            if first_part.isdigit() and second_part.isdigit():
                first_num = int(first_part)
                second_num = int(second_part)
    
                if first_num > 12:
                    return pd.to_datetime(date_series, dayfirst=True, errors='coerce')
                elif second_num > 12:
                    return pd.to_datetime(date_series, dayfirst=False, errors='coerce')
    
    result = pd.to_datetime(date_series, dayfirst=False, errors='coerce')
    success_rate = result.notna().sum() / len(date_series) if len(date_series) > 0 else 0
    if success_rate < 0.5:
        result = pd.to_datetime(date_series, dayfirst=True, errors='coerce')
    
    return result


def _read_csv_with_auto_detect(file_path: Path) -> Optional[pd.DataFrame]:
    if not file_path.exists():
        logger.warning(f"CSV file does not exist: {file_path}")
        return None
    
    separator = _detect_csv_separator(file_path)
    logger.debug(f"Detected separator '{separator}' for file {file_path.name}")
    
    try:
        df = pd.read_csv(file_path, sep=separator, encoding=CSV_ENCODING)
        
        if df.empty:
            logger.warning(f"CSV file is empty: {file_path}")
            return None
        
        if 'DATE_TIME' not in df.columns:
            logger.warning(f"CSV file missing DATE_TIME column: {file_path}")
            return None
        
        original_count = len(df)
        df['DATE_TIME'] = _parse_date_column_vectorized(df['DATE_TIME'])
        df = df.dropna(subset=['DATE_TIME'])
        
        parsed_count = len(df)
        if parsed_count == 0:
            logger.warning(f"All date values failed to parse in file: {file_path}")
            return None
        
        if parsed_count < original_count:
            logger.debug(f"Parsed {parsed_count}/{original_count} date values from {file_path.name}")
        
        return df
    except UnicodeDecodeError as e:
        logger.error(f"Encoding error reading CSV file {file_path}: {str(e)}")
        return None
    except pd.errors.EmptyDataError:
        logger.warning(f"CSV file is empty: {file_path}")
        return None
    except Exception as e:
        logger.error(f"Error reading CSV file {file_path}: {str(e)}", exc_info=True)
        return None


def get_turbine_constants(turbine: Turbines, constants_override: Optional[Dict] = None) -> Dict:
    """
    Return only constants that cannot be derived reliably from SCADA.

    Note:
    - V_cutin / V_cutout / V_rated / P_rated are derived from SCADA per computation.
    - Swept_area must be provided (physical turbine parameter).
    """
    constants_override = constants_override or {}
    constants: Dict = {}

    # Swept area (m²) is required for capacity-factor calculation.
    if "Swept_area" in constants_override and constants_override["Swept_area"] is not None:
        constants["Swept_area"] = float(constants_override["Swept_area"])
    elif DEFAULT_SWEPT_AREA is not None:
        constants["Swept_area"] = float(DEFAULT_SWEPT_AREA)

    if all(key in constants for key in REQUIRED_TURBINE_CONSTANTS):
        return constants

    required_str = ', '.join(REQUIRED_TURBINE_CONSTANTS)
    raise ValueError(
        f"Turbine constants must be configured. Required: {required_str}. "
        f"Please configure DEFAULT_SWEPT_AREA in _header.py or provide constants in request."
    )


def prepare_dataframe_from_factory_historical(
    turbine: Turbines,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None
) -> Optional[pd.DataFrame]:
    historical_data = FactoryHistorical.objects.filter(turbine=turbine)
    
    if start_time is not None and end_time is not None:
        start_dt = pd.to_datetime(start_time, unit='ms')
        end_dt = pd.to_datetime(end_time, unit='ms')
        historical_data = historical_data.filter(
            time_stamp__gte=start_dt,
            time_stamp__lte=end_dt
        )
    
    historical_data = historical_data.order_by('time_stamp')
    
    if not historical_data.exists():
        return None
    
    data_list = []
    for hist in historical_data.iterator(chunk_size=1000):
        row = {
            'TIMESTAMP': hist.time_stamp,
            'WIND_SPEED': hist.wind_speed if hist.wind_speed is not None else np.nan,
            'ACTIVE_POWER': hist.active_power if hist.active_power is not None else np.nan,
        }
        
        if hist.wind_dir is not None:
            row['DIRECTION_WIND'] = hist.wind_dir
        
        if hist.air_temp is not None:
            temp = hist.air_temp
            if temp < 223:
                temp = temp + 273.15
            row['TEMPERATURE'] = temp
        
        if hist.pressure is not None:
            row['PRESSURE'] = hist.pressure
        
        if hist.hud is not None:
            row['HUMIDITY'] = hist.hud / 100.0 if hist.hud > 1 else hist.hud
        
        data_list.append(row)
    
    if not data_list:
        return None
    
    df = pd.DataFrame(data_list)
    df = df.sort_values('TIMESTAMP')
    
    return df


def _load_all_data_from_files(
    turbine: Turbines,
    data_dir: str = None
) -> Optional[pd.DataFrame]:
    """Load all data from CSV files without time filtering"""
    if data_dir is None:
        data_dir = DEFAULT_DATA_DIR
    
    farm_id = turbine.farm.id
    turbine_id = turbine.id
    
    data_path = Path(data_dir) / f"Farm{farm_id}" / f"WT{turbine_id}"
    
    if not data_path.exists():
        logger.warning(f"Data directory not found: {data_path}")
        return None
    
    logger.debug(f"Reading all data from files for turbine {turbine_id}, farm {farm_id}")
    logger.debug(f"Data path: {data_path}")
    
    dataframes = []
    
    for filename in REQUIRED_FILES:
        file_path = data_path / filename
        if not file_path.exists():
            logger.warning(f"Required file not found: {file_path}")
            return None
        
        try:
            df = _read_csv_with_auto_detect(file_path)
            
            if df is None or df.empty:
                logger.warning(f"File {filename} is empty or could not be read")
                return None
            
            if 'DATE_TIME' not in df.columns:
                logger.warning(f"File {filename} missing DATE_TIME column")
                return None
            data_column = FIELD_MAPPING[filename]
            df = df.rename(columns={df.columns[1]: data_column})
            df = df.rename(columns={'DATE_TIME': 'TIMESTAMP'})
            df = df[['TIMESTAMP', data_column]]
            dataframes.append(df)
            
            logger.debug(f"Loaded {len(df)} rows from {filename}")
            
        except Exception as e:
            logger.error(f"Error reading {filename}: {str(e)}")
            return None
    
    if len(dataframes) < 2:
        logger.warning(f"Missing required data files for turbine {turbine_id}")
        return None
    
    df_merged = dataframes[0]
    for df in dataframes[1:]:
        df_merged = pd.merge(df_merged, df, on='TIMESTAMP', how='inner')
    
    for filename, column_name in OPTIONAL_FILES.items():
        file_path = data_path / filename
        if file_path.exists():
            try:
                df_opt = _read_csv_with_auto_detect(file_path)
                
                if df_opt is None or df_opt.empty:
                    continue
                
                if 'DATE_TIME' not in df_opt.columns:
                    continue
                df_opt = df_opt.rename(columns={df_opt.columns[1]: column_name})
                df_opt = df_opt.rename(columns={'DATE_TIME': 'TIMESTAMP'})
                df_opt = df_opt[['TIMESTAMP', column_name]]
                df_merged = pd.merge(df_merged, df_opt, on='TIMESTAMP', how='left')
                
            except Exception as e:
                logger.warning(f"Error reading optional file {filename}: {str(e)}")
                continue
    
    if 'TEMPERATURE' in df_merged.columns:
        df_merged['TEMPERATURE'] = df_merged['TEMPERATURE'].apply(
            lambda x: x + 273.15 if pd.notna(x) and x < 223 else x
        )
    
    return df_merged.sort_values('TIMESTAMP').reset_index(drop=True)


def prepare_dataframe_from_files(
    turbine: Turbines,
    start_time: int,
    end_time: int,
    data_dir: str = None
) -> Optional[pd.DataFrame]:
    """
    Đọc dữ liệu từ CSV files trong thư mục Data/Farm{farm_id}/WT{turbine_id}/
    
    Args:
        turbine: Turbine object
        start_time: Start time in milliseconds (Unix timestamp)
        end_time: End time in milliseconds (Unix timestamp)
        data_dir: Thư mục chứa dữ liệu (mặc định: từ _header.DEFAULT_DATA_DIR)
    
    Returns:
        DataFrame với cấu trúc giống prepare_dataframe_from_factory_historical
        hoặc None nếu không tìm thấy dữ liệu
    """
    if data_dir is None:
        data_dir = DEFAULT_DATA_DIR
    
    farm_id = turbine.farm.id
    turbine_id = turbine.id
    
    data_path = Path(data_dir) / f"Farm{farm_id}" / f"WT{turbine_id}"
    
    if not data_path.exists():
        logger.warning(f"Data directory not found: {data_path}")
        return None
    
    start_dt = pd.to_datetime(start_time, unit='ms')
    end_dt = pd.to_datetime(end_time, unit='ms')
    
    logger.debug(f"Reading data from files for turbine {turbine_id}, farm {farm_id}")
    logger.debug(f"Time range: {start_dt} to {end_dt}")
    logger.debug(f"Data path: {data_path}")
    
    dataframes = []
    
    # Đọc file WIND_SPEED và ACTIVE_POWER (bắt buộc)
    for filename in REQUIRED_FILES:
        file_path = data_path / filename
        if not file_path.exists():
            logger.warning(f"Required file not found: {file_path}")
            return None
        
        try:
            df = _read_csv_with_auto_detect(file_path)
            
            if df is None or df.empty:
                logger.warning(f"File {filename} is empty or could not be read")
                continue
            
            if 'DATE_TIME' not in df.columns:
                logger.warning(f"File {filename} missing DATE_TIME column")
                continue
            
            # Đổi tên cột dữ liệu
            data_column = FIELD_MAPPING[filename]
            df = df.rename(columns={df.columns[1]: data_column})
            df = df.rename(columns={'DATE_TIME': 'TIMESTAMP'})
            
            # Lọc theo thời gian ngay từ đầu để tối ưu performance
            df = df[(df['TIMESTAMP'] >= start_dt) & (df['TIMESTAMP'] <= end_dt)]
            
            if df.empty:
                logger.warning(f"No data in time range for {filename}")
                return None
            
            # Chỉ giữ lại 2 cột cần thiết
            df = df[['TIMESTAMP', data_column]]
            dataframes.append(df)
            
            logger.debug(f"Loaded {len(df)} rows from {filename}")
            
        except Exception as e:
            logger.error(f"Error reading {filename}: {str(e)}")
            return None
    
    if len(dataframes) < 2:
        logger.warning(f"Missing required data files for turbine {turbine_id}")
        return None
    
    # Merge các DataFrame trên TIMESTAMP (inner join để chỉ giữ rows có đủ dữ liệu bắt buộc)
    df_merged = dataframes[0]
    for df in dataframes[1:]:
        df_merged = pd.merge(df_merged, df, on='TIMESTAMP', how='inner')
    
    # Đọc các file optional
    for filename, column_name in OPTIONAL_FILES.items():
        file_path = data_path / filename
        if file_path.exists():
            try:
                df_opt = _read_csv_with_auto_detect(file_path)
                
                if df_opt is not None and not df_opt.empty and 'DATE_TIME' in df_opt.columns:
                    df_opt = df_opt.rename(columns={df_opt.columns[1]: column_name})
                    df_opt = df_opt.rename(columns={'DATE_TIME': 'TIMESTAMP'})
                    
                    df_opt = df_opt[(df_opt['TIMESTAMP'] >= start_dt) & (df_opt['TIMESTAMP'] <= end_dt)]
                    
                    if not df_opt.empty:
                        df_opt = df_opt[['TIMESTAMP', column_name]]
                        df_merged = pd.merge(df_merged, df_opt, on='TIMESTAMP', how='left')
                        logger.debug(f"Loaded optional field {column_name} from {filename}")
            
            except Exception as e:
                logger.warning(f"Error reading optional file {filename}: {str(e)}")
                continue
    
    # Xử lý TEMPERATURE: chuyển đổi nếu cần (giống logic DB)
    if 'TEMPERATURE' in df_merged.columns:
        df_merged['TEMPERATURE'] = df_merged['TEMPERATURE'].apply(
            lambda x: x + 273.15 if pd.notna(x) and x < 223 else x
        )
    
    # Sắp xếp theo TIMESTAMP
    df_merged = df_merged.sort_values('TIMESTAMP').reset_index(drop=True)
    
    # Đảm bảo các cột bắt buộc có giá trị NaN nếu thiếu
    if 'WIND_SPEED' not in df_merged.columns:
        df_merged['WIND_SPEED'] = np.nan
    if 'ACTIVE_POWER' not in df_merged.columns:
        df_merged['ACTIVE_POWER'] = np.nan
    
    logger.debug(f"Final DataFrame shape: {df_merged.shape}, columns: {list(df_merged.columns)}")
    
    return df_merged


def validate_time_range(start_time: int, end_time: int) -> Tuple[bool, Optional[str]]:
    if start_time >= end_time:
        return False, "start_time must be less than end_time"
    
    start_dt = pd.to_datetime(start_time, unit='ms')
    end_dt = pd.to_datetime(end_time, unit='ms')
    
    if (end_dt - start_dt).total_seconds() < 600:
        return False, "Time range must be at least 10 minutes"
    
    return True, None


def load_turbine_data(
    turbine: Turbines,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    preferred_source: str = 'db'
) -> Tuple[Optional[pd.DataFrame], str, Dict[str, str]]:
    """
    Load turbine data từ database hoặc file với fallback logic.
    
    Args:
        turbine: Turbine object
        start_time: Start time in milliseconds (optional, None means load all)
        end_time: End time in milliseconds (optional, None means load all)
        preferred_source: 'db' hoặc 'file' - nguồn ưu tiên
    
    Returns:
        Tuple of (DataFrame, data_source_used, error_info)
        - DataFrame: DataFrame với dữ liệu hoặc None nếu không có
        - data_source_used: 'db' hoặc 'file' - nguồn đã sử dụng
        - error_info: Dict chứa thông tin lỗi từ các nguồn đã thử
    """
    error_info = {}
    df = None
    data_source_used = None
    
    # Thử nguồn ưu tiên trước
    if preferred_source == 'file':
        logger.debug(f"Trying to load data from file for turbine {turbine.id}")
        if start_time is not None and end_time is not None:
            df = prepare_dataframe_from_files(turbine, start_time, end_time, DEFAULT_DATA_DIR)
        else:
            df = _load_all_data_from_files(turbine, DEFAULT_DATA_DIR)
        if df is not None and not df.empty:
            data_source_used = 'file'
        else:
            error_info['file'] = "No data found in files or files do not exist"
            logger.warning(f"Failed to load data from file for turbine {turbine.id}: {error_info['file']}")
    else:  # preferred_source == 'db'
        logger.debug(f"Trying to load data from database for turbine {turbine.id}")
        df = prepare_dataframe_from_factory_historical(turbine, start_time, end_time)
        if df is not None and not df.empty:
            data_source_used = 'db'
        else:
            error_info['db'] = "No data found in database" + (f" for the specified time range" if start_time and end_time else "")
            logger.warning(f"Failed to load data from database for turbine {turbine.id}: {error_info['db']}")
            
            # Nếu preferred là 'db' và không có dữ liệu, tự động thử 'file'
            if preferred_source == 'db':
                logger.debug(f"Attempting fallback to file source for turbine {turbine.id}")
                if start_time is not None and end_time is not None:
                    df_file = prepare_dataframe_from_files(turbine, start_time, end_time, DEFAULT_DATA_DIR)
                else:
                    df_file = _load_all_data_from_files(turbine, DEFAULT_DATA_DIR)
                if df_file is not None and not df_file.empty:
                    df = df_file
                    data_source_used = 'file'
                else:
                    error_info['file'] = "No data found in files or files do not exist"
                    logger.warning(f"Fallback to file also failed for turbine {turbine.id}: {error_info['file']}")
    
    return df, data_source_used, error_info


def _get_or_create_computation(
    turbine: Turbines,
    farm: Farm,
    computation_type: str,
    start_time: int,
    end_time: int,
    constants: Optional[Dict] = None
) -> Computation:

    Computation.objects.filter(
        turbine=turbine,
        farm=farm,
        computation_type=computation_type,
        is_latest=True
    ).update(is_latest=False)
    
    defaults = {
        'created_at': timezone.now(),
        'is_latest': True
    }
    
    if constants:
        if 'V_cutin' in constants:
            defaults['v_cutin'] = float(constants['V_cutin'])
        if 'V_cutout' in constants:
            defaults['v_cutout'] = float(constants['V_cutout'])
        if 'V_rated' in constants:
            defaults['v_rated'] = float(constants['V_rated'])
        if 'P_rated' in constants:
            defaults['p_rated'] = float(constants['P_rated'])
    
    computation, created = Computation.objects.update_or_create(
        turbine=turbine,
        farm=farm,
        computation_type=computation_type,
        start_time=start_time,
        end_time=end_time,
        defaults=defaults
    )
    
    if not created:
        computation.is_latest = True
        computation.save(update_fields=['is_latest'])
    
    return computation


@transaction.atomic
def save_computation_results(
    turbine: Turbines,
    farm: Farm,
    start_time: int,
    end_time: int,
    computation_result: Dict,
    constants: Optional[Dict] = None
) -> Dict[str, Computation]:
    """
    Lưu computation results vào database với từng computation type riêng biệt.
    
    IMPORTANT: Trước khi lưu data mới, xóa TẤT CẢ data cũ của computation với cùng
    start_time/end_time để đảm bảo không có data duplicate hoặc data cũ còn sót lại.
    """
    """
    Lưu computation results vào database với từng computation type riêng biệt.
    
    Args:
        turbine: Turbine object
        farm: Farm object
        start_time: Start time in milliseconds
        end_time: End time in milliseconds
        computation_result: Computation results dictionary
        constants: Turbine operating constants (V_cutin, V_cutout, V_rated, P_rated, Swept_area)
                   These will be saved to each Computation record for traceability.
    
    Returns:
        Dict với keys là computation_type và values là Computation objects đã lưu.
        Ví dụ: {
            'classification': Computation(...),
            'power_curve': Computation(...),
            'weibull': Computation(...),
            'indicators': Computation(...),
            'yaw_error': Computation(...)  # nếu có
        }
    """
    result_start_time = computation_result.get('start_time')
    result_end_time = computation_result.get('end_time')
    
    if result_start_time and result_start_time < 1e12:
        result_start_time = int(result_start_time * 1000)
    if result_end_time and result_end_time < 1e12:
        result_end_time = int(result_end_time * 1000)
    
    save_start_time = result_start_time if result_start_time else start_time
    save_end_time = result_end_time if result_end_time else end_time
    
    saved_computations = {}
    
    # Lưu Classification computation
    if 'classification' in computation_result:
        classification_computation = _get_or_create_computation(
            turbine, farm, 'classification', save_start_time, save_end_time, constants
        )
        save_classification(classification_computation, computation_result['classification'])
        saved_computations['classification'] = classification_computation
    
    # Lưu Power Curve computation
    if 'power_curves' in computation_result:
        power_curve_computation = _get_or_create_computation(
            turbine, farm, 'power_curve', save_start_time, save_end_time, constants
        )
        save_power_curves(power_curve_computation, computation_result['power_curves'])
        saved_computations['power_curve'] = power_curve_computation
    
    # Lưu Weibull computation
    if 'weibull' in computation_result:
        weibull_computation = _get_or_create_computation(
            turbine, farm, 'weibull', save_start_time, save_end_time, constants
        )
        save_weibull(weibull_computation, computation_result['weibull'])
        saved_computations['weibull'] = weibull_computation
    
    # Lưu Indicators computation
    indicators = computation_result.get('indicators', {})
    if indicators:
        indicators_computation = _get_or_create_computation(
            turbine, farm, 'indicators', save_start_time, save_end_time, constants
        )
        save_indicators(indicators_computation, indicators)
        saved_computations['indicators'] = indicators_computation
        
        # Lưu Yaw Error computation (nếu có)
        if 'YawLag' in indicators:
            yaw_error_computation = _get_or_create_computation(
                turbine, farm, 'yaw_error', save_start_time, save_end_time, constants
            )
            save_yaw_error(yaw_error_computation, indicators['YawLag'])
            saved_computations['yaw_error'] = yaw_error_computation
    
    return saved_computations


def save_power_curves(computation: Computation, power_curves: Dict):
    for mode, curve_data in power_curves.items():
        if mode == 'global':
            analysis, _ = PowerCurveAnalysis.objects.get_or_create(
                computation=computation,
                analysis_mode=mode,
                defaults={}
            )
            
            PowerCurveData.objects.filter(analysis=analysis).delete()
            
            for wind_speed, active_power in curve_data.items():
                PowerCurveData.objects.create(
                    analysis=analysis,
                    wind_speed=float(wind_speed),
                    active_power=float(active_power)
                )
        else:
            for split_value, curve_data in curve_data.items():
                analysis, _ = PowerCurveAnalysis.objects.get_or_create(
                    computation=computation,
                    analysis_mode=mode,
                    split_value=str(split_value),
                    defaults={}
                )
                
                PowerCurveData.objects.filter(analysis=analysis).delete()
                
                for wind_speed, active_power in curve_data.items():
                    PowerCurveData.objects.create(
                        analysis=analysis,
                        wind_speed=float(wind_speed),
                        active_power=float(active_power)
                    )


def save_classification(computation: Computation, classification: Dict):
    if 'classification_rates' in classification:
        ClassificationSummary.objects.filter(computation=computation).delete()
        
        classification_map = classification.get('classification_map', {})
        classification_rates = classification.get('classification_rates', {})
        
        total_points = sum(classification_rates.values())
        
        for status_code, count in classification_rates.items():
            if count > 0:
                status_name = classification_map.get(status_code, f'Status_{status_code}')
                percentage = (count / total_points * 100) if total_points > 0 else 0.0
                
                ClassificationSummary.objects.create(
                    computation=computation,
                    status_code=int(status_code),
                    status_name=status_name,
                    count=int(count),
                    percentage=float(percentage)
                )
    
    if 'classification_points' in classification:
        ClassificationPoint.objects.filter(computation=computation).delete()
        
        points_data = classification['classification_points']
        if isinstance(points_data, dict) and 'data' in points_data and 'index' in points_data:
            indices = points_data['index']
            data = points_data['data']
            
            wind_speed_idx = None
            active_power_idx = None
            classification_idx = None
            
            if 'columns' in points_data:
                columns = points_data['columns']
                try:
                    wind_speed_idx = columns.index('WIND_SPEED')
                    active_power_idx = columns.index('ACTIVE_POWER')
                    classification_idx = columns.index('classification')
                except ValueError:
                    pass
            
            if wind_speed_idx is not None and active_power_idx is not None and classification_idx is not None:
                points_to_create = []
                for i, idx in enumerate(indices):
                    if i < len(data):
                        row = data[i]
                        if len(row) > max(wind_speed_idx, active_power_idx, classification_idx):
                            try:
                                # Skip rows with NaN values (created by filter_error when wind speed changes too rapidly)
                                wind_speed_val = row[wind_speed_idx]
                                active_power_val = row[active_power_idx]
                                
                                # Check for NaN values
                                if pd.isna(wind_speed_val) or pd.isna(active_power_val):
                                    continue
                                
                                if isinstance(idx, pd.Timestamp):
                                    # Pandas Timestamp - convert to milliseconds
                                    timestamp_ms = int(idx.timestamp() * 1000)
                                elif isinstance(idx, (int, float)):
                                    # Handle different timestamp units
                                    if idx > 1e15:
                                        # Nanoseconds (from pandas DatetimeIndex.astype(int))
                                        timestamp_ms = int(idx / 1e6)
                                    elif idx > 1e13:
                                        # Microseconds
                                        timestamp_ms = int(idx / 1e3)
                                    elif idx > 1e12:
                                        # Already milliseconds
                                        timestamp_ms = int(idx)
                                    elif idx > 1e9:
                                        # Seconds (Unix timestamp)
                                        timestamp_ms = int(idx * 1000)
                                    else:
                                        # Try to parse as datetime string or other format
                                        timestamp_ms = int(pd.to_datetime(idx).timestamp() * 1000)
                                else:
                                    # Other types - try to convert to datetime
                                    timestamp_ms = int(pd.to_datetime(idx).timestamp() * 1000)
                                
                                if timestamp_ms:
                                    points_to_create.append(
                                        ClassificationPoint(
                                            computation=computation,
                                            timestamp=timestamp_ms,
                                            wind_speed=float(wind_speed_val),
                                            active_power=float(active_power_val),
                                            classification=int(row[classification_idx])
                                        )
                                    )
                            except (ValueError, TypeError, OverflowError):
                                continue
                
                if points_to_create:
                    ClassificationPoint.objects.bulk_create(points_to_create, batch_size=1000)


def save_indicators(computation: Computation, indicators: Dict):
    IndicatorData.objects.filter(computation=computation).delete()
    
    daily_production_list = indicators.pop('DailyProduction', [])
    capacity_factor_dict = indicators.pop('CapacityFactor', {})
    
    indicator_data = IndicatorData(
        computation=computation,
        average_wind_speed=float(indicators.get('AverageWindSpeed', 0.0)),
        reachable_energy=float(indicators.get('ReachableEnergy', 0.0)),
        real_energy=float(indicators.get('RealEnergy', 0.0)),
        loss_energy=float(indicators.get('LossEnergy', 0.0)),
        loss_percent=float(indicators.get('LossPercent', 0.0)),
        rated_power=float(indicators.get('RatedPower', 0.0)),
        tba=float(indicators.get('Tba', 0.0)),
        pba=float(indicators.get('Pba', 0.0)),
        stop_loss=float(indicators.get('StopLoss', 0.0)),
        partial_stop_loss=float(indicators.get('PartialStopLoss', 0.0)),
        under_production_loss=float(indicators.get('UnderProductionLoss', 0.0)),
        curtailment_loss=float(indicators.get('CurtailmentLoss', 0.0)),
        partial_curtailment_loss=float(indicators.get('PartialCurtailmentLoss', 0.0)),
        total_stop_points=int(indicators.get('TotalStopPoints', 0)),
        total_partial_stop_points=int(indicators.get('TotalPartialStopPoints', 0)),
        total_under_production_points=int(indicators.get('TotalUnderProductionPoints', 0)),
        total_curtailment_points=int(indicators.get('TotalCurtailmentPoints', 0)),
        mtbf=float(indicators.get('Mtbf')) if indicators.get('Mtbf') is not None else None,
        mttr=float(indicators.get('Mttr')) if indicators.get('Mttr') is not None else None,
        mttf=float(indicators.get('Mttf')) if indicators.get('Mttf') is not None else None,
        time_step=float(indicators.get('TimeStep', 600.0)),
        total_duration=float(indicators.get('TotalDuration', 0.0)),
        duration_without_error=float(indicators.get('DurationWithoutError', 0.0)),
        up_periods_count=float(indicators.get('UpPeriodsCount', 0.0)),
        down_periods_count=float(indicators.get('DownPeriodsCount', 0.0)),
        up_periods_duration=float(indicators.get('UpPerodsDuration', 0.0)),
        down_periods_duration=float(indicators.get('DownPerodsDuration', 0.0)),
        aep_weibull_turbine=float(indicators.get('AepWeibullTurbine', 0.0)),
        aep_weibull_wind_farm=float(indicators.get('AepWeibullWindFarm')) if indicators.get('AepWeibullWindFarm') is not None else None,
        aep_rayleigh_measured_4=float(indicators.get('AepRayleighMeasured4', 0.0)),
        aep_rayleigh_measured_5=float(indicators.get('AepRayleighMeasured5', 0.0)),
        aep_rayleigh_measured_6=float(indicators.get('AepRayleighMeasured6', 0.0)),
        aep_rayleigh_measured_7=float(indicators.get('AepRayleighMeasured7', 0.0)),
        aep_rayleigh_measured_8=float(indicators.get('AepRayleighMeasured8', 0.0)),
        aep_rayleigh_measured_9=float(indicators.get('AepRayleighMeasured9', 0.0)),
        aep_rayleigh_measured_10=float(indicators.get('AepRayleighMeasured10', 0.0)),
        aep_rayleigh_measured_11=float(indicators.get('AepRayleighMeasured11', 0.0)),
        aep_rayleigh_extrapolated_4=float(indicators.get('AepRayleighExtrapolated4', 0.0)),
        aep_rayleigh_extrapolated_5=float(indicators.get('AepRayleighExtrapolated5', 0.0)),
        aep_rayleigh_extrapolated_6=float(indicators.get('AepRayleighExtrapolated6', 0.0)),
        aep_rayleigh_extrapolated_7=float(indicators.get('AepRayleighExtrapolated7', 0.0)),
        aep_rayleigh_extrapolated_8=float(indicators.get('AepRayleighExtrapolated8', 0.0)),
        aep_rayleigh_extrapolated_9=float(indicators.get('AepRayleighExtrapolated9', 0.0)),
        aep_rayleigh_extrapolated_10=float(indicators.get('AepRayleighExtrapolated10', 0.0)),
        aep_rayleigh_extrapolated_11=float(indicators.get('AepRayleighExtrapolated11', 0.0)),
        yaw_misalignment=float(indicators.get('YawLag', {}).get('statistics', {}).get('mean_error')) if isinstance(indicators.get('YawLag'), dict) else None
    )
    indicator_data.save()
    
    if daily_production_list:
        DailyProduction.objects.filter(computation=computation).delete()
        daily_productions = []
        for dp in daily_production_list:
            if 'date' in dp and 'DailyProduction' in dp:
                try:
                    date = pd.to_datetime(dp['date']).date()
                    daily_productions.append(
                        DailyProduction(
                            computation=computation,
                            date=date,
                            daily_production=float(dp['DailyProduction'])
                        )
                    )
                except (ValueError, KeyError):
                    continue
        
        if daily_productions:
            DailyProduction.objects.bulk_create(daily_productions, batch_size=1000)
    
    if capacity_factor_dict:
        CapacityFactorData.objects.filter(computation=computation).delete()
        capacity_factors = []
        for wind_speed_bin, capacity_factor in capacity_factor_dict.items():
            capacity_factors.append(
                CapacityFactorData(
                    computation=computation,
                    wind_speed_bin=float(wind_speed_bin),
                    capacity_factor=float(capacity_factor)
                )
            )
        
        if capacity_factors:
            CapacityFactorData.objects.bulk_create(capacity_factors, batch_size=1000)

def save_weibull(computation: Computation, weibull_data: Dict):
    """Save weibull parameters to database"""
    if not isinstance(weibull_data, dict):
        return
    
    WeibullData.objects.filter(computation=computation).delete()
    
    scale = weibull_data.get('scale')
    shape = weibull_data.get('shape')
    
    if scale is not None and shape is not None:
        # Calculate mean wind speed from weibull parameters
        # Vmean = A * Gamma(1 + 1/k) where A=scale, k=shape
        import math
        try:
            mean_wind_speed = float(scale) * math.gamma(1 + 1.0 / float(shape))
        except (ValueError, ZeroDivisionError):
            mean_wind_speed = None
        
        WeibullData.objects.create(
            computation=computation,
            scale_parameter_a=float(scale),
            shape_parameter_k=float(shape),
            mean_wind_speed=mean_wind_speed
        )

def save_yaw_error(computation: Computation, yaw_lag: Dict):
    if not isinstance(yaw_lag, dict) or 'data' not in yaw_lag:
        return
    
    YawErrorData.objects.filter(computation=computation).delete()
    YawErrorStatistics.objects.filter(computation=computation).delete()
    
    yaw_data = yaw_lag.get('data', {})
    yaw_points = []
    for angle_str, frequency in yaw_data.items():
        try:
            angle = float(angle_str)
            yaw_points.append(
                YawErrorData(
                    computation=computation,
                    angle=angle,
                    frequency=float(frequency)
                )
            )
        except (ValueError, TypeError):
            continue
    
    if yaw_points:
        YawErrorData.objects.bulk_create(yaw_points, batch_size=1000)
    
    statistics = yaw_lag.get('statistics', {})
    if statistics:
        YawErrorStatistics.objects.update_or_create(
            computation=computation,
            defaults={
                'mean_error': float(statistics.get('mean_error', 0.0)),
                'median_error': float(statistics.get('median_error', 0.0)),
                'std_error': float(statistics.get('std_error', 0.0))
            }
        )


def replace_nan_with_none(obj):
    """
    Đệ quy thay thế NaN bằng None trong dict/list để JSON serializable.
    """
    if isinstance(obj, dict):
        return {k: replace_nan_with_none(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_nan_with_none(item) for item in obj]
    elif isinstance(obj, (pd.DataFrame, pd.Series)):
        # Convert DataFrame/Series to dict/list và xử lý NaN
        if isinstance(obj, pd.DataFrame):
            return {col: replace_nan_with_none(obj[col].tolist()) for col in obj.columns}
        else:
            return replace_nan_with_none(obj.tolist())
    elif isinstance(obj, (int, float, np.number)):
        return None if pd.isna(obj) else (int(obj) if isinstance(obj, (np.integer, np.int64)) else float(obj))
    elif hasattr(obj, 'item'):
        try:
            val = obj.item()
            return None if pd.isna(val) else val
        except:
            return obj
    else:
        return obj


def format_computation_output(computation_result: Dict) -> Dict:
    start_time = computation_result.get('start_time')
    end_time = computation_result.get('end_time')
    
    if start_time and start_time < 1e12:
        start_time = int(start_time * 1000)
    if end_time and end_time < 1e12:
        end_time = int(end_time * 1000)
    
    output = {
        'start_time': start_time,
        'end_time': end_time,
        'power_curves': computation_result.get('power_curves', {}),
        'classification': computation_result.get('classification', {}),
        'indicators': computation_result.get('indicators', {})
    }
    
    # Xử lý NaN để JSON serializable
    output = replace_nan_with_none(output)
    
    return output
