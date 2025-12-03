"""
Functions for syncing data from InfluxDB to Django database
"""
import logging
from datetime import timedelta, datetime
from typing import Dict, Any
from django.utils import timezone
from django.db import transaction
import pytz
import pandas as pd

from .influx_service import InfluxDBService
from .config_manager import sync_config_manager
from acquisition.models import FactoryHistorical
from facilities.models import Farm
from ._header import (
    DEFAULT_TIMEZONE, 
    DATA_RESAMPLE_INTERVAL,
    DEFAULT_PPC_ID,
    DEFAULT_DATA_TYPES,
    SYNC_LOOKBACK_HOURS,
    DEFAULT_FARM_ID,
    DATA_FIELD_MAPPING
)

logger = logging.getLogger(__name__)

def convert_utc_to_local(utc_dt):
    """
    Chuyển đổi từ UTC sang local time
    
    Args:
        utc_dt: datetime object (có thể có timezone hoặc naive)
        
    Returns:
        datetime object ở local time (không có timezone)
    """
    # Nếu không có timezone, coi như UTC
    if utc_dt.tzinfo is None:
        utc_dt = pytz.UTC.localize(utc_dt)
    else:
        # Nếu có timezone, convert về UTC rồi về local
        utc_dt = utc_dt.astimezone(pytz.UTC)
    
    # Chuyển sang local timezone
    local_tz = pytz.timezone(DEFAULT_TIMEZONE)
    local_dt = utc_dt.astimezone(local_tz)
    
    # Loại bỏ timezone info để lưu vào database
    return local_dt.replace(tzinfo=None)

# Cache InfluxDBService instance để tránh tạo mới nhiều lần
_influx_service_cache = None

def _get_influx_service():
    """Lấy InfluxDBService instance (cached)"""
    global _influx_service_cache
    if _influx_service_cache is None:
        _influx_service_cache = InfluxDBService()
    return _influx_service_cache

def get_data_resampled(ppc_id, data_type, start_time, end_time):
    """
    Lấy dữ liệu từ InfluxDB, chuyển timezone và resample về 15 phút
    
    Args:
        ppc_id: ID của PPC
        data_type: Loại dữ liệu (power, wind_speed, etc.)
        start_time: Thời gian bắt đầu
        end_time: Thời gian kết thúc
    
    Returns:
        DataFrame với index là local time (resampled to 15 minutes)
    """
    try:
        influx_service = _get_influx_service()
        
        # Lấy dữ liệu
        data = influx_service.get_data(
            ppc_id=ppc_id,
            data_type=data_type,
            start_time=start_time,
            end_time=end_time
        )
        
        if not data:
            logger.warning(f"No data returned for {ppc_id}_{data_type}")
            return pd.DataFrame()
        
        # Chuyển đổi thành DataFrame
        df = pd.DataFrame(data)
        
        # Kiểm tra DataFrame có dữ liệu không
        if df.empty or 'time' not in df.columns:
            logger.warning(f"Empty DataFrame or missing 'time' column for {ppc_id}_{data_type}")
            return pd.DataFrame()
        
        # Xử lý timezone: UTC -> Local (tối ưu với vectorized operations)
        df['time'] = pd.to_datetime(df['time'], utc=True)
        local_tz = pytz.timezone(DEFAULT_TIMEZONE)
        df['time'] = df['time'].dt.tz_convert(local_tz).dt.tz_localize(None)
        df.set_index('time', inplace=True)
        
        # Resample về 15 phút
        df_resampled = df['value'].resample(DATA_RESAMPLE_INTERVAL).mean()
        df_final = pd.DataFrame({'value': df_resampled}).dropna()
        
        return df_final
        
    except Exception as e:
        logger.error(f"Error getting and resampling data for {ppc_id}_{data_type}: {e}")
        return pd.DataFrame()

def get_multiple_data_types_resampled(ppc_id, data_types, start_time, end_time):
    """
    Lấy nhiều loại dữ liệu từ InfluxDB cùng lúc với vòng for
    Mỗi data_type được gọi riêng để tránh quá tải
    
    Args:
        ppc_id: ID của PPC
        data_types: List các loại dữ liệu ['power', 'wind_speed', etc.]
        start_time: Thời gian bắt đầu
        end_time: Thời gian kết thúc
    
    Returns:
        DataFrame với nhiều columns, mỗi column tương ứng với một data_type
    """
    try:
        all_dataframes = []
        
        for data_type in data_types:
            df = get_data_resampled(ppc_id, data_type, start_time, end_time)
            if not df.empty:
                df.rename(columns={'value': data_type}, inplace=True)
                all_dataframes.append(df)
        
        if not all_dataframes:
            return pd.DataFrame()
        
        # Merge tất cả DataFrames theo index (time)
        df_merged = pd.concat(all_dataframes, axis=1)
        
        return df_merged
        
    except Exception as e:
        logger.error(f"Error getting multiple data types for {ppc_id}: {e}")
        return pd.DataFrame()

def sync_multiple_data_types_to_db():
    """
    Hàm đồng bộ nhiều loại dữ liệu vào database
    Sử dụng get_multiple_data_types_resampled để lấy dữ liệu
    Lưu vào FactoryHistorical với logic bỏ qua duplicate
    
    Returns:
        Dict với thông tin kết quả sync
    """
    try:
        # Lấy requests từ config, nếu không có thì dùng default
        requests = sync_config_manager.get_requests()
        if not requests:
            # Fallback về default nếu không có requests trong config
            requests = [{'ppc_id': DEFAULT_PPC_ID, 'data_type': dt} for dt in DEFAULT_DATA_TYPES]
            logger.warning(f"No requests found in config, using defaults: PPC {DEFAULT_PPC_ID}, data_types {DEFAULT_DATA_TYPES}")
        
        # Thời gian từ _header.py
        end_time = timezone.now()
        start_time = end_time - timedelta(hours=SYNC_LOOKBACK_HOURS)
        
        start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        end_time_str = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # Lấy data mapping từ config hoặc dùng default từ _header.py
        config_mapping = sync_config_manager.get_data_mapping()
        data_mapping = {**DATA_FIELD_MAPPING, **config_mapping}  # Config override defaults
        
        # Danh sách các field hợp lệ trong FactoryHistorical model
        valid_fields = {'active_power', 'wind_speed', 'wind_dir', 'air_temp', 'pressure', 'hud'}
        
        # Stats tổng hợp cho tất cả requests
        total_stats = {
            'total_processed': 0,
            'created': 0,
            'skipped': 0,
            'errors': 0
        }
        
        # Nhóm requests theo ppc_id để xử lý hiệu quả hơn
        from collections import defaultdict
        ppc_requests = defaultdict(list)
        for request in requests:
            ppc_id = request.get('ppc_id')
            data_type = request.get('data_type')
            
            if not ppc_id or not data_type:
                logger.warning(f"Invalid request: {request}, skipping")
                continue
            
            ppc_requests[ppc_id].append(data_type)
        
        processed_ppcs = set()
        all_data_types = set()
        
        # Xử lý từng PPC
        for ppc_id, data_types in ppc_requests.items():
            processed_ppcs.add(ppc_id)
            all_data_types.update(data_types)
            
            # Lấy dữ liệu cho tất cả data_types của PPC cùng lúc
            df = get_multiple_data_types_resampled(ppc_id, data_types, start_time_str, end_time_str)
            
            if df.empty:
                logger.warning(f"No data returned from InfluxDB for PPC {ppc_id}, data_types {data_types}")
                continue
            
            logger.warning(f"Retrieved {len(df)} records from InfluxDB for PPC {ppc_id}")
            
            # Lấy farm_id từ factory_mapping dựa trên ppc_id
            farm_id = sync_config_manager.get_factory_id(ppc_id)
            if farm_id is None:
                farm_id = DEFAULT_FARM_ID
                logger.warning(f"No factory mapping found for PPC {ppc_id}, using default farm_id {DEFAULT_FARM_ID}")
            
            # Lấy farm (cache để tránh query nhiều lần)
            try:
                farm = Farm.objects.get(id=farm_id)
            except Farm.DoesNotExist:
                logger.error(f"Farm with ID {farm_id} (mapped from PPC {ppc_id}) not found, skipping PPC")
                total_stats['errors'] += 1
                continue
            
            # Chuẩn bị dữ liệu để bulk insert (tối ưu performance)
            records_to_create = []
            existing_timestamps = set()
            
            # Lấy danh sách timestamps đã tồn tại để tránh duplicate
            timestamps = [ts.to_pydatetime() if isinstance(ts, pd.Timestamp) else ts for ts in df.index]
            existing_records = FactoryHistorical.objects.filter(
                farm=farm,
                time_stamp__in=timestamps
            ).values_list('time_stamp', flat=True)
            existing_timestamps = {ts for ts in existing_records}
            
            # Xử lý từng row và chuẩn bị bulk create
            for timestamp, row in df.iterrows():
                try:
                    # Chuyển đổi timestamp sang datetime
                    if isinstance(timestamp, pd.Timestamp):
                        timestamp = timestamp.to_pydatetime()
                    elif not isinstance(timestamp, datetime):
                        logger.warning(f"Invalid timestamp type: {type(timestamp)}, value: {timestamp}")
                        total_stats['skipped'] += 1
                        total_stats['total_processed'] += 1
                        continue
                    
                    # Bỏ qua nếu đã tồn tại
                    if timestamp in existing_timestamps:
                        total_stats['skipped'] += 1
                        total_stats['total_processed'] += 1
                        continue
                    
                    # Tạo dict dữ liệu để lưu - map tên column sang field name
                    data_point = {}
                    
                    for col in df.columns:
                        if pd.notna(row[col]):
                            # Map tên column (data_type) sang field name từ config hoặc default
                            field_name = data_mapping.get(col, col)
                            # Chỉ thêm field nếu là field hợp lệ trong model
                            if field_name in valid_fields:
                                data_point[field_name] = float(row[col])
                    
                    # Bỏ qua nếu không có dữ liệu nào
                    if not data_point:
                        total_stats['skipped'] += 1
                        total_stats['total_processed'] += 1
                        continue
                    
                    # Tạo FactoryHistorical instance để bulk create
                    records_to_create.append(
                        FactoryHistorical(
                            farm=farm,
                            time_stamp=timestamp,
                            **data_point
                        )
                    )
                    total_stats['total_processed'] += 1
                        
                except Exception as e:
                    logger.error(f"Failed to prepare record at {timestamp} for PPC {ppc_id}: {e}", exc_info=True)
                    total_stats['errors'] += 1
            
            # Bulk create tất cả records cùng lúc (tối ưu performance)
            if records_to_create:
                try:
                    with transaction.atomic():
                        created_objects = FactoryHistorical.objects.bulk_create(
                            records_to_create,
                            ignore_conflicts=True  # Tránh lỗi nếu có duplicate
                        )
                        total_stats['created'] += len(created_objects)
                except Exception as e:
                    logger.error(f"Failed to bulk create records for PPC {ppc_id}: {e}", exc_info=True)
                    total_stats['errors'] += len(records_to_create)
        
        logger.warning(f"Sync completed: created={total_stats['created']}, skipped={total_stats['skipped']}, errors={total_stats['errors']}, total={total_stats['total_processed']}")
        
        return {
            'success': True,
            'error': None,
            'stats': total_stats,
            'time_range': {
                'start': start_time,
                'end': end_time
            },
            'ppcs': list(processed_ppcs),
            'data_types': list(all_data_types)
        }
        
    except Exception as e:
        logger.error(f"Multiple data types sync failed: {e}")
        return {
            'success': False,
            'error': str(e),
            'stats': {
                'total_processed': 0,
                'created': 0,
                'skipped': 0,
                'errors': 1
            }
        }
