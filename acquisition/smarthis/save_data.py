from acquisition.models import FactoryHistorical
from facilities.models import Farm
from django.utils import timezone
from django.db import transaction
from datetime import timedelta, datetime
import pandas as pd
import numpy as np
import logging

from .get_data import get_data_smartHis, get_points_collection

logger = logging.getLogger(__name__)

FIELD_MAPPING = {
    'active_power': 'active_power',
    'wind_speed': 'wind_speed',
    'wind_dir': 'wind_dir',
    'air_temp': 'air_temp',
    'pressure': 'pressure',
    'hud': 'hud',
}

SYNC_LOOKBACK_HOURS = 24
BULK_CREATE_BATCH_SIZE = 500

def get_all_farms_with_smarthis():
    try:
        return list(Farm.objects.filter(
            smarthis__isnull=False
        ).values_list('id', flat=True).distinct())
    except Exception as e:
        logger.error(f"Failed to get farms with SmartHIS: {e}", exc_info=True)
        return []

def _normalize_timestamp(timestamp):
    if isinstance(timestamp, pd.Timestamp):
        timestamp_dt = timestamp.to_pydatetime()
    elif isinstance(timestamp, str):
        timestamp_dt = pd.to_datetime(timestamp).to_pydatetime()
    elif isinstance(timestamp, datetime):
        timestamp_dt = timestamp
    else:
        return None
    
    if not isinstance(timestamp_dt, datetime):
        return None
    
    if timestamp_dt.tzinfo:
        timestamp_dt = timestamp_dt.replace(tzinfo=None)
    
    return timestamp_dt

def process_factory_row(row, points_mapping):
    result = {}
    for point_name, column_name in points_mapping.items():
        if column_name in FIELD_MAPPING:
            db_field = FIELD_MAPPING[column_name]
            if point_name in row.index:
                value = row[point_name]
                if pd.notna(value):
                    try:
                        result[db_field] = float(value)
                    except (ValueError, TypeError):
                        pass
    return result

def process_turbine_row(row, points_mapping):
    wind_speed_values = []
    wind_dir_values = []
    
    for point_name, column_name in points_mapping.items():
        if point_name in row.index:
            value = row[point_name]
            if pd.notna(value):
                try:
                    float_value = float(value)
                    if column_name.startswith('wind_speed'):
                        wind_speed_values.append(float_value)
                    elif column_name.startswith('wind_dir'):
                        wind_dir_values.append(float_value)
                except (ValueError, TypeError):
                    pass
    
    result = {}
    if wind_speed_values:
        result['wind_speed'] = float(np.mean(wind_speed_values))
    if wind_dir_values:
        result['wind_dir'] = float(np.mean(wind_dir_values))
    
    return result

def save_farm_data_to_db(farm_id):
    try:
        farm = Farm.objects.get(id=farm_id)
        
        end_time = timezone.now()
        start_time = end_time - timedelta(hours=SYNC_LOOKBACK_HOURS)
        time_range = [start_time, end_time]
        
        points_factory_mapping = get_points_collection(farm_id, target='factory')
        points_turbines_mapping = get_points_collection(farm_id, target='turbines')
        
        if not points_factory_mapping and not points_turbines_mapping:
            logger.warning(f"No active points found for farm {farm_id}")
            return {
                'success': False,
                'farm_id': farm_id,
                'error': 'No active points found',
                'created': 0,
                'skipped': 0,
                'errors': 0
            }
        
        df_factory = pd.DataFrame()
        df_turbines = pd.DataFrame()
        
        if points_factory_mapping:
            try:
                df_factory = get_data_smartHis(farm_id, 'factory', time_range)
            except Exception as e:
                logger.error(f"Failed to get factory data for farm {farm_id}: {e}", exc_info=True)
        
        if points_turbines_mapping:
            try:
                df_turbines = get_data_smartHis(farm_id, 'turbines', time_range)
            except Exception as e:
                logger.error(f"Failed to get turbines data for farm {farm_id}: {e}", exc_info=True)
        
        if df_factory.empty and df_turbines.empty:
            logger.warning(f"No data returned for farm {farm_id}")
            return {
                'success': False,
                'farm_id': farm_id,
                'error': 'No data returned',
                'created': 0,
                'skipped': 0,
                'errors': 0
            }
        
        all_timestamps = set()
        if not df_factory.empty:
            all_timestamps.update(df_factory.index)
        if not df_turbines.empty:
            all_timestamps.update(df_turbines.index)
        
        if not all_timestamps:
            logger.warning(f"No timestamp found in data for farm {farm_id}")
            return {
                'success': False,
                'farm_id': farm_id,
                'error': 'No timestamp found',
                'created': 0,
                'skipped': 0,
                'errors': 0
            }
        
        sorted_timestamps = sorted(all_timestamps)
        normalized_timestamps = {}
        valid_timestamps = []
        
        for timestamp in sorted_timestamps:
            timestamp_dt = _normalize_timestamp(timestamp)
            if timestamp_dt:
                normalized_timestamps[timestamp] = timestamp_dt
                valid_timestamps.append(timestamp_dt)
            else:
                logger.warning(f"Invalid timestamp for farm {farm_id}: {timestamp}")
        
        if not valid_timestamps:
            logger.warning(f"No valid timestamps for farm {farm_id}")
            return {
                'success': False,
                'farm_id': farm_id,
                'error': 'No valid timestamps',
                'created': 0,
                'skipped': 0,
                'errors': 0
            }
        
        existing_records = FactoryHistorical.objects.filter(
            farm=farm,
            time_stamp__in=valid_timestamps
        ).values_list('time_stamp', flat=True)
        
        existing_timestamps_set = set(existing_records)
        
        records_to_create = []
        seen_timestamps = set()
        stats = {'created': 0, 'skipped': 0, 'errors': 0}
        
        for timestamp in sorted_timestamps:
            timestamp_dt = normalized_timestamps.get(timestamp)
            if not timestamp_dt:
                stats['errors'] += 1
                continue
            
            if timestamp_dt in existing_timestamps_set:
                stats['skipped'] += 1
                continue
            
            if timestamp_dt in seen_timestamps:
                stats['skipped'] += 1
                continue
            
            seen_timestamps.add(timestamp_dt)
            
            data_point = {}
            
            if not df_factory.empty and timestamp in df_factory.index:
                factory_row = df_factory.loc[timestamp]
                factory_data_row = process_factory_row(factory_row, points_factory_mapping)
                data_point.update(factory_data_row)
            
            if not df_turbines.empty and timestamp in df_turbines.index:
                turbines_row = df_turbines.loc[timestamp]
                turbine_data_row = process_turbine_row(turbines_row, points_turbines_mapping)
                data_point.update(turbine_data_row)
            
            if not data_point:
                stats['skipped'] += 1
                continue
            
            records_to_create.append(
                FactoryHistorical(
                    farm=farm,
                    time_stamp=timestamp_dt,
                    **data_point
                )
            )
        
        if records_to_create:
            unique_records = {}
            for record in records_to_create:
                key = (farm.id, record.time_stamp)
                if key not in unique_records:
                    unique_records[key] = record
                else:
                    stats['skipped'] += 1
            
            records_to_create = list(unique_records.values())
            
            if records_to_create:
                try:
                    with transaction.atomic():
                        created_objects = FactoryHistorical.objects.bulk_create(
                            records_to_create,
                            ignore_conflicts=True,
                            batch_size=BULK_CREATE_BATCH_SIZE
                        )
                        stats['created'] = len(created_objects)
                except Exception as e:
                    logger.error(f"Failed to bulk create records for farm {farm_id}: {e}", exc_info=True)
                    stats['errors'] += len(records_to_create)
        
        logger.warning(f"Saved data for farm {farm_id}: created={stats['created']}, skipped={stats['skipped']}, errors={stats['errors']}")
        
        return {
            'success': True,
            'farm_id': farm_id,
            'error': None,
            **stats
        }
        
    except Farm.DoesNotExist:
        logger.error(f"Farm with ID {farm_id} not found")
        return {
            'success': False,
            'farm_id': farm_id,
            'error': 'Farm not found',
            'created': 0,
            'skipped': 0,
            'errors': 1
        }
    except Exception as e:
        logger.error(f"Failed to save data for farm {farm_id}: {e}", exc_info=True)
        return {
            'success': False,
            'farm_id': farm_id,
            'error': str(e),
            'created': 0,
            'skipped': 0,
            'errors': 1
        }

def save_all_farms_data_to_db():
    try:
        farm_ids = get_all_farms_with_smarthis()
        
        if not farm_ids:
            logger.warning("No farms with SmartHIS configuration found")
            return {
                'success': False,
                'error': 'No farms found',
                'total_farms': 0,
                'processed': 0,
                'total_created': 0,
                'total_skipped': 0,
                'total_errors': 0
            }
        
        logger.warning(f"Starting to save data for {len(farm_ids)} farms")
        
        total_stats = {
            'total_created': 0,
            'total_skipped': 0,
            'total_errors': 0,
            'processed': 0
        }
        
        for farm_id in farm_ids:
            try:
                result = save_farm_data_to_db(farm_id)
                if result.get('success'):
                    total_stats['total_created'] += result.get('created', 0)
                    total_stats['total_skipped'] += result.get('skipped', 0)
                    total_stats['total_errors'] += result.get('errors', 0)
                else:
                    total_stats['total_errors'] += 1
                total_stats['processed'] += 1
            except Exception as e:
                logger.error(f"Error processing farm {farm_id}: {e}", exc_info=True)
                total_stats['total_errors'] += 1
        
        logger.warning(f"Completed saving data: processed={total_stats['processed']}/{len(farm_ids)}, "
                      f"created={total_stats['total_created']}, skipped={total_stats['total_skipped']}, "
                      f"errors={total_stats['total_errors']}")
        
        return {
            'success': True,
            'error': None,
            'total_farms': len(farm_ids),
            **total_stats
        }
        
    except Exception as e:
        logger.error(f"Failed to save all farms data: {e}", exc_info=True)
        return {
            'success': False,
            'error': str(e),
            'total_farms': 0,
            'processed': 0,
            'total_created': 0,
            'total_skipped': 0,
            'total_errors': 1
        }
