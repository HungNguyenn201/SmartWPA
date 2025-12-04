import logging
import threading
from collections import deque
from typing import Dict, Any, Optional
from django.db import transaction
import pandas as pd

from acquisition.models import FactoryHistorical
from facilities.models import Farm, Turbines
from ._header import CACHE_SIZE, RESAMPLE_INTERVAL

logger = logging.getLogger(__name__)


class ModbusDataStorage:
    CACHE_SIZE = CACHE_SIZE
    RESAMPLE_INTERVAL = RESAMPLE_INTERVAL
    def __init__(self, factory_id: int = 1):
        self.factory_id = factory_id
        self.factory: Optional[Farm] = None
        self._cache: deque = deque(maxlen=self.CACHE_SIZE)
        self._turbine_cache: Dict[int, deque] = {}
        self._lock = threading.Lock()
        
        self.field_mapping = {
            'total_power': 'active_power',
            'wind_speed': 'wind_speed',
            'wind_speed_average': 'wind_speed',
            'wind_direction': 'wind_dir',
            'air_temperature': 'air_temp',
            'air_pressure': 'pressure',
            'humidity': 'hud',
        }
        
        self._turbine_name_cache: Dict[str, Optional[Turbines]] = {}
        
        self._load_factory()
        self._load_turbines()
    
    def _load_factory(self):
        try:
            self.factory = Farm.objects.get(id=self.factory_id)
        except Farm.DoesNotExist:
            logger.error("Farm with ID %d not found", self.factory_id)
            self.factory = None
    
    def _load_turbines(self):
        if not self.factory:
            return
        
        try:
            turbines = Turbines.objects.filter(farm=self.factory, is_active=True)
            for turbine in turbines:
                self._turbine_name_cache[turbine.name] = turbine
                if turbine.id not in self._turbine_cache:
                    self._turbine_cache[turbine.id] = deque(maxlen=self.CACHE_SIZE)
        except Exception as e:
            logger.error(f"Failed to load turbines for farm {self.factory_id}: {e}", exc_info=True)
    
    def _parse_turbine_from_key(self, data_key: str) -> tuple[Optional[int], str]:
        if data_key.startswith('wtg_'):
            try:
                parts = data_key.split('_')
                if len(parts) >= 3:
                    turbine_num = int(parts[1])
                    field_part = '_'.join(parts[2:])
                    return turbine_num, field_part
            except (ValueError, IndexError):
                pass
        
        return None, data_key
    
    def _get_turbine_by_number(self, turbine_number: int) -> Optional[Turbines]:
        if not self.factory:
            return None
        
        turbine_name_patterns = [
            f"Turbine{turbine_number:02d}",
            f"WTG{turbine_number:02d}",
            f"WTG_{turbine_number:02d}",
            f"Turbine{turbine_number}",
            f"WTG{turbine_number}",
        ]
        
        for pattern in turbine_name_patterns:
            if pattern in self._turbine_name_cache:
                return self._turbine_name_cache[pattern]
        
        try:
            turbines = Turbines.objects.filter(farm=self.factory, is_active=True)
            for turbine in turbines:
                if turbine.name in turbine_name_patterns or any(pattern in turbine.name for pattern in turbine_name_patterns):
                    self._turbine_name_cache[turbine.name] = turbine
                    return turbine
        except Exception as e:
            logger.error(f"Failed to find turbine {turbine_number} for farm {self.factory_id}: {e}", exc_info=True)
        
        return None
    
    def add_to_cache(self, data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        if not self.factory:
            return {
                'cached': False,
                'error': f'Farm with ID {self.factory_id} not found',
                'cache_size': len(self._cache),
                'ready_to_save': False
            }
        
        farm_cache_record = {}
        turbine_cache_records: Dict[int, Dict[str, Any]] = {}
        timestamp = None
        
        for data_key, result in data.items():
            if not (result.get("ok") and result.get("quality") == "good" and result.get("value") is not None):
                continue
            
            if timestamp is None and result.get("ts"):
                timestamp = result["ts"]
            
            turbine_num, field_key = self._parse_turbine_from_key(data_key)
            field_name = self.field_mapping.get(field_key)
            
            if not field_name:
                continue
            
            if turbine_num is not None:
                if turbine_num not in turbine_cache_records:
                    turbine_cache_records[turbine_num] = {}
                turbine_cache_records[turbine_num][field_name] = result["value"]
            else:
                farm_cache_record[field_name] = result["value"]
        
        if timestamp is None:
            return {
                'cached': False,
                'error': 'No timestamp found in data',
                'cache_size': len(self._cache),
                'ready_to_save': False
            }
        
        cached_count = 0
        ready_to_save = False
        
        with self._lock:
            if farm_cache_record:
                farm_cache_record['time_stamp'] = timestamp
                self._cache.append(farm_cache_record)
                cached_count += 1
                if len(self._cache) >= self.CACHE_SIZE:
                    ready_to_save = True
            
            for turbine_num, turbine_data in turbine_cache_records.items():
                turbine = self._get_turbine_by_number(turbine_num)
                if not turbine:
                    logger.warning(f"Turbine {turbine_num} not found for farm {self.factory_id}, skipping turbine data")
                    continue
                
                if turbine.id not in self._turbine_cache:
                    self._turbine_cache[turbine.id] = deque(maxlen=self.CACHE_SIZE)
                
                turbine_data['time_stamp'] = timestamp
                turbine_data['turbine_id'] = turbine.id
                self._turbine_cache[turbine.id].append(turbine_data)
                cached_count += 1
                
                if len(self._turbine_cache[turbine.id]) >= self.CACHE_SIZE:
                    ready_to_save = True
        
        return {
            'cached': True,
            'cache_size': len(self._cache),
            'turbine_cached': sum(len(cache) for cache in self._turbine_cache.values()),
            'ready_to_save': ready_to_save,
            'error': None
        }
    
    def _resample_cache(self) -> Optional[Dict[str, Any]]:
        try:
            cache_list = list(self._cache)
            if not cache_list:
                return None
            
            df = pd.DataFrame(cache_list)
            if df.empty:
                return None
            
            df['time_stamp'] = pd.to_datetime(df['time_stamp'])
            df = df.set_index('time_stamp').sort_index()
            
            df_resampled = df.resample(self.RESAMPLE_INTERVAL).mean()
            if df_resampled.empty:
                return None
            
            resampled_record = df_resampled.iloc[-1].to_dict()
            resampled_timestamp = df_resampled.index[-1]
            
            if resampled_timestamp.tzinfo:
                resampled_timestamp = resampled_timestamp.replace(tzinfo=None)
            
            resampled_record['time_stamp'] = resampled_timestamp
            
            return resampled_record
            
        except Exception as e:
            logger.error("Error resampling cache: %s", e)
            return None
    
    def _resample_turbine_cache(self, turbine_id: int) -> Optional[Dict[str, Any]]:
        if turbine_id not in self._turbine_cache:
            return None
        
        try:
            cache_list = list(self._turbine_cache[turbine_id])
            if not cache_list:
                return None
            
            df = pd.DataFrame(cache_list)
            if df.empty:
                return None
            
            df['time_stamp'] = pd.to_datetime(df['time_stamp'])
            df = df.set_index('time_stamp').sort_index()
            
            df_resampled = df.resample(self.RESAMPLE_INTERVAL).mean()
            if df_resampled.empty:
                return None
            
            resampled_record = df_resampled.iloc[-1].to_dict()
            resampled_timestamp = df_resampled.index[-1]
            
            if resampled_timestamp.tzinfo:
                resampled_timestamp = resampled_timestamp.replace(tzinfo=None)
            
            resampled_record['time_stamp'] = resampled_timestamp
            resampled_record['turbine_id'] = turbine_id
            
            return resampled_record
            
        except Exception as e:
            logger.error(f"Error resampling turbine {turbine_id} cache: {e}")
            return None
    
    def save_from_cache(self) -> Dict[str, Any]:
        if not self.factory:
            return {
                'success': False,
                'error': f'Farm with ID {self.factory_id} not found',
                'created': 0,
                'skipped': 0,
                'errors': 1,
                'cache_cleared': False
            }
        
        with self._lock:
            farm_ready = len(self._cache) >= self.CACHE_SIZE
            turbine_ready = any(len(cache) >= self.CACHE_SIZE for cache in self._turbine_cache.values())
            
            if not farm_ready and not turbine_ready:
                return {
                    'success': False,
                    'error': f'Cache not ready: farm={len(self._cache)}/{self.CACHE_SIZE}, turbines={sum(len(c) for c in self._turbine_cache.values())}',
                    'created': 0,
                    'skipped': 0,
                    'errors': 0,
                    'cache_cleared': False
                }
            
            total_created = 0
            total_skipped = 0
            total_errors = 0
            
            try:
                with transaction.atomic():
                    records_to_create = []
                    
                    if farm_ready:
                        resampled_record = self._resample_cache()
                        if resampled_record:
                            timestamp = resampled_record.pop('time_stamp')
                            data_point = resampled_record
                            
                            existing = FactoryHistorical.objects.filter(
                                farm=self.factory,
                                turbine__isnull=True,
                                time_stamp=timestamp
                            ).exists()
                            
                            if not existing:
                                records_to_create.append(
                                    FactoryHistorical(
                                        farm=self.factory,
                                        turbine=None,
                                        time_stamp=timestamp,
                                        **data_point
                                    )
                                )
                            else:
                                total_skipped += 1
                            
                            self._cache.clear()
                    
                    for turbine_id in list(self._turbine_cache.keys()):
                        if len(self._turbine_cache[turbine_id]) >= self.CACHE_SIZE:
                            resampled_record = self._resample_turbine_cache(turbine_id)
                            if resampled_record:
                                timestamp = resampled_record.pop('time_stamp')
                                turbine_id_val = resampled_record.pop('turbine_id')
                                data_point = resampled_record
                                
                                try:
                                    turbine = Turbines.objects.get(id=turbine_id_val)
                                    
                                    existing = FactoryHistorical.objects.filter(
                                        farm=self.factory,
                                        turbine=turbine,
                                        time_stamp=timestamp
                                    ).exists()
                                    
                                    if not existing:
                                        records_to_create.append(
                                            FactoryHistorical(
                                                farm=self.factory,
                                                turbine=turbine,
                                                time_stamp=timestamp,
                                                **data_point
                                            )
                                        )
                                    else:
                                        total_skipped += 1
                                except Turbines.DoesNotExist:
                                    logger.error(f"Turbine with ID {turbine_id_val} not found")
                                    total_errors += 1
                                
                                self._turbine_cache[turbine_id].clear()
                    
                    if records_to_create:
                        created_objects = FactoryHistorical.objects.bulk_create(
                            records_to_create,
                            ignore_conflicts=True
                        )
                        total_created = len(created_objects)
                    
                    return {
                        'success': True,
                        'error': None,
                        'created': total_created,
                        'skipped': total_skipped,
                        'errors': total_errors,
                        'cache_cleared': True
                    }
                    
            except Exception as e:
                logger.error(f"Failed to save data from cache: {e}", exc_info=True)
                return {
                    'success': False,
                    'error': str(e),
                    'created': 0,
                    'skipped': 0,
                    'errors': 1,
                    'cache_cleared': False
                }
    
    def save_direct(self, data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        if not self.factory:
            return {
                'success': False,
                'error': f'Farm with ID {self.factory_id} not found',
                'created': 0,
                'skipped': 0,
                'errors': 1
            }
        
        farm_data_point = {}
        turbine_data_points: Dict[int, Dict[str, Any]] = {}
        timestamp = None
        
        for data_key, result in data.items():
            if not (result.get("ok") and result.get("quality") == "good" and result.get("value") is not None):
                continue
            
            if timestamp is None and result.get("ts"):
                timestamp = result["ts"]
            
            turbine_num, field_key = self._parse_turbine_from_key(data_key)
            field_name = self.field_mapping.get(field_key)
            
            if not field_name:
                continue
            
            if turbine_num is not None:
                turbine = self._get_turbine_by_number(turbine_num)
                if turbine:
                    if turbine.id not in turbine_data_points:
                        turbine_data_points[turbine.id] = {}
                    turbine_data_points[turbine.id][field_name] = result["value"]
            else:
                farm_data_point[field_name] = result["value"]
        
        if timestamp is None:
            return {
                'success': False,
                'error': 'No timestamp found in data',
                'created': 0,
                'skipped': 0,
                'errors': 1
            }
        
        if not farm_data_point and not turbine_data_points:
            return {
                'success': False,
                'error': 'No valid data to save',
                'created': 0,
                'skipped': 0,
                'errors': 1
            }
        
        try:
            with transaction.atomic():
                records_to_create = []
                
                if farm_data_point:
                    existing = FactoryHistorical.objects.filter(
                        farm=self.factory,
                        turbine__isnull=True,
                        time_stamp=timestamp
                    ).exists()
                    
                    if not existing:
                        records_to_create.append(
                            FactoryHistorical(
                                farm=self.factory,
                                turbine=None,
                                time_stamp=timestamp,
                                **farm_data_point
                            )
                        )
                
                for turbine_id, turbine_data in turbine_data_points.items():
                    try:
                        turbine = Turbines.objects.get(id=turbine_id)
                        
                        existing = FactoryHistorical.objects.filter(
                            farm=self.factory,
                            turbine=turbine,
                            time_stamp=timestamp
                        ).exists()
                        
                        if not existing:
                            records_to_create.append(
                                FactoryHistorical(
                                    farm=self.factory,
                                    turbine=turbine,
                                    time_stamp=timestamp,
                                    **turbine_data
                                )
                            )
                    except Turbines.DoesNotExist:
                        logger.error(f"Turbine with ID {turbine_id} not found")
                
                if records_to_create:
                    created_objects = FactoryHistorical.objects.bulk_create(
                        records_to_create,
                        ignore_conflicts=True
                    )
                    return {
                        'success': True,
                        'error': None,
                        'created': len(created_objects),
                        'skipped': len(records_to_create) - len(created_objects),
                        'errors': 0,
                        'timestamp': timestamp
                    }
                else:
                    return {
                        'success': True,
                        'error': None,
                        'created': 0,
                        'skipped': 1,
                        'errors': 0,
                        'timestamp': timestamp
                    }
        
        except Exception as e:
            logger.error(f"Failed to save data to database: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'created': 0,
                'skipped': 0,
                'errors': 1
            }
    
    def get_cache_status(self) -> Dict[str, Any]:
        with self._lock:
            cache_list = list(self._cache)
            size = len(cache_list)
            
            if size == 0:
                return {
                    'size': 0,
                    'max_size': self.CACHE_SIZE,
                    'ready_to_save': False,
                    'oldest_timestamp': None,
                    'newest_timestamp': None
                }
            
            timestamps = [r.get('time_stamp') for r in cache_list if r.get('time_stamp')]
            
            return {
                'size': size,
                'max_size': self.CACHE_SIZE,
                'ready_to_save': size >= self.CACHE_SIZE,
                'oldest_timestamp': min(timestamps) if timestamps else None,
                'newest_timestamp': max(timestamps) if timestamps else None
            }
    
    def clear_cache(self):
        with self._lock:
            self._cache.clear()
            for cache in self._turbine_cache.values():
                cache.clear()

