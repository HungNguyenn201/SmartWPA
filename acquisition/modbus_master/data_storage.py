import logging
import threading
from collections import deque
from typing import Dict, Any, Optional
from django.db import transaction
import pandas as pd

from acquisition.models import FactoryHistorical
from facilities.models import Farm
from ._header import CACHE_SIZE, RESAMPLE_INTERVAL

logger = logging.getLogger(__name__)


class ModbusDataStorage:
    CACHE_SIZE = CACHE_SIZE
    RESAMPLE_INTERVAL = RESAMPLE_INTERVAL
    def __init__(self, factory_id: int = 1):
        self.factory_id = factory_id
        self.factory: Optional[Farm] = None
        self._cache: deque = deque(maxlen=self.CACHE_SIZE)
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
        
        self._load_factory()
    
    def _load_factory(self):
        try:
            self.factory = Farm.objects.get(id=self.factory_id)
        except Farm.DoesNotExist:
            logger.error("Farm with ID %d not found", self.factory_id)
            self.factory = None
    
    def add_to_cache(self, data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        if not self.factory:
            return {
                'cached': False,
                'error': f'Farm with ID {self.factory_id} not found',
                'cache_size': len(self._cache),
                'ready_to_save': False
            }
        
        cache_record = {}
        timestamp = None
        
        for data_type, result in data.items():
            if result.get("ok") and result.get("quality") == "good" and result.get("value") is not None:
                field_name = self.field_mapping.get(data_type)
                if field_name:
                    cache_record[field_name] = result["value"]
                    if timestamp is None and result.get("ts"):
                        timestamp = result["ts"]
        
        if not cache_record or timestamp is None:
            return {
                'cached': False,
                'error': 'No valid data to cache (all quality=bad or no values)',
                'cache_size': len(self._cache),
                'ready_to_save': False
            }
        
        cache_record['time_stamp'] = timestamp
        
        with self._lock:
            self._cache.append(cache_record)
            cache_size = len(self._cache)
            ready_to_save = cache_size >= self.CACHE_SIZE
        
        return {
            'cached': True,
            'cache_size': cache_size,
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
            if len(self._cache) < self.CACHE_SIZE:
                return {
                    'success': False,
                    'error': f'Cache not ready: {len(self._cache)}/{self.CACHE_SIZE} records',
                    'created': 0,
                    'skipped': 0,
                    'errors': 0,
                    'cache_cleared': False
                }
            
            resampled_record = self._resample_cache()
            
            if not resampled_record:
                return {
                    'success': False,
                    'error': 'Failed to resample cache',
                    'created': 0,
                    'skipped': 0,
                    'errors': 1,
                    'cache_cleared': False
                }
            
            try:
                with transaction.atomic():
                    timestamp = resampled_record.pop('time_stamp')
                    data_point = resampled_record
                    
                    historical, created = FactoryHistorical.objects.get_or_create(
                        farm=self.factory,
                        time_stamp=timestamp,
                        defaults=data_point
                    )
                    
                    if created:
                        result = {
                            'success': True,
                            'error': None,
                            'created': 1,
                            'skipped': 0,
                            'errors': 0,
                            'timestamp': timestamp
                        }
                    else:
                        result = {
                            'success': True,
                            'error': None,
                            'created': 0,
                            'skipped': 1,
                            'errors': 0,
                            'timestamp': timestamp
                        }
                    
                    self._cache.clear()
                    result['cache_cleared'] = True
                    
                    return result
                    
            except Exception as e:
                logger.error("Failed to save resampled data to database: %s", e)
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
        
        timestamp = None
        for data_type, result in data.items():
            if result.get("ok") and result.get("ts"):
                timestamp = result["ts"]
                break
        
        if not timestamp:
            return {
                'success': False,
                'error': 'No valid timestamp found in data',
                'created': 0,
                'skipped': 0,
                'errors': 1
            }
        
        data_point = {}
        for data_type, result in data.items():
            if result.get("ok") and result.get("quality") == "good" and result.get("value") is not None:
                field_name = self.field_mapping.get(data_type)
                if field_name:
                    data_point[field_name] = result["value"]
        
        if not data_point:
            return {
                'success': False,
                'error': 'No valid data to save (all quality=bad)',
                'created': 0,
                'skipped': 0,
                'errors': 0
            }
        
        try:
            with transaction.atomic():
                historical, created = FactoryHistorical.objects.get_or_create(
                    farm=self.factory,
                    time_stamp=timestamp,
                    defaults=data_point
                )
                
                if created:
                    return {
                        'success': True,
                        'error': None,
                        'created': 1,
                        'skipped': 0,
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
            logger.error("Failed to save data to database: %s", e)
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

