import struct
import math
import logging
from datetime import datetime
from typing import List, Tuple, Dict, Any
from django.utils import timezone as django_timezone
import pytz

from .connection import ModbusConnection
from ._header import (
    MODBUS_CONFIG, DATA_MAPPING, WORD_ORDER, BYTE_ORDER, 
    get_wtg_wind_speed_keys
)

logger = logging.getLogger(__name__)


class ModbusDataReader:
    def __init__(self, connection: ModbusConnection = None):
        self.connection = connection or ModbusConnection()
        self.function_code = MODBUS_CONFIG['FUNCTION_CODE']
        self.base = 0
    
    def regs_to_float32(self, r0: int, r1: int, w=WORD_ORDER, b=BYTE_ORDER) -> float:
        hi, lo = (r0, r1) if w == "big" else (r1, r0)
        hi_hi, hi_lo = (hi >> 8) & 0xFF, hi & 0xFF
        lo_hi, lo_lo = (lo >> 8) & 0xFF, lo & 0xFF
        if b == "big":
            by = bytes([hi_hi, hi_lo, lo_hi, lo_lo])
            return struct.unpack(">f", by)[0]
        else:
            by = bytes([hi_lo, hi_hi, lo_lo, lo_hi])
            return struct.unpack("<f", by)[0]
    
    def _get_local_timestamp(self) -> datetime:
        now = django_timezone.now()
        if now.tzinfo:
            local_tz = pytz.timezone('Asia/Ho_Chi_Minh')
            local_dt = now.astimezone(local_tz)
            return local_dt.replace(tzinfo=None)
        return now
    
    def read_one_value(self, address: int, function_code: int = None, unit_id: int = None) -> Dict[str, Any]:
        fc = function_code or self.function_code
        start = address + self.base
        ts = self._get_local_timestamp()
        
        try:
            rr = self.connection.read_registers(start, 2, fc, unit_id)
            
            if rr is None:
                return {
                    "ok": False, 
                    "ts": ts, 
                    "value": None, 
                    "error": f"Response is None for FC{fc} addr={start}",
                    "quality": "bad"
                }
            
            if rr.isError():
                error_msg = str(rr)
                if hasattr(rr, 'exception_code'):
                    error_msg += f" (exception_code={rr.exception_code})"
                return {
                    "ok": False, 
                    "ts": ts, 
                    "value": None, 
                    "error": f"FC{fc} error at addr={start}: {error_msg}",
                    "quality": "bad"
                }
            
            if not hasattr(rr, 'registers') or rr.registers is None:
                return {
                    "ok": False, 
                    "ts": ts, 
                    "value": None, 
                    "error": f"FC{fc} no registers in response at addr={start}: {rr}",
                    "quality": "bad"
                }
            
            if len(rr.registers) < 2:
                return {
                    "ok": False, 
                    "ts": ts, 
                    "value": None, 
                    "error": f"FC{fc} insufficient registers at addr={start}: got {len(rr.registers)}, need 2",
                    "quality": "bad"
                }
            
            value = self.regs_to_float32(rr.registers[0], rr.registers[1])
            
            if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
                return {
                    "ok": False,
                    "ts": ts,
                    "value": None,
                    "error": f"Invalid value decoded: {value} (NaN or Inf)",
                    "quality": "bad"
                }
            
            return {
                "ok": True, 
                "ts": ts, 
                "value": value, 
                "error": None,
                "quality": "good"
            }
            
        except Exception as e:
            return {
                "ok": False, 
                "ts": ts, 
                "value": None, 
                "error": f"Exception FC{fc} addr={start}: {type(e).__name__}: {str(e)}",
                "quality": "bad"
            }
    
    def _read_wtg_wind_speeds_average(self) -> Tuple[List[float], datetime]:
        wtg_values = []
        timestamp = self._get_local_timestamp()
        
        wtg_keys = get_wtg_wind_speed_keys()
        for wtg_key in wtg_keys:
            if wtg_key in DATA_MAPPING:
                mapping = DATA_MAPPING[wtg_key]
                address = mapping['address']
                r = self.read_one_value(address)
                if r.get("ok") and r.get("value") is not None and r.get("quality") == "good":
                    wtg_values.append(r["value"])
                    if len(wtg_values) == 1:
                        timestamp = r.get("ts", timestamp)
        
        return wtg_values, timestamp
    
    def read_data_types(self, data_types: List[str]) -> Dict[str, Dict[str, Any]]:
        results = {}
        
        if 'wind_speed' in data_types:
            station_mapping = DATA_MAPPING.get('wind_speed')
            if station_mapping and station_mapping.get('address') is not None:
                r = self.read_one_value(station_mapping['address'])
                r["data_type"] = 'wind_speed'
                r["address"] = station_mapping['address']
                r["description"] = station_mapping['description']
                
                if r.get("ok") and r.get("quality") == "good" and r.get("value") is not None:
                    r["source"] = "weather_station"
                    results['wind_speed'] = r
                else:
                    wtg_values, timestamp = self._read_wtg_wind_speeds_average()
                    if wtg_values:
                        avg_value = sum(wtg_values) / len(wtg_values)
                        results['wind_speed'] = {
                            "ok": True,
                            "ts": timestamp,
                            "value": avg_value,
                            "error": None,
                            "data_type": "wind_speed",
                            "address": None,  
                            "description": f"Wind Speed (from {len(wtg_values)} WTG average, station unavailable)",
                            "source": "wtg_average",
                            "wtg_count": len(wtg_values),
                            "quality": "good"
                        }
                    else:
                        results['wind_speed'] = {
                            "ok": False,
                            "ts": timestamp,
                            "value": None,
                            "error": "Failed to read from weather station and no WTG values available",
                            "data_type": "wind_speed",
                            "address": station_mapping['address'],
                            "description": station_mapping['description'],
                            "source": None,
                            "quality": "bad"
                        }
        
        for data_type in data_types:
            if data_type == 'wind_speed':
                continue
            
            if data_type not in DATA_MAPPING:
                results[data_type] = {
                    "ok": False,
                    "ts": self._get_local_timestamp(),
                    "value": None,
                    "error": f"Data type '{data_type}' not found in DATA_MAPPING",
                    "quality": "bad"
                }
                continue
            
            mapping = DATA_MAPPING[data_type]
            
            address = mapping.get('address')
            if address is None:
                results[data_type] = {
                    "ok": False,
                    "ts": self._get_local_timestamp(),
                    "value": None,
                    "error": f"Data type '{data_type}' has no address (calculated value?)",
                    "quality": "bad"
                }
                continue
            
            r = self.read_one_value(address)
            r["data_type"] = data_type
            r["address"] = address
            r["description"] = mapping['description']
            results[data_type] = r
        
        return results

