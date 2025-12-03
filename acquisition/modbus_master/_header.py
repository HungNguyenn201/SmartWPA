from typing import Dict, List, Tuple

MODBUS_CONFIG = {
    'HOST': "127.0.0.1",
    'PORT': 502,
    'UNIT_ID': 1,
    'CONNECT_TIMEOUT': 3.0,
    'FUNCTION_CODE': 4,
}
CACHE_SIZE = 15
RESAMPLE_INTERVAL = '15T'

WORD_ORDER = "big"
BYTE_ORDER = "big"

NUM_TURBINES = 31

DATA_MAPPING: Dict[str, Dict[str, any]] = {
    'total_power': {
        'address': 116,
        'description': 'Total Active Power (Tại POI)',
        'unit': 'MW',
        'data_type': 'float',
    },
    'wind_speed': {
        'address': 0,
        'description': 'Wind Speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wind_direction': {
        'address': 2,
        'description': 'Wind Direction',
        'unit': 'Degree',
        'data_type': 'float',
    },
    'air_temperature': {
        'address': 4,
        'description': 'Air Temperature',
        'unit': 'C',
        'data_type': 'float',
    },
    'air_pressure': {
        'address': 6,
        'description': 'Air Pressure',
        'unit': '',
        'data_type': 'float',
    },
    'humidity': {
        'address': 8,
        'description': 'Humidity',
        'unit': '%',
        'data_type': 'float',
    },
    'wtg_01_wind_speed': {
        'address': 24,
        'description': 'WTG#01 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_02_wind_speed': {
        'address': 48,
        'description': 'WTG#02 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_03_wind_speed': {
        'address': 72,
        'description': 'WTG#03 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_04_wind_speed': {
        'address': 96,
        'description': 'WTG#04 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_05_wind_speed': {
        'address': 120,
        'description': 'WTG#05 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_06_wind_speed': {
        'address': 144,
        'description': 'WTG#06 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_07_wind_speed': {
        'address': 168,
        'description': 'WTG#07 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_08_wind_speed': {
        'address': 192,
        'description': 'WTG#08 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_09_wind_speed': {
        'address': 216,
        'description': 'WTG#09 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_10_wind_speed': {
        'address': 240,
        'description': 'WTG#10 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_11_wind_speed': {
        'address': 264,
        'description': 'WTG#11 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_12_wind_speed': {
        'address': 288,
        'description': 'WTG#12 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_13_wind_speed': {
        'address': 312,
        'description': 'WTG#13 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_14_wind_speed': {
        'address': 336,
        'description': 'WTG#14 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_15_wind_speed': {
        'address': 360,
        'description': 'WTG#15 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_16_wind_speed': {
        'address': 384,
        'description': 'WTG#16 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_17_wind_speed': {
        'address': 408,
        'description': 'WTG#17 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_18_wind_speed': {
        'address': 432,
        'description': 'WTG#18 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_19_wind_speed': {
        'address': 456,
        'description': 'WTG#19 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_20_wind_speed': {
        'address': 480,
        'description': 'WTG#20 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_21_wind_speed': {
        'address': 504,
        'description': 'WTG#21 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_22_wind_speed': {
        'address': 528,
        'description': 'WTG#22 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_23_wind_speed': {
        'address': 552,
        'description': 'WTG#23 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_24_wind_speed': {
        'address': 576,
        'description': 'WTG#24 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_25_wind_speed': {
        'address': 600,
        'description': 'WTG#25 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_26_wind_speed': {
        'address': 624,
        'description': 'WTG#26 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_27_wind_speed': {
        'address': 648,
        'description': 'WTG#27 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_28_wind_speed': {
        'address': 672,
        'description': 'WTG#28 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_29_wind_speed': {
        'address': 696,
        'description': 'WTG#29 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_30_wind_speed': {
        'address': 720,
        'description': 'WTG#30 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wtg_31_wind_speed': {
        'address': 744,
        'description': 'WTG#31 Wind speed',
        'unit': 'm/s',
        'data_type': 'float',
    },
    'wind_speed_average': {
        'address': None,
        'description': 'Average Wind Speed (from WTG)',
        'unit': 'm/s',
        'data_type': 'float',
        'is_calculated': True,
    },
}

def get_wtg_wind_speed_keys() -> List[str]:
    return [f'wtg_{i:02d}_wind_speed' for i in range(1, NUM_TURBINES + 1)]

def get_points_list(data_types: List[str] = None) -> List[Tuple[int, str]]:
    if data_types is None:
        data_types = ['total_power', 'wind_speed']
    
    points = []
    for data_type in data_types:
        if data_type in DATA_MAPPING:
            mapping = DATA_MAPPING[data_type]
            if mapping.get('address') is not None:
                points.append((mapping['address'], mapping['description']))
    
    return points

def get_all_wtg_points() -> List[Tuple[int, str]]:
    points = []
    wtg_keys = get_wtg_wind_speed_keys()
    for key in wtg_keys:
        if key in DATA_MAPPING:
            mapping = DATA_MAPPING[key]
            points.append((mapping['address'], mapping['description']))
    return points

