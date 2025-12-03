# Timezone and Data Processing Constants
DEFAULT_TIMEZONE = 'Asia/Ho_Chi_Minh'  # GMT+7
DATA_RESAMPLE_INTERVAL = '15T'  # 15 minutes

# Constants for sync_multiple_data_types_to_db
DEFAULT_PPC_ID = "PPC1"  # PPC ID mặc định
DEFAULT_DATA_TYPES = ['power', 'wind_speed']  # Loại dữ liệu mặc định
SYNC_LOOKBACK_HOURS = 24  # Số giờ lùi lại để sync dữ liệu từ InfluxDB
DEFAULT_FARM_ID = 1  # Farm ID mặc định (fallback khi không có mapping trong config)

# Data field mapping: InfluxDB field name -> Django model field name
DATA_FIELD_MAPPING = {
    'power': 'active_power',
    'wind_speed': 'wind_speed',
    'wind_direction': 'wind_dir',
    'temperature': 'air_temp',
    'humidity': 'hud',
    'pressure': 'pressure'
}