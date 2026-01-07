# ============================================================================
# CSV File Configuration
# ============================================================================

# CSV file separator
CSV_SEPARATOR = ';'

# CSV file encoding
CSV_ENCODING = 'utf-8-sig'

# Date time format for CSV files
CSV_DATETIME_FORMAT = '%d/%m/%Y %H:%M'
CSV_DATETIME_DAYFIRST = True

# ============================================================================
# Field Mapping: CSV Filename -> DataFrame Column Name
# ============================================================================

# Mapping from CSV file names to DataFrame column names
FIELD_MAPPING = {
    'WIND_SPEED.csv': 'WIND_SPEED',
    'ACTIVE_POWER.csv': 'ACTIVE_POWER',
    'DIRECTION_WIND.csv': 'DIRECTION_WIND',
    'DIRECTION_NACELLE.csv': 'DIRECTION_NACELLE',
    'TEMPERATURE_EXTERNAL.csv': 'TEMPERATURE',
}

# Required CSV files (must exist for computation to work)
REQUIRED_FILES = [
    'WIND_SPEED.csv',
    'ACTIVE_POWER.csv',
]

# Optional CSV files (will be included if available)
OPTIONAL_FILES = {
    'DIRECTION_WIND.csv': 'DIRECTION_WIND',
    'DIRECTION_NACELLE.csv': 'DIRECTION_NACELLE',
    'TEMPERATURE_EXTERNAL.csv': 'TEMPERATURE',
}

# ============================================================================
# Turbine Constants
# ============================================================================

# Required turbine constants for computation
REQUIRED_TURBINE_CONSTANTS = [
    'V_cutin',
    'V_cutout',
    'V_rated',
    'P_rated',
    'Swept_area'
]

# Default turbine constants (có thể override trong request nếu cần)
# Cấu hình theo từng dự án
DEFAULT_TURBINE_CONSTANTS = {
    'V_cutin': 2.84,        # Cut-in wind speed (m/s)
    'V_cutout': 16.72,      # Cut-out wind speed (m/s) per bin check suggestion
    'V_rated': 12.0,        # Rated wind speed (m/s)
    'P_rated': 2000.0,      # Rated power (kW)
    'Swept_area': 20000   # Swept area (m²)
}

# ============================================================================
# Data Path Configuration
# ============================================================================

# Default data directory name
DEFAULT_DATA_DIR = 'Data'

# Data path structure: {DATA_DIR}/Farm{farm_id}/WT{turbine_id}/
# This is constructed dynamically in the code

# ============================================================================
# Distribution Configuration
# ============================================================================

# Bin name mappings for distribution calculations
BIN_NAME_MAPPING = {
    'wind_speed': 'wind_speed_bin',
    'power': 'power_bin',
    'wind_direction': 'wind_direction_bin'
}

# Default bin count for distribution calculations
DEFAULT_BIN_COUNT = 50

# Month names mapping
MONTH_NAMES = {
    1: 'January',
    2: 'February',
    3: 'March',
    4: 'April',
    5: 'May',
    6: 'June',
    7: 'July',
    8: 'August',
    9: 'September',
    10: 'October',
    11: 'November',
    12: 'December'
}

# ============================================================================
# Day/Night Period Configuration
# ============================================================================

# Day period hours (inclusive)
DAY_START_HOUR = 5  # 5 AM
DAY_END_HOUR = 18   # 6 PM (18:00)

# Alternative day period for speed analysis (6 AM to 6 PM)
DAY_START_HOUR_ALT = 6  # 6 AM
DAY_END_HOUR_ALT = 18   # 6 PM (18:00)

# Period names
PERIOD_NAMES = {
    'Day': 'Day',
    'Night': 'Night'
}

# ============================================================================
# Season Configuration
# ============================================================================

# Season mapping: month -> season name
SEASON_MAP = {
    1: 'Winter', 2: 'Winter', 3: 'Spring', 4: 'Spring',
    5: 'Spring', 6: 'Summer', 7: 'Summer', 8: 'Summer',
    9: 'Fall', 10: 'Fall', 11: 'Fall', 12: 'Winter'
}

# Season names list
SEASON_NAMES = ['Winter', 'Spring', 'Summer', 'Fall']

# Season mapping for time profile (month -> season index)
SEASON_INDEX_MAP = {
    1: 3, 2: 3, 3: 0, 4: 0, 5: 0, 6: 1,
    7: 1, 8: 1, 9: 2, 10: 2, 11: 2, 12: 3
}

# Season names by index
SEASON_NAMES_BY_INDEX = {
    0: 'Spring',
    1: 'Summer',
    2: 'Fall',
    3: 'Winter'
}

# ============================================================================
# Bin Configuration
# ============================================================================

# Minimum and maximum bins for speed analysis
MIN_BINS = 30
MAX_BINS = 100

# ============================================================================
# Source Field Mapping
# ============================================================================

# Mapping from source type to database field name (for classification points)
CLASSIFICATION_SOURCE_FIELD_MAP = {
    'wind_speed': 'wind_speed',
    'power': 'active_power',
}

# Mapping from source type to database field name (for historical data)
HISTORICAL_SOURCE_FIELD_MAP = {
    'wind_speed': 'wind_speed',
    'power': 'active_power',
    'wind_direction': 'wind_dir',
    'temperature': 'air_temp',
    'pressure': 'pressure',
    'humidity': 'hud',
}

# ============================================================================
# Time Step Configuration
# ============================================================================

# Default time step in seconds (10 minutes)
DEFAULT_TIME_STEP_SECONDS = 600.0

# ============================================================================
# Timestamp Conversion Utility
# ============================================================================

def convert_timestamp_to_datetime(timestamp_val):
    """
    Convert timestamp to pandas datetime, handling different units automatically.
    
    Args:
        timestamp_val: Timestamp value (could be nanoseconds, microseconds, milliseconds, or seconds)
    
    Returns:
        pandas.Timestamp or None if conversion fails
    """
    import pandas as pd
    
    if timestamp_val is None:
        return None
    
    # Convert to milliseconds if needed
    # Timestamps > 1e15 are likely nanoseconds, > 1e12 are microseconds, <= 1e13 are milliseconds
    if timestamp_val > 1e15:
        # Nanoseconds - convert to milliseconds
        timestamp_ms = timestamp_val / 1e6
    elif timestamp_val > 1e12:
        # Could be microseconds or already milliseconds - check by magnitude
        # If > 1e13 it's likely microseconds
        if timestamp_val > 1e13:
            timestamp_ms = timestamp_val / 1e3  # microseconds to milliseconds
        else:
            timestamp_ms = timestamp_val  # already milliseconds
    else:
        # Already in milliseconds or seconds
        if timestamp_val < 1e9:
            timestamp_ms = timestamp_val * 1000  # seconds to milliseconds
        else:
            timestamp_ms = timestamp_val  # already milliseconds
    
    try:
        return pd.to_datetime(int(timestamp_ms), unit='ms')
    except (ValueError, OverflowError, OSError):
        return None
