"""
InfluxDB Client Manager with Connection Pooling
Thread-safe singleton with automatic connection management
"""
import logging
from typing import Optional
from influxdb_client import InfluxDBClient
from influxdb_client.client.query_api import QueryApi
from influxdb_client.client.write_api import SYNCHRONOUS
from .config_manager import wind_farm_config

logger = logging.getLogger(__name__)

class InfluxDBClientManager:
    """Thread-safe InfluxDB client manager with connection pooling"""
    _instance = None
    _client: Optional[InfluxDBClient] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._client is None:
            self._initialize_client()
    
    def _initialize_client(self):
        """Initialize InfluxDB client with retry logic"""
        try:
            # Lấy cấu hình InfluxDB từ wind_farm_config
            influxdb_config = wind_farm_config.get_influxdb_config()
            
            # Validate required config
            if not influxdb_config.get('url'):
                error_msg = "InfluxDB URL must be set in config.json"
                logger.error(error_msg)
                raise ValueError(error_msg)
            if not influxdb_config.get('token'):
                error_msg = "InfluxDB token must be set in config.json"
                logger.error(error_msg)
                raise ValueError(error_msg)
            if not influxdb_config.get('org'):
                error_msg = "InfluxDB org must be set in config.json"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            client_options = {
                'url': influxdb_config['url'],
                'token': influxdb_config['token'],
                'org': influxdb_config['org'],
                'timeout': 30000,
                'verify_ssl': False,
                'enable_gzip': True,
                'retry_count': 3,
                'retry_delay': 1000,
            }
            
            self._client = InfluxDBClient(**client_options)
            # Test connection
            self._client.ping()
        except Exception as e:
            logger.error(f"[ERROR] Failed to initialize InfluxDB client: {e}")
            raise
    
    @property
    def client(self) -> InfluxDBClient:
        """Get InfluxDB client instance"""
        if self._client is None:
            self._initialize_client()
        return self._client
    
    def get_query_api(self) -> QueryApi:
        """Get Query API with optimized settings"""
        return self.client.query_api()
    
    def get_write_api(self):
        """Get Write API with optimized settings"""
        return self.client.write_api(write_options=SYNCHRONOUS)
    
    def close(self):
        """Close InfluxDB client connection"""
        if self._client:
            self._client.close()
            self._client = None
    
    def health_check(self) -> bool:
        """Check InfluxDB connection health"""
        try:
            return self.client.ping()
        except Exception as e:
            logger.error(f"[ERROR] InfluxDB health check failed: {e}")
            return False

# Export singleton instance
influx_client_manager = InfluxDBClientManager()