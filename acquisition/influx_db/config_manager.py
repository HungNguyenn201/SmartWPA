import json
import os
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)

class SyncConfigManager:
    """Quản lý cấu hình cho sync service - Đơn giản hóa"""
    
    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), 'config.json')
        
        self.config_path = config_path
        self._config = None
        self._load_config()
    
    def _load_config(self):
        """Load config từ file JSON"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self._config = json.load(f)
            logger.debug(f"Loaded sync config from {self.config_path}")
        except Exception as e:
            logger.error(f"Failed to load sync config: {e}")
            raise
    
    def get_factory_mapping(self) -> Dict[str, int]:
        """Lấy mapping PPC -> Factory ID"""
        return self._config.get('factory_mapping', {})
    
    def get_factory_id(self, ppc_id: str) -> Optional[int]:
        """Lấy factory_id của PPC"""
        mapping = self.get_factory_mapping()
        return mapping.get(ppc_id)
    
    def get_data_mapping(self) -> Dict[str, str]:
        """Lấy mapping từ InfluxDB field sang Django model field"""
        return self._config.get('data_mapping', {})
    
    def get_requests(self) -> List[Dict[str, str]]:
        """Lấy danh sách requests từ config"""
        return self._config.get('requests', [])

# Singleton instance
sync_config_manager = SyncConfigManager()


class WindFarmConfig:
    """Quản lý cấu hình wind farm cho nhiều PPC"""
    
    def __init__(self, config_file: str = None):
        if config_file is None:
            config_file = os.path.join(os.path.dirname(__file__), 'config.json')
        
        self.config_file = config_file
        self._config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load cấu hình từ file JSON"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
                return config
        except FileNotFoundError as e:
            error_msg = f"Config file not found: {self.config_file}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg) from e
        except json.JSONDecodeError as e:
            error_msg = f"Invalid JSON in config file {self.config_file}: {e}"
            logger.error(error_msg)
            raise ValueError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error loading config from {self.config_file}: {e}"
            logger.error(error_msg)
            raise
    
    def get_influxdb_config(self) -> Dict[str, str]:
        """Lấy cấu hình InfluxDB"""
        return self._config.get('influxdb', {})
    
    def get_ppc_configs(self) -> Dict[str, Dict[str, Any]]:
        """Lấy tất cả cấu hình PPC"""
        return self._config.get('ppc_configs', {})
    
    def get_ppc_config(self, ppc_id: str) -> Optional[Dict[str, Any]]:
        """Lấy cấu hình cho một PPC cụ thể"""
        return self._config.get('ppc_configs', {}).get(ppc_id)
    
    def get_ppc_list(self) -> List[str]:
        """Lấy danh sách tất cả PPC IDs"""
        return list(self._config.get('ppc_configs', {}).keys())
    
    def get_measurement(self, ppc_id: str, data_type: str) -> Optional[str]:
        """Lấy measurement ID cho một loại dữ liệu của PPC"""
        ppc_config = self.get_ppc_config(ppc_id)
        if not ppc_config:
            return None
        return ppc_config.get('measurements', {}).get(data_type)
    
    def get_field(self, ppc_id: str, data_type: str) -> Optional[str]:
        """Lấy field name cho một loại dữ liệu của PPC"""
        ppc_config = self.get_ppc_config(ppc_id)
        if not ppc_config:
            return None
        return ppc_config.get('fields', {}).get(data_type)
    
    def get_bucket(self, ppc_id: str) -> Optional[str]:
        """Lấy bucket name cho PPC"""
        ppc_config = self.get_ppc_config(ppc_id)
        if not ppc_config:
            return None
        return ppc_config.get('bucket')
    
    def get_data_types(self, ppc_id: str) -> List[str]:
        """Lấy danh sách các loại dữ liệu có sẵn cho PPC"""
        ppc_config = self.get_ppc_config(ppc_id)
        if not ppc_config:
            return []
        return list(ppc_config.get('measurements', {}).keys())

# Global instance
wind_farm_config = WindFarmConfig()