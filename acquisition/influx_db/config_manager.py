import json
import os
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)

class SyncConfigManager:
    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), 'config.json')
        
        self.config_path = config_path
        self._config = None
        self._load_config()
    
    def _load_config(self):
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self._config = json.load(f)
        except Exception as e:
            logger.error(f"Failed to load sync config: {e}", exc_info=True)
            raise
    
    def get_factory_mapping(self) -> Dict[str, int]:
        return self._config.get('factory_mapping', {})
    
    def get_factory_id(self, ppc_id: str) -> Optional[int]:
        mapping = self.get_factory_mapping()
        return mapping.get(ppc_id)
    
    def get_data_mapping(self) -> Dict[str, str]:
        return self._config.get('data_mapping', {})
    
    def get_requests(self) -> List[Dict[str, str]]:
        return self._config.get('requests', [])
    
    def get_turbine_mapping(self) -> Dict[str, Dict[str, Any]]:
        return self._config.get('turbine_mapping', {})
    
    def get_turbine_config(self, ppc_id: str) -> Optional[Dict[str, Any]]:
        turbine_mapping = self.get_turbine_mapping()
        return turbine_mapping.get(ppc_id)

sync_config_manager = SyncConfigManager()

class WindFarmConfig:
    def __init__(self, config_file: str = None):
        if config_file is None:
            config_file = os.path.join(os.path.dirname(__file__), 'config.json')
        
        self.config_file = config_file
        self._config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
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
        return self._config.get('influxdb', {})
    
    def get_ppc_configs(self) -> Dict[str, Dict[str, Any]]:
        return self._config.get('ppc_configs', {})
    
    def get_ppc_config(self, ppc_id: str) -> Optional[Dict[str, Any]]:
        return self._config.get('ppc_configs', {}).get(ppc_id)
    
    def get_ppc_list(self) -> List[str]:
        return list(self._config.get('ppc_configs', {}).keys())
    
    def get_measurement(self, ppc_id: str, data_type: str) -> Optional[str]:
        ppc_config = self.get_ppc_config(ppc_id)
        if not ppc_config:
            return None
        return ppc_config.get('measurements', {}).get(data_type)
    
    def get_field(self, ppc_id: str, data_type: str) -> Optional[str]:
        ppc_config = self.get_ppc_config(ppc_id)
        if not ppc_config:
            return None
        return ppc_config.get('fields', {}).get(data_type)
    
    def get_bucket(self, ppc_id: str) -> Optional[str]:
        ppc_config = self.get_ppc_config(ppc_id)
        if not ppc_config:
            return None
        return ppc_config.get('bucket')
    
    def get_data_types(self, ppc_id: str) -> List[str]:
        ppc_config = self.get_ppc_config(ppc_id)
        if not ppc_config:
            return []
        return list(ppc_config.get('measurements', {}).keys())

wind_farm_config = WindFarmConfig()