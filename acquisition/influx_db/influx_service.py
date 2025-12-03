"""
InfluxDB Service - Hỗ trợ nhiều PPC với cấu trúc đơn giản
Service để đọc dữ liệu từ InfluxDB cho nhiều bucket/measurement/field
"""
import logging
from typing import List, Dict, Any, Optional


from .influxdb_client import influx_client_manager
from .config_manager import wind_farm_config

logger = logging.getLogger(__name__)

class InfluxDBService:
    """Service để đọc dữ liệu từ InfluxDB cho nhiều PPC"""
    
    def __init__(self, use_cache: bool = True):
        self.use_cache = use_cache
        self.config = wind_farm_config
        self.client = influx_client_manager.client
        self.query_api = influx_client_manager.get_query_api()
    
    def get_data(self, 
                 ppc_id: str, 
                 data_type: str, 
                 start_time: str = '-24h', 
                 end_time: str = 'now()',
                 limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Lấy dữ liệu từ InfluxDB cho một PPC và loại dữ liệu cụ thể
        
        Args:
            ppc_id: ID của PPC (PPC1, PPC2, ...)
            data_type: Loại dữ liệu (power, wind_speed, temperature, humidity)
            start_time: Thời gian bắt đầu
            end_time: Thời gian kết thúc
            limit: Giới hạn số bản ghi
            
        Returns:
            List các bản ghi dữ liệu
        """
        try:
            # Lấy cấu hình cho PPC (tối ưu: cache ppc_config)
            ppc_config = self.config.get_ppc_config(ppc_id)
            if not ppc_config:
                error_msg = f"PPC {ppc_id} not found in config"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            bucket = ppc_config.get('bucket')
            measurement = ppc_config.get('measurements', {}).get(data_type)
            field = ppc_config.get('fields', {}).get(data_type)
            
            if not all([bucket, measurement, field]):
                error_msg = f"Invalid config for PPC {ppc_id}, data_type {data_type}: bucket={bucket}, measurement={measurement}, field={field}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Tạo Flux query
            query = f'''
            from(bucket: "{bucket}")
              |> range(start: {start_time}, stop: {end_time})
              |> filter(fn: (r) => r._measurement == "{measurement}")
              |> filter(fn: (r) => r._field == "{field}")
            '''
            
            if limit:
                query += f'  |> limit(n: {limit})'
            
            # Thực thi query
            tables = self.query_api.query(query)
            
            # Parse kết quả
            data = self._parse_flux_result(tables)
            
            return data
            
        except Exception as e:
            logger.error(f"[ERROR] Failed to get data for PPC {ppc_id}, {data_type}: {e}")
            raise
    
    def get_multiple_data(self, 
                         data_requests: List[Dict[str, str]], 
                         start_time: str = '-24h', 
                         end_time: str = 'now()',
                         limit: Optional[int] = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        Lấy dữ liệu từ nhiều PPC và loại dữ liệu
        
        Args:
            data_requests: List các request [{"ppc_id": "PPC1", "data_type": "power"}, ...]
            start_time: Thời gian bắt đầu
            end_time: Thời gian kết thúc
            limit: Giới hạn số bản ghi
            
        Returns:
            Dict với key là "{ppc_id}_{data_type}" và value là list dữ liệu
        """
        results = {}
        
        for request in data_requests:
            ppc_id = request.get('ppc_id')
            data_type = request.get('data_type')
            
            if not ppc_id or not data_type:
                logger.warning(f"[WARN] Invalid request: {request}")
                continue
            
            try:
                data = self.get_data(ppc_id, data_type, start_time, end_time, limit)
                key = f"{ppc_id}_{data_type}"
                results[key] = data
                
            except Exception as e:
                logger.error(f"[ERROR] Failed to get data for {ppc_id}_{data_type}: {e}")
                results[f"{ppc_id}_{data_type}"] = []
        
        return results
    
    def get_latest_data(self, ppc_id: str, data_type: str) -> Optional[Dict[str, Any]]:
        """
        Lấy dữ liệu mới nhất cho một PPC và loại dữ liệu
        
        Args:
            ppc_id: ID của PPC
            data_type: Loại dữ liệu
            
        Returns:
            Bản ghi dữ liệu mới nhất hoặc None
        """
        try:
            data = self.get_data(ppc_id, data_type, start_time='-1h', limit=1)
            return data[0] if data else None
            
        except Exception as e:
            logger.error(f"[ERROR] Failed to get latest data for PPC {ppc_id}, {data_type}: {e}")
            return None
    
    def _parse_flux_result(self, tables) -> List[Dict[str, Any]]:
        """Parse kết quả Flux query"""
        data = []
        
        for table in tables:
            for record in table.records:
                row = {
                    'time': record.get_time(),
                    'value': record.get_value(),
                    'field': record.get_field(),
                    'measurement': record.get_measurement()
                }
                data.append(row)
        
        return data
    
    def test_connection(self) -> bool:
        """Test kết nối InfluxDB"""
        try:
            # Thử query đơn giản
            query = 'buckets()'
            self.query_api.query(query)
            return True
            
        except Exception as e:
            logger.error(f"[ERROR] InfluxDB connection test failed: {e}")
            return False
    
    def get_bucket_info(self, ppc_id: str) -> Optional[Dict[str, Any]]:
        """Lấy thông tin bucket của PPC"""
        try:
            bucket = self.config.get_bucket(ppc_id)
            if not bucket:
                return None
            
            # Query để lấy thông tin bucket
            query = f'''
                    from(bucket: "{bucket}")
                    |> range(start: -1h)
                    |> limit(n: 1)
                    '''
                
            tables = self.query_api.query(query)
            
            if tables and len(tables) > 0 and len(tables[0].records) > 0:
                record = tables[0].records[0]
                return {
                    'bucket': bucket,
                    'measurement': record.get_measurement(),
                    'field': record.get_field(),
                    'value': record.get_value(),
                    'time': record.get_time()
                }
            
            return None
            
        except Exception as e:
            logger.error(f"[ERROR] Failed to get bucket info for PPC {ppc_id}: {e}")
            return None
    
    def get_ppc_status(self) -> Dict[str, Any]:
        """Lấy trạng thái tất cả PPC"""
        status = {
            'connection_healthy': self.test_connection(),
            'ppc_count': len(self.config.get_ppc_list()),
            'ppc_status': {}
        }
        
        for ppc_id in self.config.get_ppc_list():
            try:
                bucket_info = self.get_bucket_info(ppc_id)
                status['ppc_status'][ppc_id] = {
                    'healthy': bucket_info is not None,
                    'bucket': self.config.get_bucket(ppc_id),
                    'data_types': self.config.get_data_types(ppc_id),
                    'last_data': bucket_info
                }
            except Exception as e:
                status['ppc_status'][ppc_id] = {
                    'healthy': False,
                    'error': str(e)
                }
        
        return status