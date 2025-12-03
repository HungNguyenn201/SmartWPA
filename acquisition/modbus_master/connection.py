import socket
import logging
import inspect
from typing import Optional
from pymodbus.client import ModbusTcpClient

from ._header import MODBUS_CONFIG

logger = logging.getLogger(__name__)


class ModbusConnection:
    def __init__(self, host: str = None, port: int = None, unit_id: int = None, timeout: float = None):
        self.host = host or MODBUS_CONFIG['HOST']
        self.port = port or MODBUS_CONFIG['PORT']
        self.unit_id = unit_id or MODBUS_CONFIG['UNIT_ID']
        self.timeout = timeout or MODBUS_CONFIG['CONNECT_TIMEOUT']
        self.client: Optional[ModbusTcpClient] = None
    
    def tcp_handshake(self) -> bool:
        try:
            with socket.create_connection((self.host, self.port), timeout=self.timeout):
                return True
        except OSError as e:
            logger.warning("TCP handshake failed %s:%s (%s)", self.host, self.port, e)
            return False
    
    def connect(self) -> bool:
        if not self.tcp_handshake():
            return False
        
        self.client = ModbusTcpClient(self.host, port=self.port, timeout=self.timeout)
        if self.client.connect():
            for attr in ("unit_id", "unit"):
                if hasattr(self.client, attr):
                    try:
                        setattr(self.client, attr, self.unit_id)
                    except Exception:
                        pass
            return True
        
        logger.error("Failed to connect Modbus TCP %s:%s", self.host, self.port)
        return False
    
    def disconnect(self):
        if self.client:
            try:
                self.client.close()
            except Exception:
                pass
            self.client = None
    
    def is_connected(self) -> bool:
        return self.client is not None and self.client.is_socket_open()
    
    def _call_read_input(self, address: int, count: int, unit_id: int = None):
        if not self.client:
            raise RuntimeError("Modbus client not connected")
        
        unit_id = unit_id or self.unit_id
        fn = self.client.read_input_registers
        params = inspect.signature(fn).parameters
        
        try:
            if "unit" in params:
                return fn(address=address, count=count, unit=unit_id)
            if "slave" in params:
                return fn(address=address, count=count, slave=unit_id)
            return fn(address=address, count=count)
        except TypeError:
            return fn(address=address, count=count)
    
    def _call_read_holding(self, address: int, count: int, unit_id: int = None):
        if not self.client:
            raise RuntimeError("Modbus client not connected")
        
        unit_id = unit_id or self.unit_id
        fn = self.client.read_holding_registers
        params = inspect.signature(fn).parameters
        
        try:
            if "unit" in params:
                return fn(address=address, count=count, unit=unit_id)
            if "slave" in params:
                return fn(address=address, count=count, slave=unit_id)
            return fn(address=address, count=count)
        except TypeError:
            return fn(address=address, count=count)
    
    def read_registers(self, address: int, count: int, function_code: int = 4, unit_id: int = None):
        if function_code == 4:
            return self._call_read_input(address, count, unit_id)
        else:
            return self._call_read_holding(address, count, unit_id)
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

