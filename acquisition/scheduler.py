import logging
import os
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from django.conf import settings

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler(timezone='Asia/Ho_Chi_Minh')

def test_smarthis_connection():
    try:
        from acquisition.smarthis.get_data import check_and_get_token
        from facilities.models import Farm
        
        farms_with_smarthis = Farm.objects.filter(smarthis__isnull=False).first()
        if not farms_with_smarthis:
            return False
        
        token = check_and_get_token(farms_with_smarthis.id)
        if token:
            logger.warning(f"SmartHIS connection test: SUCCESS for farm {farms_with_smarthis.id}")
            return True
        else:
            logger.warning(f"SmartHIS connection test: FAILED for farm {farms_with_smarthis.id}")
            return False
    except Exception as e:
        logger.error(f"SmartHIS connection test error: {e}", exc_info=True)
        return False

def test_influxdb_connection():
    try:
        from acquisition.influx_db.influx_service import InfluxDBService
        
        service = InfluxDBService()
        result = service.test_connection()
        
        if result:
            logger.warning("InfluxDB connection test: SUCCESS")
        else:
            logger.warning("InfluxDB connection test: FAILED")
        
        return result
    except Exception as e:
        logger.error(f"InfluxDB connection test error: {e}", exc_info=True)
        return False

def test_modbus_connection():
    try:
        from acquisition.modbus_master.connection import ModbusConnection
        
        conn = ModbusConnection()
        result = conn.tcp_handshake()
        
        if result:
            logger.warning("Modbus connection test: SUCCESS")
        else:
            logger.warning("Modbus connection test: FAILED")
        
        return result
    except Exception as e:
        logger.error(f"Modbus connection test error: {e}", exc_info=True)
        return False

def sync_data_with_fallback():
    try:
        logger.warning("=" * 60)
        logger.warning(f"Starting scheduled data sync at {datetime.now()}")
        
        if test_smarthis_connection():
            logger.warning("Syncing from SmartHIS")
            try:
                from acquisition.smarthis.save_data import save_all_farms_data_to_db
                save_all_farms_data_to_db()
                logger.warning("SmartHIS sync completed")
                logger.warning("=" * 60)
                return
            except Exception as e:
                logger.error(f"SmartHIS sync failed: {e}", exc_info=True)
        
        if test_influxdb_connection():
            logger.warning("Syncing from InfluxDB")
            try:
                from acquisition.influx_db.sync_service import sync_multiple_data_types_to_db
                sync_multiple_data_types_to_db()
                logger.warning("InfluxDB sync completed")
                logger.warning("=" * 60)
                return
            except Exception as e:
                logger.error(f"InfluxDB sync failed: {e}", exc_info=True)
        
        if test_modbus_connection():
            logger.warning("Collecting from Modbus")
            try:
                from acquisition.modbus_master.connection import ModbusConnection
                from acquisition.modbus_master.data_reader import ModbusDataReader
                from acquisition.modbus_master.data_storage import ModbusDataStorage
                from acquisition.modbus_master._header import DATA_MAPPING
                
                conn = ModbusConnection()
                if conn.connect():
                    try:
                        reader = ModbusDataReader(connection=conn)
                        storage = ModbusDataStorage(factory_id=1)
                        
                        data_to_save = {}
                        for data_key, config in DATA_MAPPING.items():
                            if isinstance(config, dict) and config.get('address') is not None:
                                result = reader.read_one_value(config['address'])
                                data_to_save[data_key] = result
                        
                        cache_result = storage.add_to_cache(data_to_save)
                        if cache_result.get('ready_to_save'):
                            storage.save_to_db()
                            logger.warning("Modbus data saved to DB")
                    finally:
                        conn.disconnect()
                logger.warning("=" * 60)
                return
            except Exception as e:
                logger.error(f"Modbus collection failed: {e}", exc_info=True)
        
        logger.error("All data sources failed")
        logger.warning("=" * 60)
    except Exception as e:
        logger.error(f"Data sync error: {e}", exc_info=True)

def collect_modbus_data():
    try:
        logger.warning(f"Starting dedicated Modbus collection at {datetime.now()}")
        
        if not test_modbus_connection():
            logger.warning("Modbus connection not available, skipping collection")
            return
        
        from acquisition.modbus_master.connection import ModbusConnection
        from acquisition.modbus_master.data_reader import ModbusDataReader
        from acquisition.modbus_master.data_storage import ModbusDataStorage
        from acquisition.modbus_master._header import DATA_MAPPING
        
        conn = ModbusConnection()
        if not conn.connect():
            logger.error("Failed to connect to Modbus server")
            return
        
        try:
            reader = ModbusDataReader(connection=conn)
            storage = ModbusDataStorage(factory_id=1)
            
            data_to_save = {}
            for data_key, config in DATA_MAPPING.items():
                if isinstance(config, dict) and config.get('address') is not None:
                    result = reader.read_one_value(config['address'])
                    data_to_save[data_key] = result
            
            cache_result = storage.add_to_cache(data_to_save)
            if cache_result.get('ready_to_save'):
                storage.save_to_db()
                logger.warning("Modbus data saved to DB")
        finally:
            conn.disconnect()
    except Exception as e:
        logger.error(f"Modbus collection error: {e}", exc_info=True)

def start_scheduler(main_interval_minutes=15, modbus_interval_minutes=5):
    if scheduler.running:
        logger.warning("Scheduler is already running")
        return
    
    logger.warning("Adding job: sync_data_with_fallback")
    scheduler.add_job(
        sync_data_with_fallback,
        'interval', minutes=main_interval_minutes,
        id='sync_data_with_fallback',
        replace_existing=True,
        max_instances=1
    )
    
    logger.warning("Adding job: collect_modbus_data")
    scheduler.add_job(
        collect_modbus_data,
        'interval', minutes=modbus_interval_minutes,
        id='collect_modbus_data',
        replace_existing=True,
        max_instances=1
    )
    
    scheduler.start()
    logger.warning(f"Scheduler started with main interval: {main_interval_minutes}min, modbus interval: {modbus_interval_minutes}min")

def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown(wait=True)
        logger.warning("Scheduler stopped")
    else:
        logger.warning("Scheduler is not running")
