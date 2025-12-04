from django.core.management.base import BaseCommand
from acquisition.scheduler import test_smarthis_connection, test_influxdb_connection, test_modbus_connection

class Command(BaseCommand):
    help = 'Test connections to all data sources (SmartHIS, InfluxDB, Modbus)'
    
    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('=' * 60))
        self.stdout.write(self.style.SUCCESS('Testing Data Source Connections'))
        self.stdout.write(self.style.SUCCESS('=' * 60))
        
        self.stdout.write('\nTesting SmartHIS connection...')
        smarthis_ok = test_smarthis_connection()
        if smarthis_ok:
            self.stdout.write(self.style.SUCCESS('  ✓ SmartHIS: Connected'))
        else:
            self.stdout.write(self.style.ERROR('  ✗ SmartHIS: Failed'))
        
        self.stdout.write('\nTesting InfluxDB connection...')
        influxdb_ok = test_influxdb_connection()
        if influxdb_ok:
            self.stdout.write(self.style.SUCCESS('  ✓ InfluxDB: Connected'))
        else:
            self.stdout.write(self.style.ERROR('  ✗ InfluxDB: Failed'))
        
        self.stdout.write('\nTesting Modbus connection...')
        modbus_ok = test_modbus_connection()
        if modbus_ok:
            self.stdout.write(self.style.SUCCESS('  ✓ Modbus: Connected'))
        else:
            self.stdout.write(self.style.ERROR('  ✗ Modbus: Failed'))
        
        self.stdout.write('\n' + '=' * 60)
        
        active_sources = []
        if smarthis_ok:
            active_sources.append('SmartHIS')
        if influxdb_ok:
            active_sources.append('InfluxDB')
        if modbus_ok:
            active_sources.append('Modbus')
        
        if active_sources:
            self.stdout.write(self.style.SUCCESS(
                f'Available sources: {", ".join(active_sources)}'
            ))
            self.stdout.write(self.style.SUCCESS(
                f'Priority order: SmartHIS > InfluxDB > Modbus'
            ))
        else:
            self.stdout.write(self.style.ERROR('No data sources available!'))
        
        self.stdout.write('=' * 60)

