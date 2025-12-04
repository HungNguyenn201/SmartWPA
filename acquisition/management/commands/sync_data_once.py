from django.core.management.base import BaseCommand
from acquisition.scheduler import sync_data_with_fallback, collect_modbus_data
from acquisition.smarthis.save_data import save_all_farms_data_to_db
from acquisition.influx_db.sync_service import sync_multiple_data_types_to_db

class Command(BaseCommand):
    help = 'Run data synchronization once (for testing or manual sync)'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--source',
            type=str,
            choices=['auto', 'smarthis', 'influxdb', 'modbus'],
            default='auto',
            help='Data source to use (default: auto - uses priority order)'
        )
    
    def handle(self, *args, **options):
        source = options['source']
        
        self.stdout.write(self.style.SUCCESS('=' * 60))
        self.stdout.write(self.style.SUCCESS('Manual Data Synchronization'))
        self.stdout.write(self.style.SUCCESS('=' * 60))
        
        if source == 'auto':
            self.stdout.write('\nUsing automatic source selection (priority order)...')
            sync_data_with_fallback()
            self.stdout.write(self.style.SUCCESS('\nSync completed - check logs for details'))
        elif source == 'smarthis':
            self.stdout.write('\nSyncing from SmartHIS...')
            try:
                save_all_farms_data_to_db()
                self.stdout.write(self.style.SUCCESS('\n✓ Successfully synced from SmartHIS'))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'\n✗ Failed to sync from SmartHIS: {e}'))
        elif source == 'influxdb':
            self.stdout.write('\nSyncing from InfluxDB...')
            try:
                sync_multiple_data_types_to_db()
                self.stdout.write(self.style.SUCCESS('\n✓ Successfully synced from InfluxDB'))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'\n✗ Failed to sync from InfluxDB: {e}'))
        elif source == 'modbus':
            self.stdout.write('\nCollecting from Modbus...')
            try:
                collect_modbus_data()
                self.stdout.write(self.style.SUCCESS('\n✓ Successfully collected from Modbus'))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'\n✗ Failed to collect from Modbus: {e}'))
        
        self.stdout.write('=' * 60)

