from django.core.management.base import BaseCommand
from acquisition.scheduler import start_scheduler, stop_scheduler
import signal
import sys
import time

class Command(BaseCommand):
    help = 'Start the data acquisition scheduler'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--main-interval',
            type=int,
            default=15,
            help='Main sync interval in minutes (default: 15)'
        )
        parser.add_argument(
            '--modbus-interval',
            type=int,
            default=5,
            help='Modbus collection interval in minutes (default: 5)'
        )
    
    def handle(self, *args, **options):
        main_interval = options['main_interval']
        modbus_interval = options['modbus_interval']
        
        self.stdout.write(self.style.SUCCESS(
            f'Starting Data Acquisition Scheduler...'
        ))
        self.stdout.write(self.style.SUCCESS(
            f'Main sync interval: {main_interval} minutes'
        ))
        self.stdout.write(self.style.SUCCESS(
            f'Modbus collection interval: {modbus_interval} minutes'
        ))
        
        def signal_handler(sig, frame):
            self.stdout.write(self.style.WARNING('\nShutting down scheduler...'))
            stop_scheduler()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        try:
            start_scheduler(
                main_interval_minutes=main_interval,
                modbus_interval_minutes=modbus_interval
            )
            
            self.stdout.write(self.style.SUCCESS('Scheduler started successfully!'))
            self.stdout.write(self.style.SUCCESS('Press Ctrl+C to stop'))
            
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING('\nShutting down...'))
            stop_scheduler()
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error: {e}'))
            stop_scheduler()
            raise

