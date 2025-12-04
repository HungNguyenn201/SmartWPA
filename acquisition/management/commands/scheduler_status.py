from django.core.management.base import BaseCommand
from acquisition.scheduler import scheduler

class Command(BaseCommand):
    help = 'Check the status of data acquisition scheduler'
    
    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('=' * 60))
        self.stdout.write(self.style.SUCCESS('Data Acquisition Scheduler Status'))
        self.stdout.write(self.style.SUCCESS('=' * 60))
        
        if scheduler.running:
            self.stdout.write(self.style.SUCCESS('Status: RUNNING'))
        else:
            self.stdout.write(self.style.WARNING('Status: STOPPED'))
        
        jobs = scheduler.get_jobs()
        if jobs:
            self.stdout.write('\nScheduled Jobs:')
            for job in jobs:
                self.stdout.write(f"  - {job.name or job.id} (ID: {job.id})")
                self.stdout.write(f"    Next run: {job.next_run_time}")
        else:
            self.stdout.write('\nNo scheduled jobs')
        
        self.stdout.write(self.style.SUCCESS('=' * 60))

