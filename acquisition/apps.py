import os
import sys
import logging
from django.apps import AppConfig
from django.conf import settings

logger = logging.getLogger(__name__)
_scheduler_started = False

class AcquisitionConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'acquisition'
    
    def ready(self):
        global _scheduler_started
        
        if 'migrate' in sys.argv or 'makemigrations' in sys.argv or 'collectstatic' in sys.argv:
            return
        
        if _scheduler_started:
            logger.warning("Scheduler already started, skipping...")
            return
        
        scheduler_file = os.path.join(settings.BASE_DIR, 'scheduler_autostart.txt')
        logger.warning(f"Checking scheduler file: {scheduler_file}")
        
        if os.path.exists(scheduler_file):
            with open(scheduler_file, 'r') as f:
                state = f.readline().strip().lower()
            logger.warning(f"Scheduler file found. State: '{state}'")
            
            if state in ['yes', 'true', '1']:
                try:
                    logger.warning("Attempting to start scheduler...")
                    from acquisition.scheduler import start_scheduler
                    start_scheduler()
                    _scheduler_started = True
                    logger.warning("Scheduler started successfully.")
                except Exception as e:
                    logger.error(f"Error starting scheduler: {e}", exc_info=True)
            else:
                logger.warning(f"Scheduler is disabled. Current value in scheduler_autostart.txt: '{state}'")
        else:
            logger.warning(f"Scheduler is disabled. File not found: {scheduler_file}")
