"""Production orchestration script for Chicago Dig Bot."""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
import pytz
import logging
from src.scripts.post_random_permit import main as post_random_permit
from src.utils.logging import get_logger, setup_logging

# Set up logging
setup_logging()
logger = get_logger(__name__)
logger.setLevel(logging.INFO)

def run_permit_post():
    """Wrapper for permit posting to handle any errors."""
    try:
        logger.info("Starting scheduled permit post")
        post_random_permit()
        logger.info("Completed scheduled permit post")
    except Exception as e:
        logger.error(f"Error in scheduled permit post: {str(e)}")

def main():
    """Main function to start the production scheduler."""
    try:
        logger.info("Starting production scheduler")
        
        # Initialize scheduler with timezone
        scheduler = BlockingScheduler(timezone=pytz.timezone('America/Chicago'))
        
        # Schedule daily permit post at 10am Central
        scheduler.add_job(
            run_permit_post,
            CronTrigger(hour=10, minute=0),
            name='daily_permit_post'
        )
        
        # Schedule roulette post every 3 hours
        # Starting at midnight: 12am, 3am, 6am, 9am, 12pm, 3pm, 6pm, 9pm
        scheduler.add_job(
            run_permit_post,
            CronTrigger(hour='*/3', minute=0),
            name='roulette_post'
        )
        
        # Log next run times
        jobs = scheduler.get_jobs()
        for job in jobs:
            next_run = job.next_run_time.strftime('%Y-%m-%d %H:%M:%S %Z')
            logger.info(f"Next run for {job.name}: {next_run}")
        
        # Start the scheduler
        logger.info("Starting scheduler...")
        scheduler.start()
        
    except Exception as e:
        logger.error(f"Error in production scheduler: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
