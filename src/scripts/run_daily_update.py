"""Main script for running the daily update pipeline."""
import time
import logging
from datetime import datetime, timedelta
import pytz
from functools import wraps
from src.data.fetcher import DataFetcher
from src.data.storage import DataStorage
from src.analytics.stats import StatsGenerator
from src.visualization.charts import ChartGenerator
from src.social.bluesky import BlueskyPoster
from src.utils.logging import setup_logging, get_logger
from src.config import config
from pathlib import Path

logger = get_logger(__name__)

def retry_on_error(func):
    """Decorator to implement retry logic based on config settings."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        max_attempts = config.error_config['retry']['max_attempts']
        delay = config.error_config['retry']['delay_seconds']
        exponential = config.error_config['retry']['exponential_backoff']
        
        for attempt in range(max_attempts):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                current_delay = delay * (2 ** attempt if exponential else 1)
                
                if attempt + 1 == max_attempts:
                    logger.error(f"Final attempt failed for {func.__name__}: {str(e)}")
                    raise
                
                logger.warning(
                    f"Attempt {attempt + 1}/{max_attempts} failed for {func.__name__}: {str(e)}. "
                    f"Retrying in {current_delay} seconds..."
                )
                time.sleep(current_delay)
    return wrapper

class DataValidationError(Exception):
    """Custom exception for data validation failures."""
    pass

@retry_on_error
def run_pipeline():
    """Run the complete data pipeline with logging and error handling."""
    logger.info("Starting daily update pipeline")
    
    try:
        # Initialize components
        logger.info("Initializing pipeline components")
        fetcher = DataFetcher()
        storage = DataStorage()
        stats = StatsGenerator()
        charts = ChartGenerator()
        poster = BlueskyPoster()
        
        # Track pipeline status
        pipeline_status = {
            'fetch_success': False,
            'storage_success': False,
            'stats_success': False,
            'min_records_threshold': 10  # Minimum records needed to consider data valid
        }
        
        # Fetch data
        logger.info("Fetching recent data from Chicago 811")
        data = fetcher.fetch_recent_data()
        records_fetched = len(data)
        logger.info(f"Retrieved {records_fetched} records")
        
        # Validate fetch results
        if records_fetched == 0:
            logger.warning("No new records fetched, skipping remaining pipeline steps")
            return
        
        if data.empty or 'dig_ticket_number' not in data.columns:
            raise DataValidationError("Fetched data is invalid or missing required columns")
            
        pipeline_status['fetch_success'] = True
        
        # Store data with improved tracking
        logger.info("Processing and storing data")
        storage_stats = storage.process_and_store(data)
        
        # Log detailed storage statistics
        logger.info("Storage operation completed:")
        logger.info(f"- Total records processed: {storage_stats['total_records']}")
        logger.info(f"- New records inserted: {storage_stats['inserts']}")
        logger.info(f"- Existing records updated: {storage_stats['updates']}")
        
        # Validate storage results
        if storage_stats['inserts'] == 0 and storage_stats['updates'] == 0:
            logger.warning("No changes to stored data, skipping remaining pipeline steps")
            return
            
        if storage_stats['total_records'] < pipeline_status['min_records_threshold']:
            raise DataValidationError(
                f"Insufficient records processed: {storage_stats['total_records']} "
                f"(minimum: {pipeline_status['min_records_threshold']})"
            )
            
        pipeline_status['storage_success'] = True
        
        # Generate stats and comparisons
        logger.info("Generating statistics and comparisons")
        daily_stats = stats.generate_daily_stats()
        
        # Use yesterday's date for comparisons in Chicago timezone
        chicago_tz = pytz.timezone('America/Chicago')
        chicago_now = datetime.now(chicago_tz)
        yesterday = (chicago_now - timedelta(days=1)).strftime('%Y-%m-%d')
        logger.info(f"Getting day comparison for yesterday: {yesterday}")
        day_comparison = stats.get_day_of_week_comparison(yesterday)
        leaderboard = stats.get_contractor_leaderboard(limit=5)
        
        # Calculate permit stats
        permit_stats = {
            'total_count': day_comparison['actual_total'],
            'emergency_count': day_comparison['actual_emergency'],
            'regular_count': day_comparison['actual_regular'],
            'emergency_percent': round((day_comparison['actual_emergency'] / day_comparison['actual_total']) * 100, 1)
        }
        
        # Mark stats generation as successful
        pipeline_status['stats_success'] = True
        
        # Create visualizations
        logger.info("Creating visualizations")
        
        # Ensure output directory exists
        output_dir = Path('output')
        output_dir.mkdir(exist_ok=True)
        
        # Create emergency heatmap
        emergency_heatmap_path = str(output_dir / 'emergency_heatmap.png')
        charts.create_emergency_heatmap(data, emergency_heatmap_path)
        logger.info(f"Emergency heatmap saved to {emergency_heatmap_path}")
        
        # Verify all validation checks passed before posting
        if all([
            pipeline_status['fetch_success'],
            pipeline_status['storage_success'],
            pipeline_status['stats_success']
        ]):
            # Post thread
            if not config.test_mode:
                logger.info("All validation checks passed - posting thread to Bluesky")
                poster.post_daily_thread(
                    day_comparison=day_comparison,
                    leaderboard=leaderboard,
                    permit_stats=permit_stats,
                    heatmap_path=emergency_heatmap_path,
                    emergency_heatmap_path=emergency_heatmap_path
                )
                logger.info("Bluesky thread posted successfully")
            else:
                logger.info("Test mode enabled - skipping Bluesky post")
        else:
            failed_checks = [
                check for check, status in pipeline_status.items()
                if isinstance(status, bool) and not status
            ]
            raise DataValidationError(
                f"Cannot post to Bluesky - validation checks failed: {failed_checks}"
            )
        
        # Log final summary
        logger.info("Daily update pipeline completed successfully:")
        logger.info(f"- Records fetched: {records_fetched}")
        logger.info(f"- Records processed: {storage_stats['total_records']}")
        logger.info(f"- New records: {storage_stats['inserts']}")
        logger.info(f"- Updated records: {storage_stats['updates']}")
        logger.info(f"- Day of week comparison: {day_comparison['day_name']}")
        logger.info(f"- Total permits: {permit_stats['total_count']}")
        logger.info(f"- Emergency permits: {permit_stats['emergency_count']} ({permit_stats['emergency_percent']}%)")
        logger.info(f"- Regular permits: {permit_stats['regular_count']}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

def main():
    """Main entry point with logging setup."""
    try:
        # Initialize logging
        setup_logging()
        logger.info("Starting Chicago Dig Bot daily update")
        
        # Run the pipeline
        run_pipeline()
        
        # Exit successfully
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Fatal error in daily update: {str(e)}")
        sys.exit(1)
    finally:
        logger.info("Chicago Dig Bot daily update finished")

if __name__ == "__main__":
    import sys
    main()
