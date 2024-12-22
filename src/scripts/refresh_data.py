#!/usr/bin/env python3
"""Script to perform a full refresh of data from the Chicago 811 SODA API."""
import time
import shutil
from pathlib import Path
import glob
from functools import wraps
from data.fetcher import DataFetcher
from data.storage import DataStorage
from utils.logging import setup_logging, get_logger
from config import config

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

def clean_data_directory():
    """Remove existing data files and state tracking files."""
    data_dir = Path(config.data_dir)
    
    # Remove state tracking files
    state_files = [
        data_dir / "initial_fetch_complete.json",
        data_dir / "last_fetch.json"
    ]
    for file in state_files:
        if file.exists():
            logger.info(f"Removing state file: {file}")
            file.unlink()
    
    # Remove existing parquet files
    parquet_pattern = str(data_dir / "chicago811_*.parquet")
    for file in glob.glob(parquet_pattern):
        logger.info(f"Removing data file: {file}")
        Path(file).unlink()

@retry_on_error
def run_refresh():
    """Run the complete data refresh pipeline with logging and error handling."""
    logger.info("Starting full data refresh pipeline")
    
    try:
        # Track pipeline status
        pipeline_status = {
            'cleanup_success': False,
            'fetch_success': False,
            'storage_success': False,
            'min_records_threshold': 100  # Higher threshold for full refresh
        }
        
        # Clean existing data
        logger.info("Cleaning existing data files")
        clean_data_directory()
        pipeline_status['cleanup_success'] = True
        
        # Initialize components
        logger.info("Initializing pipeline components")
        fetcher = DataFetcher()
        storage = DataStorage()
        
        # Fetch full dataset
        logger.info("Fetching full dataset from Chicago 811")
        data = fetcher.fetch_full_dataset()
        records_fetched = len(data)
        logger.info(f"Retrieved {records_fetched} records")
        
        # Validate fetch results
        if records_fetched == 0:
            raise DataValidationError("No records fetched during full refresh")
        
        # Check for required columns using normalized names
        required_columns = ['dig_ticket_number', 'request_date', 'is_emergency']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            raise DataValidationError(f"Fetched data is missing required columns: {missing_columns}")
            
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
        if storage_stats['total_records'] < pipeline_status['min_records_threshold']:
            raise DataValidationError(
                f"Insufficient records processed: {storage_stats['total_records']} "
                f"(minimum: {pipeline_status['min_records_threshold']})"
            )
            
        pipeline_status['storage_success'] = True
        
        # Verify all validation checks passed
        if all([
            pipeline_status['cleanup_success'],
            pipeline_status['fetch_success'],
            pipeline_status['storage_success']
        ]):
            logger.info("Full data refresh completed successfully:")
            logger.info(f"- Records fetched: {records_fetched}")
            logger.info(f"- Records processed: {storage_stats['total_records']}")
            logger.info(f"- New records: {storage_stats['inserts']}")
            logger.info(f"- Updated records: {storage_stats['updates']}")
        else:
            failed_checks = [
                check for check, status in pipeline_status.items()
                if isinstance(status, bool) and not status
            ]
            raise DataValidationError(
                f"Data refresh validation checks failed: {failed_checks}"
            )
        
    except Exception as e:
        logger.error(f"Refresh pipeline failed: {str(e)}")
        raise

def main():
    """Main entry point with logging setup."""
    try:
        # Initialize logging
        setup_logging()
        logger.info("Starting Chicago Dig Bot data refresh")
        
        # Run the refresh pipeline
        run_refresh()
        
    except Exception as e:
        logger.error(f"Fatal error in data refresh: {str(e)}")
        raise
    finally:
        logger.info("Chicago Dig Bot data refresh finished")

if __name__ == "__main__":
    main()
