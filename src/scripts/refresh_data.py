#!/usr/bin/env python3
"""Script to perform a full refresh of data from the Chicago 811 SODA API."""
import time
import shutil
from pathlib import Path
import glob
from functools import wraps

from src.data.fetcher import DataFetcher
from src.data.storage import DataStorage
from src.utils.logging import setup_logging, get_logger
from src.config import config

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
        pipeline_status = {
            'cleanup_success': False,
            'fetch_success': False,
            'storage_success': False,
            'min_records_threshold': 100
        }
        
        # 1) Clean existing files (parquet, JSON state files)
        logger.info("Cleaning existing data files")
        clean_data_directory()
        pipeline_status['cleanup_success'] = True
        
        # 2) Initialize components
        logger.info("Initializing pipeline components")
        fetcher = DataFetcher()
        storage = DataStorage()
        
        # 3) Drop the permits table so we start from zero
        logger.info("Dropping any existing 'permits' table to purge old data")
        storage.drop_permits_table()
        
        # 4) Fetch full dataset (CSV from Chicago data portal)
        logger.info("Fetching full dataset from Chicago 811 CSV")
        data = fetcher.fetch_full_dataset()
        records_fetched = len(data)
        logger.info(f"Retrieved {records_fetched} records")
        
        if records_fetched == 0:
            raise DataValidationError("No records fetched during full refresh")
        
        pipeline_status['fetch_success'] = True
        
        # 5) Bulk insert in one shot
        logger.info("Performing bulk insert into DuckDB (full refresh)")
        storage_stats = storage.store_full_data(data)  # <-- Single-statement approach
        total_records_inserted = storage_stats.get('total_records', 0)
        
        logger.info(f"Storage operation completed: {total_records_inserted} records now in 'permits' table")
        
        if total_records_inserted < pipeline_status['min_records_threshold']:
            raise DataValidationError(
                f"Insufficient records processed: {total_records_inserted} "
                f"(minimum: {pipeline_status['min_records_threshold']})"
            )
            
        pipeline_status['storage_success'] = True
        
        # Final checks
        if all([pipeline_status['cleanup_success'],
                pipeline_status['fetch_success'],
                pipeline_status['storage_success']]):
            logger.info("Full data refresh completed successfully.")
            logger.info(f"- Records fetched: {records_fetched}")
            logger.info(f"- Final record count in DB: {total_records_inserted}")
        else:
            failed_checks = [
                key for key, val in pipeline_status.items()
                if isinstance(val, bool) and not val
            ]
            raise DataValidationError(
                f"Data refresh validation checks failed: {failed_checks}"
            )
        
    except Exception as e:
        logger.error(f"Refresh pipeline failed: {str(e)}")
        raise

def main():
    try:
        setup_logging()
        logger.info("Starting Chicago Dig Bot data refresh")
        run_refresh()
    except Exception as e:
        logger.error(f"Fatal error in data refresh: {str(e)}")
        raise
    finally:
        logger.info("Chicago Dig Bot data refresh finished")

if __name__ == "__main__":
    main()
