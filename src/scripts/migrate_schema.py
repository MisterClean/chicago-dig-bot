#!/usr/bin/env python3
"""One-time script to migrate existing parquet files to the new schema."""
import pandas as pd
from pathlib import Path
import shutil
from datetime import datetime
from data.schema import normalize_dataframe, SCHEMA
from utils.logging import setup_logging, get_logger
from config import config

logger = get_logger(__name__)

def backup_parquet_file(file_path: Path) -> Path:
    """Create a backup of the original parquet file.
    
    Args:
        file_path: Path to the parquet file to backup
        
    Returns:
        Path to the backup file
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = file_path.parent / f"{file_path.stem}_backup_{timestamp}.parquet"
    shutil.copy2(file_path, backup_path)
    logger.info(f"Created backup at {backup_path}")
    return backup_path

def log_dataframe_info(df: pd.DataFrame, label: str = "DataFrame") -> None:
    """Log information about a DataFrame's schema.
    
    Args:
        df: DataFrame to analyze
        label: Label to use in log messages
    """
    logger.info(f"{label} info:")
    logger.info(f"  Shape: {df.shape}")
    logger.info("  Columns:")
    for col in df.columns:
        non_null = df[col].count()
        total = len(df)
        null_pct = ((total - non_null) / total) * 100 if total > 0 else 0
        logger.info(f"    {col}")
        logger.info(f"      Type: {df[col].dtypes}")
        logger.info(f"      Null: {null_pct:.1f}% ({total - non_null:,} / {total:,})")
        if df[col].nunique() <= 10:  # Only show value counts for categorical-like columns
            logger.info(f"      Values: {df[col].value_counts().head().to_dict()}")

def migrate_file(file_path: Path) -> None:
    """Migrate a single parquet file to the new schema.
    
    Args:
        file_path: Path to the parquet file to migrate
    """
    logger.info(f"Migrating {file_path}")
    
    # Create backup
    backup_file = backup_parquet_file(file_path)
    
    try:
        # Read existing data
        df = pd.read_parquet(file_path)
        logger.info(f"Loaded {len(df):,} records")
        
        # Log original schema
        log_dataframe_info(df, "Original data")
        
        # Normalize to new schema
        normalized_df = normalize_dataframe(df)
        
        # Log normalized schema
        log_dataframe_info(normalized_df, "Normalized data")
        
        # Verify all required columns are present
        missing_cols = [col for col in SCHEMA.keys() if col not in normalized_df.columns]
        if missing_cols:
            logger.warning(f"Missing columns in final schema: {missing_cols}")
        
        # Save with new schema
        normalized_df.to_parquet(file_path, index=False)
        logger.info(f"Successfully migrated {len(normalized_df):,} records")
        
        # Log sample of migrated data
        logger.info("Sample of migrated data (first 5 rows):")
        logger.info("\n" + normalized_df.head().to_string())
        
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        # Restore from backup
        logger.info(f"Restoring from backup {backup_file}")
        shutil.copy2(backup_file, file_path)
        raise
        
def main():
    """Main migration function."""
    try:
        setup_logging()
        logger.info("Starting schema migration")
        
        data_dir = Path(config.data_dir)
        parquet_files = list(data_dir.glob("chicago811_*.parquet"))
        
        if not parquet_files:
            logger.info("No parquet files found to migrate")
            return
            
        logger.info(f"Found {len(parquet_files)} parquet files to migrate")
        
        for file_path in parquet_files:
            migrate_file(file_path)
            
        logger.info("Migration completed successfully")
        
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        raise
    finally:
        logger.info("Migration script finished")

if __name__ == "__main__":
    main()
