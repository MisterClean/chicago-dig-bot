"""Module for storing and managing Chicago 811 dig permit data."""
import os
import sqlite3
from datetime import datetime
import pandas as pd
from pathlib import Path
from utils.logging import get_logger
from config import config

logger = get_logger(__name__)

class DataStorage:
    """Handles storage and retrieval of dig permit data in SQLite and Parquet formats."""
    
    def __init__(self):
        """Initialize the DataStorage with configuration."""
        self.data_dir = Path(config.data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        # Database configuration
        self.db_path = Path(config.db_file)
        
        # Initialize database if needed
        self._init_database()
    
    def _init_database(self):
        """Initialize the SQLite database schema if it doesn't exist."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create permits table if it doesn't exist
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS permits (
                        dig_ticket_number TEXT PRIMARY KEY,
                        permit_number TEXT,
                        request_date TIMESTAMP,
                        dig_date TIMESTAMP,
                        expiration_date TIMESTAMP,
                        is_emergency BOOLEAN,
                        street_name TEXT,
                        street_direction TEXT,
                        street_number_from INTEGER,
                        street_number_to INTEGER,
                        street_suffix TEXT,
                        dig_location TEXT,
                        latitude REAL,
                        longitude REAL,
                        contact_first_name TEXT,
                        contact_last_name TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Create index on dig_date for efficient querying
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_dig_date 
                    ON permits(dig_date)
                """)
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error initializing database: {str(e)}")
            raise
    
    def _save_to_parquet(self, df):
        """Save DataFrame to a single parquet file."""
        try:
            # Create parquet filename
            parquet_path = self.data_dir / "chicago811_permits.parquet"
            
            # Save to parquet with compression
            df.to_parquet(
                parquet_path,
                compression='snappy',
                index=False
            )
            
            logger.info(f"Saved {len(df)} records to {parquet_path}")
            
        except Exception as e:
            logger.error(f"Error saving to parquet: {str(e)}")
            raise
    
    def process_and_store(self, df):
        """Process and store permit data in both SQLite and Parquet formats."""
        logger.info("Processing and storing permit data")
        
        try:
            # Track statistics
            stats = {
                'total_records': len(df),
                'inserts': 0,
                'updates': 0
            }
            
            # Ensure minimum required columns exist
            required_columns = [
                'dig_ticket_number', 'permit_number', 'request_date', 'dig_date', 
                'expiration_date', 'is_emergency', 'street_name', 'street_direction',
                'street_number_from', 'street_number_to', 'street_suffix', 'dig_location',
                'latitude', 'longitude', 'contact_first_name', 'contact_last_name'
            ]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.warning(f"Missing some columns: {missing_columns}")
                # Add missing columns as None/NaN
                for col in missing_columns:
                    df[col] = None
            
            # Connect to database
            with sqlite3.connect(self.db_path) as conn:
                # Get existing ticket numbers
                existing_tickets = pd.read_sql(
                    "SELECT dig_ticket_number FROM permits",
                    conn
                )['dig_ticket_number'].tolist()
                
                # Split into new and existing records
                new_records = df[~df['dig_ticket_number'].isin(existing_tickets)]
                existing_records = df[df['dig_ticket_number'].isin(existing_tickets)]
                
                # Insert new records
                if not new_records.empty:
                    # Convert timestamps to strings for SQLite
                    for col in ['request_date', 'dig_date', 'expiration_date']:
                        new_records[col] = new_records[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Convert boolean to integer for SQLite
                    new_records['is_emergency'] = new_records['is_emergency'].astype(int)
                    
                    new_records.to_sql(
                        'permits',
                        conn,
                        if_exists='append',
                        index=False
                    )
                    stats['inserts'] = len(new_records)
                    logger.info(f"Inserted {stats['inserts']} new records")
                
                # Update existing records
                if not existing_records.empty:
                    for _, record in existing_records.iterrows():
                        # Convert timestamps to strings for SQLite
                        params = (
                            str(record['permit_number']) if pd.notna(record['permit_number']) else None,
                            record['request_date'].strftime('%Y-%m-%d %H:%M:%S') if pd.notna(record['request_date']) else None,
                            record['dig_date'].strftime('%Y-%m-%d %H:%M:%S') if pd.notna(record['dig_date']) else None,
                            record['expiration_date'].strftime('%Y-%m-%d %H:%M:%S') if pd.notna(record['expiration_date']) else None,
                            int(bool(record['is_emergency'])) if pd.notna(record['is_emergency']) else None,
                            str(record['street_name']) if pd.notna(record['street_name']) else None,
                            str(record['street_direction']) if pd.notna(record['street_direction']) else None,
                            int(record['street_number_from']) if pd.notna(record['street_number_from']) else None,
                            int(record['street_number_to']) if pd.notna(record['street_number_to']) else None,
                            str(record['street_suffix']) if pd.notna(record['street_suffix']) else None,
                            str(record['dig_location']) if pd.notna(record['dig_location']) else None,
                            float(record['latitude']) if pd.notna(record['latitude']) else None,
                            float(record['longitude']) if pd.notna(record['longitude']) else None,
                            str(record['contact_first_name']) if pd.notna(record['contact_first_name']) else None,
                            str(record['contact_last_name']) if pd.notna(record['contact_last_name']) else None,
                            str(record['dig_ticket_number'])
                        )
                        
                        conn.execute("""
                            UPDATE permits 
                            SET 
                                permit_number = ?,
                                request_date = ?,
                                dig_date = ?,
                                expiration_date = ?,
                                is_emergency = ?,
                                street_name = ?,
                                street_direction = ?,
                                street_number_from = ?,
                                street_number_to = ?,
                                street_suffix = ?,
                                dig_location = ?,
                                latitude = ?,
                                longitude = ?,
                                contact_first_name = ?,
                                contact_last_name = ?,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE dig_ticket_number = ?
                        """, params)
                    
                    stats['updates'] = len(existing_records)
                    logger.info(f"Updated {stats['updates']} existing records")
                
                conn.commit()
            
            # Save all data to a single parquet file
            self._save_to_parquet(df)
            
            return stats
            
        except Exception as e:
            logger.error(f"Error processing and storing data: {str(e)}")
            raise
    
    def get_recent_permits(self, days=30):
        """Get permits from the last N days."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = f"""
                    SELECT * FROM permits 
                    WHERE dig_date >= date('now', '-{days} days')
                    ORDER BY dig_date DESC
                """
                df = pd.read_sql(query, conn)
                
                # Convert date columns
                date_columns = ['request_date', 'dig_date', 'expiration_date', 'created_at', 'updated_at']
                for col in date_columns:
                    df[col] = pd.to_datetime(df[col])
                
                return df
                
        except Exception as e:
            logger.error(f"Error getting recent permits: {str(e)}")
            raise
