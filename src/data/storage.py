"""Module for storing and managing Chicago 811 dig permit data."""
import duckdb
from datetime import datetime
import pandas as pd
from pathlib import Path
from src.utils.logging import get_logger
from src.config import config

logger = get_logger(__name__)

class DataStorage:
    """Handles storage and retrieval of dig permit data in DuckDB and Parquet formats."""
    
    def __init__(self):
        """Initialize the DataStorage with configuration."""
        self.data_dir = Path(config.data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        # Database configuration
        self.db_path = Path(config.db_file)
        
        # Initialize database if needed
        self._init_database()
    
    def _init_database(self):
        """Initialize the DuckDB database schema if it doesn't exist."""
        try:
            conn = duckdb.connect(str(self.db_path))
            
            # Drop any existing index to avoid conflicts
            conn.execute("DROP INDEX IF EXISTS idx_dig_date")
            
            # Create permits table if it doesn't exist
            conn.execute("""
                CREATE TABLE IF NOT EXISTS permits (
                    dig_ticket_number VARCHAR PRIMARY KEY,
                    permit_number VARCHAR,
                    request_date TIMESTAMP,
                    dig_date TIMESTAMP,
                    expiration_date TIMESTAMP,
                    is_emergency BOOLEAN,
                    street_name VARCHAR,
                    street_direction VARCHAR,
                    street_number_from INTEGER,
                    street_number_to INTEGER,
                    street_suffix VARCHAR,
                    dig_location VARCHAR,
                    latitude DOUBLE,
                    longitude DOUBLE,
                    contact_first_name VARCHAR,
                    contact_last_name VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create index on dig_date for efficient querying
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_dig_date 
                ON permits(dig_date)
            """)
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Error initializing database: {str(e)}")
            raise
    
    def _save_to_parquet(self, df):
        """Save DataFrame to a single parquet file."""
        if df.empty:
            logger.info("No data to save to parquet.")
            return
        
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
        """Process and store permit data in both DuckDB and Parquet formats."""
        logger.info("Processing and storing permit data")
        
        # If no data is passed, log and return gracefully
        if df.empty:
            logger.warning("Received empty DataFrame — no new data to process.")
            return {
                'total_records': 0,
                'inserts': 0,
                'updates': 0
            }
        
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
            conn = duckdb.connect(str(self.db_path))
            
            # Get existing ticket numbers
            existing_tickets_query = "SELECT dig_ticket_number FROM permits"
            existing_tickets = []
            try:
                existing_tickets = conn.execute(existing_tickets_query).fetchdf()['dig_ticket_number'].tolist()
            except duckdb.BinderException:
                # If the table doesn't exist, it means a fresh DB
                logger.info("No existing 'permits' table found; proceeding with initial load.")
            
            # Split into new and existing records
            new_records = df[~df['dig_ticket_number'].isin(existing_tickets)]
            existing_records = df[df['dig_ticket_number'].isin(existing_tickets)]
            
            # Insert new records
            if not new_records.empty:
                # Convert timestamps to strings in ISO format for DuckDB
                insert_df = new_records.copy()
                for col in ['request_date', 'dig_date', 'expiration_date']:
                    if col in insert_df.columns:
                        insert_df[col] = insert_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

                # Convert DataFrame to list of tuples for insertion
                records_to_insert = insert_df[[
                    'dig_ticket_number', 'permit_number', 'request_date', 'dig_date',
                    'expiration_date', 'is_emergency', 'street_name', 'street_direction',
                    'street_number_from', 'street_number_to', 'street_suffix', 'dig_location',
                    'latitude', 'longitude', 'contact_first_name', 'contact_last_name'
                ]].values.tolist()
                
                conn.executemany("""
                    INSERT INTO permits (
                        dig_ticket_number, permit_number, request_date, dig_date,
                        expiration_date, is_emergency, street_name, street_direction,
                        street_number_from, street_number_to, street_suffix, dig_location,
                        latitude, longitude, contact_first_name, contact_last_name
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, records_to_insert)
                stats['inserts'] = len(new_records)
                logger.info(f"Inserted {stats['inserts']} new records")
            
            # Update existing records
            if not existing_records.empty:
                for _, record in existing_records.iterrows():
                    # Convert timestamps to strings
                    update_record = record.copy()
                    for col in ['request_date', 'dig_date', 'expiration_date']:
                        if pd.notnull(update_record[col]):
                            update_record[col] = update_record[col].strftime('%Y-%m-%d %H:%M:%S')

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
                    """, [
                        update_record['permit_number'],
                        update_record['request_date'],
                        update_record['dig_date'],
                        update_record['expiration_date'],
                        update_record['is_emergency'],
                        update_record['street_name'],
                        update_record['street_direction'],
                        update_record['street_number_from'],
                        update_record['street_number_to'],
                        update_record['street_suffix'],
                        update_record['dig_location'],
                        update_record['latitude'],
                        update_record['longitude'],
                        update_record['contact_first_name'],
                        update_record['contact_last_name'],
                        update_record['dig_ticket_number']
                    ])
                
                stats['updates'] = len(existing_records)
                logger.info(f"Updated {stats['updates']} existing records")
            
            conn.close()
            
            # Save all data (new + existing) to a single parquet file
            # You might want to re-query the entire table or just use df combined with prior data
            # For simplicity, we’ll just save `df` here.
            self._save_to_parquet(df)
            
            return stats
            
        except Exception as e:
            logger.error(f"Error processing and storing data: {str(e)}")
            raise
    
    def get_recent_permits(self, days=30):
        """Get permits from the last N days."""
        try:
            conn = duckdb.connect(str(self.db_path))
            query = f"""
                SELECT * FROM permits 
                WHERE dig_date >= CURRENT_DATE - INTERVAL '{days} days'
                ORDER BY dig_date DESC
            """
            df = conn.execute(query).fetchdf()
            conn.close()
            
            return df
                
        except Exception as e:
            logger.error(f"Error getting recent permits: {str(e)}")
            raise

    def drop_permits_table(self):
        """Remove the permits table for a true full refresh."""
        try:
            conn = duckdb.connect(str(self.db_path))
            conn.execute("DROP TABLE IF EXISTS permits")
            conn.execute("DROP INDEX IF EXISTS idx_dig_date")
            conn.close()
            logger.info("Dropped 'permits' table and indexes for a true full refresh.")
        except Exception as e:
            logger.error(f"Error dropping permits table: {str(e)}")
            raise

    def store_full_data(self, df: pd.DataFrame) -> dict:
        """
        Create the 'permits' table in one bulk operation directly from the DataFrame.
        Returns a dictionary with stats about the load.
        """
        logger.info("Storing full data in one bulk operation")

        if df.empty:
            logger.warning("DataFrame is empty. Nothing to store.")
            return {
                "total_records": 0,
                "message": "No data"
            }

        try:
            # Connect to DuckDB
            conn = duckdb.connect(str(self.db_path))

            # (Optionally) Drop table if it still exists
            conn.execute("DROP TABLE IF EXISTS permits")

            # Register the DataFrame as a DuckDB virtual table
            conn.register("temp_df", df)

            # Create the table in one statement from the DataFrame
            conn.execute("""
                CREATE TABLE permits AS
                SELECT
                    dig_ticket_number,
                    permit_number,
                    request_date,
                    dig_date,
                    expiration_date,
                    is_emergency,
                    street_name,
                    street_direction,
                    street_number_from,
                    street_number_to,
                    street_suffix,
                    dig_location,
                    latitude,
                    longitude,
                    contact_first_name,
                    contact_last_name,
                    CURRENT_TIMESTAMP as created_at,
                    CURRENT_TIMESTAMP as updated_at
                FROM temp_df
            """)

            # Create an index on dig_date for faster queries
            conn.execute("CREATE INDEX IF NOT EXISTS idx_dig_date ON permits(dig_date)")

            # (Optional) If you need a PRIMARY KEY, you can do:
            # DuckDB doesn’t fully support “ALTER TABLE ... ADD PRIMARY KEY” as of now,
            # but you can add a UNIQUE constraint:
            # conn.execute("ALTER TABLE permits ADD CONSTRAINT unique_dig_ticket UNIQUE (dig_ticket_number)")

            # Count how many rows are in the new table
            result = conn.execute("SELECT COUNT(*) as cnt FROM permits").fetchone()
            total_records = result[0]

            # Close connection
            conn.close()

            # (Optional) Save to Parquet if you want a file-based snapshot
            # self._save_to_parquet(df)

            logger.info(f"Bulk insert complete: {total_records} records in 'permits' table.")
            return {
                "total_records": total_records
            }

        except Exception as e:
            logger.error(f"Error storing full data in bulk: {str(e)}")
            raise
