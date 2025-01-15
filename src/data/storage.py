"""Module for storing and managing Chicago 811 dig permit data."""
import sqlite3
import pandas as pd
from pathlib import Path
from src.utils.logging import get_logger
from src.config import config

logger = get_logger(__name__)

class DataStorage:
    """Handles storage and retrieval of dig permit data in SQLite and Parquet formats."""
    
    def __init__(self):
        """Initialize the DataStorage with configuration."""
        self.data_dir = Path(config.data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.db_path = Path(config.db_file)
        self._init_database()
    
    def _init_database(self):
        """Initialize the SQLite database schema if it doesn't exist."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Enable WAL mode for better write performance
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA synchronous=NORMAL")
                cursor.execute("PRAGMA cache_size=-2000") # Use 2MB of cache
                
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
                
                # Create indexes for efficient querying and upserts
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_dig_date 
                    ON permits(DATE(dig_date))
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_ticket_number
                    ON permits(dig_ticket_number)
                """)
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error initializing database: {str(e)}")
            raise

    def drop_permits_table(self):
        """Drop the permits table if it exists."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("DROP TABLE IF EXISTS permits")
                conn.commit()
                logger.info("Successfully dropped permits table")
        except Exception as e:
            logger.error(f"Error dropping permits table: {str(e)}")
            raise

    def _prepare_dataframe(self, df):
        """Prepare DataFrame by ensuring correct types and handling nulls."""
        # Work on a copy to avoid SettingWithCopyWarning
        df = df.copy()
        
        logger.info("Preparing DataFrame for storage")
        
        # Pre-allocate numeric columns with correct types
        numeric_cols = {
            'street_number_from': 'Int64',
            'street_number_to': 'Int64',
            'latitude': 'float64',
            'longitude': 'float64'
        }
        df[list(numeric_cols.keys())] = df[list(numeric_cols.keys())].astype(numeric_cols)
        
        # Handle date columns more efficiently
        date_columns = ['request_date', 'dig_date', 'expiration_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Optimize boolean conversion
        if 'is_emergency' in df.columns:
            df['is_emergency'] = df['is_emergency'].map({'Y': 1, 'N': 0, True: 1, False: 0}).fillna(0).astype(int)
        
        # Handle string columns more efficiently
        string_columns = [
            'dig_ticket_number', 'permit_number', 'street_name', 
            'street_direction', 'street_suffix', 'dig_location',
            'contact_first_name', 'contact_last_name'
        ]
        # Convert all string columns at once
        df[string_columns] = df[string_columns].astype(str).replace('nan', None)
        
        return df

    def process_and_store(self, df):
        """Process and store permit data in both SQLite and Parquet formats using UPSERT."""
        logger.info("Processing and storing permit data")
        
        try:
            stats = {
                'total_records': len(df),
                'processed': 0
            }
            
            # Prepare DataFrame with correct types
            df = self._prepare_dataframe(df)
            
            # Get existing ticket numbers for statistics only
            with sqlite3.connect(self.db_path) as conn:
                existing_tickets = set(pd.read_sql_query(
                    "SELECT dig_ticket_number FROM permits",
                    conn
                )['dig_ticket_number'])
                
                # Process records in chunks
                chunk_size = 1000  # Adjust based on memory constraints
                for chunk_start in range(0, len(df), chunk_size):
                    chunk = df.iloc[chunk_start:chunk_start + chunk_size]
                    
                    # Create temporary table for the chunk
                    chunk.to_sql(
                        'temp_permits',
                        conn,
                        if_exists='replace',
                        index=False
                    )
                    
                    # Perform UPSERT operation
                    conn.execute("""
                        INSERT OR REPLACE INTO permits (
                            dig_ticket_number, permit_number, request_date,
                            dig_date, expiration_date, is_emergency,
                            street_name, street_direction, street_number_from,
                            street_number_to, street_suffix, dig_location,
                            latitude, longitude, contact_first_name,
                            contact_last_name, created_at, updated_at
                        )
                        SELECT 
                            t.*,
                            COALESCE(
                                (SELECT created_at FROM permits WHERE dig_ticket_number = t.dig_ticket_number),
                                CURRENT_TIMESTAMP
                            ),
                            CURRENT_TIMESTAMP
                        FROM temp_permits t
                    """)
                    
                    conn.commit()
                    stats['processed'] += len(chunk)
                    logger.info(f"Processed {stats['processed']} records")
                
                # Clean up temporary table
                conn.execute("DROP TABLE IF EXISTS temp_permits")
                
                # Calculate inserts vs updates for reporting
                processed_tickets = set(df['dig_ticket_number'])
                stats['inserts'] = len(processed_tickets - existing_tickets)
                stats['updates'] = len(processed_tickets & existing_tickets)
            
            # Save to parquet more efficiently
            df.to_parquet(
                self.data_dir / "chicago811_permits.parquet",
                compression='snappy',
                index=False,
                engine='fastparquet'  # Use fastparquet engine for better performance
            )
            logger.info(f"Saved {len(df)} records to {self.data_dir}/chicago811_permits.parquet")
            
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

    def store_full_data(self, df):
        """Store complete dataset during full refresh operation."""
        logger.info("Storing full dataset")
        
        try:
            # Prepare DataFrame with correct types
            df = self._prepare_dataframe(df)
            
            stats = {
                'total_records': len(df)
            }
            
            # Store in SQLite efficiently
            with sqlite3.connect(self.db_path) as conn:
                # Direct bulk insert since we're doing a full refresh
                df.to_sql(
                    'permits',
                    conn,
                    if_exists='append',  # Table was already dropped, so we can just append
                    index=False,
                    method='multi',  # Use multiple INSERT statements for better performance
                    chunksize=5000  # Process in larger chunks for full refresh
                )
                
                # Verify final count
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM permits")
                final_count = cursor.fetchone()[0]
                stats['total_records'] = final_count
            
            # Save to parquet
            df.to_parquet(
                self.data_dir / "chicago811_permits.parquet",
                compression='snappy',
                index=False,
                engine='fastparquet'
            )
            logger.info(f"Saved {len(df)} records to parquet file")
            
            return stats
            
        except Exception as e:
            logger.error(f"Error storing full dataset: {str(e)}")
            raise
