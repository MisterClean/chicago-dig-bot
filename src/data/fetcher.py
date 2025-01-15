"""Module for fetching Chicago 811 dig permit data from various sources."""
import os
import json
from datetime import datetime, timedelta
import pytz
import pandas as pd
import requests
from pathlib import Path
from src.utils.logging import get_logger
from src.config import config

logger = get_logger(__name__)

class DataFetcher:
    """Handles fetching data from Chicago Data Portal via CSV and SODA API."""
    
    def __init__(self):
        """Initialize the DataFetcher with configuration."""
        self.data_dir = Path(config.data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        # Configure API settings
        self.api_url = config.soda_api_url
        self.api_token = os.getenv('CHICAGO_DATA_PORTAL_TOKEN')
        self.api_timeout = 60  # Default timeout
        
        # Track last fetch time
        self.last_fetch_file = self.data_dir / 'last_fetch.json'
        
        # Days to fetch from config
        self.days_to_fetch = config.soda_days_to_fetch
        # Additional overlap (e.g., 2 days) to catch late-arriving tickets
        self.overlap_days = getattr(config, 'overlap_days', 2)
        # If you want a threshold for consecutive no-data
        self.consecutive_no_data_threshold = getattr(config, 'consecutive_no_data_threshold', 3)
        # Local counter file
        self.no_data_counter_file = self.data_dir / 'no_data_counter.json'
    
    def _normalize_columns(self, df):
        """Normalize column names and data types for consistency."""
        column_map = {
            # CSV format (original column names)
            'DIG_TICKET#': 'dig_ticket_number',
            'PERMIT#': 'permit_number',
            'REQUESTDATE': 'request_date',
            'DIGDATE': 'dig_date',
            'EMERGENCY': 'is_emergency',
            'STNAME': 'street_name',
            'DIRECTION': 'street_direction',
            'STNOFROM': 'street_number_from',
            'STNOTO': 'street_number_to',
            'SUFFIX': 'street_suffix',
            'PLACEMENT': 'dig_location',
            'LATITUDE': 'latitude',
            'LONGITUDE': 'longitude',
            'EXPIRATIONDATE': 'expiration_date',
            'PRIMARYCONTACTFIRST': 'contact_first_name',
            'PRIMARYCONTACTLAST': 'contact_last_name',
            # API format (SODA API field names)
            'dig_ticket_': 'dig_ticket_number',
            'permit_': 'permit_number',
            'requestdate': 'request_date',
            'digdate': 'dig_date',
            'emergency': 'is_emergency',
            'stname': 'street_name',
            'direction': 'street_direction',
            'stnofrom': 'street_number_from',
            'stnoto': 'street_number_to',
            'suffix': 'street_suffix',
            'placement': 'dig_location',
            'latitude': 'latitude',
            'longitude': 'longitude',
            'expirationdate': 'expiration_date',
            'primarycontactfirst': 'contact_first_name',
            'primarycontactlast': 'contact_last_name'
        }
        
        existing_columns = set(df.columns)
        rename_map = {old: new for old, new in column_map.items() if old in existing_columns}
        df = df.rename(columns=rename_map)
        
        # Convert date columns with Chicago timezone
        chicago_tz = pytz.timezone('America/Chicago')
        date_columns = ['request_date', 'dig_date', 'expiration_date']
        for col in date_columns:
            if col in df.columns:
                # Convert to datetime and localize to Chicago timezone
                df[col] = pd.to_datetime(df[col], errors='coerce').apply(
                    lambda x: x.tz_localize(chicago_tz) if pd.notnull(x) else None
                )
        
        # Convert emergency to boolean
        if 'is_emergency' in df.columns:
            df['is_emergency'] = df['is_emergency'].astype(str).str.lower().isin(['true', 't', 'yes', 'y', '1'])
        
        # Convert numeric columns
        numeric_columns = {
            'street_number_from': 'Int64',
            'street_number_to': 'Int64',
            'latitude': 'float64',
            'longitude': 'float64'
        }
        for col, dtype in numeric_columns.items():
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype(dtype)
        
        # Convert text columns
        text_columns = ['street_name', 'street_direction', 'street_suffix', 'dig_location']
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].astype('string')
        
        return df
    
    def fetch_full_dataset(self):
        """Fetch the complete dataset from CSV endpoint (used in full refresh)."""
        logger.info("Fetching full dataset from CSV endpoint")
        
        try:
            csv_url = config.initial_csv_path
            logger.info(f"Downloading CSV from {csv_url}")
            df = pd.read_csv(csv_url, dtype=str)
            
            # Update last fetch time
            self._update_last_fetch()
            
            # Normalize columns
            df = self._normalize_columns(df)
            
            logger.info(f"Successfully fetched {len(df)} records from CSV")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching full dataset: {str(e)}")
            raise
    
    def fetch_recent_data(self):
        """
        Fetch recent records using SODA API, overlapping the last X days + overlap_days
        to handle late-arriving or updated tickets.
        """
        logger.info("Fetching recent data from SODA API")
        
        try:
            # Chicago local time
            chicago_tz = pytz.timezone('America/Chicago')
            chicago_now = datetime.now(chicago_tz)
            
            # Overlapping window: 
            # e.g., if config says 7 days, we add overlap_days (say 2) => 9-day window
            total_days = self.days_to_fetch + self.overlap_days
            cutoff_date = chicago_now - timedelta(days=total_days)
            
            # Prepare API parameters
            params = {
                '$order': 'requestdate DESC',
                '$limit': config.soda_records_limit,
                '$where': f"requestdate >= '{cutoff_date.strftime('%Y-%m-%d')}'"
            }
            
            headers = {}
            if self.api_token:
                headers['X-App-Token'] = self.api_token
            
            response = requests.get(
                self.api_url,
                params=params,
                headers=headers,
                timeout=self.api_timeout
            )
            response.raise_for_status()
            
            data = response.json()
            df = pd.DataFrame(data)
            
            # Normalize columns and types
            df = self._normalize_columns(df)
            
            logger.info(f"Fetched {len(df)} records from the last {total_days} days.")
            
            # Update last fetch time
            self._update_last_fetch()
            
            # Handle no data scenario
            if df.empty:
                logger.warning("No new data retrieved from SODA API. Data portal may not have updated.")
                self._increment_no_data_counter()
            else:
                self._reset_no_data_counter()
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching recent data: {str(e)}")
            raise
    
    def _update_last_fetch(self):
        """Update the last fetch timestamp."""
        try:
            with open(self.last_fetch_file, 'w') as f:
                json.dump({
                    'last_fetch': datetime.now(pytz.timezone('America/Chicago')).isoformat()
                }, f)
        except Exception as e:
            logger.warning(f"Failed to update last fetch time: {str(e)}")
    
    def _increment_no_data_counter(self):
        """Increment the local no-data counter and check if we need a full refresh fallback."""
        count = 0
        if self.no_data_counter_file.exists():
            try:
                with open(self.no_data_counter_file, 'r') as f:
                    data = json.load(f)
                    count = data.get('no_data_count', 0)
            except Exception as e:
                logger.warning(f"Failed to read no_data_counter.json: {e}")
        
        count += 1
        logger.info(f"No-data days in a row: {count}")
        
        # Save back
        try:
            with open(self.no_data_counter_file, 'w') as f:
                json.dump({'no_data_count': count}, f)
        except Exception as e:
            logger.warning(f"Failed to write no_data_counter.json: {e}")
        
        # Optional: if count >= threshold, trigger or log advice for a full refresh fallback
        if count >= self.consecutive_no_data_threshold:
            logger.warning(
                f"No new data for {count} consecutive fetches. "
                f"Consider running a full refresh or investigating data portal status."
            )
    
    def _reset_no_data_counter(self):
        """Reset the no-data counter to zero."""
        if self.no_data_counter_file.exists():
            self.no_data_counter_file.unlink()
        logger.info("Reset no-data counter to 0.")
