"""Module for fetching Chicago 811 dig permit data from various sources."""
import os
import json
from datetime import datetime, timedelta
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
    
    def _normalize_columns(self, df):
        """Normalize column names and data types for consistency."""
        # Map CSV/API column names to our normalized names based on data portal docs
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
        
        # Rename columns if they exist
        existing_columns = set(df.columns)
        rename_map = {old: new for old, new in column_map.items() if old in existing_columns}
        df = df.rename(columns=rename_map)
        
        # Convert date columns (Floating Timestamp in SODA)
        date_columns = ['request_date', 'dig_date', 'expiration_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        
        # Convert emergency request to boolean (Checkbox in SODA)
        if 'is_emergency' in df.columns:
            df['is_emergency'] = df['is_emergency'].astype(str).str.lower().isin(['true', 't', 'yes', 'y', '1'])
        
        # Convert numeric columns
        numeric_columns = {
            'street_number_from': 'Int64',  # nullable integer
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
        """Fetch the complete dataset from CSV endpoint."""
        logger.info("Fetching full dataset from CSV endpoint")
        
        try:
            # Get CSV URL from config
            csv_url = config.initial_csv_path
            
            # Download and parse CSV with correct dtypes
            logger.info(f"Downloading CSV from {csv_url}")
            df = pd.read_csv(csv_url, dtype={
                'DIG_TICKET#': str,
                'PERMIT#': str,
                'STNOFROM': str,
                'STNOTO': str,
                'DIRECTION': str,
                'STNAME': str,
                'SUFFIX': str,
                'PLACEMENT': str,
                'PRIMARYCONTACTFIRST': str,
                'PRIMARYCONTACTLAST': str
            })
            
            # Update last fetch time
            self._update_last_fetch()
            
            # Normalize column names and data types
            df = self._normalize_columns(df)
            
            logger.info(f"Successfully fetched {len(df)} records from CSV")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching full dataset: {str(e)}")
            raise
    
    def fetch_recent_data(self):
        """Fetch recent records using SODA API."""
        logger.info("Fetching recent data from SODA API")
        
        try:
            # Calculate date range
            days_to_fetch = config.soda_days_to_fetch
            cutoff_date = datetime.now() - timedelta(days=days_to_fetch)
            
            # Prepare API parameters
            params = {
                '$order': 'requestdate DESC',
                '$limit': config.soda_records_limit,
                '$where': f"requestdate > '{cutoff_date.strftime('%Y-%m-%d')}'"
            }
            
            headers = {}
            if self.api_token:
                headers['X-App-Token'] = self.api_token
            
            # Make API request
            logger.info(f"Requesting data from {self.api_url}")
            response = requests.get(
                self.api_url,
                params=params,
                headers=headers,
                timeout=self.api_timeout
            )
            response.raise_for_status()
            
            # Parse JSON response into DataFrame
            data = response.json()
            df = pd.DataFrame(data)
            
            # Update last fetch time
            self._update_last_fetch()
            
            # Normalize column names and data types
            df = self._normalize_columns(df)
            
            logger.info(f"Successfully fetched {len(df)} records from API")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching recent data: {str(e)}")
            raise
    
    def _update_last_fetch(self):
        """Update the last fetch timestamp."""
        try:
            with open(self.last_fetch_file, 'w') as f:
                json.dump({
                    'last_fetch': datetime.now().isoformat()
                }, f)
        except Exception as e:
            logger.warning(f"Failed to update last fetch time: {str(e)}")
